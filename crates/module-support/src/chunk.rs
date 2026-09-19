//! Ingest-side text handling: the one shared tokenizer and the
//! overlapping-window chunker.

use std::collections::{BTreeMap, HashSet};

use sha2::{Digest, Sha256};

/// Lowercased alphanumeric terms, in order of appearance.
///
/// Shared by ingest and query so the two can never drift — a query
/// tokenised differently from the index finds nothing, so there is one
/// tokenizer and both sides call it.
///
/// The rules: split on every character that is not
/// [`char::is_alphanumeric`], so punctuation, underscore and whitespace
/// all separate terms; lowercase what survives; keep terms of 2..=48
/// Unicode characters (a 1-character term is noise — a stray letter or
/// digit — and only spam reaches 49). Length is counted in characters,
/// not bytes, so `café` is one 4-character term.
///
/// Known limitation, deliberately not papered over: this cannot segment
/// scripts written without spaces (Chinese, Japanese, Thai). A run of Han
/// characters is alphanumeric throughout, so it is indexed as one long
/// unsegmented token and matches only that exact run. Segmentation needs
/// a real dictionary-based tokenizer, which is out of scope for v1.
pub fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .map(str::to_lowercase)
        .filter(|term| {
            let chars = term.chars().count();
            (2..=48).contains(&chars)
        })
        .collect()
}

/// A chunk of a source document: an overlapping window of its words, its
/// content address, and the term statistics BM25 needs.
///
/// `terms` and `length` are the ingest side's whole output contract:
/// `length` is the BM25 document length, and `terms` is the chunk's
/// would-be `sg_postings` rows, sorted by term so the caller's inserts
/// are deterministic.
pub struct Chunk {
    /// Content address: the first 32 hex chars of
    /// `sha256(tenant_id || 0x00 || source_id || chunk_text)`. Stable
    /// across re-ingest of unchanged text, and stable across an edit
    /// elsewhere in the document, because only the chunk's own text (plus
    /// tenant and source) feeds the hash.
    pub id: String,
    /// Which window of the source this is, 0-based. Windows are
    /// deduplicated by id after numbering, so a document full of repeated
    /// boilerplate can leave gaps in the ordinals: the dropped duplicates
    /// keep the ordinals they would have had.
    pub ordinal: u32,
    /// The original text of the window: a slice of the source from the
    /// first word's first byte to the last word's last byte, so
    /// punctuation and inner whitespace survive verbatim and the chunk
    /// can be quoted to an agent exactly as it was written.
    pub text: String,
    /// Term -> term frequency within this chunk, sorted by term, no
    /// duplicate terms.
    pub terms: Vec<(String, u32)>,
    /// Total term count — the BM25 document length, i.e. the sum of the
    /// frequencies in `terms`.
    pub length: u32,
}

/// Splits source text into overlapping word windows.
///
/// Windows are counted in words — whitespace-delimited runs of the
/// original text — never in characters, so a chunk never cuts a word in
/// half. Window `i` starts at word `i * (max_words - overlap_words)` and
/// covers up to `max_words` words; the overlap is what keeps a sentence
/// that straddles a boundary findable from both sides.
pub struct Chunker {
    max_words: usize,
    overlap_words: usize,
}

impl Chunker {
    /// 180 words is roughly one screen of prose: long enough that the
    /// terms of one idea co-occur in a chunk, short enough that an answer
    /// quoting one chunk stays readable.
    pub const DEFAULT_MAX_WORDS: usize = 180;
    /// 30 words of overlap against 180 is a sixth of a window: enough to
    /// carry a straddled sentence, without making every window
    /// near-duplicate its predecessor.
    pub const DEFAULT_OVERLAP_WORDS: usize = 30;

    /// Builds a chunker, clamping degenerate arguments instead of
    /// erroring: `max_words` is raised to at least 1 and `overlap_words`
    /// is capped at `max_words - 1`, so the stride
    /// `max_words - overlap_words` is always at least 1 and every window
    /// makes forward progress.
    pub fn new(max_words: usize, overlap_words: usize) -> Self {
        let max_words = max_words.max(1);
        Self {
            max_words,
            overlap_words: overlap_words.min(max_words - 1),
        }
    }

    /// Splits `text` into the chunks to ingest for one source of one
    /// tenant.
    ///
    /// Empty and whitespace-only text yield zero chunks. Window `i` starts
    /// at word `i * (max_words - overlap_words)` and covers `max_words`
    /// words, the last window clamped to the text's end. Chunking stops
    /// once the last word is covered; since the stride is at least 1 and
    /// every non-final window ends past its predecessor's end, no window
    /// can be wholly contained in the previous one.
    ///
    /// Chunk ids are content addresses (see [`Chunk::id`]), so identical
    /// window text yields identical ids — and a document of repeated
    /// boilerplate would otherwise primary-key-clash on insert. `split`
    /// therefore deduplicates by id, keeping the **first** occurrence and
    /// dropping its duplicates. The consequence is the one the id design
    /// wants: repeated boilerplate is indexed once, not once per
    /// repetition, and the caller can insert the result as-is. The id
    /// deliberately excludes the ordinal — including it would make every
    /// id depend on the chunk's position, destroying the stability that
    /// is the reason for content addressing.
    ///
    /// Because a window's id hashes only its own text, ids of unchanged
    /// windows survive edits elsewhere: editing a document's start does
    /// not churn the ids of the chunks after the edit. Appending to the
    /// end leaves every window but the final (clamped) one byte-identical.
    pub fn split(&self, tenant_id: &str, source_id: &str, text: &str) -> Vec<Chunk> {
        let words = word_spans(text);
        let mut chunks = Vec::new();
        if words.is_empty() {
            return chunks;
        }

        let stride = self.max_words - self.overlap_words;
        let mut seen = HashSet::new();
        let (mut window, mut start) = (0u32, 0usize);
        loop {
            let end = (start + self.max_words).min(words.len());
            let chunk_text = &text[words[start].0..words[end - 1].1];
            let candidate = build_chunk(tenant_id, source_id, chunk_text, window);
            // Keeping the first occurrence is what makes re-ingest stable:
            // the retained chunk is the earliest window with this text.
            if seen.insert(candidate.id.clone()) {
                chunks.push(candidate);
            }

            if end == words.len() {
                break;
            }
            start += stride;
            window += 1;
        }
        chunks
    }
}

impl Default for Chunker {
    fn default() -> Self {
        Self::new(Self::DEFAULT_MAX_WORDS, Self::DEFAULT_OVERLAP_WORDS)
    }
}

/// Byte spans `(start, end)` of the whitespace-delimited words of `text`,
/// in order. Word-anchored chunking is what guarantees a chunk never cuts
/// a word in half: chunk text is always a slice between word boundaries.
fn word_spans(text: &str) -> Vec<(usize, usize)> {
    let mut words = Vec::new();
    let mut start = None;
    for (offset, ch) in text.char_indices() {
        if ch.is_whitespace() {
            if let Some(word_start) = start.take() {
                words.push((word_start, offset));
            }
        } else {
            start.get_or_insert(offset);
        }
    }
    if let Some(word_start) = start {
        words.push((word_start, text.len()));
    }
    words
}

/// Assembles one chunk: content address, verbatim text, term statistics.
fn build_chunk(tenant_id: &str, source_id: &str, chunk_text: &str, ordinal: u32) -> Chunk {
    let (terms, length) = term_frequencies(chunk_text);
    Chunk {
        id: chunk_id(tenant_id, source_id, chunk_text),
        ordinal,
        text: chunk_text.to_string(),
        terms,
        length,
    }
}

/// Terms and their frequencies within one chunk: sorted by term
/// ([`BTreeMap`] iteration order), no duplicate terms, with `length` the
/// summed frequencies.
fn term_frequencies(text: &str) -> (Vec<(String, u32)>, u32) {
    let mut counts: BTreeMap<String, u32> = BTreeMap::new();
    for term in tokenize(text) {
        let tf = counts.entry(term).or_default();
        // The ingest contract caps a chunk at 48 KiB of text, so a
        // frequency nowhere near u32 saturation; saturating anyway is
        // kinder than a debug-mode overflow if a caller skips the cap.
        *tf = tf.saturating_add(1);
    }

    let mut length: u64 = 0;
    let mut terms = Vec::with_capacity(counts.len());
    for (term, tf) in counts {
        length += u64::from(tf);
        terms.push((term, tf));
    }
    // Same contract: one chunk's summed frequencies fit u32 with room to
    // spare, and a capped length beats a wrapped one.
    let length = u32::try_from(length).unwrap_or(u32::MAX);
    (terms, length)
}

/// `sha256(tenant_id || 0x00 || source_id || 0x00 || chunk_text)`, as
/// lowercase hex, truncated to 32 characters — 128 bits of digest, far
/// beyond collision reach for any corpus a support desk will hold.
///
/// The `0x00` separators make the three fields unambiguous (identifiers
/// never contain NUL) while folding them into one hash. Hashing the
/// source id along with the text is what keeps two sources' identical
/// boilerplate distinct: deleting one source cannot orphan the other's
/// chunks, because the other's ids never depended on shared content
/// alone. Truncation to the first 16 bytes preserves stability across
/// the digest's full width and keeps the id short enough to index.
fn chunk_id(tenant_id: &str, source_id: &str, chunk_text: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut hasher = Sha256::new();
    hasher.update(tenant_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(source_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(chunk_text.as_bytes());
    let digest = hasher.finalize();

    let mut id = String::with_capacity(32);
    for byte in digest.iter().take(16) {
        id.push(char::from(HEX[usize::from(byte >> 4)]));
        id.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    id
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words_of(chunk: &Chunk) -> Vec<String> {
        chunk.text.split_whitespace().map(str::to_string).collect()
    }

    fn ids_of(chunks: &[Chunk]) -> Vec<&str> {
        chunks.iter().map(|c| c.id.as_str()).collect()
    }

    #[test]
    fn tokenize_folds_case_and_drops_punctuation() {
        assert_eq!(
            tokenize("Hello, World! It's 2026."),
            ["hello", "world", "it", "2026"]
        );
    }

    #[test]
    fn tokenize_enforces_length_bounds() {
        let forty_eight = "x".repeat(48);
        let forty_nine = "y".repeat(49);
        let text = format!("a ab {forty_eight} {forty_nine}");
        // "a" is below the 2-char floor, the 49-char token above the
        // ceiling; the 2-char and 48-char tokens survive.
        assert_eq!(tokenize(&text), ["ab", forty_eight.as_str()]);
    }

    #[test]
    fn tokenize_keeps_non_ascii_words() {
        assert_eq!(
            tokenize("Café MÜNCHEN — overridden"),
            ["café", "münchen", "overridden"]
        );
    }

    #[test]
    fn tokenize_does_not_segment_cjk() {
        // Pinned on purpose: the documented v1 limitation, so a fix shows
        // up as a deliberate test change.
        assert_eq!(tokenize("客服系统状态"), ["客服系统状态"]);
    }

    #[test]
    fn overlap_is_real() {
        // 24 words, max 10, overlap 3 -> stride 7 -> windows [0..10],
        // [7..17], [14..24].
        let text = (0..24)
            .map(|i| format!("w{i:02}"))
            .collect::<Vec<_>>()
            .join(" ");
        let chunks = Chunker::new(10, 3).split("tenant", "source", &text);
        assert_eq!(chunks.len(), 3);
        assert_eq!(
            chunks.iter().map(|c| c.ordinal).collect::<Vec<_>>(),
            [0, 1, 2]
        );

        let (c0, c1, c2) = (
            words_of(&chunks[0]),
            words_of(&chunks[1]),
            words_of(&chunks[2]),
        );
        // Each window really covers its word range.
        let expected = |range: std::ops::Range<usize>| -> Vec<String> {
            range.map(|i| format!("w{i:02}")).collect()
        };
        assert_eq!(c0, expected(0..10));
        assert_eq!(c1, expected(7..17));
        assert_eq!(c2, expected(14..24));
        // ... and the overlap is the same words, not a re-split.
        assert_eq!(c0[7..10], c1[0..3]);
        assert_eq!(c1[7..10], c2[0..3]);
    }

    #[test]
    fn chunk_text_preserves_punctuation_and_inner_whitespace() {
        // A double space between the first two words and punctuation
        // everywhere: chunks are verbatim slices, so both survive.
        let text = "Hello,  world! It's fine — really: ok.";
        let chunks = Chunker::new(3, 0).split("tenant", "source", text);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].text, "Hello,  world! It's");
        assert_eq!(chunks[1].text, "fine — really:");
        assert_eq!(chunks[2].text, "ok.");
    }

    #[test]
    fn ids_are_stable_and_scoped() {
        let text = (0..18)
            .map(|i| format!("word{i}"))
            .collect::<Vec<_>>()
            .join(" ");
        let chunker = Chunker::new(10, 2); // stride 8: windows [0..10], [8..18]

        let again = chunker.split("tenant-a", "src-1", &text);
        let first = chunker.split("tenant-a", "src-1", &text);
        assert_eq!(ids_of(&first), ids_of(&again));

        // A different tenant, or a different source, changes every id:
        // identical boilerplate in two sources stays distinct.
        for other in [
            chunker.split("tenant-b", "src-1", &text),
            chunker.split("tenant-a", "src-2", &text),
        ] {
            assert_eq!(other.len(), first.len());
            for (a, b) in first.iter().zip(&other) {
                assert_ne!(a.id, b.id);
            }
        }

        // Appending to the end: windows that were unclamped keep their
        // ids, because their text did not move. The final window is
        // clamped to the old end, so IT changes — the honest cost of
        // content addressing.
        let longer = format!("{text} tail0 tail1 tail2 tail3 tail4 tail5 tail6 tail7");
        let extended = chunker.split("tenant-a", "src-1", &longer);
        assert_eq!(extended.len(), 3);
        assert_eq!(extended[0].id, first[0].id);
        assert_eq!(extended[1].id, first[1].id);
        assert_ne!(extended[2].id, first[0].id);
        assert_ne!(extended[2].id, first[1].id);
    }

    #[test]
    fn chunk_id_is_the_documented_hash() {
        // Golden vector: sha256("tenant-a\0src-1\0Hello, world!") truncated
        // to 32 hex chars. Pins the byte layout of the hash input.
        let chunks = Chunker::default().split("tenant-a", "src-1", "Hello, world!");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].id, "4db6b3083c130728202bea9934478f37");
        assert_eq!(chunks[0].id.len(), 32);
        assert!(
            chunks[0]
                .id
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase())
        );
    }

    #[test]
    fn short_text_is_a_single_chunk() {
        let chunks = Chunker::default().split("tenant", "source", "one two three");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text, "one two three");
        assert_eq!(chunks[0].ordinal, 0);
    }

    #[test]
    fn empty_or_whitespace_text_yields_no_chunks() {
        assert!(Chunker::default().split("tenant", "source", "").is_empty());
        assert!(
            Chunker::default()
                .split("tenant", "source", "  \n\t  ")
                .is_empty()
        );
    }

    #[test]
    fn duplicate_windows_are_indexed_once() {
        // 10 identical words, max 4, overlap 1 -> stride 3 -> windows
        // [0..4], [3..7], [6..10], every one with the same text
        // "alpha alpha alpha alpha", so all three share an id and only the
        // first is kept. (10 words makes the last window full; a clamped
        // shorter tail is honestly a different chunk.)
        let text = "alpha ".repeat(10);
        let chunks = Chunker::new(4, 1).split("tenant", "source", &text);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].ordinal, 0);
        assert_eq!(chunks[0].text, "alpha alpha alpha alpha");
    }

    #[test]
    fn repeated_sentence_yields_no_duplicate_ids() {
        // Nine windows of a repeated sentence: whatever dedupe collapses,
        // the ids the caller inserts must be unique (no PK clash).
        let text = "check the status page for updates ".repeat(9);
        let chunks = Chunker::new(5, 1).split("tenant", "source", &text);
        assert!(!chunks.is_empty());
        let ids: HashSet<&str> = chunks.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(ids.len(), chunks.len());
    }

    #[test]
    fn terms_are_sorted_unique_and_sum_to_length() {
        let chunks =
            Chunker::default().split("tenant", "source", "Zebra alpha zebra BETA beta gamma");
        assert_eq!(chunks.len(), 1);
        let chunk = &chunks[0];

        assert!(chunk.terms.windows(2).all(|pair| pair[0].0 < pair[1].0));
        assert_eq!(
            chunk.length,
            chunk.terms.iter().map(|(_, tf)| tf).sum::<u32>()
        );
        assert_eq!(
            chunk
                .terms
                .iter()
                .find(|(term, _)| term == "zebra")
                .map(|(_, tf)| *tf),
            Some(2)
        );
        // The term list is exactly the tokenized chunk text, counted.
        let mut counted: BTreeMap<String, u32> = BTreeMap::new();
        for term in tokenize(&chunk.text) {
            *counted.entry(term).or_default() += 1;
        }
        assert_eq!(chunk.terms, counted.into_iter().collect::<Vec<_>>());
    }

    #[test]
    fn new_clamps_so_overlap_is_below_max_words() {
        let chunker = Chunker::new(3, 10);
        assert_eq!(chunker.max_words, 3);
        assert_eq!(chunker.overlap_words, 2);

        let degenerate = Chunker::new(0, 0);
        assert_eq!(degenerate.max_words, 1);
        assert_eq!(degenerate.overlap_words, 0);
    }

    #[test]
    fn every_word_can_be_its_own_chunk() {
        // max_words 1 forces overlap 0 and stride 1: one chunk per word,
        // and the loop must terminate.
        let chunks = Chunker::new(1, 5).split("tenant", "source", "alpha beta gamma");
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].text, "alpha");
        assert_eq!(chunks[2].text, "gamma");
    }

    #[test]
    fn a_trailing_window_is_never_wholly_contained() {
        // 10 words, max 4, overlap 3 -> stride 1: windows [0..4], [1..5],
        // ..., [6..10] — starts 0 through 6, so seven windows, each adding
        // exactly one new word.
        let text = (0..10)
            .map(|i| format!("w{i}"))
            .collect::<Vec<_>>()
            .join(" ");
        let chunks = Chunker::new(4, 3).split("tenant", "source", &text);
        assert_eq!(chunks.len(), 7);
        for pair in chunks.windows(2) {
            let (previous, next) = (words_of(&pair[0]), words_of(&pair[1]));
            // The next window must contain words the previous one does not.
            assert_ne!(previous.last(), next.last());
            assert_eq!(previous[1..], next[..3]);
        }
    }
}
