//! Ingest-side text handling: the one shared tokenizer and the
//! overlapping-window chunker.

use std::collections::{BTreeMap, HashSet};

use sha2::{Digest, Sha256};

/// The tokenizer's behaviour version, stamped on every `sg_chunks` row at
/// ingest and bumped whenever [`tokenize`]'s output changes. The
/// scheduled re-index ([`crate::reindex_stale_chunks`]) re-tokenizes
/// every chunk whose stamp is older, so an index never keeps terms a
/// query can no longer produce.
pub const TOKENIZER_VERSION: u32 = 2;

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
/// Scripts written without spaces cannot be split on anything: Chinese,
/// Japanese kanji, Thai and Hangul runs are indexed as **overlapping
/// character bigrams** instead of one long unsegmented token. A run of
/// five Han characters yields four terms, and a query for any contiguous
/// pair finds it; a lone character run yields that character (the one
/// exception to the 2-character floor). Other text tokenizes exactly as
/// before, so a mixed token like `iphone用設定` splits into `iphone` plus
/// the CJK bigrams. Bigrams are a retrieval fallback, not segmentation —
/// they match any text sharing two adjacent characters — but they are
/// what makes a Japanese or Thai question findable at all, without
/// pulling a dictionary into a Worker isolate.
pub fn tokenize(text: &str) -> Vec<String> {
    let mut terms = Vec::new();
    // At most one of these is ever non-empty: a character fills the word
    // buffer or starts an unspaced run, and each flush empties its own.
    let (mut word, mut run) = (String::new(), String::new());
    for ch in text.chars() {
        if is_unspaced_script(ch) {
            flush_word(&mut word, &mut terms);
            run.push(ch);
        } else {
            flush_run(&mut run, &mut terms);
            if ch.is_alphanumeric() {
                word.push(ch);
            } else {
                flush_word(&mut word, &mut terms);
            }
        }
    }
    flush_run(&mut run, &mut terms);
    flush_word(&mut word, &mut terms);
    terms
}

/// Whether `ch` belongs to a script written without spaces between
/// words, by code point range: Han (including the extension and
/// compatibility blocks), Hiragana and Katakana (halfwidth included),
/// Hangul (jamo and syllables) and Thai — whose combining marks are not
/// [`char::is_alphanumeric`] and would otherwise split a word, so they
/// are named by range and kept inside the run.
fn is_unspaced_script(ch: char) -> bool {
    matches!(ch as u32,
        0x3400..=0x4DBF    // CJK extension A (Han)
        | 0x4E00..=0x9FFF  // Han
        | 0xF900..=0xFAFF  // Han compatibility ideographs
        | 0x2_0000..=0x2_FFFF // Han extensions B and beyond (plane 2)
        | 0x3040..=0x309F  // Hiragana
        | 0x30A0..=0x30FF  // Katakana
        | 0x31F0..=0x31FF  // Katakana phonetic extensions
        | 0xFF66..=0xFF9F  // Halfwidth katakana
        | 0x1100..=0x11FF  // Hangul jamo
        | 0x3130..=0x318F  // Hangul compatibility jamo
        | 0xAC00..=0xD7AF  // Hangul syllables
        | 0x0E00..=0x0E7F  // Thai, combining marks included
    )
}

/// Emits and clears the pending space-delimited word: lowercased, kept
/// only within the 2..=48 character bounds.
fn flush_word(word: &mut String, terms: &mut Vec<String>) {
    if word.is_empty() {
        return;
    }
    let term = std::mem::take(word).to_lowercase();
    let chars = term.chars().count();
    if (2..=48).contains(&chars) {
        terms.push(term);
    }
}

/// Emits and clears the pending unspaced-script run as overlapping
/// character bigrams (`日本語` is `日本`, `本語`), or the character itself
/// when the run is a lone one.
fn flush_run(run: &mut String, terms: &mut Vec<String>) {
    if run.is_empty() {
        return;
    }
    let chars: Vec<char> = std::mem::take(run).chars().collect();
    if chars.len() == 1 {
        terms.push(chars[0].to_string());
        return;
    }
    for pair in chars.windows(2) {
        terms.push(pair.iter().collect());
    }
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
/// summed frequencies. Shared by ingest and by the scheduled re-index,
/// which re-tokenizes a stored chunk and must write exactly what ingest
/// would have.
pub(crate) fn term_frequencies(text: &str) -> (Vec<(String, u32)>, u32) {
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
    fn tokenize_splits_japanese_into_overlapping_bigrams() {
        // Ten kana in a row: nine overlapping bigrams, every adjacent
        // pair present, so a query for any two neighbouring characters
        // finds the run.
        assert_eq!(
            tokenize("パスワードをリセット"),
            [
                "パス", "スワ", "ワー", "ード", "ドを", "をリ", "リセ", "セッ", "ット"
            ]
        );
    }

    #[test]
    fn tokenize_emits_a_lone_unspaced_character_despite_the_two_char_floor() {
        // The one-character exception: a single Thai or CJK character is
        // a term, where a single Latin letter is noise.
        assert_eq!(tokenize("ก"), ["ก"]);
        assert_eq!(tokenize("我就问一下"), ["我就", "就问", "问一", "一下"]);
    }

    #[test]
    fn tokenize_keeps_thai_combining_marks_inside_the_run() {
        // ป ึ ก: the vowel mark U+0E31 is not `is_alphanumeric`, but it
        // belongs to the Thai block, so it joins the run instead of
        // splitting the word — and the query side produces the same
        // bigrams.
        assert_eq!(tokenize("ปึกใหญ่"), ["ปึ", "ึก", "กใ", "ให", "หญ", "ญ่"]);
    }

    #[test]
    fn tokenize_splits_mixed_scripts_the_same_way_on_both_sides() {
        // A mixed token splits into the Latin word plus CJK bigrams, and
        // an adjacent run of a *different* unspaced script is one run —
        // Japanese freely mixes kanji, hiragana and katakana without
        // spaces.
        assert_eq!(tokenize("iphone用設定"), ["iphone", "用設", "設定"]);
        assert_eq!(
            tokenize("日本語のテキスト"),
            ["日本", "本語", "語の", "のテ", "テキ", "キス", "スト"]
        );
    }

    #[test]
    fn tokenize_punctuation_still_separates_unspaced_runs() {
        // Two sentences: the punctuation between the runs keeps their
        // bigrams from straddling it, exactly as it separates words.
        assert_eq!(
            tokenize("状態を確認。ステータスは？"),
            [
                "状態", "態を", "を確", "確認", "ステ", "テー", "ータ", "タス", "スは"
            ]
        );
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
