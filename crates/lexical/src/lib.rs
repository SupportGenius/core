//! The shared lexical core: the one tokenizer and Okapi BM25, as pure
//! Rust with no I/O of any kind, so the same code runs in a Cloudflare
//! Worker isolate and in `cargo test`.
//!
//! [`tokenize`] is shared by every caller that indexes or queries — the
//! support module's ingest and search, and the escalation module's
//! duplicate-candidate scoring — which is the point: a query tokenised
//! differently from the index it searches finds nothing, so there is
//! exactly one tokenizer and both sides call it. [`bm25::rank`] scores
//! whatever postings a caller hands it, this module's tables or a
//! candidate set built in memory.

pub mod bm25;

/// The tokenizer's behaviour version, bumped whenever [`tokenize`]'s
/// output changes. The support module stamps it on every `sg_chunks` row
/// at ingest, so its scheduled re-index re-tokenizes every chunk whose
/// stamp is older and an index never keeps terms a query can no longer
/// produce.
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
