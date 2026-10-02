//! Pure metric helpers: text normalisation, the per-reference retrieval
//! scores and the guarded ratio every report field is built from.

use std::fmt::Write as _;

/// Case-folded, whitespace-collapsed — the comparison both gold phrases
/// and chunk bodies go through, so a phrase split across a line matches.
pub(crate) fn normalize(text: &str) -> String {
    text.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

/// `hits / total`, `None` when there is nothing to divide by.
pub(crate) fn ratio(hits: usize, total: usize) -> Option<f64> {
    if total == 0 {
        return None;
    }
    Some(f64::from(u32::try_from(hits).ok()?) / f64::from(u32::try_from(total).ok()?))
}

/// One reference's rank in a result list, or `None` if never retrieved.
pub(crate) type Rank = Option<usize>;

/// `recall@k`: share of references retrieved within the top `k`.
pub(crate) fn recall_at(ranks: &[Rank], k: usize) -> Option<f64> {
    let hits = ranks
        .iter()
        .filter(|rank| rank.is_some_and(|r| r <= k))
        .count();
    ratio(hits, ranks.len())
}

/// Mean reciprocal rank over references: `1/rank` for one retrieved, `0`
/// for one that was not.
#[allow(clippy::cast_precision_loss, reason = "counts are small")]
pub(crate) fn mrr(ranks: &[Rank]) -> Option<f64> {
    if ranks.is_empty() {
        return None;
    }
    let sum: f64 = ranks
        .iter()
        .map(|rank| rank.map_or(0.0, |r| 1.0 / r as f64))
        .sum();
    Some(sum / ranks.len() as f64)
}

/// Percent-encode a query value, leaving RFC 3986 unreserved bytes alone.
pub(crate) fn encode_query(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(byte));
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{encode_query, mrr, normalize, ratio, recall_at};

    #[test]
    fn normalization_case_folds_and_collapses_whitespace() {
        assert_eq!(normalize("Reset   Your\n\tPassword"), "reset your password");
        assert_eq!(normalize("thirty\n  days"), normalize("Thirty days"));
    }

    #[test]
    fn recall_counts_references_within_k() {
        let ranks = [Some(1), Some(4), None, Some(6)];
        assert_eq!(recall_at(&ranks, 1), Some(0.25));
        assert_eq!(recall_at(&ranks, 6), Some(0.75));
        assert_eq!(recall_at(&ranks, 10), Some(0.75));
        assert_eq!(recall_at(&[], 10), None);
    }

    #[test]
    fn mrr_is_zero_for_a_reference_that_was_never_retrieved() {
        assert_eq!(mrr(&[Some(1), None]), Some(0.5));
        assert_eq!(mrr(&[Some(2), Some(2)]), Some(0.5));
        assert_eq!(mrr(&[]), None);
    }

    #[test]
    fn ratio_guards_its_denominator() {
        assert_eq!(ratio(1, 4), Some(0.25));
        assert_eq!(ratio(0, 4), Some(0.0));
        assert_eq!(ratio(1, 0), None);
    }

    #[test]
    fn query_encoding_leaves_unreserved_bytes_and_escapes_the_rest() {
        assert_eq!(encode_query("reset password"), "reset%20password");
        assert_eq!(encode_query("a-b_c.d~e"), "a-b_c.d~e");
        assert_eq!(encode_query("q&x=1/2?"), "q%26x%3D1%2F2%3F");
        assert_eq!(encode_query("café"), "caf%C3%A9");
    }
}
