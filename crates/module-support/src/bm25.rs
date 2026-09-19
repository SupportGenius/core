//! Query-side ranking: Okapi BM25 computed in Rust over the postings rows
//! the caller fetched.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

/// BM25's two knobs.
///
/// `k1` is term-frequency saturation: every extra occurrence helps, but
/// each contributes less and the contribution is bounded, so a page that
/// repeats a term ten times beats one that says it once without a page
/// that says it a hundred times running away with every query. `b` is how
/// strongly length normalises: 0 ignores length entirely, 1 rescales by
/// document length fully. 1.2 and 0.75 are the standard Okapi values —
/// support articles do get less relevant per term as they pad, but a
/// short chunk is not automatically the better answer, and 0.75 is the
/// usual compromise between those two forces.
pub struct Params {
    /// Term-frequency saturation, typically around 1.2.
    pub k1: f64,
    /// Length-normalisation strength, normally within `0.0..=1.0`.
    pub b: f64,
}

impl Default for Params {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

/// One `sg_postings` row joined to the chunk length it indexes.
pub struct Posting {
    /// The chunk this row belongs to (`sg_chunks.id`).
    pub chunk_id: String,
    /// The indexed term. Compared exactly against the query terms, so
    /// both sides must have gone through the same
    /// [`tokenize`](crate::chunk::tokenize) — the shared tokenizer is what
    /// guarantees that.
    pub term: String,
    /// How often the term occurs in the chunk.
    pub tf: u32,
    /// The chunk's total term count (`sg_chunks.length`).
    pub length: u32,
}

/// Corpus-wide statistics for one tenant.
pub struct Corpus {
    /// Number of chunks in the tenant — BM25's `N`. Must be the tenant's
    /// full chunk count, not the number of rows fetched.
    pub chunk_count: u64,
    /// Mean of `length` over those chunks. Degenerate values are guarded:
    /// `avg_length <= 0.0` (as is `chunk_count == 0`) switches length
    /// normalisation off instead of producing NaN or infinity.
    pub avg_length: f64,
}

/// A ranked chunk: its id and its BM25 score.
pub struct Scored {
    pub chunk_id: String,
    pub score: f64,
}

/// Ranks chunks for `query_terms` against `postings` — the rows the
/// caller fetched for exactly those terms, scoped to the tenant.
///
/// # Preconditions
///
/// The result is only as honest as `postings` is complete: [`Corpus`]
/// gives the tenant's `N`, and `df(term)` is counted as the number of
/// distinct chunk ids **in `postings`** with that term. That is exact if
/// and only if the caller fetched every posting for the query's terms
/// within the tenant. A `LIMIT` on that SQL is the silent way to break
/// ranking: it deflates `df`, inflates `idf`, and reorders results with
/// no error anywhere. Duplicate query terms are counted once (standard
/// BM25 with qtf = 1), postings whose term is not in the query are
/// ignored, and a chunk's score is the sum over its postings:
///
/// ```text
/// idf(t)   = ln(1 + (N - df + 0.5) / (df + 0.5))
/// score(c) = Σ_t idf(t) · tf · (k1 + 1) / (tf + k1 · (1 - b + b·len/avgdl))
/// ```
///
/// The `ln(1 + ...)` idf variant is always positive — a term present in
/// every chunk scores near zero rather than dragging scores negative —
/// and, algebraically equal to `ln((N + 1)/(df + 0.5))`, is finite for
/// every `N >= 0` and `df >= 0`, so no input combination can produce a
/// NaN or infinite idf.
///
/// # Ordering
///
/// Returned sorted by score descending, ties broken by `chunk_id`
/// ascending, using [`f64::total_cmp`] so the order is deterministic and
/// NaN-free even for equal or pathological scores.
pub fn rank(
    query_terms: &[String],
    postings: &[Posting],
    corpus: &Corpus,
    params: &Params,
) -> Vec<Scored> {
    let query: BTreeSet<&str> = query_terms.iter().map(String::as_str).collect();
    if query.is_empty() {
        return Vec::new();
    }

    // df: term -> distinct chunk ids among the fetched postings. A set,
    // not a count, because defensive duplicates in the rows must not
    // count twice.
    let mut df: HashMap<&str, HashSet<&str>> = HashMap::new();
    for posting in postings {
        if query.contains(posting.term.as_str()) {
            df.entry(posting.term.as_str())
                .or_default()
                .insert(posting.chunk_id.as_str());
        }
    }

    // N and df as f64: a support corpus is nowhere near 2^53 chunks, so
    // the precision this cast loses cannot surface in an idf.
    #[expect(clippy::cast_precision_loss)]
    let n = corpus.chunk_count as f64;
    let idf: HashMap<&str, f64> = df
        .iter()
        .map(|(term, chunks)| {
            #[expect(clippy::cast_precision_loss)]
            let df = chunks.len() as f64;
            (*term, (1.0 + (n - df + 0.5) / (df + 0.5)).ln())
        })
        .collect();

    // Accumulate in postings order, not map order: the sum for a chunk is
    // then the same sequence of float additions on every run and platform,
    // which is what makes equal scores reliably equal.
    let mut scores: BTreeMap<&str, f64> = BTreeMap::new();
    for posting in postings {
        let Some(term_idf) = idf.get(posting.term.as_str()) else {
            continue;
        };
        let tf = f64::from(posting.tf);
        let length = f64::from(posting.length);
        let length_norm = if corpus.avg_length > 0.0 && corpus.chunk_count > 0 {
            1.0 - params.b + params.b * (length / corpus.avg_length)
        } else {
            // Degenerate corpus: normalisation factor 1.0. `avg_length`
            // that is zero or NaN would otherwise put an infinity or a
            // NaN inside every denominator.
            1.0
        };
        *scores.entry(posting.chunk_id.as_str()).or_insert(0.0) +=
            term_idf * (tf * (params.k1 + 1.0)) / (tf + params.k1 * length_norm);
    }

    let mut results: Vec<Scored> = scores
        .into_iter()
        .map(|(chunk_id, score)| Scored {
            chunk_id: chunk_id.to_string(),
            score,
        })
        .collect();
    results.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.chunk_id.cmp(&b.chunk_id))
    });
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    fn posting(chunk_id: &str, term: &str, tf: u32, length: u32) -> Posting {
        Posting {
            chunk_id: chunk_id.to_string(),
            term: term.to_string(),
            tf,
            length,
        }
    }

    #[test]
    fn score_matches_the_formula_hand_computed() {
        // One term, one posting, tiny corpus. N = 4, df = 1, tf = 2,
        // len = 6, avgdl = 5, k1 = 1.2, b = 0.75 — worked out longhand
        // below so the test fails loudly if the implementation drifts
        // from the definition in the doc comment.
        let postings = [posting("c1", "retry", 2, 6)];
        let corpus = Corpus {
            chunk_count: 4,
            avg_length: 5.0,
        };
        let query = ["retry".to_string()];

        let ranked = rank(&query, &postings, &corpus, &Params::default());
        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].chunk_id, "c1");

        let idf: f64 = (1.0_f64 + (4.0 - 1.0 + 0.5) / (1.0 + 0.5)).ln();
        let length_norm = 1.0 - 0.75 + 0.75 * (6.0 / 5.0);
        let expected = idf * (2.0 * (1.2 + 1.0)) / (2.0 + 1.2 * length_norm);
        assert!((ranked[0].score - expected).abs() < 1e-9);
    }

    #[test]
    fn rarer_terms_score_higher() {
        // A term in 1 of 100 chunks vs a term in 90 of 100, tf and length
        // held equal so the idf is the only moving part.
        let mut postings = vec![posting("rare", "rarity", 2, 10)];
        for i in 0..90 {
            postings.push(posting(&format!("common{i}"), "common", 2, 10));
        }
        let corpus = Corpus {
            chunk_count: 100,
            avg_length: 10.0,
        };
        let query = ["rarity".to_string(), "common".to_string()];

        let ranked = rank(&query, &postings, &corpus, &Params::default());
        let score_of = |id: &str| {
            ranked
                .iter()
                .find(|scored| scored.chunk_id == id)
                .map(|scored| scored.score)
                .expect("chunk was posted")
        };
        // Rare: idf(1) = ln(1 + 99.5/1.5). Common: idf(90) = ln(1 + 10.5/90.5).
        // Both positive, the rare one much larger.
        assert!(score_of("rare") > score_of("common0"));
        assert!(score_of("common0") > 0.0);
    }

    #[test]
    fn shorter_chunk_ranks_first_for_equal_tf() {
        let postings = [
            posting("long", "timeout", 2, 50),
            posting("short", "timeout", 2, 5),
        ];
        let corpus = Corpus {
            chunk_count: 2,
            avg_length: 27.5,
        };
        let query = ["timeout".to_string()];

        let ranked = rank(&query, &postings, &corpus, &Params::default());
        assert_eq!(ranked.len(), 2);
        assert_eq!(ranked[0].chunk_id, "short");
        assert!(ranked[0].score > ranked[1].score);
    }

    #[test]
    fn tf_saturates_instead_of_scaling_linearly() {
        // Same length, same term, tf 10 vs tf 1: the saturated chunk wins
        // but must not win tenfold — that is k1's whole job.
        let postings = [
            posting("saturated", "outage", 10, 10),
            posting("sparse", "outage", 1, 10),
        ];
        let corpus = Corpus {
            chunk_count: 2,
            avg_length: 10.0,
        };
        let query = ["outage".to_string()];

        let ranked = rank(&query, &postings, &corpus, &Params::default());
        assert_eq!(ranked[0].chunk_id, "saturated");
        let (saturated, sparse) = (ranked[0].score, ranked[1].score);
        assert!(saturated > sparse);
        assert!(saturated < 10.0 * sparse);
    }

    #[test]
    fn equal_scores_order_by_chunk_id_and_repeat() {
        // Inserted "beta" first: if the order came from insertion or map
        // iteration, "beta" could win. Ties must fall to chunk_id order.
        let postings = [
            posting("beta-chunk", "term", 2, 10),
            posting("alpha-chunk", "term", 2, 10),
        ];
        let corpus = Corpus {
            chunk_count: 2,
            avg_length: 10.0,
        };
        let query = ["term".to_string()];

        let first = rank(&query, &postings, &corpus, &Params::default());
        let second = rank(&query, &postings, &corpus, &Params::default());
        assert_eq!(first[0].chunk_id, "alpha-chunk");
        assert_eq!(first[1].chunk_id, "beta-chunk");
        // Bit equality, not a float comparison: identical arithmetic must
        // produce identical floats, or ties would not be ties.
        assert_eq!(first[0].score.to_bits(), first[1].score.to_bits());
        assert_eq!(
            first
                .iter()
                .map(|s| s.chunk_id.as_str())
                .collect::<Vec<_>>(),
            second
                .iter()
                .map(|s| s.chunk_id.as_str())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn degenerate_corpus_stays_finite() {
        let query = ["recovery".to_string()];
        for corpus in [
            Corpus {
                chunk_count: 0,
                avg_length: 0.0,
            },
            Corpus {
                chunk_count: 2,
                avg_length: 0.0,
            },
        ] {
            let ranked = rank(
                &query,
                &[posting("c1", "recovery", 3, 7)],
                &corpus,
                &Params::default(),
            );
            assert_eq!(ranked.len(), 1);
            assert!(ranked[0].score.is_finite(), "score must not be NaN/inf");
        }
    }

    #[test]
    fn the_document_that_answers_the_query_ranks_first() {
        // "billing-doc" is the answer: it says both query terms, several
        // times, in a short chunk. "glossary-doc" mentions each once,
        // buried in a long chunk.
        let query = ["refund".to_string(), "window".to_string()];
        let postings = [
            posting("glossary-doc", "refund", 1, 200),
            posting("glossary-doc", "window", 1, 200),
            posting("billing-doc", "refund", 3, 20),
            posting("billing-doc", "window", 2, 20),
        ];
        let corpus = Corpus {
            chunk_count: 2,
            avg_length: 110.0,
        };

        let ranked = rank(&query, &postings, &corpus, &Params::default());
        assert_eq!(ranked.len(), 2);
        assert_eq!(ranked[0].chunk_id, "billing-doc");
        assert!(ranked[0].score > ranked[1].score);
    }

    #[test]
    fn duplicate_query_terms_are_counted_once() {
        let postings = [posting("c1", "cat", 2, 8)];
        let corpus = Corpus {
            chunk_count: 3,
            avg_length: 8.0,
        };
        let once = ["cat".to_string()];
        let twice = ["cat".to_string(), "cat".to_string()];

        let a = rank(&once, &postings, &corpus, &Params::default());
        let b = rank(&twice, &postings, &corpus, &Params::default());
        // Bit equality: "counted once" means the duplicate never entered
        // the sum, not merely that it made no visible difference.
        assert_eq!(a[0].score.to_bits(), b[0].score.to_bits());
    }

    #[test]
    fn postings_outside_the_query_are_ignored() {
        let with_noise = [
            posting("c1", "cat", 2, 8),
            posting("c1", "dog", 5, 8),
            posting("c2", "cat", 2, 8),
        ];
        let without_noise = [posting("c1", "cat", 2, 8), posting("c2", "cat", 2, 8)];
        let corpus = Corpus {
            chunk_count: 3,
            avg_length: 8.0,
        };
        let query = ["cat".to_string()];

        let a = rank(&query, &with_noise, &corpus, &Params::default());
        let b = rank(&query, &without_noise, &corpus, &Params::default());
        assert_eq!(a.len(), 2);
        for (scored, expected) in a.iter().zip(&b) {
            assert_eq!(scored.chunk_id, expected.chunk_id);
            // Bit equality: the ignored term contributed exactly nothing.
            assert_eq!(scored.score.to_bits(), expected.score.to_bits());
        }
    }

    #[test]
    fn empty_query_or_postings_rank_nothing() {
        let corpus = Corpus {
            chunk_count: 5,
            avg_length: 10.0,
        };
        assert!(
            rank(
                &[],
                &[posting("c1", "x", 1, 4)],
                &corpus,
                &Params::default()
            )
            .is_empty()
        );
        assert!(rank(&["x".to_string()], &[], &corpus, &Params::default()).is_empty());
    }
}
