//! The answer decision, as a pure function: `(model reply, retrieved
//! chunk ids, threshold, prior clarifies) -> outcome`. No database, no
//! clock, no ports — which is what makes it unit-testable directly
//! (module `tests` below) instead of only through the route.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::text_model::TextModelError;

/// How many clarify turns one conversation may produce before the module
/// stops guessing and hands off. Two keeps a clarification a *narrowing*
/// question for the user; a third turn on the same conversation reads, to
/// the person waiting, as a machine that did not listen.
pub(crate) const MAX_CLARIFY_TURNS: u32 = 2;

pub(crate) const OUTCOME_ANSWERED: &str = "answered";
pub(crate) const OUTCOME_CLARIFY: &str = "clarify";
pub(crate) const OUTCOME_HANDOFF: &str = "handoff";

/// The reply the model is asked for, parsed from its text.
///
/// These types are deliberately `snake_case` (`chunk_id`), unlike the
/// module's HTTP response body (`chunkId`): the model's JSON schema is
/// fixed by the issue's contract, which specifies it literally as
/// `{"chunk_id": "…", "quote": "…"}`, while the HTTP surface the module
/// serves follows this codebase's camelCase house style. The schema, the
/// system prompt and the parser all derive from these types, so the three
/// cannot drift apart.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub(crate) struct ModelReply {
    /// The answer text. Quoted when citing; a plain answer otherwise.
    pub answer: String,
    /// Chunks the answer is grounded in, with the exact span quoted.
    #[serde(default)]
    pub citations: Vec<ModelCitation>,
    /// The model's own confidence in 0.0..=1.0.
    pub confidence: f32,
}

/// One citation: which chunk and the exact span that grounds the answer.
/// Fields are `snake_case` for the model, like [`ModelReply`] — see the
/// note there.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub(crate) struct ModelCitation {
    pub chunk_id: String,
    pub quote: String,
}

/// What the module decided to do with this turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    Answered,
    Clarify,
    Handoff,
}

impl Outcome {
    /// The wire form, persisted on the assistant message and returned in
    /// the response body.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Answered => OUTCOME_ANSWERED,
            Self::Clarify => OUTCOME_CLARIFY,
            Self::Handoff => OUTCOME_HANDOFF,
        }
    }
}

/// The decision plus the citations it is willing to publish.
#[derive(Debug, Clone)]
pub(crate) struct Decision {
    pub outcome: Outcome,
    /// Non-empty only when `outcome == Answered`. A clarify or a handoff
    /// never carries citations out of the module — see [`decide`].
    pub citations: Vec<ModelCitation>,
}

/// The whole feature rests on the first rule.
///
/// **Rule 1 — a citation must name a chunk that was actually retrieved.**
/// If *any* citation names an id that was not in the retrieved context,
/// the answer is downgraded: it can never be `answered`, regardless of
/// confidence. The reason is that a citation which can name anything is
/// decoration. The chunk ids are the only thing standing between an
/// answer and a fluent invention; the retrieval list is the entire
/// evidence base this turn was shown, so a citation pointing outside it
/// is either a hallucinated source or a quote from nothing. Checking the
/// quoted text is beyond us — the quote is prose — but checking the id is
/// cheap, total, and it is the difference between "the model read this"
/// and "the model says it read this".
///
/// **Rule 2** — `answered` requires all citations valid, at least one
/// citation, and `confidence >= threshold`. An answer the model will not
/// stand behind, or one with nothing to stand on, is not an answer.
///
/// **Rule 3** — everything else is `clarify`, except when the module
/// should stop guessing: no chunks were retrieved at all (nothing could
/// have been grounded), or the conversation already spent
/// [`MAX_CLARIFY_TURNS`] clarifies (a third "could you rephrase?" is a
/// worse experience than a human).
///
/// A `handoff` marks the conversation `needs_escalation` — delivery of
/// that escalation is the next issue's, not this one's.
pub(crate) fn decide(
    reply: &ModelReply,
    retrieved_ids: &[&str],
    threshold: f32,
    prior_clarifies: u32,
) -> Decision {
    // Rule 1 + rule 2. `all` on an empty list is vacuously true, so "at
    // least one" is checked separately — an answer with no citations has
    // nothing behind it even at full confidence.
    let citable = !reply.citations.is_empty()
        && reply
            .citations
            .iter()
            .all(|citation| retrieved_ids.contains(&citation.chunk_id.as_str()));
    if citable && reply.confidence >= threshold {
        return Decision {
            outcome: Outcome::Answered,
            citations: reply.citations.clone(),
        };
    }

    let stop_guessing = retrieved_ids.is_empty() || prior_clarifies >= MAX_CLARIFY_TURNS;
    Decision {
        outcome: if stop_guessing {
            Outcome::Handoff
        } else {
            Outcome::Clarify
        },
        // Rule 1's second half: never return citations the module could
        // not stand behind.
        citations: Vec::new(),
    }
}

/// Parses the model's text into a [`ModelReply`], rejecting an answer the
/// schema alone would accept: a confidence outside 0.0..=1.0, or an
/// answer with nothing in it, is an unusable answer, and this module
/// treats unusable as a bad gateway rather than a decision.
pub(crate) fn parse_reply(text: &str) -> Result<ModelReply, TextModelError> {
    let reply: ModelReply = serde_json::from_str(text).map_err(|err| {
        TextModelError::Invalid(format!("did not parse as the answer schema: {err}"))
    })?;
    if !(0.0..=1.0).contains(&reply.confidence) {
        return Err(TextModelError::Invalid(format!(
            "confidence {} is outside 0.0..=1.0",
            reply.confidence
        )));
    }
    if reply.answer.trim().is_empty() {
        return Err(TextModelError::Invalid("answer is empty".to_owned()));
    }
    Ok(reply)
}

/// The model's confidence as the stored precision: an integer percentage.
/// Rounding is the *only* float-to-int step in the crate, and the clamp
/// in front of the cast makes it exact.
pub(crate) fn confidence_pct(confidence: f32) -> i64 {
    let scaled = (confidence * 100.0).round().clamp(0.0, 100.0);
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "clamped to 0.0..=100.0 above, so the cast is exact"
    )]
    let pct = scaled as i64;
    pct
}

/// The stored percentage back as a fraction, for threshold comparison.
pub(crate) fn pct_confidence(pct: i64) -> f32 {
    let clamped = pct.clamp(0, 100);
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "clamped to 0..=100 above, so the conversion is exact"
    )]
    let small = clamped as u8;
    f32::from(small) / 100.0
}

/// The stored percentage back as a fraction, for the wire. The division
/// happens in `f64` on purpose: an `f32` 0.9 widens to
/// 0.8999999761581421 the moment JSON takes it, which is not the number
/// the model said nor the percentage that was stored.
pub(crate) fn pct_confidence_f64(pct: i64) -> f64 {
    let clamped = pct.clamp(0, 100);
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "clamped to 0..=100 above, so the conversion is exact"
    )]
    let small = clamped as u8;
    f64::from(small) / 100.0
}

#[cfg(test)]
mod tests {
    use super::{
        Decision, MAX_CLARIFY_TURNS, ModelCitation, ModelReply, Outcome, confidence_pct, decide,
        parse_reply, pct_confidence, pct_confidence_f64,
    };

    fn reply(answer: &str, citations: &[&str], confidence: f32) -> ModelReply {
        ModelReply {
            answer: answer.to_owned(),
            citations: citations
                .iter()
                .map(|chunk_id| ModelCitation {
                    chunk_id: (*chunk_id).to_owned(),
                    quote: format!("quote from {chunk_id}"),
                })
                .collect(),
            confidence,
        }
    }

    #[test]
    fn valid_citations_above_threshold_answer() {
        let decision = decide(&reply("a", &["c1"], 0.9), &["c1", "c2"], 0.6, 0);
        assert_eq!(decision.outcome, Outcome::Answered);
        assert_eq!(decision.citations.len(), 1);
    }

    #[test]
    fn confidence_at_the_threshold_answers() {
        // `>=`, not `>`: the threshold is the bar, not one above it.
        let decision = decide(&reply("a", &["c1"], 0.6), &["c1"], 0.6, 0);
        assert_eq!(decision.outcome, Outcome::Answered);
    }

    #[test]
    fn a_citation_to_an_unretrieved_chunk_can_never_answer() {
        // The headline rule: high confidence, real answer, one fabricated
        // chunk id — downgraded all the same, with no citations published.
        let decision = decide(&reply("a", &["c1", "fabricated"], 0.99), &["c1"], 0.6, 0);
        assert_eq!(decision.outcome, Outcome::Clarify);
        assert!(decision.citations.is_empty());
    }

    #[test]
    fn no_citations_is_not_an_answer() {
        let decision = decide(&reply("a", &[], 0.99), &["c1"], 0.6, 0);
        assert_eq!(decision.outcome, Outcome::Clarify);
    }

    #[test]
    fn below_threshold_clarifies_while_there_is_still_room() {
        let decision = decide(&reply("a", &["c1"], 0.4), &["c1"], 0.6, 0);
        assert_eq!(decision.outcome, Outcome::Clarify);
    }

    #[test]
    fn nothing_retrieved_hands_off() {
        // With no chunks, no citation can be valid — and the module should
        // not burn a clarify turn asking the user to rephrase a question
        // the knowledge base cannot answer at all.
        let decision = decide(&reply("a", &[], 0.99), &[], 0.6, 0);
        assert_eq!(decision.outcome, Outcome::Handoff);
    }

    #[test]
    fn the_max_clarify_turn_hands_off() {
        // At MAX the conversation has produced its quota of clarifies; the
        // next non-answer stops guessing. Below it, clarify.
        let under = decide(
            &reply("a", &["c1"], 0.4),
            &["c1"],
            0.6,
            MAX_CLARIFY_TURNS - 1,
        );
        assert_eq!(under.outcome, Outcome::Clarify);
        let at = decide(&reply("a", &["c1"], 0.4), &["c1"], 0.6, MAX_CLARIFY_TURNS);
        assert_eq!(at.outcome, Outcome::Handoff);
    }

    #[test]
    fn decisions_compare_by_value() {
        // Decision carries citations, so equality must not be assumed;
        // this pins the shape the handlers match on.
        let a = Decision {
            outcome: Outcome::Answered,
            citations: vec![ModelCitation {
                chunk_id: "c1".to_owned(),
                quote: "q".to_owned(),
            }],
        };
        assert_eq!(a.outcome, Outcome::Answered);
        assert_eq!(a.citations[0].chunk_id, "c1");
    }

    #[test]
    fn parse_rejects_a_reply_that_is_not_the_schema() {
        assert!(parse_reply("not json").is_err());
        assert!(parse_reply(r#"{"wrong":"shape"}"#).is_err());
    }

    #[test]
    fn parse_rejects_an_out_of_range_or_empty_answer() {
        let over = parse_reply(r#"{"answer":"a","citations":[],"confidence":1.2}"#);
        assert!(over.is_err(), "confidence above 1.0 is unusable");
        let empty = parse_reply(r#"{"answer":"  ","citations":[],"confidence":0.5}"#);
        assert!(empty.is_err(), "an empty answer is unusable");
    }

    #[test]
    fn parse_accepts_a_well_formed_reply() {
        // The literal shape the issue specifies: `chunk_id`, snake_case.
        // A `chunkId` spelling is NOT the schema and must not parse into a
        // usable citation.
        let parsed = parse_reply(
            r#"{"answer":"a","citations":[{"chunk_id":"c1","quote":"q"}],"confidence":0.82}"#,
        )
        .expect("parses");
        assert_eq!(parsed.citations[0].chunk_id, "c1");
        assert!((parsed.confidence - 0.82).abs() < f32::EPSILON);

        let camel = parse_reply(
            r#"{"answer":"a","citations":[{"chunkId":"c1","quote":"q"}],"confidence":0.82}"#,
        )
        .expect_err("camelCase is not the model schema");
        assert!(matches!(
            camel,
            crate::text_model::TextModelError::Invalid(_)
        ));
    }

    #[test]
    fn confidence_rounds_to_the_stored_percentage() {
        assert_eq!(confidence_pct(0.824), 82);
        assert_eq!(confidence_pct(0.0), 0);
        assert_eq!(confidence_pct(1.0), 100);
        // Out-of-range never reaches storage: parse rejects it, and the
        // rounding clamps anyway.
        assert_eq!(confidence_pct(1.5), 100);
        assert_eq!(confidence_pct(-0.2), 0);
        assert!((pct_confidence(82) - 0.82).abs() < f32::EPSILON);
        // The wire fraction renders exactly, where the f32 path would not:
        // 90/100 in `f64` and the literal `0.9` round to the same `f64`,
        // so the comparison is bit-exact.
        assert_eq!(pct_confidence_f64(90).to_bits(), 0.9_f64.to_bits());
    }
}
