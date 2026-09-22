//! The answer decision, as a pure function: `(model reply, retrieved
//! chunk ids, threshold, prior clarifies) -> outcome`. No database, no
//! clock, no ports — which is what makes it unit-testable directly
//! (module `tests` below) instead of only through the route.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use text_model::Completion;

/// The answer threshold that applies when a tenant has not stored one:
/// `answered` needs a model confidence of at least this. Override it per
/// tenant with `PUT /v1/support/admin/tenants/{tenant_id}/settings`.
pub const DEFAULT_ANSWER_THRESHOLD: f32 = 0.60;

/// How many clarify turns one conversation may produce before the module
/// stops guessing and hands off. Two keeps a clarification a *narrowing*
/// question for the user; a third turn on the same conversation reads, to
/// the person waiting, as a machine that did not listen.
pub(crate) const MAX_CLARIFY_TURNS: u32 = 2;

pub(crate) const OUTCOME_ANSWERED: &str = "answered";
pub(crate) const OUTCOME_CLARIFY: &str = "clarify";
pub(crate) const OUTCOME_HANDOFF: &str = "handoff";

/// The reply the model is asked for, parsed from its answer.
///
/// These types are `snake_case` (`chunk_id`), like the module's HTTP
/// bodies: the model's JSON schema is fixed by the issue's contract,
/// which specifies it literally as `{"chunk_id": "…", "quote": "…"}`.
/// [`reply_schema`] is written by hand next to these types so the prompt's
/// contract and the parser are read side by side.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ModelReply {
    /// The answer text.
    pub answer: String,
    /// Chunks the answer is grounded in, with the exact span quoted.
    #[serde(default)]
    pub citations: Vec<ModelCitation>,
    /// The model's own confidence in 0.0..=1.0.
    pub confidence: f32,
}

/// One citation: which chunk and the exact span that grounds the answer.
/// Fields are `snake_case` for the model, like [`ModelReply`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ModelCitation {
    pub chunk_id: String,
    pub quote: String,
}

/// The JSON Schema (draft 2020-12) the answer prompt constrains the model
/// with — exactly [`ModelReply`]'s fields, `chunk_id` in `snake_case`.
pub(crate) fn reply_schema() -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "SupportAnswer",
        "type": "object",
        "additionalProperties": false,
        "required": ["answer", "citations", "confidence"],
        "properties": {
            "answer": { "type": "string", "minLength": 1 },
            "citations": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["chunk_id", "quote"],
                    "properties": {
                        "chunk_id": { "type": "string" },
                        "quote": { "type": "string" }
                    }
                }
            },
            "confidence": { "type": "number", "minimum": 0, "maximum": 1 }
        }
    })
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
/// **Rule 1 — a citation must name a chunk that was actually retrieved
/// for this request.** If *any* citation names an id that was not in the
/// retrieved context, the answer is downgraded: it can never be
/// `answered`, regardless of confidence. A citation which can name
/// anything is decoration. The retrieval list is the entire evidence base
/// this turn was shown, so a citation pointing outside it — a fabricated
/// id, or a real chunk that belongs to another tenant — is a quote from
/// nothing the model read. Checking the quoted text is beyond us — the
/// quote is prose — but checking the id is cheap and total.
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
/// A `handoff` marks the conversation `needs_escalation`.
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

/// Parses the model's completion into a [`ModelReply`] — the structured
/// value when the adapter parsed one ([`Completion::json`]), the raw text
/// otherwise — rejecting an answer the schema alone would accept: a
/// confidence outside 0.0..=1.0, or an answer with nothing in it, is an
/// unusable answer, and this module treats unusable as a bad gateway
/// rather than a decision. The `Err` is a diagnostic, never shown to a
/// caller.
pub(crate) fn parse_reply(completion: &Completion) -> Result<ModelReply, String> {
    let parsed = match &completion.json {
        Some(value) => ModelReply::deserialize(value),
        None => serde_json::from_str(&completion.text),
    };
    let reply: ModelReply =
        parsed.map_err(|err| format!("did not parse as the answer schema: {err}"))?;
    if !(0.0..=1.0).contains(&reply.confidence) {
        return Err(format!(
            "confidence {} is outside 0.0..=1.0",
            reply.confidence
        ));
    }
    if reply.answer.trim().is_empty() {
        return Err("answer is empty".to_owned());
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
    f32::from(pct_u8(pct)) / 100.0
}

/// The stored percentage back as a fraction, for the wire. The division
/// happens in `f64` on purpose: an `f32` 0.9 widens to
/// 0.8999999761581421 the moment JSON takes it, which is not the number
/// the model said nor the percentage that was stored.
pub(crate) fn pct_confidence_f64(pct: i64) -> f64 {
    f64::from(pct_u8(pct)) / 100.0
}

fn pct_u8(pct: i64) -> u8 {
    // Clamped to 0..=100 first, so the conversion cannot fail.
    u8::try_from(pct.clamp(0, 100)).unwrap_or(100)
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_CLARIFY_TURNS, ModelCitation, ModelReply, Outcome, confidence_pct, decide, parse_reply,
        pct_confidence, pct_confidence_f64, reply_schema,
    };
    use text_model::Completion;

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

    fn parse(text: &str) -> Result<ModelReply, String> {
        parse_reply(&Completion::new(text, "fake-fast"))
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
    fn parse_rejects_a_reply_that_is_not_the_schema() {
        assert!(parse("not json").is_err());
        assert!(parse(r#"{"wrong":"shape"}"#).is_err());
    }

    #[test]
    fn parse_rejects_an_out_of_range_or_empty_answer() {
        let over = parse(r#"{"answer":"a","citations":[],"confidence":1.2}"#);
        assert!(over.is_err(), "confidence above 1.0 is unusable");
        let empty = parse(r#"{"answer":"  ","citations":[],"confidence":0.5}"#);
        assert!(empty.is_err(), "an empty answer is unusable");
    }

    #[test]
    fn parse_accepts_a_well_formed_reply() {
        // The literal shape the issue specifies: `chunk_id`, snake_case.
        // A `chunkId` spelling is NOT the schema and must not parse into a
        // usable citation.
        let parsed = parse(
            r#"{"answer":"a","citations":[{"chunk_id":"c1","quote":"q"}],"confidence":0.82}"#,
        )
        .expect("parses");
        assert_eq!(parsed.citations[0].chunk_id, "c1");
        assert!((parsed.confidence - 0.82).abs() < f32::EPSILON);

        let camel =
            parse(r#"{"answer":"a","citations":[{"chunkId":"c1","quote":"q"}],"confidence":0.82}"#);
        assert!(camel.is_err(), "camelCase is not the model schema");
    }

    #[test]
    fn parse_prefers_the_adapters_structured_value() {
        // An adapter that honoured the schema hands back the parsed value;
        // that is what gets read, whatever the text says.
        let completion =
            Completion::new("prose the adapter kept", "fake-fast").json(serde_json::json!({
                "answer": "a",
                "citations": [{ "chunk_id": "c1", "quote": "q" }],
                "confidence": 0.7
            }));
        let parsed = parse_reply(&completion).expect("the json value parses");
        assert_eq!(parsed.citations[0].chunk_id, "c1");
    }

    #[test]
    fn the_schema_names_snake_case_chunk_id_and_requires_every_field() {
        let schema = reply_schema();
        assert_eq!(
            schema["required"],
            serde_json::json!(["answer", "citations", "confidence"])
        );
        assert_eq!(
            schema["properties"]["citations"]["items"]["required"],
            serde_json::json!(["chunk_id", "quote"])
        );
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
