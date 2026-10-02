//! Hand-written stand-ins for the outbound ports, compiled in only when
//! the `dev-fakes` feature is selected and activated only when
//! `SUPPORTGENIUS_DEV_FAKES` is set at boot (see `main.rs`, which logs a
//! loud warning when either happens, so a fake-backed process can never
//! be mistaken for a production boot).
//!
//! Written by hand rather than lifted from `cratefield-testing`: a
//! shipping binary must not depend on the testing crate, and these stubs
//! are a tenth of what it carries. They live **in the binary** — never in
//! the modules and never in the composition crate — so the module
//! contracts stay honest in every configuration.
//!
//! The `StubTextModel` is the same shape as the other two, added when the
//! `TextModel` port got wired (issue #22): without a key and with dev
//! fakes enabled, `POST /v1/support/messages` exercises the real
//! grounded-answer flow — retrieval, citation checking, the threshold
//! decision — against a canned completion, instead of degrading to
//! `503 text-model-not-configured`. It is **not** a second
//! `Answer`-quality path: every reply is labelled `stub` in the
//! completion's model id, the boot warning above is the marker that no
//! provider was called, and a real `ANTHROPIC_API_KEY` in the environment
//! wins over the stub (`main.rs` mounts the real adapters first).

use async_trait::async_trait;
use cratefield_core::{
    Captcha, CaptchaError, Completion, MailError, Mailer, Message, Prompt, Role, SendOutcome,
    TextModel, TextModelError, Verdict,
};
use serde_json::json;

/// Accepts every mail and records it, so a developer can exercise the
/// join flow without a Resend key and read what *would* have gone out in
/// the log. Reports `Sent`, not `NotConfigured`: the flow is being
/// exercised, and the warning in `main.rs` is the honest marker that the
/// send was not real.
pub(crate) struct StubMailer;

#[async_trait]
impl Mailer for StubMailer {
    async fn send(&self, message: Message) -> Result<SendOutcome, MailError> {
        tracing::warn!(
            to = %message.to,
            subject = %message.subject,
            "StubMailer: recorded mail, did not send (dev fakes active)"
        );
        Ok(SendOutcome::Sent {
            id: "dev-fake".to_owned(),
        })
    }
}

/// Accepts the fixed token any Turnstile widget produces — including the
/// dummy token `1x00000000000000000000AA`, which is what a site
/// integration is developed against.
pub(crate) struct StubCaptcha;

#[async_trait]
impl Captcha for StubCaptcha {
    async fn verify(
        &self,
        _token: &str,
        _remote_ip: Option<&str>,
    ) -> Result<Verdict, CaptchaError> {
        tracing::warn!("StubCaptcha: accepted token without verification (dev fakes active)");
        Ok(Verdict {
            ok: true,
            reason: Some("dev-fakes: accepted without verification".to_owned()),
        })
    }
}

/// The line the support module's answer prompt introduces its retrieved
/// chunks with (`crates/module-support` `messages.rs` `build_prompt`).
/// The stub grounds on the first chunk below it, exactly the way the
/// system prompt tells the real model to.
const RETRIEVED_CONTEXT_MARKER: &str = "Retrieved context — cite only these chunk ids:";

/// How much of the grounding chunk the stub quotes back. A quote is
/// checked for its chunk id only (module-support `answer::decide`), so
/// this is a display bound, not a correctness one.
const MAX_QUOTE_CHARS: usize = 160;

/// Answers the support module's prompt with the reply schema it asks for,
/// grounded in the first chunk the prompt actually retrieved — a canned
/// completion that travels the module's whole decision path (citation
/// ids checked against this turn's retrieval, confidence against the
/// tenant threshold) without a provider call.
///
/// Grounding honestly matters more than the wording: the reply cites a
/// chunk id taken from the retrieved context, never an invented one, so
/// a stub answer can pass `answer::decide`'s rule 1 only when retrieval
/// actually returned something. With no retrieved chunk at all the stub
/// returns a zero-confidence, citation-free reply — which the module
/// downgrades to `clarify`, or `handoff` when nothing was retrieved —
/// instead of manufacturing an answer from nothing.
pub(crate) struct StubTextModel;

#[async_trait]
impl TextModel for StubTextModel {
    async fn complete(&self, prompt: &Prompt) -> Result<Completion, TextModelError> {
        tracing::warn!("StubTextModel: returned a canned answer (dev fakes active)");
        let reply = match first_retrieved_chunk(prompt) {
            Some((chunk_id, quote)) => json!({
                "answer": format!(
                    "Dev-fakes stub answer: this is grounded in retrieved chunk \
                     {chunk_id}, quoted below, and produced without a provider call."
                ),
                "citations": [{ "chunk_id": chunk_id, "quote": quote }],
                "confidence": 0.9,
            }),
            None => json!({
                "answer": "Dev-fakes stub: nothing was retrieved to ground an answer on.",
                "citations": [],
                "confidence": 0.0,
            }),
        };
        // The same value in `json` and `text`, the way an adapter that
        // honoured the prompt's schema answers (`answer::parse_reply`
        // prefers the parsed value). `stub` is the model id every log
        // line carries, so a stub answer cannot pass for a provider's.
        Ok(Completion::new(reply.to_string(), "stub").json(reply))
    }
}

/// The first `[chunk_id] body` line of the prompt's retrieved context.
///
/// The support prompt is the only prompt this binary is asked today, and
/// it embeds one `[chunk_id] body` line per retrieved chunk under
/// [`RETRIEVED_CONTEXT_MARKER`]; scanning from the marker (not from the
/// string start) keeps a question that itself contains bracketed text
/// from grounding the answer. A line with a blank id or body is skipped,
/// so the module's empty-retrieval placeholder cannot ground the answer.
fn first_retrieved_chunk(prompt: &Prompt) -> Option<(&str, String)> {
    // The user turn carries the context; the system turn never does.
    let user = prompt
        .messages
        .iter()
        .rev()
        .find(|turn| turn.role == Role::User)?;
    let context = user.content.split_once(RETRIEVED_CONTEXT_MARKER)?.1;
    for line in context.lines() {
        let Some((id, body)) = line.strip_prefix('[').and_then(|rest| rest.split_once(']')) else {
            continue;
        };
        let id = id.trim();
        let body = body.trim();
        if id.is_empty() || body.is_empty() {
            continue;
        }
        return Some((id, short_quote(body)));
    }
    None
}

/// The grounding quote: the chunk's opening, capped at
/// [`MAX_QUOTE_CHARS`] characters on a char boundary.
fn short_quote(body: &str) -> String {
    body.chars().take(MAX_QUOTE_CHARS).collect()
}

#[cfg(test)]
mod tests {
    use super::{StubTextModel, first_retrieved_chunk, short_quote};
    use cratefield_core::{ModelTier, Prompt, TextModel};

    /// The support prompt's shape (`messages.rs` `build_prompt`): system
    /// turn, then a user turn with the question and the `[chunk_id] body`
    /// lines under the retrieved-context marker.
    fn prompt_with_context(context: &str) -> Prompt {
        Prompt::new(ModelTier::Fast)
            .system("answer from the retrieved context")
            .user(format!(
                "Question:\nHow do I reset my password?\n\nRetrieved context — cite only \
                 these chunk ids:\n{context}"
            ))
    }

    #[tokio::test]
    async fn grounds_on_the_first_retrieved_chunk() {
        let prompt = prompt_with_context(
            "[chunk01] To reset your password, open Settings.\n[chunk02] Unrelated.\n",
        );
        let completion = StubTextModel.complete(&prompt).await.expect("stub answers");
        let reply = completion
            .json
            .expect("the stub always returns parsed JSON");
        assert_eq!(reply["citations"][0]["chunk_id"], "chunk01");
        assert_eq!(
            reply["citations"][0]["quote"],
            "To reset your password, open Settings."
        );
        assert_eq!(reply["confidence"], 0.9);
        // The answer names itself as a stub, and so does the model id.
        assert!(
            reply["answer"]
                .as_str()
                .unwrap_or_default()
                .contains("stub")
        );
        assert_eq!(completion.model, "stub");
    }

    #[tokio::test]
    async fn a_question_with_brackets_does_not_ground_the_answer() {
        // The bracketed line appears *before* the marker (in the
        // question), so scanning must not take it.
        let prompt = Prompt::new(ModelTier::Fast).user(
            "Question:\nIs [not-a-chunk] a chunk id?\n\nRetrieved context — cite only \
             these chunk ids:\n[chunk07] Real grounding text.\n",
        );
        let completion = StubTextModel.complete(&prompt).await.expect("stub answers");
        let reply = completion.json.expect("parsed JSON");
        assert_eq!(reply["citations"][0]["chunk_id"], "chunk07");
    }

    #[tokio::test]
    async fn nothing_retrieved_answers_without_citations_and_at_zero_confidence() {
        // The shape `build_prompt` sends when retrieval came back empty —
        // the module's `decide` then downgrades to clarify (or handoff).
        let prompt = prompt_with_context("(nothing was retrieved for this question)\n");
        let completion = StubTextModel.complete(&prompt).await.expect("stub answers");
        let reply = completion.json.expect("parsed JSON");
        assert_eq!(reply["citations"].as_array().map(Vec::len), Some(0));
        assert_eq!(reply["confidence"], 0.0);
        assert!(!reply["answer"].as_str().unwrap_or_default().is_empty());
    }

    #[tokio::test]
    async fn a_prompt_without_the_marker_is_answered_honestly_empty() {
        let prompt = Prompt::new(ModelTier::Fast).user("no context in here");
        let completion = StubTextModel.complete(&prompt).await.expect("stub answers");
        let reply = completion.json.expect("parsed JSON");
        assert_eq!(reply["citations"].as_array().map(Vec::len), Some(0));
    }

    #[test]
    fn the_quote_is_capped_on_char_boundaries() {
        let quote = short_quote(&"héllo ".repeat(100));
        assert!(quote.chars().count() <= 160);
        assert!(!quote.ends_with('\u{fffd}'));
    }

    #[test]
    fn empty_and_whitespace_chunk_bodies_are_skipped() {
        let prompt = prompt_with_context("[bad] \n[good] Real body.\n");
        let (id, quote) = first_retrieved_chunk(&prompt).expect("finds the grounded chunk");
        assert_eq!(id, "good");
        assert_eq!(quote, "Real body.");
    }
}
