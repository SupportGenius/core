//! The `TextModel` port: a text completion asked for by **tier**, never by
//! vendor. A module says [`ModelTier::Fast`] for its drafting and
//! [`ModelTier::Strong`] for its judging; which provider answers each tier
//! is the venture's wiring, decided once and invisible to the module.
//!
//! **No tools, no streaming, no embeddings in v1.** A completion is one
//! request and one buffered answer. Streaming is the deliberate omission,
//! not a gap: a harness response is buffered whole (`MAX_RESPONSE_BUFFER`,
//! 1 MiB, in `cratefield-runtime-cloudflare`), so a streamed completion has
//! nowhere to arrive on this runtime — a port that promised deltas would be
//! a port the Workers twin could not keep. Tools and embeddings change the
//! shape of the call and the answer, and nothing in the tree needs either
//! yet.
//!
//! There is no outcome enum on this port, unlike the mail and push ports,
//! and that is deliberate: a completion has no "delivered but not
//! configured" middle state — either text came back or nothing did. The
//! unwired answer is therefore [`TextModelError::NotConfigured`], an error
//! variant the caller can match, so a module that cannot degrade without
//! its model fails loudly instead of silently producing nothing.
//!
//! Mirrored verbatim from harness `main`. The mirroring rules (what was
//! copied, what was dropped, and how the swap to `cratefield_core` goes
//! when core publishes the port) are in `module-escalation`'s
//! `src/ports/mod.rs`. This is a shared, non-module crate so that every
//! module can name the port without depending on another module — the
//! support module answers through it, the escalation module drafts and
//! judges through it. `testing::FakeTextModel` sits behind the `testing`
//! feature so the Worker build never carries it.

#[cfg(feature = "testing")]
pub mod testing;

use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The token ceiling a [`Prompt`] starts with: enough for a drafted reply,
/// small enough that a forgotten `.max_tokens(..)` cannot turn into a run
/// away bill. Explicit in the type so "how long can the answer get" is a
/// field a caller can read, not an adapter's private default.
pub const DEFAULT_MAX_TOKENS: u32 = 1024;

/// Which class of model a completion asks for — a **tier**, never a vendor
/// or a model name. A venture maps each tier onto a provider in its own
/// wiring, and can move drafting from one vendor to another without
/// touching a module.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelTier {
    /// The cheap, quick class: drafting, summarising, classifying.
    Fast,
    /// The best-available class: judging, long synthesis, the one call
    /// where quality is the point.
    Strong,
}

impl ModelTier {
    /// The name used in errors and logs.
    pub fn name(&self) -> &'static str {
        match self {
            ModelTier::Fast => "fast",
            ModelTier::Strong => "strong",
        }
    }
}

impl std::fmt::Display for ModelTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// Who is speaking in a [`Turn`]. A prompt is a conversation, and a
/// provider needs to know which side each part came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// The human (or module) side of the conversation.
    User,
    /// The model's side, as a previous completion was recorded.
    Assistant,
}

impl Role {
    /// The name used in errors and logs.
    pub fn name(&self) -> &'static str {
        match self {
            Role::User => "user",
            Role::Assistant => "assistant",
        }
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// One message of the conversation a [`Prompt`] carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Turn {
    pub role: Role,
    pub content: String,
}

impl Turn {
    /// A turn spoken by the [`Role::User`] side.
    #[must_use]
    pub fn user(content: impl Into<String>) -> Self {
        Turn {
            role: Role::User,
            content: content.into(),
        }
    }

    /// A turn spoken by the [`Role::Assistant`] side — a previous
    /// completion, quoted back as context.
    #[must_use]
    pub fn assistant(content: impl Into<String>) -> Self {
        Turn {
            role: Role::Assistant,
            content: content.into(),
        }
    }
}

/// One completion request: the tier asked for, the conversation so far, and
/// what the caller will accept back.
///
/// `#[non_exhaustive]`: build one with [`Prompt::new`] and the builder
/// methods rather than a struct literal. What a v2 port has to carry —
/// tools, temperature, a stop sequence — should not be a breaking change
/// for every caller.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Prompt {
    pub tier: ModelTier,
    pub system: Option<String>,
    pub messages: Vec<Turn>,
    /// A JSON Schema (draft 2020-12) the answer must conform to. When set,
    /// the adapter asks its provider for structured output and a
    /// successful [`Completion`] carries the parsed value in
    /// [`Completion::json`].
    pub json_schema: Option<Value>,
    pub max_tokens: u32,
}

impl Prompt {
    /// An empty prompt for `tier`: no system prompt, no messages, no
    /// schema, and [`DEFAULT_MAX_TOKENS`] as the ceiling. Everything else
    /// is a builder method.
    #[must_use]
    pub fn new(tier: ModelTier) -> Self {
        Self {
            tier,
            system: None,
            messages: Vec::new(),
            json_schema: None,
            max_tokens: DEFAULT_MAX_TOKENS,
        }
    }

    /// The standing instruction the model answers under.
    #[must_use]
    pub fn system(mut self, system: impl Into<String>) -> Self {
        self.system = Some(system.into());
        self
    }

    /// Appends a [`Turn::user`] — the common case, a one-message prompt.
    #[must_use]
    pub fn user(mut self, content: impl Into<String>) -> Self {
        self.messages.push(Turn::user(content));
        self
    }

    /// Appends a [`Turn::assistant`].
    #[must_use]
    pub fn assistant(mut self, content: impl Into<String>) -> Self {
        self.messages.push(Turn::assistant(content));
        self
    }

    /// Appends a whole turn, for a conversation built elsewhere.
    #[must_use]
    pub fn turn(mut self, turn: Turn) -> Self {
        self.messages.push(turn);
        self
    }

    /// Asks for structured output conforming to `schema`; a successful
    /// completion then carries the parsed value in [`Completion::json`].
    #[must_use]
    pub fn json_schema(mut self, schema: Value) -> Self {
        self.json_schema = Some(schema);
        self
    }

    /// The token ceiling for the answer.
    #[must_use]
    pub fn max_tokens(mut self, max_tokens: u32) -> Self {
        self.max_tokens = max_tokens;
        self
    }
}

/// A completed completion: the text, the model that answered (a provider
/// identifier, for the log line), and the token usage.
///
/// `#[non_exhaustive]` for the same reason [`Prompt`] is: what a provider
/// reports back grows, and it should not break every caller.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Completion {
    pub text: String,
    /// The parsed answer, when the prompt carried a
    /// [`Prompt::json_schema`]. `None` when it did not, or the provider's
    /// answer could not be parsed — in which case `text` still holds what
    /// came back.
    pub json: Option<Value>,
    pub model: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
}

impl Completion {
    /// A completion with just the text and the model that wrote it; the
    /// usage and parsed JSON are builder methods.
    #[must_use]
    pub fn new(text: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            json: None,
            model: model.into(),
            input_tokens: 0,
            output_tokens: 0,
        }
    }

    /// The parsed structured answer, for a prompt that asked for one.
    #[must_use]
    pub fn json(mut self, json: Value) -> Self {
        self.json = Some(json);
        self
    }

    /// The token counts the provider reported.
    #[must_use]
    pub fn usage(mut self, input_tokens: u64, output_tokens: u64) -> Self {
        self.input_tokens = input_tokens;
        self.output_tokens = output_tokens;
        self
    }
}

/// Completion failures.
///
/// [`NotConfigured`](Self::NotConfigured) sits on the **error** enum here,
/// unlike the mail and push ports' outcome enums: a completion has no
/// "delivered but not configured" middle state, so an unwired tier is an
/// error the caller matches, not an outcome it inspects.
///
/// `Transient` deliberately carries **only** `retry_after`: with no
/// provider text of its own there is nothing to scrub, and provider text
/// belongs on [`Rejected`](Self::Rejected) and [`Transport`](Self::Transport).
///
/// The two variants that carry provider text are sanitized in `Display`
/// the way the harness sanitizes its error text: what an adapter wraps is
/// the provider's own words, and a prompt is exactly the kind of value
/// that rides back in them — a drafting module quotes a customer's note,
/// and the provider's `4xx` quotes it straight back. `Display` therefore
/// runs it through [`cratefield_core::scrub_text`]; `Debug` still shows
/// the raw string for tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TextModelError {
    /// The tier asked for has no adapter — the venture did not wire it.
    /// Nothing is wrong with the prompt: a module may degrade, the way it
    /// degrades on a not-configured mailer.
    NotConfigured,
    /// The provider refused the request (a `4xx`), or refused the prompt's
    /// content or schema; not retryable without a change.
    Rejected(String),
    /// A transient failure (a `5xx`, a `429`, a transport error): retry
    /// later, and not before `retry_after` when the provider named one.
    /// Carries no message — provider text belongs on
    /// [`Rejected`](Self::Rejected) and [`Transport`](Self::Transport).
    Transient {
        /// How long the provider asked the caller to wait, where it said.
        retry_after: Option<Duration>,
    },
    /// The request never completed as a conversation — the adapter could
    /// not reach the provider, or the answer did not survive the hop.
    Transport(String),
}

impl std::fmt::Display for TextModelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotConfigured => f.write_str("no text model is wired for this tier"),
            Self::Rejected(message) => {
                write!(
                    f,
                    "completion rejected: {}",
                    cratefield_core::scrub_text(message)
                )
            }
            Self::Transient { .. } => f.write_str("completion failed, retryable"),
            Self::Transport(message) => {
                write!(
                    f,
                    "completion transport failed: {}",
                    cratefield_core::scrub_text(message)
                )
            }
        }
    }
}

impl std::error::Error for TextModelError {}

impl TextModelError {
    /// How long the provider asked the caller to wait, where it said.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            TextModelError::Transient { retry_after } => *retry_after,
            _ => None,
        }
    }
}

/// Completes a prompt, over whichever provider the venture wired for the
/// [`Prompt::tier`] it was asked for. An adapter serves any tier it is
/// configured for (typically one).
#[async_trait]
pub trait TextModel: Send + Sync {
    /// Completes `prompt`.
    ///
    /// [`Prompt::json_schema`] is a request: an adapter whose provider
    /// cannot honour structured output answers with plain text in
    /// [`Completion::text`] and `None` in [`Completion::json`], rather than
    /// failing the call.
    ///
    /// # Errors
    ///
    /// [`TextModelError::NotConfigured`] when no adapter is wired for the
    /// tier, [`TextModelError::Rejected`] when the provider refuses the
    /// request, [`TextModelError::Transient`] (with the provider's delay,
    /// where it gave one) when the call can be retried, and
    /// [`TextModelError::Transport`] when it never completed.
    async fn complete(&self, prompt: &Prompt) -> Result<Completion, TextModelError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------
    // ModelTier, Role, and the wire form

    #[test]
    fn a_tier_and_a_role_round_trip_through_json() {
        for tier in [ModelTier::Fast, ModelTier::Strong] {
            let json = serde_json::to_string(&tier).expect("serialises");
            let back: ModelTier = serde_json::from_str(&json).expect("deserialises");
            assert_eq!(tier, back);
        }
        for role in [Role::User, Role::Assistant] {
            let json = serde_json::to_string(&role).expect("serialises");
            let back: Role = serde_json::from_str(&json).expect("deserialises");
            assert_eq!(role, back);
        }
    }

    #[test]
    fn the_wire_form_is_snake_case() {
        // The point of `rename_all`: the persisted name is the prose name,
        // so a config file reads `"strong"`, not `"Strong"`.
        assert_eq!(serde_json::to_value(ModelTier::Strong).unwrap(), "strong");
        assert_eq!(serde_json::to_value(Role::User).unwrap(), "user");
    }

    #[test]
    fn the_tier_names_itself_for_logs_and_errors() {
        assert_eq!(ModelTier::Fast.name(), "fast");
        assert_eq!(ModelTier::Strong.to_string(), "strong");
        assert_eq!(Role::Assistant.name(), "assistant");
        assert_eq!(Role::User.to_string(), "user");
    }

    // -----------------------------------------------------------------
    // Prompt and Completion builders

    #[test]
    fn a_prompt_starts_empty_and_builds_up() {
        let prompt = Prompt::new(ModelTier::Fast);
        assert_eq!(prompt.tier, ModelTier::Fast);
        assert_eq!(prompt.system, None);
        assert!(prompt.messages.is_empty());
        assert_eq!(prompt.json_schema, None);
        assert_eq!(prompt.max_tokens, DEFAULT_MAX_TOKENS);
        assert_eq!(DEFAULT_MAX_TOKENS, 1024);

        let prompt = prompt
            .system("Draft the reply.")
            .user("Hello")
            .assistant("Hi there")
            .turn(Turn::user("And again"))
            .json_schema(serde_json::json!({ "type": "object" }))
            .max_tokens(256);

        assert_eq!(prompt.system.as_deref(), Some("Draft the reply."));
        assert_eq!(
            prompt.messages,
            vec![
                Turn::user("Hello"),
                Turn::assistant("Hi there"),
                Turn::user("And again"),
            ]
        );
        assert_eq!(
            prompt.json_schema,
            Some(serde_json::json!({ "type": "object" }))
        );
        assert_eq!(prompt.max_tokens, 256);
    }

    #[test]
    fn a_completion_starts_with_text_and_model_and_builds_up() {
        let completion = Completion::new("the answer", "vendor-1");
        assert_eq!(completion.text, "the answer");
        assert_eq!(completion.model, "vendor-1");
        assert_eq!(completion.json, None);
        assert_eq!(completion.input_tokens, 0);
        assert_eq!(completion.output_tokens, 0);

        let completion = completion
            .json(serde_json::json!({ "reply": "the answer" }))
            .usage(12, 34);
        assert_eq!(
            completion.json,
            Some(serde_json::json!({ "reply": "the answer" }))
        );
        assert_eq!(completion.input_tokens, 12);
        assert_eq!(completion.output_tokens, 34);
    }

    // -----------------------------------------------------------------
    // TextModelError

    #[test]
    fn display_sanitizes_the_provider_text() {
        // The text an adapter wraps is the provider's own words, and a
        // drafting prompt quotes whatever a user wrote: what the provider
        // echoes back must not survive into a log line or a dead-letter
        // row carrying its URLs, tokens or addresses.
        let error = TextModelError::Rejected(
            "provider 400 for https://api.example.test/v1/complete?token=secret-abcdef".to_owned(),
        );
        let text = error.to_string();
        assert!(text.contains("completion rejected"), "{text}");
        assert!(!text.contains("secret-abcdef"), "{text}");
        assert!(text.contains("?[redacted]"), "{text}");

        let error = TextModelError::Transport("timeout quoting alice@example.test".to_owned());
        let text = error.to_string();
        assert!(text.contains("completion transport failed"), "{text}");
        assert!(!text.contains('@'), "{text}");

        // `Debug` still shows the raw string for a failing test to read.
        assert!(format!("{error:?}").contains("alice@example.test"));
    }

    #[test]
    fn transient_says_retryable_and_carries_no_text_to_scrub() {
        // `Transient` has no message field on purpose — the fixed sentence
        // is the whole `Display`, and a provider's words would be an
        // unsanitised leak by construction.
        let error = TextModelError::Transient {
            retry_after: Some(Duration::from_secs(30)),
        };
        assert_eq!(error.to_string(), "completion failed, retryable");
    }

    #[test]
    fn transient_carries_an_optional_retry_after() {
        let throttled = TextModelError::Transient {
            retry_after: Some(Duration::from_secs(30)),
        };
        assert_eq!(throttled.retry_after(), Some(Duration::from_secs(30)));
        let plain = TextModelError::Transient { retry_after: None };
        assert_eq!(plain.retry_after(), None);
        assert_eq!(TextModelError::NotConfigured.retry_after(), None);
        assert_eq!(
            TextModelError::Rejected("422".to_owned()).retry_after(),
            None,
            "a rejection is not a back-off"
        );
        assert_eq!(
            TextModelError::Transport("timed out".to_owned()).retry_after(),
            None
        );
    }

    #[test]
    fn not_configured_names_the_missing_wiring() {
        assert_eq!(
            TextModelError::NotConfigured.to_string(),
            "no text model is wired for this tier"
        );
    }
}
