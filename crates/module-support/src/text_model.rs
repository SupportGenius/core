//! The `TextModel` port, defined in this crate for now — and only for
//! now. cratefield-core has no such port yet (absent from 0.4.3 and
//! 0.5.0; the repository README names it as an upstream blocker), so
//! this is the smallest port that carries the module: one method, one
//! request, one completion. It moves to the harness unchanged in shape
//! when the upstream port lands, and until then nothing outside this
//! crate may name a provider — the module asks for a [`ModelTier`] and
//! never for a vendor.

use async_trait::async_trait;

/// How much model the question is worth. The module picks the tier; the
/// deployment maps tiers onto whatever engines it actually runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelTier {
    /// Cheap and quick: first-answer work, where a wrong turn costs one
    /// clarify question, not a support ticket.
    Fast,
    /// Slower and better: reserved for the deep pass a later issue adds.
    Deep,
}

/// One ask: a system instruction, the user's prompt, the JSON schema the
/// answer must parse as, and a budget.
#[derive(Debug, Clone)]
pub struct TextRequest {
    pub tier: ModelTier,
    pub system: String,
    pub prompt: String,
    /// The JSON schema the completion must parse against. Delivered as a
    /// value, not a generic parameter, so the port stays object-safe.
    pub schema: serde_json::Value,
    pub max_output_tokens: u32,
}

/// The model's answer as raw text. Parsing it is the caller's job — the
/// module owns the schema and therefore the judgement of what "usable"
/// means.
#[derive(Debug, Clone)]
pub struct TextCompletion {
    pub text: String,
}

/// Why a completion did not happen, or did not help.
#[derive(Debug)]
pub enum TextModelError {
    /// No model is configured for this deployment. Permanent until the
    /// operator acts.
    NotConfigured,
    /// The model was reachable but could not answer right now. The caller
    /// may retry.
    Transient(String),
    /// The model answered, but the answer is unusable (unparseable,
    /// schema-violating).
    Invalid(String),
}

impl std::fmt::Display for TextModelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotConfigured => f.write_str("no text model is configured"),
            // `detail` is whatever the implementing port chose to say; the
            // port contract keeps provider names and wire shapes out of it.
            Self::Transient(detail) => write!(f, "the text model could not answer: {detail}"),
            Self::Invalid(detail) => write!(f, "the text model's answer is unusable: {detail}"),
        }
    }
}

impl std::error::Error for TextModelError {}

/// The port a support module answers questions through.
#[async_trait]
pub trait TextModel: Send + Sync {
    /// # Errors
    ///
    /// [`TextModelError::NotConfigured`] when the deployment has no model,
    /// [`TextModelError::Transient`] when it could not answer right now,
    /// [`TextModelError::Invalid`] when it answered unusably.
    async fn complete(&self, request: TextRequest) -> Result<TextCompletion, TextModelError>;
}
