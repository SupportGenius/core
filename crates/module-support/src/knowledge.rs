//! The port the support module reaches a customer-safe knowledge source
//! through.
//!
//! A turn answers from the module's own index and the model. A deployment
//! may also know a second, external source of already-public answers —
//! SupportGenius's Living Brain — and prefer it when it has one: it is the
//! vendor's own material, authoritative about it in a way no retrieved
//! chunk of the customer's workspace can be. Support cannot depend on the
//! adapter that speaks that service (the adapter is vendor-aware, the
//! module is not), so it declares the smallest port it needs and the
//! *composition*, which depends on the adapter, adapts it.
//!
//! The split of work is deliberate. The port answers one question; the
//! safety rule that a customer may only see public material lives in the
//! adapter, on the wire contract, so every caller inherits it (see
//! `adapter-livingbrain`). A `Support` built with
//! [`Support::new`](crate::Support::new) and no knowledge source behaves
//! exactly as it did before one existed.

use async_trait::async_trait;

use cratefield_core::ModuleContext;

/// A source of customer-safe answers, asked before retrieval.
///
/// `Some` is an answer to publish: the turn is `answered` and neither
/// retrieval nor the model is consulted. `None` — no source wired, nothing
/// known, or an answer the source would not stand behind — leaves the
/// turn to the module's own path unchanged.
#[async_trait]
pub trait PublicKnowledge: Send + Sync {
    /// The answer to `question` for `tenant_id`, or `None` to fall through.
    /// `question` has already been scrubbed of emails, tokens and links by
    /// the caller.
    async fn answer(
        &self,
        ctx: &ModuleContext,
        tenant_id: &str,
        question: &str,
    ) -> Option<PublicAnswer>;
}

/// A customer-safe answer and the public pages behind it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicAnswer {
    pub answer: String,
    pub citations: Vec<PublicCitation>,
}

/// One public page an answer is grounded in. Stored on the turn as
/// `{title, url}` — the shape the widened citation read accepts — and
/// shown to the visitor verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicCitation {
    pub title: String,
    pub url: String,
}
