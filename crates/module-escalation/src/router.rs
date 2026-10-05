//! The port the escalation file stage reaches an owner lookup through.
//!
//! A filed ticket goes where the tenant configured it to go, but a
//! deployment may also know *who owns* a topic — SupportGenius configures
//! its Living Brain to answer exactly that — and prefer that owner.
//! Escalation cannot depend on the adapter that speaks that service (the
//! adapter is vendor-aware, the module is not), so it declares the smallest
//! port it needs and the *composition*, which depends on the adapter,
//! adapts it. The port gets only what a lookup needs: the config naming the
//! service, and, when the runtime resolved one, the HTTP port — the file
//! stage runs inside a [`Pipeline`](crate::Pipeline) with no
//! [`ModuleContext`](cratefield_core::ModuleContext) to pass along. `None`
//! from any cause files the ticket exactly where the module would have:
//! routing enriches a filing, it never gates one.

use async_trait::async_trait;

use cratefield_core::{Config, HttpClient};

/// Who owns a topic, asked by the file stage before it drafts labels.
#[async_trait]
pub trait OwnerRouter: Send + Sync {
    /// The owner `topic` routes to — an opaque target the caller turns into
    /// an `owner:<target>` label and a body line — or `None` when nobody
    /// owns it, no router is configured, or the lookup failed. `topic` has
    /// already been scrubbed of emails, tokens and links by the caller.
    async fn route(
        &self,
        config: &dyn Config,
        http: Option<&dyn HttpClient>,
        tenant_id: &str,
        topic: &str,
    ) -> Option<String>;
}
