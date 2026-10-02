//! Whether a tenant may use the destination routes right now.
//!
//! A signed `sg_…` key proves *who* is asking; it does not prove the
//! tenant is still in good standing. Tenant status (`active`, `suspended`,
//! `closed`) lives in `module-support`'s `sg_tenants`, and modules never
//! depend on each other, so this module declares the smallest port it
//! needs — the same seam `module-support`'s `HandoffSink` is — and the
//! composition, which depends on both, adapts the support module to it.
//!
//! **Fail closed.** An [`Escalation`](crate::Escalation) built without a
//! directory has no way to tell an active tenant from a suspended one, so
//! its destination routes refuse every tenant (`401` on the tenant mount,
//! `404` on the admin mount) rather than trusting the key alone.

use cratefield_core::{DbError, ModuleContext};

/// Answers whether a tenant is active.
#[async_trait::async_trait]
pub trait TenantDirectory: Send + Sync {
    /// `Ok(true)` only for a tenant that exists **and** is active;
    /// `Ok(false)` for an unknown, suspended or closed one. `Err` is an
    /// infrastructure failure (a database outage), which the routes answer
    /// with a `500`, never as evidence about the tenant.
    async fn is_active(&self, ctx: &ModuleContext, tenant_id: &str) -> Result<bool, DbError>;

    /// `Ok(true)` only while the tenant's API key `key_id` (the per-key id
    /// `tenancy::verify` returns) is still on record. A key the tenant has
    /// deleted — revoked — is `Ok(false)`, so a signature that still
    /// verifies is refused here on its very next request, exactly as
    /// `module-support`'s own routes refuse it. `Err` is an infrastructure
    /// failure, answered with a `500`.
    async fn key_is_live(
        &self,
        ctx: &ModuleContext,
        tenant_id: &str,
        key_id: &str,
    ) -> Result<bool, DbError>;
}
