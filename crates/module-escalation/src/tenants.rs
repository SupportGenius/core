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
}
