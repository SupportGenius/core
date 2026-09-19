//! The venture identity and the module list, written down exactly once.
//!
//! Two link targets build SupportGenius: the Cloudflare Worker
//! (`ventures/supportgenius`) and the static self-hosted binary
//! (`bin/supportgenius`). If each listed the modules itself, the two
//! lists would drift — a module added to one and not the other — and the
//! site's promise ("the same modules as one static binary") quietly
//! becomes false. Both call [`modules`], so the list is one list, and
//! both read their identity from [`venture`], so the name, domain and
//! CORS allowlist cannot diverge either.
//!
//! `HarnessBuilder` is not generic over the runtime — it holds an
//! `Arc<dyn Runtime>` — so a plain function over the builder is all the
//! sharing that is needed: no trait, no generics, and the same call
//! works for a Worker composition and a native one.
//!
//! # Registering a new module
//!
//! [`modules`] is **the one place a new module is registered**:
//! `crates/module-support` and `crates/module-escalation` land here when
//! their colonies merge, and both link targets pick them up for free.
//!
//! # The self-hosted port floor
//!
//! Without Redis the native runtime provides no `RateLimiter` and no
//! `KeyValue`, so a module that *requires* either turns the self-hosted
//! binary into a brick — `Harness::build` would refuse, and the binary
//! would boot for nobody. That property is enforced, not hoped for: the
//! tests in `tests/` compose against the exact port set a Redis-less
//! native boot offers, and fail loudly if a module starts hard-requiring
//! a Redis port.

use cratefield_core::{HarnessBuilder, Venture};
use cratefield_module_waitlist::Waitlist;

/// The venture name, kebab-case.
pub const NAME: &str = "supportgenius";

/// The apex domain; the API serves `api.<domain>`.
pub const DOMAIN: &str = "supportgeni.us";

/// The address confirmation mail is sent from: the sending subdomain
/// verified in Resend. The waitlist module's own default is
/// `no-reply@send.<venture domain>`, and the venture domain here is
/// [`DOMAIN`], so this is the same address by construction — kept
/// explicit anyway so the composition does not depend on the module's
/// default surviving a module upgrade.
pub const MAIL_FROM: &str = "no-reply@send.supportgeni.us";

/// The venture identity both link targets present.
///
/// Deliberately no `.env(VentureEnv::Production)`: the environment is
/// the deployment's to declare (`ENV` in wrangler.toml on Workers, `ENV`
/// in the process environment natively), and `HarnessBuilder::build`
/// takes no config, so it cannot see the operator's recorded acceptance
/// — a hardcoded production env would make the composition refuse to
/// build at all, a panic at boot instead of a serving deployment that
/// says loudly what it is missing.
pub fn venture() -> Venture {
    Venture::new(NAME, DOMAIN)
        .public_url("https://supportgeni.us")
        .cors_origins(["https://supportgeni.us", "https://www.supportgeni.us"])
}

/// Adds every venture module — and its templates — to any builder.
///
/// ── THE ONE PLACE A NEW MODULE IS REGISTERED ──────────────────────────
/// `crates/module-support` and `crates/module-escalation` land in this
/// function when their colonies merge; both link targets pick them up
/// for free. New modules must declare any `RateLimiter`/`KeyValue` use
/// in `optional()`, never `requires()` — the tests in `tests/` enforce
/// it, because a hard requirement is a self-hosted brick (see the crate
/// docs).
pub fn modules(builder: HarnessBuilder) -> HarnessBuilder {
    builder
        // No `/ui` is mounted on either link target, so send the
        // post-confirm landing to the site rather than the module's
        // default status page, which neither serves.
        .module(
            Waitlist::new()
                .products(["supportgenius"])
                .status_redirect("https://supportgeni.us/"),
        )
        // Templates register on the harness builder — `Waitlist` itself
        // has no `.templates` method.
        .templates(cratefield_module_waitlist::default_templates())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// These strings are baked into DNS, the Resend-verified sending
    /// subdomain and the CORS allowlist of both link targets; pinning
    /// them makes a change here a deliberate act rather than a slip.
    #[test]
    fn identity_is_pinned() {
        assert_eq!(NAME, "supportgenius");
        assert_eq!(DOMAIN, "supportgeni.us");
        // Same address the waitlist module derives by default, so the
        // composition cannot drift from the module's sending domain.
        assert_eq!(MAIL_FROM, "no-reply@send.supportgeni.us");
    }
}
