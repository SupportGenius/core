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
//! [`modules`] is **the one place a new module is registered**, and both
//! link targets pick a new one up for free.
//!
//! It registers a third thing beyond the modules: this crate is the one
//! that depends on both `module-support` and `module-escalation`, so it is
//! where support's handoff port is adapted to escalation
//! ([`EscalationHandoff`]). A support handoff and the escalation ticket
//! behind it then commit in the same atomic write, and escalation is kicked
//! to run immediately instead of waiting for its cron. Either module built
//! without the other is unchanged: `Support::new()` carries no sink, and
//! `Escalation` drains on its own schedule where nothing kicks it.
//!
//! # The model and tracker ports
//!
//! Escalation *requires* `TextModel` and `Tracker`, and neither link target
//! has a real one yet, so [`modules`] cannot build without one. Rather than
//! leave the composition unbuildable, the crate ships
//! [`UnconfiguredTextModel`] and [`UnconfiguredTracker`] — ports that
//! answer `NotConfigured`, the error meant for exactly this — and each
//! link target wires them (see the venture's `compose` and the binary's
//! boot). A deployment that wires a real adapter replaces these two and
//! nothing else changes; one that does not answers `503
//! text-model-not-configured` rather than pretending.
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
//!
//! # The cron schedule
//!
//! [`CRONS`] is the venture's schedule in one place: the five-minute
//! escalation drain and the daily waitlist purge. The Worker mirrors it
//! into `[triggers] crons` and the native binary feeds it to the runtime's
//! scheduler (a test pins the wrangler copy). A module does not get its
//! own cron — both runtimes fan every expression out to every module — so
//! a module whose work is **not** safe to run on any cron is wrapped in
//! [`OnCron`] (the waitlist purge is: it must run daily, not 288 times a
//! day).

use std::sync::Arc;

use cratefield_core::{
    Completion, ConfigError, Credential, Defer, Destination, Filed, HarnessBuilder, Module,
    ModuleContext, Port, Prompt, Statement, TextModel, TextModelError, TicketDraft, TicketStatus,
    Tracker, TrackerError, Venture,
};
use cratefield_module_waitlist::Waitlist;
use module_escalation::Escalation;
use module_support::{HandoffSink, Support};

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

/// The five-minute tick: the escalation outbox drain. A handoff kicks its
/// pipeline immediately (see [`support`]), but a kick can be lost — the
/// `Defer` port is best-effort, and a Worker slice can end mid-flight — so
/// the outbox row is durable and this is the tick that catches whatever a
/// missed kick left staged.
pub const CRON_ESCALATION_OUTBOX: &str = "*/5 * * * *";

/// The daily tick: the waitlist retention purge and mail-cooldown prune.
/// Daily because the purge is a retention sweep, not a queue drain —
/// running it more often would delete pending entries with the same
/// outcome, only more often, so it stays where it has always been.
pub const CRON_DAILY: &str = "23 4 * * *";

/// Every cron this venture registers, kept beside the modules whose work
/// they drive and mirrored, in this order, into the Worker's
/// `[triggers] crons` in `ventures/supportgenius/wrangler.toml` (a test
/// pins the two together) and into the native binary's default schedule
/// (the runtime reads `CRONS` from the environment).
pub const CRONS: &[&str] = &[CRON_ESCALATION_OUTBOX, CRON_DAILY];

/// The crons the waitlist module may purge on: the daily tick, and no
/// other. See [`OnCron`] for why a module needs gating once a venture has
/// more than one cron.
const WAITLIST_CRONS: &[&str] = &[CRON_DAILY];

/// Every cron the composition gates a module on ([`OnCron`]): an
/// expression that module's scheduled work is only safe on. An operator's
/// `CRONS` override that drops one of these switches that work off with no
/// error anywhere, so the native binary checks an override against this
/// list before it boots (see [`missing_gated_crons`]). Today the waitlist
/// retention purge is the only gated module.
pub const GATED_CRONS: &[&str] = WAITLIST_CRONS;

/// [`CRONS`] as the owned strings `cratefield_runtime_native`'s
/// `spawn_cron_scheduler` takes. Exists so the native binary names the
/// schedule through this crate rather than repeating the expressions.
#[must_use]
pub fn cron_expressions() -> Vec<String> {
    CRONS.iter().map(|expr| (*expr).to_owned()).collect()
}

/// The [`GATED_CRONS`] entries absent from `schedule`, in [`GATED_CRONS`]
/// order. Pure so the native binary's startup check and its test share one
/// implementation: an override missing one of these would leave a gated
/// module's scheduled work — today the waitlist retention purge — never
/// running, with no error anywhere.
#[must_use]
pub fn missing_gated_crons(schedule: &[String]) -> Vec<&'static str> {
    GATED_CRONS
        .iter()
        .copied()
        .filter(|gated| !schedule.iter().any(|expr| expr == gated))
        .collect()
}

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

/// The `waitlist` module as this venture composes it: the module's own
/// configuration, wrapped so its retention purge runs on the daily cron
/// only (see [`OnCron`]).
///
/// Public so a test can stand the same module up in a harness, and so the
/// venture's health test exercises the wrapped module the composition
/// actually registers rather than a bare `Waitlist`.
#[must_use]
pub fn waitlist() -> OnCron<Waitlist> {
    OnCron::new(
        // No `/ui` is mounted on either link target, so send the
        // post-confirm landing to the site rather than the module's
        // default status page, which neither serves.
        Waitlist::new()
            .products(["supportgenius"])
            .status_redirect("https://supportgeni.us/"),
        WAITLIST_CRONS,
    )
}

/// The `support` module as this venture composes it: the module's own
/// behaviour, plus the [`EscalationHandoff`] sink so an escalating turn
/// files an escalation ticket.
///
/// Public so a test can stand the same module up in a harness without
/// reaching into the private sink.
#[must_use]
pub fn support() -> Support {
    Support::new().with_handoff(Arc::new(EscalationHandoff))
}

/// The `escalation` module as this venture composes it. A thin wrapper
/// today; it exists so the module list is written once as functions, and a
/// test can build the same set the composition registers.
#[must_use]
pub fn escalation() -> Escalation {
    Escalation::new()
}

/// Adds every venture module — and its templates — to any builder.
///
/// ── THE ONE PLACE A NEW MODULE IS REGISTERED ──────────────────────────
/// Both link targets pick a module registered here up for free. New
/// modules must declare any `RateLimiter`/`KeyValue` use in `optional()`,
/// never `requires()` — the tests in `tests/` enforce it, because a hard
/// requirement is a self-hosted brick (see the crate docs).
pub fn modules(builder: HarnessBuilder) -> HarnessBuilder {
    builder
        .module(waitlist())
        // Both mounted on both link targets, not just the Worker: the
        // whole point of this crate is that the binary and the Worker
        // cannot drift, and either module reaching only one of them would
        // be exactly that drift.
        .module(support())
        .module(escalation())
        // Templates register on the harness builder — `Waitlist` itself
        // has no `.templates` method.
        .templates(cratefield_module_waitlist::default_templates())
}

/// A module wrapper that forwards every [`Module`] method to `inner`
/// unchanged **except** [`scheduled`](Module::scheduled), which it calls
/// only for the crons in its list.
///
/// Both runtimes fan one cron out to **every** module's `scheduled(ctx,
/// cron)`, passing the same expression — there is no per-module schedule
/// in the harness (`serve_scheduled` on Workers, `fan_out` natively). Most
/// modules ignore `cron`, because the work they do on a tick is the same
/// work whatever the trigger; the escalation drain is idempotent that way
/// and runs on both crons safely. The waitlist purge is not: it deletes
/// pending entries past the retention window whenever it is called, so
/// once this venture registers a second, five-minute cron, a bare
/// `Waitlist` would purge 288 times a day instead of once. This is the
/// gate that keeps a module's schedule its own.
///
/// Forwarding, not re-implementing, is the point: `name`, `tables`,
/// `migrations`, `personal_data`, `surface` and the rest are the inner
/// module's, so duplicate-table detection, `fz migrations collect` output
/// and the `/__health` listing are byte-for-byte what they were without
/// the wrapper.
pub struct OnCron<M> {
    inner: M,
    /// The expressions the inner `scheduled` is allowed to run on.
    crons: &'static [&'static str],
}

impl<M> OnCron<M> {
    /// Wraps `inner` so its `scheduled` runs only for the expressions in
    /// `crons`. The list is `&'static`: it is compiled-in deployment
    /// configuration (see [`CRONS`]), never request data.
    #[must_use]
    pub fn new(inner: M, crons: &'static [&'static str]) -> Self {
        Self { inner, crons }
    }
}

impl<M: Module> Module for OnCron<M> {
    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn version(&self) -> &'static str {
        self.inner.version()
    }

    fn harness_api(&self) -> u32 {
        self.inner.harness_api()
    }

    fn requires(&self) -> &'static [Port] {
        self.inner.requires()
    }

    fn optional(&self) -> &'static [Port] {
        self.inner.optional()
    }

    fn depends_on(&self) -> &'static [&'static str] {
        self.inner.depends_on()
    }

    fn tables(&self) -> &'static [&'static str] {
        self.inner.tables()
    }

    fn personal_data(&self) -> &'static [cratefield_core::PersonalDataSet] {
        self.inner.personal_data()
    }

    fn emits(&self) -> &'static [&'static str] {
        self.inner.emits()
    }

    fn public_writes(&self) -> bool {
        self.inner.public_writes()
    }

    fn public_write_policy(&self) -> cratefield_core::RoutePolicy {
        self.inner.public_write_policy()
    }

    fn signature_verification(&self) -> cratefield_core::SignatureVerification {
        self.inner.signature_verification()
    }

    fn migrations(&self) -> cratefield_core::Migrations {
        self.inner.migrations()
    }

    fn validate_config(&self, cfg: &dyn cratefield_core::Config) -> Result<(), ConfigError> {
        self.inner.validate_config(cfg)
    }

    fn max_body_bytes(&self, cfg: &dyn cratefield_core::Config) -> usize {
        self.inner.max_body_bytes(cfg)
    }

    fn self_check(&self) -> Vec<String> {
        self.inner.self_check()
    }

    fn router(&self, ctx: ModuleContext) -> cratefield_core::axum::Router {
        self.inner.router(ctx)
    }

    fn well_known(&self) -> Option<cratefield_core::axum::Router> {
        self.inner.well_known()
    }

    fn surface(&self) -> cratefield_core::Surface {
        self.inner.surface()
    }

    fn events(&self) -> Vec<(cratefield_core::EventName, cratefield_core::EventHandler)> {
        self.inner.events()
    }

    /// The gate: outside the wrapper's crons this is the trait's default
    /// no-op, so the inner module never sees a tick it did not opt into.
    fn scheduled<'a>(
        &'a self,
        ctx: &'a ModuleContext,
        cron: &'a str,
    ) -> cratefield_core::BoxFuture<'a, Result<(), cratefield_core::AnyError>> {
        if !self.crons.contains(&cron) {
            return Box::pin(async { Ok(()) });
        }
        self.inner.scheduled(ctx, cron)
    }
}

/// Adapts `module-escalation` to support's [`HandoffSink`] port.
///
/// Support never names escalation — it hands an escalating turn to whatever
/// sink the composition wired, and this type, in the one crate that depends
/// on both, is that sink. [`enqueue`](HandoffSink::enqueue) delegates to
/// escalation's own intake, so escalation keeps sole ownership of the
/// ticket schema; [`kick`](HandoffSink::kick) delegates to escalation's own
/// deferred drain. Neither module learns about the other.
struct EscalationHandoff;

impl HandoffSink for EscalationHandoff {
    fn enqueue(
        &self,
        _ctx: &ModuleContext,
        tenant_id: &str,
        conversation_id: &str,
        transcript: &str,
    ) -> Vec<Statement> {
        // The port carries no clock or id generator: `Escalation::intake`
        // stamps rows with core's `SystemClock`/`UlidIdGen`, the same
        // defaults the module runs with when the harness drives it.
        Escalation::new()
            .intake()
            .enqueue(tenant_id, conversation_id, transcript)
    }

    fn kick(&self, ctx: &ModuleContext, defer: Arc<dyn Defer>) {
        Escalation::kick(ctx, defer);
    }
}

/// The `TextModel` a deployment gets before an operator wires a real one.
///
/// Escalation *requires* the port, so a harness refuses to build without
/// one, and neither link target has a model adapter in its graph yet. This
/// answers [`TextModelError::NotConfigured`] — the error the port defines
/// for exactly this — so a route that needs a model fails loudly and
/// specifically (`POST /messages` answers `503 text-model-not-configured`)
/// instead of the composition failing to build at all. Wiring a real model
/// later replaces this and nothing else.
#[derive(Debug, Clone, Copy, Default)]
pub struct UnconfiguredTextModel;

#[async_trait::async_trait]
impl TextModel for UnconfiguredTextModel {
    async fn complete(&self, _prompt: &Prompt) -> Result<Completion, TextModelError> {
        Err(TextModelError::NotConfigured)
    }
}

/// The `Tracker` counterpart of [`UnconfiguredTextModel`]: escalation
/// requires the port, and no tracker adapter is wired yet, so this reports
/// [`TrackerError::NotConfigured`] — a ticket that cannot be filed must
/// look like a failure to the code that needed it filed, never a silent
/// success.
#[derive(Debug, Clone, Copy, Default)]
pub struct UnconfiguredTracker;

#[async_trait::async_trait]
impl Tracker for UnconfiguredTracker {
    async fn file(
        &self,
        _dest: &Destination,
        _cred: &Credential,
        _draft: &TicketDraft,
    ) -> Result<Filed, TrackerError> {
        Err(TrackerError::NotConfigured)
    }

    async fn status(
        &self,
        _dest: &Destination,
        _cred: &Credential,
        _external_id: &str,
    ) -> Result<TicketStatus, TrackerError> {
        Err(TrackerError::NotConfigured)
    }
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

    /// An operator override that drops a gated cron is caught before boot:
    /// the full schedule misses nothing, while the five-minute expression
    /// alone is missing the daily tick the waitlist purge is gated to.
    #[test]
    fn an_override_missing_a_gated_cron_is_named() {
        assert!(missing_gated_crons(&cron_expressions()).is_empty());
        let escalation_only = vec![CRON_ESCALATION_OUTBOX.to_owned()];
        assert_eq!(missing_gated_crons(&escalation_only), vec![CRON_DAILY]);
    }
}
