//! The SupportGenius Worker: one stateless Cloudflare Worker standing on
//! the Factory Zero harness with the `waitlist` module composed in, and
//! the `support` module (routes under `/v1/support`) mounted alongside it.
//!
//! The site's form posts `POST /v1/waitlist` with `product:
//! "supportgenius"`, the entry lands in this Worker's own D1 database,
//! and a double opt-in confirmation mail goes out through Resend;
//! `GET /v1/waitlist/confirm` turns a pending entry into a confirmed one
//! and redirects the browser to the site with the status token appended
//! (this Worker does not mount the module's status UI), while
//! `GET /v1/waitlist/status` answers the entry's status as JSON.
//!
//! **Mail.** When the `RESEND_API_KEY` secret is set, the Resend adapter
//! sends; when it is absent the adapter reports `SendOutcome::NotConfigured`
//! and every join fails loudly rather than silently capturing an address
//! whose confirmation never arrives (see `build_mailer` for why this
//! venture deliberately has no no-op mailer). Mail is sent from
//! `no-reply@send.supportgeni.us`, the same address the waitlist module
//! derives from the venture domain by default, so the two cannot drift.
//!
//! **Captcha.** When the `TURNSTILE_SECRET` secret is set, the Turnstile
//! adapter verifies the widget token the site form posts; when it is
//! absent no `Captcha` port is mounted at all. Both secrets are read from
//! the Worker `Env` binding at init — never `std::env`, which is empty on
//! Workers.
//!
//! **One runtime, twice used.** The `Cloudflare` runtime is built exactly
//! once per composition and then handed to both `Harness::builder` and
//! `serve`/`serve_scheduled`; see `compose`.

use std::sync::{Arc, OnceLock};

use cratefield_adapter_resend::Resend;
use cratefield_adapter_turnstile::Turnstile;
use cratefield_core::{ConfigError, Harness, Mailer, Venture};
use cratefield_module_waitlist::Waitlist;
use cratefield_runtime_cloudflare::{
    Cloudflare, FetchClient, WorkersClock, serve, serve_scheduled,
};
use module_support::Support;
use worker::{Context, Env, Request, Response, event};

/// The address confirmation mail is sent from: the sending subdomain
/// verified in Resend. The waitlist module's own default is
/// `no-reply@send.<venture domain>`, and the venture domain here is
/// `supportgeni.us`, so this is the same address by construction — kept
/// explicit anyway so the composition does not depend on the module's
/// default surviving a module upgrade.
const MAIL_FROM: &str = "no-reply@send.supportgeni.us";

/// Builds the `Cloudflare` runtime the harness is validated against AND
/// serves with: one instance, cloned into the builder, the original
/// returned alongside the harness.
///
/// The duplication is the shape of a silent bug, in the runtime's own
/// words (`cratefield-runtime-cloudflare` `runtime.rs`): "`Clone`, like
/// the native runtime's `Native`, so a venture can hand one instance to
/// `Harness::builder().runtime(..)` and keep the same one to serve with.
/// Two separately-built instances are the shape of a silent bug:
/// `Harness::build` validates every module's `requires()` against the
/// ports of the instance it was given, and the one that actually serves
/// is the other." Composition is therefore: build once, `clone()` into
/// `Harness::builder().runtime(..)`, serve with the original.
///
/// # Errors
///
/// [`ConfigError`] listing every problem when the composition is invalid.
pub fn compose(
    mailer: Arc<dyn Mailer>,
    captcha: Option<Turnstile>,
) -> Result<(Harness, Cloudflare), ConfigError> {
    let mut runtime = Cloudflare::new().db("DB").mailer_arc(mailer);
    if let Some(captcha) = captcha {
        runtime = runtime.captcha(captcha);
    }

    // The environment is the deployment's to declare, through `ENV` in
    // wrangler.toml (set to production there). Deliberately not hardcoded
    // here with `.env(VentureEnv::Production)`: `HarnessBuilder::build`
    // takes no config, so it cannot see the operator's recorded
    // acceptance, and a hardcoded production env would make the
    // composition refuse to build at all — a panic at boot instead of a
    // serving Worker that says loudly what it is missing.
    let venture = Venture::new("supportgenius", "supportgeni.us")
        .public_url("https://supportgeni.us")
        .cors_origins(["https://supportgeni.us", "https://www.supportgeni.us"]);

    let harness = Harness::builder()
        .venture(venture)
        // No `/ui` is mounted, so send the post-confirm landing to the
        // site rather than the module's default status page, which this
        // Worker does not serve.
        .module(
            Waitlist::new()
                .products(["supportgenius"])
                .status_redirect("https://supportgeni.us/"),
        )
        .module(Support::new())
        // Templates register on the harness builder — `Waitlist` itself
        // has no `.templates` method.
        .templates(cratefield_module_waitlist::default_templates())
        // The clone is what `Harness::build` validates `requires()`
        // against; the original below is what `serve` resolves ports
        // from. Same instance, so the two cannot disagree.
        .runtime(runtime.clone())
        .build()?;

    Ok((harness, runtime))
}

/// Resend when `RESEND_API_KEY` is present on the Worker `Env`, else a
/// keyless Resend adapter that reports `SendOutcome::NotConfigured`
/// without a network call.
///
/// Deliberately different from upstream's fallback, a `NoopMailer` that
/// reports `SendOutcome::Sent` without sending: reporting success for mail
/// that was never sent is precisely the silent-failure shape this venture
/// refuses. With no key configured, a join fails loudly and the site can
/// say so, instead of capturing an address whose confirmation never
/// arrives.
fn build_mailer(env: &Env) -> Arc<dyn Mailer> {
    let key = env
        .secret("RESEND_API_KEY")
        .ok()
        .map(|secret| secret.to_string())
        .filter(|key| !key.is_empty());
    Arc::new(Resend::new(
        Arc::new(FetchClient),
        Arc::new(WorkersClock),
        key,
        MAIL_FROM,
        None,
    ))
}

/// Turnstile when `TURNSTILE_SECRET` is present on the Worker `Env`, else
/// no `Captcha` port at all — an unbound adapter reports itself not
/// effectively configured, so mounting one without a secret would only
/// move the failure to first use.
///
/// Read from the binding rather than `Turnstile::from_env`, which reads
/// `std::env` and is therefore always empty on Workers.
///
/// `supportgeni.us` is the only expected hostname: the widget is solved on
/// the site, and the `www.` origin is listed in `cors_origins` for the
/// form post, not for the widget.
fn build_captcha(env: &Env) -> Option<Turnstile> {
    let secret = env
        .secret("TURNSTILE_SECRET")
        .ok()
        .map(|secret| secret.to_string())
        .filter(|secret| !secret.is_empty())?;
    Some(
        Turnstile::new(Arc::new(FetchClient), Arc::new(WorkersClock), secret)
            .expected_hostname("supportgeni.us"),
    )
}

/// Composes once per isolate, from the secrets actually set on this
/// deployment. The returned pair is the same harness/runtime pair
/// [`compose`] builds, so what was validated is what serves.
fn instance(env: &Env) -> &'static (Harness, Cloudflare) {
    static INSTANCE: OnceLock<(Harness, Cloudflare)> = OnceLock::new();
    INSTANCE.get_or_init(|| {
        let mailer = build_mailer(env);
        let captcha = build_captcha(env);
        compose(mailer, captcha).expect("supportgenius harness is valid")
    })
}

/// The harness with no secrets configured, for the venture-linked `fz`
/// bin (`migrations collect`, `doctor`): a keyless mailer answers
/// `NotConfigured` if anything tried to send, which `fz` never does.
///
/// # Panics
///
/// Panics if the composition is invalid, which would be a programming
/// error caught by the tests, not an operational condition.
pub fn harness() -> Harness {
    compose(build_mailer_no_secrets(), None)
        .expect("supportgenius harness is valid")
        .0
}

/// The no-secrets mailer [`harness`] composes with: the Resend adapter
/// with no key, which reports `SendOutcome::NotConfigured` rather than
/// pretending a send happened.
fn build_mailer_no_secrets() -> Arc<dyn Mailer> {
    Arc::new(Resend::new(
        Arc::new(FetchClient),
        Arc::new(WorkersClock),
        None,
        MAIL_FROM,
        None,
    ))
}

/// Worker fetch entry point.
///
/// # Errors
///
/// Propagates `worker::Error` from the harness router.
#[event(fetch)]
pub async fn fetch(req: Request, env: Env, ctx: Context) -> worker::Result<Response> {
    let (harness, runtime) = instance(&env);
    serve(harness, runtime, req, env, ctx).await
}

/// The other half of wrangler.toml's `[triggers]` cron (`23 4 * * *`
/// daily): fans the event out to every module's `scheduled` hook. For the
/// waitlist module that purges pending entries past the retention window
/// and prunes expired mail-cooldown claims.
#[event(scheduled)]
pub async fn scheduled(event: worker::ScheduledEvent, env: Env, ctx: worker::ScheduleContext) {
    let (harness, runtime) = instance(&env);
    serve_scheduled(harness, runtime, event, env, ctx).await;
}
