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
//! Workers. Absence is not "off in production": `ENV = "production"`
//! (wrangler.toml `[vars]`) makes the harness production-readiness gate
//! answer `503 not-production-ready` to every `/v1/*` request until the
//! Captcha port is effectively configured (`cratefield-core`
//! `production_readiness`), and the waitlist join itself fails closed on a
//! missing port in production (`verify_human_form`). So Turnstile is
//! effectively required in production; in development/staging joins skip
//! human verification, which is why the join must not stand on captcha
//! alone (see **Rate limit**).
//!
//! **Rate limit.** A `RateLimiter` is mounted **unconditionally** (issue
//! #16 / #17): the public `POST /v1/waitlist` mail send and the
//! authenticated `/v1/support/{search,sources}` full-corpus fetch must
//! have a volume ceiling that does not depend on any optional secret. It
//! is backed by the Workers Rate Limiting binding `RATE_LIMITER` declared
//! in wrangler.toml; the waitlist path keys on IP and normalized email and
//! the support path on tenant id, all through the one shared port.
//!
//! **Text model.** When the `ANTHROPIC_API_KEY` secret is set, one
//! `Anthropic` adapter per tier — fast and strong, ids from the
//! `SUPPORTGENIUS_MODEL_FAST` / `SUPPORTGENIUS_MODEL_STRONG` vars — stands
//! behind one `RoutingTextModel` on the `TextModel` port (issue #22);
//! `POST /v1/support/messages` asks it for grounded answers. Without the
//! key no port is mounted at all and the messages route answers
//! `503 text-model-not-configured` while everything else works — the same
//! keyless-adapter policy as the mailer, minus the adapter: an unmounted
//! port is the honest report, and `/__ready` (decorated by this file's
//! `fetch`) says `text_model: "missing"` until the secret lands.
//!
//! **One runtime, twice used.** The `Cloudflare` runtime is built exactly
//! once per composition and then handed to both `Harness::builder` and
//! `serve`/`serve_scheduled`; see `compose`.

use std::sync::{Arc, OnceLock};

use cratefield_adapter_anthropic::Anthropic;
use cratefield_adapter_resend::Resend;
use cratefield_adapter_turnstile::Turnstile;
use cratefield_core::{ConfigError, Harness, Mailer, RoutingTextModel, TextModel};
use cratefield_runtime_cloudflare::{
    Cloudflare, FetchClient, WorkersClock, serve, serve_scheduled,
};
use serde_json::Value;
use supportgenius_composition::MAIL_FROM;
use worker::{Context, Env, Request, Response, event};

/// The model id the fast tier calls when `SUPPORTGENIUS_MODEL_FAST` is
/// not set. Mirrored in `bin/supportgenius` (`main.rs`) — the binary does
/// not depend on this crate, so the two constants are pinned to each
/// other by their comments; the README's tier table documents both.
pub(crate) const DEFAULT_MODEL_FAST: &str = "claude-haiku-4-5";

/// The model id the strong tier calls when `SUPPORTGENIUS_MODEL_STRONG`
/// is not set. Mirrored in `bin/supportgenius` (`main.rs`); see
/// [`DEFAULT_MODEL_FAST`].
pub(crate) const DEFAULT_MODEL_STRONG: &str = "claude-sonnet-5";

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
    text_model: Option<Arc<dyn TextModel>>,
) -> Result<(Harness, Cloudflare), ConfigError> {
    // The `RateLimiter` is mounted unconditionally (issue #16 / #17): the
    // public mail path and the support search/sources routes must have a
    // volume ceiling regardless of which optional secrets an operator set.
    // Backed by the `RATE_LIMITER` Workers Rate Limiting binding in
    // wrangler.toml; if that binding is missing from a deployment the
    // runtime logs once and leaves the port unmounted (fail-open), so the
    // binding is part of the deploy, not an option.
    let mut runtime = Cloudflare::new()
        .db("DB")
        .mailer_arc(mailer)
        .rate_limiter("RATE_LIMITER");
    if let Some(captcha) = captcha {
        runtime = runtime.captcha(captcha);
    }
    // The `TextModel` port only when a key chose an adapter (issue #22):
    // mounting nothing is the deployment's honest state, the same way the
    // captcha above is skipped when `TURNSTILE_SECRET` is absent.
    if let Some(text_model) = text_model {
        runtime = runtime.text_model_arc(text_model);
    }

    // The environment is the deployment's to declare, through `ENV` in
    // wrangler.toml (set to production there). Deliberately not hardcoded
    // here — or in the composition crate this function now shares with
    // the static binary — with `.env(VentureEnv::Production)`:
    // `HarnessBuilder::build` takes no config, so it cannot see the
    // operator's recorded acceptance, and a hardcoded production env
    // would make the composition refuse to build at all — a panic at
    // boot instead of a serving Worker that says loudly what it is
    // missing.
    let harness = supportgenius_composition::modules(
        Harness::builder().venture(supportgenius_composition::venture()),
    )
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

/// One `Anthropic` adapter per tier behind a [`RoutingTextModel`], when
/// `ANTHROPIC_API_KEY` is present on the Worker `Env`; `None` — no port
/// at all — when it is absent. Read from the binding rather than
/// `Anthropic::from_env`, which reads `std::env` and is therefore always
/// empty on Workers (the same reason `build_captcha` avoids
/// `Turnstile::from_env`).
///
/// A keyless `Anthropic` would answer `TextModelError::NotConfigured` —
/// the exact degradation the module already serves without a port — so
/// mounting the adapter without a key would only move the decision one
/// hop; the port stays unmounted and `/__ready` reports `missing`.
///
/// The tier ids come from the `SUPPORTGENIUS_MODEL_FAST` /
/// `SUPPORTGENIUS_MODEL_STRONG` vars, defaulting to
/// [`DEFAULT_MODEL_FAST`] / [`DEFAULT_MODEL_STRONG`]. Both adapters share
/// the one key: Anthropic is the only vendor with an adapter in the pin
/// block, so the two tiers are one vendor (see the README's caveat about
/// what that does to the escalation judge).
fn build_text_model(env: &Env) -> Option<Arc<dyn TextModel>> {
    let key = env
        .secret("ANTHROPIC_API_KEY")
        .ok()
        .map(|secret| secret.to_string().trim().to_owned())
        .filter(|key| !key.is_empty())?;
    let var = |name: &str, default: &str| {
        env.var(name)
            .ok()
            .map(|var| var.to_string().trim().to_owned())
            .filter(|model| !model.is_empty())
            .unwrap_or_else(|| default.to_owned())
    };
    Some(Arc::new(
        RoutingTextModel::new()
            .fast(Arc::new(Anthropic::new(
                Arc::new(FetchClient),
                Arc::new(WorkersClock),
                Some(key.clone()),
                var("SUPPORTGENIUS_MODEL_FAST", DEFAULT_MODEL_FAST),
            )))
            .strong(Arc::new(Anthropic::new(
                Arc::new(FetchClient),
                Arc::new(WorkersClock),
                Some(key),
                var("SUPPORTGENIUS_MODEL_STRONG", DEFAULT_MODEL_STRONG),
            ))),
    ))
}

/// Composes once per isolate, from the secrets actually set on this
/// deployment. The returned triple is the harness/runtime pair
/// [`compose`] builds — so what was validated is what serves — plus
/// whether a text model was mounted, which `fetch` needs to decorate
/// `/__ready` (the harness's own probe cannot see the port).
fn instance(env: &Env) -> &'static (Harness, Cloudflare, bool) {
    static INSTANCE: OnceLock<(Harness, Cloudflare, bool)> = OnceLock::new();
    INSTANCE.get_or_init(|| {
        let mailer = build_mailer(env);
        let captcha = build_captcha(env);
        let text_model = build_text_model(env);
        let text_model_mounted = text_model.is_some();
        let (harness, runtime) =
            compose(mailer, captcha, text_model).expect("supportgenius harness is valid");
        (harness, runtime, text_model_mounted)
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
    compose(build_mailer_no_secrets(), None, None)
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
/// The one interposition this venture makes on the harness router: the
/// harness's `/__ready` is a DB-only probe that cannot see the ports the
/// venture mounts on top (`cratefield-core` `harness.rs`
/// `ready_handler`), so for that one path the response body is rebuilt
/// with the `text_model` field added. The status code stays the
/// harness's — a missing model is a degradation, not unreadiness, which
/// is exactly how the module serves it (`503 text-model-not-configured`
/// from `POST /v1/support/messages`).
///
/// # Errors
///
/// Propagates `worker::Error` from the harness router.
#[event(fetch)]
pub async fn fetch(req: Request, env: Env, ctx: Context) -> worker::Result<Response> {
    let (harness, runtime, text_model_mounted) = instance(&env);
    let is_ready_probe = req.path() == "/__ready";
    let response = serve(harness, runtime, req, env, ctx).await;
    if is_ready_probe {
        report_text_model(response, *text_model_mounted).await
    } else {
        response
    }
}

/// Adds `"text_model": "configured" | "missing"` to a 200 `/__ready`
/// body. Any other status — the probe's `503` when the database did not
/// answer — is passed through untouched: readiness is the harness's
/// verdict to give, and the field is this deployment's annotation on the
/// healthy answer, not a second opinion about it.
///
/// An annotation must never be the reason a healthy deployment reads as
/// unready, so the two ways the body can surprise us do not fail the
/// probe. A body that no longer parses as a JSON object (a harness
/// change) is passed through as-is, and a body that cannot be read at all
/// yields the original status and headers with an empty body — there is
/// no body to annotate. Every rebuilt response re-applies the status and
/// headers the harness set, so the `application/json` content type its
/// `Json` responder chose stays correct for the body rebuilt here.
async fn report_text_model(
    response: worker::Result<Response>,
    mounted: bool,
) -> worker::Result<Response> {
    let mut response = response?;
    let status = response.status_code();
    if status != 200 {
        return Ok(response);
    }
    // Read the body once. Everything past this point either rebuilds the
    // response or must, because reading a streamed body consumes it — the
    // original `response` no longer carries it.
    let headers = response.headers().clone();
    let Ok(body) = response.text().await else {
        // The body could not be read, so there is nothing to annotate or
        // pass through: answer with the status and headers the harness
        // set and an empty body rather than turn a healthy probe into a
        // failure.
        return Response::from_bytes(Vec::new())
            .map(|rebuilt| rebuilt.with_status(status).with_headers(headers));
    };
    let Ok(Value::Object(mut ready)) = serde_json::from_str::<Value>(&body) else {
        // Not JSON, or not a JSON object (a harness change): pass the
        // probe's own body through rather than fail it — an annotation
        // must never be the reason a healthy deployment reads as unready.
        // Rebuilt from the text just read, with the status and headers
        // the harness set.
        return Response::from_bytes(body.into_bytes())
            .map(|rebuilt| rebuilt.with_status(status).with_headers(headers));
    };
    ready.insert(
        "text_model".to_owned(),
        Value::from(if mounted { "configured" } else { "missing" }),
    );
    Response::from_json(&Value::Object(ready))
        .map(|rebuilt| rebuilt.with_status(status).with_headers(headers))
}

/// The other half of wrangler.toml's `[triggers]` cron (`23 4 * * *`
/// daily): fans the event out to every module's `scheduled` hook. For the
/// waitlist module that purges pending entries past the retention window
/// and prunes expired mail-cooldown claims.
#[event(scheduled)]
pub async fn scheduled(event: worker::ScheduledEvent, env: Env, ctx: worker::ScheduleContext) {
    let (harness, runtime, _) = instance(&env);
    serve_scheduled(harness, runtime, event, env, ctx).await;
}
