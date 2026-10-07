//! SupportGenius as a **static native binary** (issue #6): the same
//! module list as the Cloudflare Worker — one list, shared through
//! `supportgenius_composition` so the two link targets cannot drift —
//! served by `cratefield-runtime-native` on tokio, with SQLite behind
//! the `Database` port by default and Postgres behind the `postgres`
//! feature, and, when `REDIS_URL` is set, Redis behind `RateLimiter` and
//! `KeyValue`.
//!
//! ```sh
//! export HARNESS_SECRET=$(openssl rand -hex 32)
//! # DATABASE_URL unset -> SQLite at ./supportgenius.db; or:
//! export DATABASE_URL=sqlite:///var/lib/supportgenius/state.db   # or postgres://…
//! export REDIS_URL=redis://127.0.0.1:6379        # rate limiter + KV; without
//!                                                # it rate limiting fails open
//!                                                # in dev/staging, and the
//!                                                # binary refuses to boot when
//!                                                # ENV=production (issues #16/#17)
//! export BLOB_DIR=/var/lib/supportgenius/blobs   # chunked-upload part storage
//!                                                # (issue #30); without it the
//!                                                # upload routes answer
//!                                                # `503 not-ready`
//! export CRONS="23 4 * * *"                      # module scheduled hooks: the
//!                                                # upload GC, the connector
//!                                                # re-sync, the waitlist purge,
//!                                                # the escalation retry sweeps
//! LISTEN_ADDR=127.0.0.1:8080 ./supportgenius
//! curl -fsS http://127.0.0.1:8080/__health && curl -fsS http://127.0.0.1:8080/__ready
//! ```
//!
//! One more variable turns on grounded answers (`POST /v1/support/messages`,
//! issue #22): `ANTHROPIC_API_KEY` mounts the `TextModel` port — one
//! Anthropic adapter per tier, ids overridable with
//! `SUPPORTGENIUS_MODEL_FAST` / `SUPPORTGENIUS_MODEL_STRONG`. Without it the
//! route answers `503 text-model-not-configured` and everything else works
//! (the `dev-fakes` build mounts a labelled stub instead — see
//! `dev_fakes.rs`).
//!
//! `CRONS` is comma-separated five-field cron expressions (UTC), the
//! environment counterpart of a Worker's `[triggers] crons`. Without it,
//! connector fetches still run when a connector is created (through
//! `Defer`); they just never re-check their sources.
//!
//! `--check-ready` runs the container health check (GET `/__ready`
//! against the configured listen address, exit 0/1) — distroless ships
//! no curl, so the binary checks itself.
//!
//! Single tenant: core resolves every host to the implicit `"default"`
//! tenant when no `TenantRouting` port is present, which *is* the
//! single-tenant mode. The `SUPPORTGENIUS_*` variables below override
//! the venture's compiled public identity (domain, URL, CORS), not a
//! tenant registry.

#[cfg(feature = "dev-fakes")]
mod dev_fakes;

use std::sync::Arc;

use cratefield_adapter_anthropic::Anthropic;
use cratefield_adapter_owlpost::Owlpost;
use cratefield_adapter_resend::Resend;
use cratefield_adapter_sqlite::SqliteDatabase;
use cratefield_adapter_turnstile::Turnstile;
use cratefield_core::{
    Captcha, Clock, Config, Database, Harness, HttpClient, KeyValue, Mailer, RateLimiter,
    RoutingTextModel, TextModel, Venture, VentureEnv,
};
use cratefield_runtime_native::{
    EnvConfig, Native, OutboundOptions, ReqwestClient, TokioClock, install_tracing, serve,
};
use supportgenius_composition as composition;
use supportgenius_composition::built_with::{self, Kind, StackEntry, Status};

#[cfg(feature = "postgres")]
use cratefield_adapter_postgres::Postgres;

/// The model id the fast tier calls when `SUPPORTGENIUS_MODEL_FAST` is
/// unset, and the strong tier's equivalent — the same ids, secrets and
/// variables the Worker uses (`ventures/supportgenius` `src/lib.rs`).
/// Written twice on purpose: the binary does not depend on the venture
/// crate (and the venture crate is wasm-only), so the two constant pairs
/// are pinned to each other by this comment and the README's tier table.
const DEFAULT_MODEL_FAST: &str = "claude-haiku-4-5";
const DEFAULT_MODEL_STRONG: &str = "claude-sonnet-5";

/// The database this process booted with — kept concretely typed so
/// migrations can run through the adapter's own runner after the
/// harness (which needs the `Arc<dyn Database>` first) is built.
enum BootDb {
    #[cfg(feature = "postgres")]
    Postgres(Arc<Postgres>),
    Sqlite(Arc<SqliteDatabase>),
}

impl BootDb {
    fn port(&self) -> Arc<dyn Database> {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(db) => Arc::clone(db) as Arc<dyn Database>,
            Self::Sqlite(db) => Arc::clone(db) as Arc<dyn Database>,
        }
    }
}

#[tokio::main]
async fn main() {
    // Scanned for equality, not read off `argv[1]`, exactly like
    // `--check-ready` below: no argument-parsing framework ships with this
    // binary, and the two flags must answer before any config, database or
    // tracing setup so `supportgenius about` works in a bare shell.
    if std::env::args().any(|arg| arg == "--check-ready") {
        check_ready().await;
        return;
    }
    if std::env::args().any(|arg| arg == "about" || arg == "--about") {
        print!("{}", about_text(built_with::stack()));
        return;
    }
    install_tracing();
    let booted = match refuse_local_kms_outside_development(EnvConfig) {
        Ok(()) => run().await,
        Err(err) => Err(err),
    };
    if let Err(err) = booted {
        tracing::error!(error = %err, "supportgenius failed");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = EnvConfig;

    let db = open_database(config).await?;

    let mut runtime = Native::new().db_arc(db.port());

    // The Redis degradation, logged once at boot — never per request.
    // This is the self-hosted contract (issue #6): without Redis the
    // `RateLimiter` and `KeyValue` ports simply stay unmounted, and core
    // resolves a `None` limiter to `RateLimit::Allowed` ("composition
    // chose to run without one"). Rate limiting therefore degrades to
    // allow-all, not to downtime — which is only sound because every
    // module call site must pick `RateLimitFailure::FailOpen`, the rule
    // `supportgenius-composition`'s `no_module_requires_a_limiter_or_
    // key_value` test enforces on `requires()`.
    if let Some(redis) = cratefield_runtime_native::redis_from_env(&config).await? {
        let rate_limiter: Arc<dyn RateLimiter> = redis.rate_limiter;
        let kv: Arc<dyn KeyValue> = redis.kv;
        runtime = runtime.rate_limiter_arc(rate_limiter).kv_arc(kv);
        tracing::info!("redis rate limiter and key-value ports configured");
    } else if is_production(config) {
        // Production must not silently fail rate limiting open: without a
        // limiter the public mail path (issue #16) and the support
        // search/sources routes (issue #17) have no volume ceiling, and
        // the core `production_readiness` gate does not cover the limiter
        // the way it covers captcha/signer. A self-host that declares
        // `ENV=production` has opted into production posture, so require
        // Redis there; a dev/staging box keeps the fail-open degradation
        // below and runs with zero configuration.
        return Err(
            "ENV=production but REDIS_URL is unset: refusing to boot without a \
             RateLimiter on the public mail and support endpoints. Set REDIS_URL (Redis \
             backs the RateLimiter and KeyValue ports), or run with ENV=development or \
             ENV=staging to accept fail-open rate limiting."
                .into(),
        );
    } else {
        tracing::warn!(
            "REDIS_URL unset: RateLimiter and KeyValue ports not configured (fail-open; \
             ENV=production would require Redis here)"
        );
    }

    // The Blob port backs the chunked-upload routes (issue #30): a
    // directory store under `BLOB_DIR`. Without it those routes answer
    // `503 not-ready` and documents arrive only through the 48 KiB
    // inline form — a loud degradation, unlike the rate limiter's quiet
    // fail-open, so it is a warning rather than a boot requirement.
    if let Some(dir) = config.get("BLOB_DIR").filter(|dir| !dir.is_empty()) {
        runtime = runtime.blob_arc(cratefield_runtime_native::DirBlob::arc(&dir));
        tracing::info!(blob_dir = %dir, "blob port configured (directory store)");
    } else {
        tracing::warn!(
            "BLOB_DIR unset: chunked-upload routes answer 503 not-ready (documents arrive \
             only through the 48 KiB inline form)"
        );
    }

    let http: Arc<dyn HttpClient> = Arc::new(ReqwestClient::new());
    let clock: Arc<dyn Clock> = Arc::new(TokioClock);

    // The Mailer port is wired always — the waitlist module `requires()`
    // Db + Mailer + Signer, so the port must be present to boot. With
    // neither `OWLPOST_API_KEY` (which takes precedence) nor
    // `RESEND_API_KEY` set the adapter answers `SendOutcome::NotConfigured`
    // and sends nothing: the same no-`NoopMailer` policy the Worker
    // documents, reporting success for mail that was never sent is the
    // silent-failure shape this venture refuses.
    let dev_fakes = config.get("SUPPORTGENIUS_DEV_FAKES").is_some();
    let mailer: Arc<dyn Mailer> = if dev_fakes {
        #[cfg(feature = "dev-fakes")]
        {
            tracing::warn!(
                "SUPPORTGENIUS_DEV_FAKES: dev fakes active — StubMailer \
                 records mail instead of sending; never serve production \
                 traffic from this process"
            );
            Arc::new(dev_fakes::StubMailer)
        }
        #[cfg(not(feature = "dev-fakes"))]
        {
            tracing::warn!(
                "SUPPORTGENIUS_DEV_FAKES set, but this build was compiled \
                 without the `dev-fakes` feature: wiring the real adapters"
            );
            real_mailer(config, &http, &clock)
        }
    } else {
        real_mailer(config, &http, &clock)
    };
    runtime = runtime.mailer_arc(mailer);

    // The Captcha port is mounted when a Turnstile secret is configured
    // (`Turnstile::from_env` reads `std::env`, which is the native
    // config); without one, no port — the module decides per request how
    // to treat that, per the venture env it reads from `ENV`.
    let captcha: Option<Arc<dyn Captcha>> = if dev_fakes {
        #[cfg(feature = "dev-fakes")]
        {
            Some(Arc::new(dev_fakes::StubCaptcha))
        }
        #[cfg(not(feature = "dev-fakes"))]
        {
            None
        }
    } else {
        Turnstile::from_env(Arc::clone(&http), Arc::clone(&clock))
            .map(|turnstile| Arc::new(turnstile) as Arc<dyn Captcha>)
    };
    if let Some(captcha) = captcha {
        runtime = runtime.captcha_arc(captcha);
    }

    // The TextModel port (issue #22), wired like the Mailer one above:
    // `build_text_model` carries the policy — a real key mounts the
    // adapters, a dev-fakes build falls back to its labelled stub. With
    // neither, the composition's `UnconfiguredTextModel` stands in: the
    // escalation module *requires* the port, so the harness would not
    // build without one, and it answers `NotConfigured`, so
    // `POST /v1/support/messages` degrades exactly as with no port (`503
    // text-model-not-configured`) instead of the boot failing outright.
    //
    // Tracker: the composition's unconfigured port, for the same reason —
    // escalation requires it and no tracker adapter is wired into this
    // binary yet. Deliberately not `cratefield-testing`'s fake: a shipping
    // binary must not depend on the testing crate, and a fake that
    // answered *something* would hide that no provider is wired.
    runtime = match build_text_model(config, dev_fakes, &http, &clock) {
        Some(text_model) => runtime.text_model_arc(text_model),
        None => runtime.text_model(composition::UnconfiguredTextModel),
    };
    runtime = runtime.tracker_arc(Arc::new(composition::UnconfiguredTracker));

    // Single tenant, seeded at boot from env: the compiled identity is
    // the default, and these variables exist for operators who front the
    // binary with a different hostname. Core resolves every host to the
    // implicit `"default"` tenant when no `TenantRouting` port is
    // present — that *is* the single-tenant mode — so this is about the
    // venture's public identity, not a tenant registry.
    let venture = venture_from_env(config);

    let harness = Arc::new(
        composition::modules(Harness::builder().venture(venture))
            // The clone is what `Harness::build` validates `requires()`
            // against; the original below is what `serve` resolves ports
            // from. Same instance, so the two cannot disagree.
            .runtime(runtime.clone())
            .build()?,
    );

    apply_migrations(&harness, &db).await?;

    check_crons_override(config)?;

    // The native counterpart of wrangler.toml's `[triggers] crons`.
    // `serve` reads `CRONS` from the environment and starts one task per
    // expression, but this venture's two ticks are compiled in
    // (`composition::CRONS`, pinned to the wrangler copy), and the binary
    // must not require an operator to retype them. The environment is the
    // only channel `serve` offers, and this process cannot write it (the
    // workspace forbids `unsafe`, so `set_var` is out), so instead the
    // default schedule is spawned here, from the same consts, through the
    // runtime's own public scheduler — exactly the fan-out `serve` would
    // have run. `CRONS` set is an operator override, not an addition:
    // `serve` then owns the schedule and this branch is skipped, so the
    // two paths never both fire.
    if config.get("CRONS").is_none() {
        cratefield_runtime_native::spawn_cron_scheduler(
            &harness,
            &runtime.ports(),
            &composition::cron_expressions(),
        )?;
        tracing::info!(
            crons = ?composition::CRONS,
            "CRONS unset: running the composition's default schedule"
        );
    }

    serve(harness, runtime).await?;
    Ok(())
}

/// Refuses an operator `CRONS` override that drops an expression the
/// composition gates a module on ([`composition::GATED_CRONS`]).
///
/// `serve` reads `CRONS` and, when set, spawns one task per expression
/// *instead of* the compiled default the boot spawns otherwise, so an
/// override that omits a gated expression would silently switch that
/// module's scheduled work off — today the daily waitlist retention purge,
/// which would simply never run. Catch it here, where the error can name
/// the missing expression, rather than as a task that answers 200 and does
/// nothing.
fn check_crons_override(config: EnvConfig) -> Result<(), Box<dyn std::error::Error>> {
    let Some(raw) = config.get("CRONS") else {
        return Ok(());
    };
    // Split the way `runtime-native`'s own `cron_expressions` does, so the
    // schedule this checks is the schedule `serve` will run.
    let schedule: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(str::to_owned)
        .collect();
    let missing: Vec<String> = composition::missing_gated_crons(&schedule)
        .into_iter()
        .map(|expr| format!("{expr:?}"))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    Err(format!(
        "CRONS omits {}, which the composition gates scheduled work on (the waitlist \
         retention purge would never run). Add every gated expression, or unset CRONS \
         to run the compiled default schedule {:?}.",
        missing.join(", "),
        composition::CRONS,
    )
    .into())
}

/// `DATABASE_URL`: `postgres://`/`postgresql://` opens a Postgres pool
/// (only with the `postgres` feature; the clear error says so otherwise),
/// `sqlite://<path>` or `sqlite::memory:` opens SQLite. **Unset defaults
/// to SQLite** at `./supportgenius.db` — a self-hostable static binary
/// should run with zero configuration, and the chosen path is logged so
/// the operator knows where the state landed.
///
/// `EnvConfig` is a zero-sized reader, so it passes by value; the
/// function stays `async` for the Postgres path, whose connect awaits —
/// without the feature there is no `await` and the lint says so.
#[cfg_attr(not(feature = "postgres"), allow(clippy::unused_async))]
async fn open_database(config: EnvConfig) -> Result<BootDb, Box<dyn std::error::Error>> {
    let Some(url) = config.get("DATABASE_URL") else {
        tracing::info!(
            path = "supportgenius.db",
            "DATABASE_URL unset: defaulting to SQLite in the working directory"
        );
        return Ok(BootDb::Sqlite(Arc::new(SqliteDatabase::open(
            "supportgenius.db",
        )?)));
    };
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        #[cfg(feature = "postgres")]
        {
            Ok(BootDb::Postgres(Arc::new(Postgres::connect(&url).await?)))
        }
        #[cfg(not(feature = "postgres"))]
        {
            // Name the scheme, not the URL: `DATABASE_URL` carries
            // credentials, and the error is a log line.
            let scheme = url.split("://").next().unwrap_or_default().to_owned();
            Err(format!(
                "DATABASE_URL names `{scheme}` but this binary was built \
                 without the `postgres` feature: rebuild with \
                 `cargo build --release --features postgres`"
            )
            .into())
        }
    } else if let Some(path) = url
        .strip_prefix("sqlite://")
        .map(str::to_owned)
        .or_else(|| (url == "sqlite::memory:").then(|| ":memory:".to_owned()))
    {
        Ok(BootDb::Sqlite(Arc::new(SqliteDatabase::open(&path)?)))
    } else {
        Err(format!(
            "DATABASE_URL must start with postgres://, postgresql:// or \
             sqlite:// (got a URL whose scheme is {:?})",
            url.split("://").next().unwrap_or("<none>")
        )
        .into())
    }
}

/// Whether this deployment declares itself production (`ENV=production`),
/// matching `cratefield-core`'s own parse (unset/blank is development).
/// Production requires a real `RateLimiter`; development and staging accept
/// the fail-open degradation so a self-host runs with zero configuration.
/// The escalation module's development file KMS outside an explicit
/// `ENV=development` (issue #23): its destination routes would already
/// refuse to store credentials (no KMS, `503`), but a self-host that
/// pointed `ESCALATION_KMS_KEY_FILE` at a key with `ENV` unset or
/// production-like meant something else, so the binary refuses to boot
/// and names the setting instead of serving degraded.
fn refuse_local_kms_outside_development(
    config: EnvConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    match composition::local_kms_refusal(&config) {
        Some(refusal) => Err(refusal.into()),
        None => Ok(()),
    }
}

fn is_production(config: EnvConfig) -> bool {
    config.get("ENV").as_deref().and_then(VentureEnv::parse) == Some(VentureEnv::Production)
}

/// The compiled venture identity with `SUPPORTGENIUS_*` env applied on
/// top: `SUPPORTGENIUS_DOMAIN`, `SUPPORTGENIUS_PUBLIC_URL`,
/// `SUPPORTGENIUS_CORS_ORIGINS` (comma-separated). Unset variables leave
/// the compiled values standing. `Venture` has no `.domain()` builder, so
/// a domain override rebuilds the venture and carries the compiled
/// URL/CORS across as the defaults the overrides then replace.
fn venture_from_env(config: EnvConfig) -> Venture {
    let base = composition::venture();
    Venture::new(
        composition::NAME,
        config
            .get("SUPPORTGENIUS_DOMAIN")
            .unwrap_or_else(|| base.domain.clone()),
    )
    .public_url(
        config
            .get("SUPPORTGENIUS_PUBLIC_URL")
            .unwrap_or_else(|| base.public_url.clone()),
    )
    .cors_origins(match config.get("SUPPORTGENIUS_CORS_ORIGINS") {
        Some(raw) => raw
            .split(',')
            .map(str::trim)
            .filter(|origin| !origin.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>(),
        None => base.cors_origins.clone(),
    })
}

/// The production mailer, in precedence order: **Owlpost** when
/// `OWLPOST_API_KEY` is set, else **Resend** when `RESEND_API_KEY` is, else
/// a keyless Resend that answers `NotConfigured` rather than pretending —
/// see `run`. `OWLPOST_BASE_URL` points Owlpost at a self-hosted or proxied
/// instance when set (unset uses `https://api.owlpost.to`); `MAIL_FROM` and
/// `MAIL_REPLY_TO` apply to whichever adapter is chosen.
fn real_mailer(
    config: EnvConfig,
    http: &Arc<dyn HttpClient>,
    clock: &Arc<dyn Clock>,
) -> Arc<dyn Mailer> {
    let non_empty = |key: &str| {
        config
            .get(key)
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
    };
    let from = config
        .get("MAIL_FROM")
        .unwrap_or_else(|| composition::MAIL_FROM.to_owned());
    let reply_to = config.get("MAIL_REPLY_TO");
    if let Some(key) = non_empty("OWLPOST_API_KEY") {
        let adapter = Owlpost::new(
            Arc::clone(http),
            Arc::clone(clock),
            Some(key),
            from,
            reply_to,
        );
        return Arc::new(match non_empty("OWLPOST_BASE_URL") {
            Some(base_url) => adapter.with_base_url(base_url),
            None => adapter,
        });
    }
    Arc::new(Resend::new(
        Arc::clone(http),
        Arc::clone(clock),
        config.get("RESEND_API_KEY"),
        from,
        reply_to,
    ))
}

/// The `TextModel` port (issue #22), wired like the mailer above: an
/// `ANTHROPIC_API_KEY` mounts one Anthropic adapter per tier behind a
/// `RoutingTextModel` — the same adapters and the same variable names as
/// the Worker. **A real key wins over the dev stub**: the stub exists so
/// a developer can exercise the grounded-answer flow without
/// credentials, and a credential in the environment is the stronger
/// claim on the port — answering from the real provider beats answering
/// from a fake, and the build that carries the key should not have to
/// unset `SUPPORTGENIUS_DEV_FAKES` to get honest answers.
///
/// `None` — no port — is the honest degradation the support module
/// documents: `POST /v1/support/messages` answers
/// `503 text-model-not-configured` while every other route works.
fn build_text_model(
    config: EnvConfig,
    dev_fakes: bool,
    http: &Arc<dyn HttpClient>,
    clock: &Arc<dyn Clock>,
) -> Option<Arc<dyn TextModel>> {
    let api_key = config
        .get("ANTHROPIC_API_KEY")
        .map(|key| key.trim().to_owned())
        .filter(|key| !key.is_empty());
    match api_key {
        Some(key) => {
            let tier = |name: &str, default: &str| {
                config
                    .get(name)
                    .map(|model| model.trim().to_owned())
                    .filter(|model| !model.is_empty())
                    .unwrap_or_else(|| default.to_owned())
            };
            let fast = tier("SUPPORTGENIUS_MODEL_FAST", DEFAULT_MODEL_FAST);
            let strong = tier("SUPPORTGENIUS_MODEL_STRONG", DEFAULT_MODEL_STRONG);
            tracing::info!(%fast, %strong, "anthropic text model configured on both tiers");
            Some(Arc::new(
                RoutingTextModel::new()
                    .fast(Arc::new(Anthropic::new(
                        Arc::clone(http),
                        Arc::clone(clock),
                        Some(key.clone()),
                        fast,
                    )))
                    .strong(Arc::new(Anthropic::new(
                        Arc::clone(http),
                        Arc::clone(clock),
                        Some(key),
                        strong,
                    ))),
            ))
        }
        // No key, dev fakes asked for: the stub answers, loudly labelled.
        None if dev_fakes => {
            #[cfg(feature = "dev-fakes")]
            {
                tracing::warn!(
                    "SUPPORTGENIUS_DEV_FAKES: StubTextModel active — answers are \
                     canned and cite whatever was retrieved; never serve \
                     production traffic from this process"
                );
                Some(Arc::new(dev_fakes::StubTextModel))
            }
            #[cfg(not(feature = "dev-fakes"))]
            {
                tracing::warn!(
                    "SUPPORTGENIUS_DEV_FAKES set, but this build was compiled \
                     without the `dev-fakes` feature: no text model will be mounted"
                );
                None
            }
        }
        None => {
            tracing::warn!(
                "ANTHROPIC_API_KEY unset: TextModel port not mounted, \
                 POST /v1/support/messages will answer 503 text-model-not-configured"
            );
            None
        }
    }
}

/// Applies every module's migrations on boot, idempotently, through the
/// adapter's own runner — the native counterpart of
/// `wrangler d1 migrations apply`. Opt out with `FZ_APPLY_MIGRATIONS=0`
/// when your deploy pipeline applies them explicitly.
///
/// Async for the Postgres path (its runner awaits); the SQLite runner is
/// synchronous, so without the feature there is no `await` to find.
#[cfg_attr(not(feature = "postgres"), allow(clippy::unused_async))]
async fn apply_migrations(
    harness: &Harness,
    db: &BootDb,
) -> Result<(), Box<dyn std::error::Error>> {
    let skip = EnvConfig
        .get("FZ_APPLY_MIGRATIONS")
        .is_some_and(|raw| matches!(raw.as_str(), "0" | "false" | "no" | "off"));
    if skip {
        tracing::info!("FZ_APPLY_MIGRATIONS=0: skipping migrations on boot");
        return Ok(());
    }
    match db {
        #[cfg(feature = "postgres")]
        BootDb::Postgres(db) => db.apply_harness_migrations(harness).await?,
        BootDb::Sqlite(db) => {
            for module in harness.modules() {
                db.apply_migrations(module.name(), module.migrations().sqlite)?;
            }
        }
    }
    tracing::info!("module migrations applied");
    Ok(())
}

/// The container health check: `GET /__ready` on the configured listen
/// address, exit 0 on 200, exit 1 otherwise. Wildcard binds
/// (`0.0.0.0`, `::`) are self-connected over loopback.
async fn check_ready() {
    let raw = EnvConfig
        .get("LISTEN_ADDR")
        .unwrap_or_else(|| "127.0.0.1:8080".to_owned());
    let target = match raw.parse::<std::net::SocketAddr>() {
        Ok(addr) => loopback(addr),
        Err(_) => "127.0.0.1:8080".parse().expect("loopback parses"),
    };

    let request = http::Request::builder()
        .uri(format!("http://{target}/__ready"))
        .body(bytes::Bytes::new())
        .expect("static request builds");
    // The probed destination IS this process on loopback, so the probe
    // opts into the hardened client's `allow_loopback` — left off, the
    // outbound policy refuses 127.0.0.1 before the request is ever sent.
    let outcome = ReqwestClient::with_options(OutboundOptions {
        allow_loopback: true,
        ..OutboundOptions::default()
    })
    .send(request)
    .await;
    let ready = matches!(outcome, Ok(response) if response.status() == 200);
    if !ready {
        eprintln!("not ready: {target}/__ready did not answer 200");
    }
    std::process::exit(i32::from(!ready));
}

fn loopback(addr: std::net::SocketAddr) -> std::net::SocketAddr {
    let ip = match addr.ip() {
        std::net::IpAddr::V4(ip) if ip.is_unspecified() => {
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        }
        std::net::IpAddr::V6(ip) if ip.is_unspecified() => {
            std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)
        }
        ip => ip,
    };
    std::net::SocketAddr::new(ip, addr.port())
}

/// The venture's "built with" list (issue #66), as plain text on stdout.
///
/// Every entry is printed with the role it plays, a link to the product
/// and its status in words, so nothing that is only planned can be read
/// as live. The entries themselves come from the vendored Factory Zero
/// registry entry (FZ-008) compiled into
/// `supportgenius_composition::built_with`: **nothing is fetched at
/// runtime**, so the answer is the same offline, in CI and in a distroless
/// container, and it cannot go stale mid-request.
///
/// `entries` is the parameter rather than a call to `built_with::stack()`
/// inside the function so the rendering is testable on its own; the
/// command passes the registry's own order (live first, then planned),
/// which this function preserves — it never re-sorts.
fn about_text(entries: &[StackEntry]) -> String {
    use std::fmt::Write as _;

    // Column widths are the widest value in the list, so the columns line
    // up whatever the registry holds. Padding is plain `format_args!`
    // formatting on purpose: no table crate for a fixed list of a dozen
    // rows.
    let role_width = entries.iter().map(|e| e.phrase.len()).max().unwrap_or(0);
    let name_width = entries.iter().map(|e| e.name.len()).max().unwrap_or(0);
    // Notes are indented under the name column, two spaces past the role,
    // and wrapped so the whole block stays inside an 80-column terminal.
    let note_indent = " ".repeat(2 + role_width + 2);
    let note_width = 80_usize.saturating_sub(note_indent.len()).max(20);

    let mut out = String::with_capacity(1024);
    let _ = writeln!(out, "{} — built with", built_with::VENTURE_NAME);
    let _ = writeln!(out);
    for entry in entries {
        // The chip is the whole answer to "which of these process my
        // data": it marks the third-party rows here, so the subprocessors
        // section below can be a link rather than a second copy of them.
        let chip = if entry.kind == Kind::ThirdParty {
            "  · subprocessor"
        } else {
            ""
        };
        let _ = writeln!(
            out,
            "  {:<role$}  {:<name$}  {:<7}  {url}{chip}",
            entry.phrase,
            entry.name,
            status_word(entry.status),
            url = entry.url,
            role = role_width,
            name = name_width,
        );
        let note = entry.note.trim();
        if !note.is_empty() {
            for line in wrap(note, note_width) {
                let _ = writeln!(out, "{note_indent}{line}");
            }
        }
    }

    let _ = writeln!(out);
    // Every entry is already a row above, third parties included, so this
    // section points at the published list rather than reprinting those
    // rows — reprinting them put Cloudflare and Polar on the screen twice
    // each, which read as two products rather than one used two ways.
    let _ = writeln!(out, "Subprocessors: {}", built_with::SUBPROCESSORS_URL);
    let _ = writeln!(
        out,
        "\nListed in the Factory Zero registry: {}",
        built_with::SOURCE
    );
    out
}

/// Greedy word wrap to `width`, on character count — a note is prose, and
/// an unbroken registry URL inside one is better left long than hyphenated.
/// Returns at least one line for a non-empty input.
fn wrap(text: &str, width: usize) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut start = 0;
    while start < text.len() {
        let rest = &text[start..];
        let end = match rest.char_indices().nth(width) {
            // Break at the last whitespace inside the window. With none
            // there — a word longer than the window, usually a URL — run
            // to the end of that word instead of cutting it: a split URL
            // is a broken link, and an over-wide line beats a lost
            // character.
            Some((cut, _)) => rest[..cut].rfind(char::is_whitespace).unwrap_or_else(|| {
                rest[cut..]
                    .find(char::is_whitespace)
                    .map_or(rest.len(), |offset| cut + offset)
            }),
            None => rest.len(),
        };
        let (line, next) = if end == 0 {
            (rest, rest.len())
        } else {
            (&rest[..end], end)
        };
        lines.push(line.trim_end());
        start += next + usize::from(next < rest.len());
    }
    lines.retain(|line| !line.is_empty());
    lines
}

/// The status as the issue words it: `live` or `planned`. Matched here
/// rather than through a `Display` impl so this file depends on nothing
/// but the enum's own variants, and so a planned entry can never be
/// printed as anything but `planned`.
fn status_word(status: Status) -> &'static str {
    match status {
        Status::Live => "live",
        Status::Planned => "planned",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// No name is printed twice. The table already carries every entry,
    /// third parties included, so the subprocessors section must add the
    /// link and nothing else — a second copy of those rows put Cloudflare
    /// and Polar on the screen twice each.
    #[test]
    fn no_entry_name_is_printed_twice() {
        let text = about_text(built_with::stack());
        // Notes name sibling products ("via the Cratefield Payments port"),
        // so counting the raw name over the whole output would count prose
        // rather than rows. Take the notes back out: what is left is the
        // table and the links, where a name may appear exactly once.
        let notes = built_with::stack()
            .iter()
            .map(|entry| entry.note)
            .collect::<Vec<_>>()
            .join(" ");

        for entry in built_with::stack() {
            let outside_notes =
                text.matches(entry.name).count() - notes.matches(entry.name).count();
            assert_eq!(
                outside_notes, 1,
                "{} is printed {} times outside the notes",
                entry.name, outside_notes
            );
        }
    }

    /// The command's own promise: every product named, its role and link
    /// present, and Cloudflare live while Cratefield is still planned —
    /// the two statuses must not be swapped, and the subprocessors list
    /// must still be offered.
    #[test]
    fn about_lists_every_entry_with_its_own_status() {
        let text = about_text(built_with::stack());

        for entry in built_with::stack() {
            assert!(text.contains(entry.name), "missing {}", entry.name);
            assert!(text.contains(entry.phrase), "missing {}", entry.phrase);
            assert!(text.contains(entry.url), "missing {}", entry.url);
        }
        assert!(text.contains(built_with::SUBPROCESSORS_URL));
        assert!(text.contains(built_with::SOURCE));

        let cloudflare = built_with::stack()
            .iter()
            .find(|e| e.name == "Cloudflare")
            .expect("Cloudflare is in the registry");
        assert_eq!(status_word(cloudflare.status), "live");
        let cratefield = built_with::stack()
            .iter()
            .find(|e| e.name == "Cratefield")
            .expect("Cratefield is in the registry");
        assert_eq!(status_word(cratefield.status), "planned");
    }

    /// Wrapping breaks on whitespace and never loses or duplicates a
    /// word: the notes are the only prose on the command's output, and a
    /// dropped word there would be a quiet lie about what a product does.
    #[test]
    fn wrapping_keeps_every_word_and_respects_the_width() {
        let text = "The ticketing and routing core is being written on the Cratefield harness.";
        let lines = wrap(text, 40);
        assert!(lines.len() > 1, "the note should not fit on one line");
        for line in &lines {
            assert!(line.len() <= 40, "{line:?} is too wide");
        }
        assert_eq!(
            lines.join(" ").split_whitespace().collect::<Vec<_>>(),
            text.split_whitespace().collect::<Vec<_>>()
        );
        // A word longer than the width is kept whole rather than cut.
        assert_eq!(wrap("short", 40), vec!["short"]);
        assert_eq!(
            wrap("https://example.com/a/very/long/path", 10),
            vec!["https://example.com/a/very/long/path"]
        );
    }

    /// The renderer is a function of its argument: entries handed in are
    /// what comes out, in the order given. A registry row with no note
    /// prints no dangling indented line.
    #[test]
    fn about_text_renders_the_entries_it_is_given() {
        let entries = [
            StackEntry {
                id: "example-live",
                name: "Example Live",
                kind: Kind::ThirdParty,
                url: "https://example.com/live",
                role: "hosting",
                phrase: "Hosted on",
                status: Status::Live,
                note: "Serves the site.",
            },
            StackEntry {
                id: "example-planned",
                name: "Example Planned",
                kind: Kind::FactoryZero,
                url: "https://example.com/planned",
                role: "framework",
                phrase: "Built with",
                status: Status::Planned,
                note: "",
            },
        ];

        let text = about_text(&entries);
        let row = |name: &str| {
            text.lines()
                .find(|line| line.contains(name))
                .unwrap_or_else(|| panic!("no row for {name}"))
                .to_owned()
        };
        // The row carries the phrase, the name, the status in words and
        // the link; the status word is what the columns are padded to, so
        // match on it rather than on the exact run of spaces.
        let live = row("Example Live");
        assert!(live.starts_with("  Hosted on"), "{live:?}");
        assert!(live.contains("live"), "{live:?}");
        assert!(live.contains("https://example.com/live"), "{live:?}");
        let planned = row("Example Planned");
        assert!(planned.starts_with("  Built with"), "{planned:?}");
        assert!(planned.contains("planned"), "{planned:?}");
        assert!(
            planned.contains("https://example.com/planned"),
            "{planned:?}"
        );
        // A planned entry must never carry the word live, and vice versa.
        assert!(!planned.contains("live"), "{planned:?}");
        assert!(!live.contains("planned"), "{live:?}");
        assert!(text.contains("Serves the site."));
        // The role slug is the registry's, the phrase is what a reader
        // is shown; printing the slug would leak the wire vocabulary.
        assert!(!text.contains("hosting"), "the slug is not the column");
        // Order preserved: live first because that is the order given.
        assert!(
            text.find("Example Live").unwrap() < text.find("Example Planned").unwrap(),
            "entries must not be re-sorted"
        );
    }
}
