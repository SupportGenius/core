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
//! export REDIS_URL=redis://127.0.0.1:6379        # optional; without it
//!                                                # rate limiting fails open
//! LISTEN_ADDR=127.0.0.1:8080 ./supportgenius
//! curl -fsS http://127.0.0.1:8080/__health && curl -fsS http://127.0.0.1:8080/__ready
//! ```
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

use cratefield_adapter_resend::Resend;
use cratefield_adapter_sqlite::SqliteDatabase;
use cratefield_adapter_turnstile::Turnstile;
use cratefield_core::{
    Captcha, Clock, Config, Database, Harness, HttpClient, KeyValue, Mailer, RateLimiter, Venture,
};
use cratefield_runtime_native::{
    EnvConfig, Native, OutboundOptions, ReqwestClient, TokioClock, install_tracing, serve,
};
use supportgenius_composition as composition;

#[cfg(feature = "postgres")]
use cratefield_adapter_postgres::Postgres;

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
    if std::env::args().any(|arg| arg == "--check-ready") {
        check_ready().await;
        return;
    }
    install_tracing();
    if let Err(err) = run().await {
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
    } else {
        tracing::warn!("REDIS_URL unset: RateLimiter and KeyValue ports not configured");
    }

    let http: Arc<dyn HttpClient> = Arc::new(ReqwestClient::new());
    let clock: Arc<dyn Clock> = Arc::new(TokioClock);

    // The Mailer port is wired always — the waitlist module `requires()`
    // Db + Mailer + Signer, so the port must be present to boot. With no
    // `RESEND_API_KEY`, Resend answers `SendOutcome::NotConfigured` and
    // sends nothing: the same no-`NoopMailer` policy the Worker
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

    // Landing site for the `dev-fakes` TextModel stub, when one becomes
    // possible: published cratefield-core 0.4 (and 0.5) has no
    // `TextModel` port, and `crates/module-support` — the module that
    // would consume it — has not merged. When both land, a
    // `StubTextModel` goes here, selected by `SUPPORTGENIUS_DEV_FAKES`
    // alongside the stubs above and written by hand in `src/dev_fakes.rs`
    // (never `cratefield-testing` in a shipping binary, never a stub in
    // the modules or the composition crate).

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
    serve(harness, runtime).await?;
    Ok(())
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

/// The production mailer: Resend, with the key from the environment. No
/// key answers `NotConfigured` rather than pretending — see `run`.
fn real_mailer(
    config: EnvConfig,
    http: &Arc<dyn HttpClient>,
    clock: &Arc<dyn Clock>,
) -> Arc<dyn Mailer> {
    Arc::new(Resend::new(
        Arc::clone(http),
        Arc::clone(clock),
        config.get("RESEND_API_KEY"),
        config
            .get("MAIL_FROM")
            .unwrap_or_else(|| composition::MAIL_FROM.to_owned()),
        config.get("MAIL_REPLY_TO"),
    ))
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
