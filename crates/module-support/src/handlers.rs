//! HTTP handlers for `/v1/support` (paths here are relative to that
//! mount). Two credentials guard this module and never mix: the harness
//! admin token for tenant provisioning, and a tenant API key (the
//! `tenancy` crate's signed `sg_…` bearer) for everything a tenant does
//! to its own index.

use axum::extract::{Path, Query, RawQuery, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;

use cratefield_core::{
    Clock, Database, HttpClient, IdGen, Json, ModuleConfig, ModuleContext, Problem, ProblemDef,
    RateLimit, RateLimitFailure, Scope, Signer, TextModel, check_rate_limit, rate_limited,
    require_admin,
};

use crate::bm25;
use crate::chunk::{Chunker, tokenize};
use crate::connectors::{self, ConnectorConfig, Kind};
use crate::messages;
use crate::store::{self, ApiKeyRow, ChunkRow, ConnectorRow, STATUS_ACTIVE, SourceRow, TenantRow};
use crate::uploads;

/// The inline `text` ceiling for `POST /sources`, and one part's ceiling
/// for the chunked-upload routes (`uploads::PART_BYTES`). Not arbitrary:
/// `/v1/*` request bodies are already capped at 64 KiB
/// (`cratefield_core::MAX_BODY_BYTES`) and `Blob::signed_url` is GET-only,
/// so the only way a larger document arrives is as upload parts, one
/// request each. 48 KiB keeps a maximal document — or part — inside a
/// maximal request with room for the rest of the JSON.
pub(crate) const MAX_TEXT_BYTES: usize = 48 * 1024;

/// Tenant display name ceiling, in bytes of UTF-8. A name is prose for a
/// dashboard, not a document; anything past this is a mistake. The
/// upload routes borrow it for `filename` (a document's name is prose
/// for a search hit, not a path).
pub(crate) const MAX_NAME_BYTES: usize = 200;

/// `limit` handling for `GET /search`: default 10, hard range 1..=50.
const DEFAULT_LIMIT: u32 = 10;
const MAX_LIMIT: u32 = 50;

/// `limit` handling for `GET /sources`: default 50, hard range 1..=100.
const DEFAULT_SOURCES_LIMIT: u32 = 50;
const MAX_SOURCES_LIMIT: u32 = 100;

/// `external_id` ceiling, in bytes. It is stored per source row and
/// indexed (`idx_sg_sources_tenant_external`), so a caller can give a
/// URL, a slug or a natural key — but not an unbounded one.
const MAX_EXTERNAL_ID_BYTES: usize = 512;

/// Every way key authentication can fail — no header, a malformed key, a
/// valid signature from a revoked kid, a key naming a tenant that does
/// not exist, a tenant that is not active — answers with this one
/// indistinguishable 401. Which of these it is would be a handout to
/// anyone probing the API; the holder of a genuine key never needs the
/// distinction, because re-minting fixes all of them the same way.
const UNAUTHORIZED: ProblemDef = ProblemDef {
    slug: "unauthorized",
    status: StatusCode::UNAUTHORIZED,
    title: "Support API key unauthorized",
    description: "A key-guarded route was reached without a valid, unrevoked API key belonging \
                  to an active tenant.",
};

/// `409` for a body `external_id` another source of the same tenant
/// already holds (`PUT /sources/{id}`): the per-tenant unique index
/// would otherwise abort the write with a bare 500.
const EXTERNAL_ID_CONFLICT: ProblemDef = ProblemDef {
    slug: "source-external-id-conflict",
    status: StatusCode::CONFLICT,
    title: "external_id already in use",
    description: "Another source of this tenant already carries the external_id the request \
                  tried to set.",
};

pub(crate) struct ModuleState {
    pub ctx: Arc<ModuleContext>,
    /// The model `POST /messages` asks. `None` is a deployment that was
    /// not given one: the route answers `503 text-model-not-configured`
    /// rather than pretending to answer.
    pub text_model: Option<Arc<dyn TextModel>>,
}

pub(crate) fn router(
    ctx: Arc<ModuleContext>,
    text_model: Option<Arc<dyn TextModel>>,
) -> axum::Router {
    let state = Arc::new(ModuleState { ctx, text_model });
    axum::Router::new()
        .route("/admin/tenants", post(create_tenant))
        .route(
            "/admin/tenants/{tenant_id}/settings",
            put(messages::put_settings),
        )
        .route("/sources", post(ingest_source).get(list_sources))
        .route(
            "/sources/{source_id}",
            get(get_source).put(put_source).delete(delete_source),
        )
        .route("/uploads", post(uploads::create_upload))
        .route("/uploads/{upload_id}/parts/{n}", put(uploads::put_part))
        .route(
            "/uploads/{upload_id}/complete",
            post(uploads::complete_upload),
        )
        .route("/uploads/{upload_id}", get(uploads::get_upload))
        .route("/connectors", post(create_connector))
        .route("/search", get(search))
        .route("/messages", post(messages::post_message))
        .with_state(state)
}

/// A required port, absent. `requires()` names every port used here, so
/// this is a harness bug, not a request problem — but a `panic!` in a
/// Worker isolate costs more than a 500, so it is answered instead.
pub(crate) fn required_port<'a, T: ?Sized>(
    port: Option<&'a T>,
    name: &'a str,
) -> Result<&'a T, Problem> {
    port.ok_or_else(|| Problem::internal().with_detail(format!("required port {name} is missing")))
}

/// Verifies the `Authorization` bearer as a tenant API key and returns
/// the tenant id every downstream query must filter on. Revoked kids come
/// from module config (`SUPPORT_REVOKED_KIDS`, parsed by
/// [`tenancy::parse_revoked_kids`]); the tenant row must exist and be
/// active. All failure paths collapse into [`UNAUTHORIZED`].
pub(crate) async fn authenticate(
    ctx: &ModuleContext,
    headers: &HeaderMap,
) -> Result<String, Problem> {
    let unauthorized = || Problem::new(&UNAUTHORIZED);
    let Some(signer) = ctx.ports.signer.as_deref() else {
        return Err(unauthorized());
    };
    let Some(raw) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    else {
        return Err(unauthorized());
    };
    let Some(presented) = tenancy::bearer(raw) else {
        return Err(unauthorized());
    };
    let module = ModuleConfig::new(crate::MODULE_NAME, ctx.config.as_ref());
    let revoked = tenancy::parse_revoked_kids(
        &module
            .get_opt(tenancy::REVOKED_KIDS_KEY)
            .unwrap_or_default(),
    );
    let Ok(tenant_key) = tenancy::verify(signer, presented, &revoked) else {
        return Err(unauthorized());
    };
    let db = required_port(ctx.ports.db.as_deref(), "Db")?;
    match store::find_tenant(db, &tenant_key.tenant_id).await {
        Ok(Some(tenant)) if tenant.status == STATUS_ACTIVE => Ok(tenant.id),
        Ok(_) => Err(unauthorized()),
        // A database outage is an infrastructure failure, not evidence
        // about the key: it gets the 500 the `DbError` carries, not the
        // 401 above.
        Err(err) => Err(err.into()),
    }
}

/// Applies the `RateLimiter` port, one bucket per tenant. Keyed by tenant
/// id rather than IP: these routes are authenticated, so the budget that
/// matters is the tenant's, and an IP bucket would let one office full of
/// colleagues exhaust each other.
///
/// The limiter is a budget guard, not the cost story: since the corpus
/// statistics moved into `sg_terms`/`sg_tenant_stats`, a `/search` request
/// reads a fixed budget of rows no matter how large the tenant's corpus
/// grows — one stats row, at most [`store::MAX_QUERY_TERMS`] df rows, at
/// most that many postings fetches of [`store::MAX_POSTINGS_PER_TERM`]
/// rows each, and the result's chunk rows (the invariant is spelled out on
/// [`store::postings_for`]). `/messages` adds a paid model call on top.
/// `FailClosed` therefore governs a *transport failure of a limiter that
/// is present*: once one is composed, a flaky limiter denies rather than
/// waving the request (or the model call) through. It does **not**
/// manufacture a backstop from nothing — an *absent* limiter resolves to
/// `RateLimit::Allowed` (`check_rate_limit` short-circuits on `None`),
/// independent of `FailClosed`. So this guard only bites when the
/// composition actually mounts a `RateLimiter`: the Cloudflare Worker does,
/// unconditionally (`RATE_LIMITER` in wrangler.toml), and the native binary
/// does when `REDIS_URL` is set — which it requires in production.
///
/// `Some` is the 429 response the handler returns verbatim.
pub(crate) async fn guard_rate_limit(ctx: &ModuleContext, tenant_id: &str) -> Option<Response> {
    let keys = [format!("support:{tenant_id}")];
    if let RateLimit::Denied { decision } = check_rate_limit(
        ctx.ports.rate_limiter.as_ref(),
        &keys,
        RateLimitFailure::FailClosed,
    )
    .await
    {
        return Some(rate_limited(&decision));
    }
    None
}

/// What a source route needs once its preamble has run: the module
/// context, the authenticated tenant id and the Db port.
struct Authorized<'a> {
    ctx: &'a ModuleContext,
    tenant_id: String,
    db: &'a dyn Database,
}

/// What [`authorize`] decided: work in the returned scope, or send the
/// response the guards produced (the rendered `401`, the limiter's `429`).
/// An enum rather than a `Result` because the error side is a whole
/// response, and handlers return `Result<Response, Problem>` anyway.
enum Authed<'a> {
    Ready(Authorized<'a>),
    Done(Response),
}

/// The preamble every source route runs before touching its inputs — key
/// authentication, then the per-tenant rate limit — plus the Db port
/// nearly all of them need next.
async fn authorize<'a>(ctx: &'a ModuleContext, headers: &HeaderMap) -> Authed<'a> {
    let tenant_id = match authenticate(ctx, headers).await {
        Ok(tenant_id) => tenant_id,
        Err(problem) => return Authed::Done(problem.into_response()),
    };
    if let Some(rate_limited) = guard_rate_limit(ctx, &tenant_id).await {
        return Authed::Done(rate_limited);
    }
    match required_port(ctx.ports.db.as_deref(), "Db") {
        Ok(db) => Authed::Ready(Authorized { ctx, tenant_id, db }),
        Err(problem) => Authed::Done(problem.into_response()),
    }
}

#[derive(Deserialize)]
struct TenantBody {
    name: String,
}

/// `POST /admin/tenants` — provision a tenant and its first API key,
/// guarded by the harness admin token (`Authorization: Bearer
/// $ADMIN_TOKEN`; unset means 401, the same as a wrong token).
///
/// The response carries the minted key **exactly once**: the key is a
/// bearer credential over the `Signer` port and nothing about it — not
/// the key, not its MAC — is stored, so this response cannot be replayed
/// later. Lose it and the tenant mints a new one (or the operator re-runs
/// this endpoint against the same tenant with a future admin route).
async fn create_tenant(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, Problem> {
    // The admin guard runs before the body is parsed. A `Json<T>`
    // extractor would reject a malformed body with its own 4xx before
    // the handler ever ran, telling an unauthenticated caller the route
    // exists and how its request shape is wrong — the guard must answer
    // first.
    require_admin(&*state.ctx.config, &headers)?;
    let body: TenantBody = serde_json::from_slice(&body).map_err(|_| {
        Problem::validation_failed("body: expected a JSON object with a string \"name\"")
            .instance(&scope.request_id)
    })?;
    let ctx = &state.ctx;
    let name = body.name.trim();
    if name.is_empty() || name.len() > MAX_NAME_BYTES {
        return Err(Problem::validation_failed(format!(
            "name: required, 1..={MAX_NAME_BYTES} bytes"
        ))
        .instance(&scope.request_id));
    }

    let clock: &dyn Clock = required_port(ctx.ports.clock.as_deref(), "Clock")?;
    let id_gen: &dyn IdGen = required_port(ctx.ports.id_gen.as_deref(), "IdGen")?;
    let signer: &dyn Signer = required_port(ctx.ports.signer.as_deref(), "Signer")?;
    let db: &dyn Database = required_port(ctx.ports.db.as_deref(), "Db")?;

    let created_at = store::iso_now(clock);
    let tenant = TenantRow {
        id: id_gen.ulid(),
        name: name.to_owned(),
        status: STATUS_ACTIVE.to_owned(),
        created_at: created_at.clone(),
    };
    store::insert_tenant(db, &tenant).await?;

    let minted = tenancy::mint(signer, &tenant.id)
        .map_err(|_| Problem::internal().with_detail("api key minting failed"))?;
    store::insert_api_key(
        db,
        &ApiKeyRow {
            id: id_gen.ulid(),
            tenant_id: tenant.id.clone(),
            kid: minted.kid.clone(),
            label: store::FIRST_KEY_LABEL.to_owned(),
            created_at,
        },
    )
    .await?;

    Ok((
        StatusCode::CREATED,
        Json(json!({
            "tenant_id": tenant.id,
            "name": name,
            "api_key": minted.key,
            "kid": minted.kid,
        })),
    )
        .into_response())
}

/// One `POST /sources` / `PUT /sources/{id}` body: inline text, or a URL
/// to fetch, in both cases with an optional title and the caller's
/// optional `external_id`.
#[derive(Deserialize)]
#[serde(untagged)]
enum SourceIngest {
    Text {
        title: Option<String>,
        external_id: Option<String>,
        text: String,
    },
    Url {
        title: Option<String>,
        external_id: Option<String>,
        url: String,
    },
}

/// What [`parse_source`] hands the ingest and replace handlers: the
/// document text (fetched already, for the `{"url"}` form), its title,
/// where it came from and its external id. A URL source without an
/// explicit `external_id` defaults to the URL itself.
struct ParsedSource {
    title: Option<String>,
    text: String,
    url: Option<String>,
    external_id: Option<String>,
}

fn indexed_bytes(text: &str) -> i64 {
    i64::try_from(text.len()).unwrap_or(i64::MAX)
}

/// The byte cap binds only an explicitly supplied `external_id`. The
/// URL-derived default is exempt: a long URL was a valid identity long
/// before this module named one, and callers re-post the exact URL they
/// fetched.
fn checked_external_id(scope: &Scope, id: Option<String>) -> Result<Option<String>, Problem> {
    match id.map(|id| id.trim().to_owned()) {
        Some(id) if id.is_empty() => Err(Problem::validation_failed(
            "external_id: 1..=512 bytes when present",
        )
        .instance(&scope.request_id)),
        Some(id) if id.len() > MAX_EXTERNAL_ID_BYTES => Err(Problem::validation_failed(format!(
            "external_id: {MAX_EXTERNAL_ID_BYTES} bytes maximum, got {} bytes",
            id.len()
        ))
        .instance(&scope.request_id)),
        other => Ok(other),
    }
}

/// Parses and validates a `POST /sources` / `PUT /sources/{id}` body —
/// the two routes accept exactly the same shapes — and fetches URL
/// sources through the `HttpClient` port before returning.
async fn parse_source(
    scope: &Scope,
    ctx: &ModuleContext,
    body: Bytes,
) -> Result<ParsedSource, Problem> {
    let body: SourceIngest = serde_json::from_slice(&body).map_err(|_| {
        Problem::validation_failed(
            "body: expected a JSON object with a string \"text\" or a string \"url\"",
        )
        .instance(&scope.request_id)
    })?;

    let (title, external_id, text, url) = match body {
        SourceIngest::Text {
            title,
            external_id,
            text,
        } => {
            if text.len() > MAX_TEXT_BYTES {
                return Err(Problem::validation_failed(format!(
                    "text: {MAX_TEXT_BYTES} bytes maximum, got {} bytes. The /v1/* request \
                     body is capped at 64 KiB and there is no presigned upload path, so \
                     inline text cannot grow past this ceiling; use the {{\"url\"}} form \
                     instead.",
                    text.len()
                ))
                .instance(&scope.request_id));
            }
            (title, checked_external_id(scope, external_id)?, text, None)
        }
        SourceIngest::Url {
            title,
            external_id,
            url,
        } => {
            let http: &dyn HttpClient = ctx.ports.http.as_deref().ok_or_else(|| {
                Problem::not_ready(
                    "URL ingest needs the HttpClient port; this deployment did not \
                     configure one. Use the inline {\"text\"} form.",
                )
            })?;
            let fetched = fetch_text(http, &url).await?;
            let external_id =
                checked_external_id(scope, external_id)?.or_else(|| Some(url.clone()));
            (title, external_id, fetched, Some(url))
        }
    };

    Ok(ParsedSource {
        title,
        text,
        url,
        external_id,
    })
}

/// `POST /sources` — index one document: `{"title"?, "text"}` or
/// `{"title"?, "url"}`, each with an optional `external_id`. A body
/// `external_id` the tenant has already indexed **replaces that source in
/// place** — same id and code path as `PUT /sources/{id}`, answered `200`
/// instead of `201`; a fresh id creates a new source, `201`. Without an
/// `external_id` every request creates a new source.
async fn ingest_source(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, Problem> {
    // The guards run before the body is parsed: an extractor would reject
    // a malformed body before `authenticate` ever ran, and since every
    // key failure here shares one indistinguishable 401, that 4xx would
    // leak that auth was never reached.
    let auth = match authorize(&state.ctx, &headers).await {
        Authed::Ready(auth) => auth,
        Authed::Done(done) => return Ok(done),
    };
    let parsed = parse_source(&scope, auth.ctx, body).await?;
    let clock: &dyn Clock = required_port(auth.ctx.ports.clock.as_deref(), "Clock")?;
    let id_gen: &dyn IdGen = required_port(auth.ctx.ports.id_gen.as_deref(), "IdGen")?;

    let target = match parsed.external_id.as_deref() {
        Some(external_id) => {
            store::find_source_by_external_id(auth.db, &auth.tenant_id, external_id).await?
        }
        None => None,
    };
    if let Some(target) = target {
        let (source, chunk_count) = upsert_source(auth.db, clock, &target, parsed).await?;
        return Ok((
            StatusCode::OK,
            Json(json!({ "source_id": source.id, "chunks": chunk_count })),
        )
            .into_response());
    }

    let source_id = id_gen.ulid();
    let chunks = Chunker::default().split(&auth.tenant_id, &source_id, &parsed.text);
    let now = store::iso_now(clock);
    // The optional fields are cloned for the raced-insert retry below;
    // the text itself is never copied.
    let source = SourceRow {
        id: source_id,
        tenant_id: auth.tenant_id.clone(),
        title: clean_title(parsed.title.clone()),
        url: parsed.url.clone(),
        external_id: parsed.external_id.clone(),
        byte_len: indexed_bytes(&parsed.text),
        created_at: now.clone(),
        updated_at: now,
    };
    match store::insert_source_with_chunks(auth.db, &source, &chunks).await {
        Ok(()) => Ok((
            StatusCode::CREATED,
            Json(json!({ "source_id": source.id, "chunks": chunks.len() })),
        )
            .into_response()),
        // Two POSTs can race on the same fresh external_id: both lookups
        // miss, one insert wins the unique index and ours is rejected
        // whole. Replace the winner if it is there now; anything else is
        // a real failure.
        Err(err) => {
            let winner = match source.external_id.as_deref() {
                Some(external_id) => {
                    store::find_source_by_external_id(auth.db, &auth.tenant_id, external_id).await?
                }
                None => None,
            };
            if let Some(target) = winner {
                let (source, chunk_count) = upsert_source(auth.db, clock, &target, parsed).await?;
                return Ok((
                    StatusCode::OK,
                    Json(json!({ "source_id": source.id, "chunks": chunk_count })),
                )
                    .into_response());
            }
            Err(err.into())
        }
    }
}

/// One `POST /connectors` body. The kind decides which fields are
/// required; the rest are optional with the documented defaults.
#[derive(Deserialize)]
struct ConnectorBody {
    kind: String,
    url: Option<String>,
    owner: Option<String>,
    repo: Option<String>,
    path_glob: Option<String>,
    r#ref: Option<String>,
    credential_ref: Option<String>,
    max_pages: Option<i64>,
    max_bytes: Option<i64>,
    max_depth: Option<i64>,
}

/// Validates a connector body's kind-specific fields into the
/// [`ConnectorConfig`] stored with the row. Every problem is a 400 naming
/// the field; this is the shape the route documents, and the only place a
/// body field is interpreted.
fn connector_kind_config(
    body: &ConnectorBody,
    request_id: &str,
) -> Result<(Kind, ConnectorConfig), Problem> {
    let kind = Kind::parse(body.kind.trim()).ok_or_else(|| {
        Problem::validation_failed(format!(
            "kind: expected \"sitemap\", \"url_prefix\" or \"github\", got {:?}",
            body.kind
        ))
        .instance(request_id)
    })?;
    let config = match kind {
        Kind::Sitemap | Kind::UrlPrefix => {
            let url = body.url.clone().unwrap_or_default();
            let usable = url.parse::<http::Uri>().is_ok_and(|uri| {
                matches!(uri.scheme_str(), Some("http" | "https")) && uri.authority().is_some()
            });
            if !usable {
                return Err(Problem::validation_failed(
                    "url: required, an absolute http(s) URL — the sitemap's address, or the \
                     prefix's seed page",
                )
                .instance(request_id));
            }
            ConnectorConfig::Web { url }
        }
        Kind::Github => {
            let clean = |raw: Option<&str>| -> Option<String> {
                let trimmed = raw?.trim();
                (!trimmed.is_empty()
                    && !trimmed.contains('/')
                    && !trimmed.chars().any(char::is_whitespace))
                .then(|| trimmed.to_owned())
            };
            let Some(owner) = clean(body.owner.as_deref()) else {
                return Err(Problem::validation_failed(
                    "owner: required for a github connector, the repository's owner with no \
                     slashes or spaces",
                )
                .instance(request_id));
            };
            let Some(repo) = clean(body.repo.as_deref()) else {
                return Err(Problem::validation_failed(
                    "repo: required for a github connector, the repository's name with no \
                     slashes or spaces",
                )
                .instance(request_id));
            };
            let optional = |raw: Option<&str>| -> Option<String> {
                let trimmed = raw?.trim();
                (!trimmed.is_empty()).then(|| trimmed.to_owned())
            };
            // `ref` defaults to the repository's default branch when
            // absent; when present it must be one usable git ref — no
            // empty value, whitespace, `?`/`#` (they would splice a query
            // or fragment into the API URLs), `..` (git's own range
            // separator) or control characters.
            let bad_ref = || {
                Problem::validation_failed(
                    "ref: when present, a git ref — no empty value, whitespace, \"?\", \
                     \"#\", \"..\" or control characters",
                )
                .instance(request_id)
            };
            let r#ref = match body.r#ref.as_deref().map(str::trim) {
                None => None,
                Some("") => return Err(bad_ref()),
                Some(git_ref)
                    if git_ref.contains(['?', '#'])
                        || git_ref.contains("..")
                        || git_ref.chars().any(|c| c.is_whitespace() || c.is_control()) =>
                {
                    return Err(bad_ref());
                }
                Some(git_ref) => Some(git_ref.to_owned()),
            };
            ConnectorConfig::Github {
                owner,
                repo,
                path_glob: optional(body.path_glob.as_deref()),
                r#ref,
            }
        }
    };
    Ok((kind, config))
}

/// The ports `POST /connectors` runs on, in [`connector_ports`]' order.
type ConnectorPorts = (
    Arc<dyn HttpClient>,
    Arc<dyn Database>,
    Arc<dyn Clock>,
    Arc<dyn IdGen>,
);

/// Collects those ports, or says which one is missing. The `HttpClient`
/// check doubles as the route's not-ready answer and comes before any
/// validation output and before any write: a connector row without an
/// `HttpClient` is a crawl that can never start, and the caller should
/// hear that from the platform shape, not from a job that silently does
/// nothing.
fn connector_ports(ctx: &ModuleContext) -> Result<ConnectorPorts, Problem> {
    let http: Arc<dyn HttpClient> = ctx.ports.http.clone().ok_or_else(|| {
        Problem::not_ready(
            "connectors need the HttpClient port; this deployment did not configure one. \
             Use the {\"text\"} form of POST /sources instead.",
        )
    })?;
    let db: Arc<dyn Database> = ctx
        .ports
        .db
        .clone()
        .ok_or_else(|| Problem::internal().with_detail("required port Db is missing"))?;
    let clock: Arc<dyn Clock> = ctx
        .ports
        .clock
        .clone()
        .ok_or_else(|| Problem::internal().with_detail("required port Clock is missing"))?;
    let id_gen: Arc<dyn IdGen> = ctx
        .ports
        .id_gen
        .clone()
        .ok_or_else(|| Problem::internal().with_detail("required port IdGen is missing"))?;
    Ok((http, db, clock, id_gen))
}

/// The effective caps a connector row stores: absent fields take the
/// documented defaults, and nothing may exceed the hard maxima (or fall
/// below the minimum of 1 — a cap of zero would be a connector that
/// cannot run at all, which the caller should say with a deletion, not
/// a typo).
fn clamped_caps(body: &ConnectorBody) -> (i64, i64, i64) {
    (
        body.max_pages
            .unwrap_or(connectors::DEFAULT_MAX_PAGES)
            .clamp(1, connectors::MAX_MAX_PAGES),
        body.max_bytes
            .unwrap_or(connectors::DEFAULT_MAX_BYTES)
            .clamp(1, connectors::MAX_MAX_BYTES),
        body.max_depth
            .unwrap_or(connectors::DEFAULT_MAX_DEPTH)
            .clamp(1, connectors::MAX_MAX_DEPTH),
    )
}

/// `POST /connectors` — register a crawl root (issue #29): a sitemap, a
/// URL prefix, or a GitHub repository. The row and its seed fetch job
/// land in one atomic batch, so a connector that exists always has work
/// queued; the fetches then run on the module's `scheduled` hook (cron),
/// with the `Defer` port pulling the first sweep into this request where
/// the platform allows it.
///
/// Caps are clamped, not rejected — a caller asking for a million pages
/// gets the maximum, which keeps one tenant's typo from being a
/// deployment incident. The response is the created connector's id and
/// effective caps; the crawl itself is asynchronous from here.
async fn create_connector(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, Problem> {
    let ctx = &state.ctx;
    // Guards before body parsing, as everywhere above: a 4xx from an
    // extractor would tell an unauthenticated caller the route's shape.
    let tenant_id = authenticate(ctx, &headers).await?;
    if let Some(rate_limited) = guard_rate_limit(ctx, &tenant_id).await {
        return Ok(rate_limited);
    }
    let body: ConnectorBody = serde_json::from_slice(&body).map_err(|_| {
        Problem::validation_failed(
            "body: expected a JSON object with a \"kind\" of \"sitemap\", \"url_prefix\" \
             or \"github\"",
        )
        .instance(&scope.request_id)
    })?;

    let (http, db, clock, id_gen) = connector_ports(ctx)?;
    let (kind, config) = connector_kind_config(&body, &scope.request_id)?;
    // The credential is a *reference* to a Config key (the escalation
    // module's rule): the secret itself never appears in a request, a
    // row or a response.
    let credential_ref = body
        .credential_ref
        .as_deref()
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_owned);
    let (max_pages, max_bytes, max_depth) = clamped_caps(&body);

    let now = store::iso_now(clock.as_ref());
    let connector = ConnectorRow {
        id: id_gen.ulid(),
        tenant_id: tenant_id.clone(),
        kind: kind.as_str().to_owned(),
        config: serde_json::to_string(&config)
            .map_err(|_| Problem::internal().with_detail("connector config did not serialize"))?,
        credential_ref,
        max_pages,
        max_bytes,
        max_depth,
        created_at: now.clone(),
    };
    let runner = connectors::Runner::new(
        db.clone(),
        http,
        ctx.ports.config.clone(),
        clock,
        id_gen.clone(),
        Some(scope.defer.clone()),
    );
    let job = connectors::seed_job(&connector, &config);
    let payload = serde_json::to_string(&job)
        .map_err(|_| Problem::internal().with_detail("seed job did not serialize"))?;
    // The row and its first job: one batch, so the connector cannot
    // exist for even a moment without work queued.
    db.batch_atomic(&[
        store::insert_connector_stmt(&connector),
        store::enqueue_fetch_stmt(
            runner.outbox(),
            &id_gen.ulid(),
            &payload,
            &connector.id,
            &now,
        ),
    ])
    .await?;
    // An execution opportunity for the seed (and whatever it discovers),
    // durable either way: the scheduled re-sync is the backstop.
    runner.defer_sweep();

    Ok((
        StatusCode::CREATED,
        Json(json!({
            "connector_id": connector.id,
            "kind": kind.as_str(),
            "seed_url": config.seed_url(),
            "max_pages": connector.max_pages,
            "max_bytes": connector.max_bytes,
            "max_depth": connector.max_depth,
        })),
    )
        .into_response())
}

/// Title fallback: an absent or whitespace title becomes `Untitled`,
/// which is what a result list should say rather than an empty string.
pub(crate) fn clean_title(raw: Option<String>) -> String {
    match raw {
        Some(title) => {
            let trimmed = title.trim();
            if trimmed.is_empty() {
                "Untitled".to_owned()
            } else {
                trimmed.to_owned()
            }
        }
        None => "Untitled".to_owned(),
    }
}

/// Fetches `url` and reduces the response body to indexable text,
/// truncated to [`MAX_TEXT_BYTES`] — the same ceiling the inline form
/// enforces, so a source's indexed size does not depend on how it
/// arrived. Fetch failures surface as `503 not-ready`: from the caller's
/// side a missing or failing outbound client is a deployment condition,
/// not a bad request.
async fn fetch_text(http: &dyn HttpClient, url: &str) -> Result<String, Problem> {
    let request = http::Request::builder()
        .method(http::Method::GET)
        .uri(url)
        .body(Bytes::new())
        .map_err(|_| Problem::validation_failed("url: not a usable HTTP URL"))?;
    let response = http
        .send(request)
        .await
        .map_err(|err| Problem::not_ready(format!("url fetch failed: {err}")))?;
    let is_html = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|content_type| content_type.to_ascii_lowercase().contains("html"));
    let bytes = response.body();
    let truncated = &bytes[..bytes.len().min(MAX_TEXT_BYTES)];
    let text = String::from_utf8_lossy(truncated);
    Ok(if is_html {
        html_to_text(&text)
    } else {
        text.into_owned()
    })
}

/// HTML to text, deliberately crude. This path feeds the tokenizer, not a
/// reader: it drops `script`/`style` blocks so their contents do not
/// become searchable "words", cuts the remaining tags, decodes the small
/// set of entities that survive in prose, and collapses whitespace. It
/// makes no attempt at structure, semantics or completeness — a page it
/// mangles still indexes, just badly.
pub(crate) fn html_to_text(html: &str) -> String {
    let scripts_dropped = drop_block(html, "<script", "</script>");
    let blocks_dropped = drop_block(&scripts_dropped, "<style", "</style>");
    let without_tags = strip_tags(&blocks_dropped);
    let without_entities = decode_entities(&without_tags);
    without_entities
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Removes `<open … close>` regions; an unterminated block drops the rest
/// of the document, which for a tokenizer is the safer direction.
fn drop_block(html: &str, open: &str, close: &str) -> String {
    let lower = html.to_ascii_lowercase();
    let mut out = String::with_capacity(html.len());
    let mut cursor = 0;
    while let Some(offset) = lower[cursor..].find(open) {
        let start = cursor + offset;
        out.push_str(&html[cursor..start]);
        match lower[start..].find(close) {
            Some(end) => cursor = start + end + close.len(),
            None => return out,
        }
    }
    out.push_str(&html[cursor..]);
    out
}

fn strip_tags(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut depth = 0_usize;
    for ch in html.chars() {
        match ch {
            '<' => depth += 1,
            '>' => depth = depth.saturating_sub(1),
            c if depth == 0 => out.push(c),
            _ => {}
        }
    }
    out
}

/// The five entities a prose corpus actually contains; the rest are
/// dropped with their markup and were noise anyway. `pub(crate)` for the
/// connectors' sitemap parser, whose `<loc>` values are escaped the same
/// way.
pub(crate) fn decode_entities(text: &str) -> String {
    text.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
        .replace("&nbsp;", " ")
}

// ---------------------------------------------------------------------
// Source management: list, read, replace and delete one tenant's sources.
// Every route authenticates and rate-limits before anything else, and
// every query is scoped to the authenticated tenant — an id that belongs
// to another tenant does not exist here.
// ---------------------------------------------------------------------

#[derive(Deserialize, Default)]
struct ListQuery {
    limit: Option<u32>,
    after: Option<String>,
}

/// One source as the source-management routes show it.
fn source_json(item: &store::SourceSummary) -> Value {
    json!({
        "id": item.id,
        "title": item.title,
        "origin": if item.url.is_some() { "url" } else { "text" },
        "external_id": item.external_id,
        "bytes": item.byte_len,
        "chunk_count": item.chunk_count,
        "updated_at": item.updated_at,
    })
}

/// The shared replace tail of `POST /sources` (external-id leg and raced
/// insert) and `PUT /sources/{id}`: chunk the parsed text under the
/// target's own id — chunk ids are content addresses of
/// (tenant, source, text), so this is what keeps a surviving window's
/// row — write the replacement, and answer with the updated row and its
/// window count.
async fn upsert_source(
    db: &dyn Database,
    clock: &dyn Clock,
    target: &SourceRow,
    parsed: ParsedSource,
) -> Result<(SourceRow, usize), Problem> {
    let chunks = Chunker::default().split(&target.tenant_id, &target.id, &parsed.text);
    let source = SourceRow {
        id: target.id.clone(),
        tenant_id: target.tenant_id.clone(),
        title: clean_title(parsed.title),
        url: parsed.url,
        external_id: parsed.external_id,
        byte_len: indexed_bytes(&parsed.text),
        created_at: target.created_at.clone(),
        updated_at: store::iso_now(clock),
    };
    store::replace_source_chunks(db, &source, &chunks).await?;
    Ok((source, chunks.len()))
}

/// `GET /sources?limit=…&after=…` — the tenant's sources, keyset
/// paginated by id. `limit` (default 50, clamped to 1..=100) bounds the
/// page; `after` names the id to start strictly after, so a page boundary
/// is stable no matter what ingests land between two requests. The
/// response is `{"sources": […], "next": …}`, where `next` is the id to
/// pass as the next `after`, or `null` at the end of the list.
async fn list_sources(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let auth = match authorize(&state.ctx, &headers).await {
        Authed::Ready(auth) => auth,
        Authed::Done(done) => return Ok(done),
    };
    // The query string is parsed here — after the guards, not by an
    // extractor before them — so a malformed `limit` earns the 401/429
    // any other request would, and only an authenticated, under-budget
    // caller sees the 400.
    let bad_query = || {
        Problem::validation_failed("query: limit must be an integer, after a source id")
            .instance(&scope.request_id)
    };
    let query = match query.as_deref() {
        None | Some("") => ListQuery::default(),
        Some(raw) => {
            let uri: http::Uri = format!("https://support.local/?{raw}")
                .parse()
                .map_err(|_| bad_query())?;
            Query::<ListQuery>::try_from_uri(&uri)
                .map_err(|_| bad_query())?
                .0
        }
    };

    let limit = query
        .limit
        .unwrap_or(DEFAULT_SOURCES_LIMIT)
        .clamp(1, MAX_SOURCES_LIMIT);
    // `limit` is clamped to 1..=100, so this widening cannot fail; the
    // page math below wants `usize`.
    let page = usize::try_from(limit).unwrap_or(usize::MAX);
    // One row past the page end answers "is there a next page" without a
    // second query; it is dropped before responding.
    let mut items =
        store::list_sources(auth.db, &auth.tenant_id, limit + 1, query.after.as_deref()).await?;
    let next = if items.len() > page {
        items.truncate(page);
        items.last().map(|item| item.id.clone())
    } else {
        None
    };
    let sources: Vec<Value> = items.iter().map(source_json).collect();

    Ok(Json(json!({ "sources": sources, "next": next })).into_response())
}

/// `GET /sources/{source_id}` — one source, in the list's item shape. An
/// id that does not exist — or exists for another tenant — is the same
/// `404`.
async fn get_source(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    Path(source_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let auth = match authorize(&state.ctx, &headers).await {
        Authed::Ready(auth) => auth,
        Authed::Done(done) => return Ok(done),
    };
    let item = store::find_source_summary(auth.db, &auth.tenant_id, &source_id)
        .await?
        .ok_or_else(|| Problem::not_found().instance(&scope.request_id))?;

    Ok(Json(source_json(&item)).into_response())
}

/// `PUT /sources/{source_id}` — replace a source's content and metadata,
/// body shaped exactly like `POST /sources`. Windows are re-derived under
/// the same id, so unchanged text keeps its chunk rows. A missing or
/// foreign id is the same `404`; a body `external_id` another source of
/// the tenant already holds is `409`, before anything is written.
async fn put_source(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    Path(source_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, Problem> {
    let auth = match authorize(&state.ctx, &headers).await {
        Authed::Ready(auth) => auth,
        Authed::Done(done) => return Ok(done),
    };
    let parsed = parse_source(&scope, auth.ctx, body).await?;
    let clock: &dyn Clock = required_port(auth.ctx.ports.clock.as_deref(), "Clock")?;

    let target = store::find_source(auth.db, &auth.tenant_id, &source_id)
        .await?
        .ok_or_else(|| Problem::not_found().instance(&scope.request_id))?;
    // The unique index would abort the write batch if the body's
    // external_id belongs to a different source of this tenant; say 409
    // instead. Setting the source's own id back is not a conflict.
    if let Some(external_id) = parsed.external_id.as_deref()
        && let Some(holder) =
            store::find_source_by_external_id(auth.db, &auth.tenant_id, external_id).await?
        && holder.id != target.id
    {
        return Err(Problem::new(&EXTERNAL_ID_CONFLICT)
            .with_detail(format!(
                "external_id: already indexed as source {}",
                holder.id
            ))
            .instance(&scope.request_id));
    }

    let (source, chunk_count) = upsert_source(auth.db, clock, &target, parsed).await?;
    let summary = store::SourceSummary {
        id: source.id,
        title: source.title,
        url: source.url,
        external_id: source.external_id,
        byte_len: source.byte_len,
        updated_at: source.updated_at,
        chunk_count: i64::try_from(chunk_count).unwrap_or(i64::MAX),
    };
    Ok(Json(source_json(&summary)).into_response())
}

/// `DELETE /sources/{source_id}` — remove a source and its whole index in
/// one `batch_atomic`; `204` with no body. A missing or foreign id is the
/// same `404` and deletes nothing.
async fn delete_source(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    Path(source_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let auth = match authorize(&state.ctx, &headers).await {
        Authed::Ready(auth) => auth,
        Authed::Done(done) => return Ok(done),
    };
    if store::find_source(auth.db, &auth.tenant_id, &source_id)
        .await?
        .is_none()
    {
        return Err(Problem::not_found().instance(&scope.request_id));
    }
    store::delete_source(auth.db, &auth.tenant_id, &source_id).await?;

    Ok(StatusCode::NO_CONTENT.into_response())
}

#[derive(Deserialize)]
struct SearchQuery {
    q: Option<String>,
    limit: Option<u32>,
}

/// `GET /search?q=…&limit=…` — BM25 over the tenant's own index. An
/// empty or absent `q` is an empty result set, not an error: a search box
/// that has not been typed into yet is a normal state, and a 4xx would
/// turn it into console noise.
async fn search(
    State(state): State<Arc<ModuleState>>,
    Query(query): Query<SearchQuery>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let ctx = &state.ctx;
    let tenant_id = authenticate(ctx, &headers).await?;
    if let Some(rate_limited) = guard_rate_limit(ctx, &tenant_id).await {
        return Ok(rate_limited);
    }
    let db: &dyn Database = required_port(ctx.ports.db.as_deref(), "Db")?;

    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let hits = retrieve(
        db,
        &tenant_id,
        query.q.as_deref().unwrap_or(""),
        limit as usize,
    )
    .await?;
    let results: Vec<Value> = hits
        .iter()
        .map(|hit| {
            json!({
                "chunk_id": hit.chunk.id,
                "source_id": hit.chunk.source_id,
                "title": hit.chunk.title,
                "score": hit.score,
                "text": hit.chunk.body,
            })
        })
        .collect();

    Ok(Json(json!({ "results": results })).into_response())
}

/// One ranked hit from [`retrieve`]: the chunk and its BM25 score.
pub(crate) struct Retrieved {
    pub chunk: ChunkRow,
    pub score: f64,
}

/// The query's search terms: tokenised (the indexer's own tokenizer, CJK
/// and Thai bigrams included), de-duplicated with the first occurrence's
/// order kept (a repeated term must not double-count, and the
/// left-to-right order is what fixes the postings fetch order, so scores
/// are reproducible), and capped at [`store::MAX_QUERY_TERMS`] so a
/// keyword-stuffed query cannot buy more reads than the invariant allows.
fn query_terms(query: &str) -> Vec<String> {
    let mut terms: Vec<String> = Vec::new();
    for term in tokenize(query) {
        if !terms.contains(&term) {
            terms.push(term);
        }
        if terms.len() == store::MAX_QUERY_TERMS {
            break;
        }
    }
    terms
}

/// BM25 retrieval over one tenant's index: the top `k` chunks for `query`,
/// best first. The one retrieval path — `GET /search` shows its result,
/// `POST /messages` grounds the model in it — so what a tenant finds by
/// searching is exactly what an answer can cite.
///
/// Cost is flat in the corpus size: one `sg_tenant_stats` row, one
/// `sg_terms` row per distinct query term, then — only for terms that
/// survive the stopword rules — a bounded per-term top-k postings fetch
/// ([`store::MAX_POSTINGS_PER_TERM`]), and the result's chunk rows.
/// Ranking stays exact under that truncation because df and N come from
/// the persisted statistics, not from the fetched rows (see
/// [`bm25::rank`]).
///
/// A query that tokenises to nothing retrieves nothing, without touching
/// the database — and a query whose every term is dropped (df 0, or a
/// stopword by ratio) retrieves nothing without a postings fetch. A
/// ranked id whose chunk row is gone (a source deleted or replaced by a
/// concurrent request between the postings fetch and the row read) is
/// skipped rather than failing the request.
pub(crate) async fn retrieve(
    db: &dyn Database,
    tenant_id: &str,
    query: &str,
    k: usize,
) -> Result<Vec<Retrieved>, cratefield_core::DbError> {
    let terms = query_terms(query);
    if terms.is_empty() {
        return Ok(Vec::new());
    }

    // The statistics reads: N and avg length from the one tenant row, df
    // for the query's terms from their `sg_terms` rows. Both are primary
    // -key lookups; neither scans anything.
    let corpus = store::corpus_stats(db, tenant_id).await?;
    let dfs = store::term_dfs(db, tenant_id, &terms).await?;

    // Two reasons to drop a term before paying for its postings. df 0:
    // the term indexes no chunk here, so it has no rows and no idf. Too
    // common: past [`store::STOPWORD_DF_RATIO`] of the corpus a term is a
    // stopword — below [`store::STOPWORD_MIN_CHUNKS`] the ratio is
    // information-free (one chunk in two makes 0.5 of any small tenant),
    // so the rule waits for a corpus worth measuring, and idf already
    // discounts what the ratio would.
    // df and N as f64: counts, and a corpus near 2^53 chunks would lose
    // a precision nobody could observe in a 0.5 comparison.
    #[expect(clippy::cast_precision_loss)]
    let too_common = |df: u64| df as f64 / corpus.chunk_count as f64 > store::STOPWORD_DF_RATIO;
    let kept: Vec<String> = terms
        .into_iter()
        .filter(|term| match dfs.get(term) {
            Some(df) if *df > 0 => {
                corpus.chunk_count < store::STOPWORD_MIN_CHUNKS || !too_common(*df)
            }
            _ => false,
        })
        .collect();
    if kept.is_empty() {
        return Ok(Vec::new());
    }

    let mut postings = Vec::new();
    for term in &kept {
        postings
            .extend(store::postings_for(db, tenant_id, term, store::MAX_POSTINGS_PER_TERM).await?);
    }

    let mut ranked = bm25::rank(&kept, &postings, &dfs, &corpus, &bm25::Params::default());
    ranked.truncate(k);

    let top: Vec<String> = ranked
        .iter()
        .map(|scored| scored.chunk_id.clone())
        .collect();
    let mut by_id: HashMap<String, ChunkRow> = store::chunks_by_id(db, tenant_id, &top)
        .await?
        .into_iter()
        .map(|chunk| (chunk.id.clone(), chunk))
        .collect();

    Ok(ranked
        .into_iter()
        .filter_map(|scored| {
            let chunk = by_id.remove(&scored.chunk_id)?;
            Some(Retrieved {
                chunk,
                score: scored.score,
            })
        })
        .collect())
}

#[cfg(test)]
mod query_terms_tests {
    use super::*;

    #[test]
    fn duplicates_drop_keeping_first_order() {
        assert_eq!(
            query_terms("retry the retry login the retry"),
            vec!["retry", "the", "login"]
        );
    }

    #[test]
    fn the_cap_leaves_the_first_max_query_terms() {
        // MAX_QUERY_TERMS + 10 distinct tokens: the tail is cut, and what
        // survives is the head in order — the cap bounds reads, it does
        // not reorder or reselect.
        let stuffed = (0..store::MAX_QUERY_TERMS + 10)
            .map(|i| format!("term{i}"))
            .collect::<Vec<_>>()
            .join(" ");
        let expected = (0..store::MAX_QUERY_TERMS)
            .map(|i| format!("term{i}"))
            .collect::<Vec<_>>();
        assert_eq!(query_terms(&stuffed), expected);
    }

    #[test]
    fn terms_come_out_exactly_as_the_indexer_tokenises() {
        // The query goes through the indexer's own tokenizer: a query must
        // meet its documents in the same alphabet or the postings lookup
        // misses. Only de-duplication and the cap are added on top.
        for query in ["Retry! It's the RETRY...", "v2 outage", "!!! ... a"] {
            let mut expected: Vec<String> = Vec::new();
            for term in tokenize(query) {
                if !expected.contains(&term) {
                    expected.push(term);
                }
            }
            assert_eq!(query_terms(query), expected, "{query}");
        }
        assert_eq!(query_terms("v2 outage"), vec!["v2", "outage"]);
    }

    #[test]
    fn an_unspaced_run_is_capped_in_bigrams() {
        // A long CJK run tokenises into one bigram per character, so it is
        // exactly the query the cap exists for: the head survives, in
        // order, and never more than MAX_QUERY_TERMS of them.
        let run: String =
            "数据库连接超时重试失败请检查网络配置并联系管理员获取帮助文档说明书第一章第二节"
                .repeat(2);
        let bigrams = tokenize(&run);
        assert!(bigrams.len() > store::MAX_QUERY_TERMS, "{bigrams:?}");
        let terms = query_terms(&run);
        assert_eq!(terms.len(), store::MAX_QUERY_TERMS);
        let mut expected: Vec<String> = Vec::new();
        for term in bigrams {
            if !expected.contains(&term) {
                expected.push(term);
            }
        }
        expected.truncate(store::MAX_QUERY_TERMS);
        assert_eq!(terms, expected);
    }
}
