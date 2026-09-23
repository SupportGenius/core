//! HTTP handlers for `/v1/support` (paths here are relative to that
//! mount). Two credentials guard this module and never mix: the harness
//! admin token for tenant provisioning, and a tenant API key (the
//! `tenancy` crate's signed `sg_…` bearer) for everything a tenant does
//! to its own index.

use axum::extract::{Query, State};
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
    RateLimit, RateLimitFailure, Scope, Signer, check_rate_limit, rate_limited, require_admin,
};

use text_model::TextModel;

use crate::bm25;
use crate::chunk::{Chunker, tokenize};
use crate::messages;
use crate::store::{self, ApiKeyRow, ChunkRow, STATUS_ACTIVE, SourceRow, TenantRow};

/// The inline `text` ceiling for `POST /sources`. Not arbitrary: `/v1/*`
/// request bodies are already capped at 64 KiB
/// (`cratefield_core::MAX_BODY_BYTES`), and `Blob::signed_url` is
/// GET-only, so there is no presigned upload path a larger document could
/// arrive through. 48 KiB keeps a maximal document inside a maximal
/// request with room for the rest of the JSON.
pub(crate) const MAX_TEXT_BYTES: usize = 48 * 1024;

/// Tenant display name ceiling, in bytes of UTF-8. A name is prose for a
/// dashboard, not a document; anything past this is a mistake.
const MAX_NAME_BYTES: usize = 200;

/// `limit` handling for `GET /search`: default 10, hard range 1..=50.
const DEFAULT_LIMIT: u32 = 10;
const MAX_LIMIT: u32 = 50;

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
        .route("/sources", post(ingest_source))
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
/// The limiter is the only backstop on `/search` and `/messages`: `/search`
/// is a full-corpus postings fetch and BM25 rank per request (since
/// `store::postings_for` is deliberately unbounded — a `LIMIT` would
/// corrupt ranking; see its docs), and `/messages` adds a paid model call.
/// `FailClosed` therefore governs a *transport failure of a limiter that
/// is present*: once one is composed, a flaky limiter denies rather than
/// waving the corpus scan (or the model call) through. It does **not**
/// manufacture a backstop from nothing — an *absent* limiter resolves to
/// `RateLimit::Allowed` (`check_rate_limit` short-circuits on `None`),
/// independent of `FailClosed`. So this guard only bites when the
/// composition actually mounts a `RateLimiter`: the Cloudflare Worker does,
/// unconditionally (`RATE_LIMITER` in wrangler.toml), and the native binary
/// does when `REDIS_URL` is set — which it requires in production. The
/// limiter is the effective per-tenant ceiling; bounding `postings_for`
/// itself is a separate correctness problem and is not attempted here.
///
/// `Some` is the 429 response the handler returns verbatim.
pub(crate) async fn guard_rate_limit(ctx: &ModuleContext, tenant_id: &str) -> Option<Response> {
    let keys = [format!("support:{tenant_id}")];
    if let RateLimit::Denied { retry_after } = check_rate_limit(
        ctx.ports.rate_limiter.as_ref(),
        &keys,
        RateLimitFailure::FailClosed,
    )
    .await
    {
        return Some(rate_limited(retry_after));
    }
    None
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

/// One `POST /sources` body: inline text, or a URL to fetch.
#[derive(Deserialize)]
#[serde(untagged)]
enum SourceIngest {
    Text { title: Option<String>, text: String },
    Url { title: Option<String>, url: String },
}

/// `POST /sources` — index one document for the authenticated tenant.
/// The body is either `{"title": "...", "text": "..."}` (title optional)
/// or `{"url": "...", "title": "..."}`. Chunks, postings and the source
/// row are written in one atomic batch, so a search can never see a
/// half-indexed source.
async fn ingest_source(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, Problem> {
    let ctx = &state.ctx;
    // The auth guards run before the body is parsed, exactly as
    // `create_tenant` above: a `Json<T>` extractor would reject a
    // malformed body with its own 4xx before `authenticate` ever ran,
    // and since every key failure here shares one indistinguishable 401,
    // an extractor 4xx would leak that auth was never reached.
    let tenant_id = authenticate(ctx, &headers).await?;
    if let Some(rate_limited) = guard_rate_limit(ctx, &tenant_id).await {
        return Ok(rate_limited);
    }
    let body: SourceIngest = serde_json::from_slice(&body).map_err(|_| {
        Problem::validation_failed(
            "body: expected a JSON object with a string \"text\" or a string \"url\"",
        )
        .instance(&scope.request_id)
    })?;

    let clock: &dyn Clock = required_port(ctx.ports.clock.as_deref(), "Clock")?;
    let id_gen: &dyn IdGen = required_port(ctx.ports.id_gen.as_deref(), "IdGen")?;
    let db: &dyn Database = required_port(ctx.ports.db.as_deref(), "Db")?;

    let (title, text, url) = match body {
        SourceIngest::Text { title, text } => {
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
            (title, text, None)
        }
        SourceIngest::Url { title, url } => {
            let http: &dyn HttpClient = ctx.ports.http.as_deref().ok_or_else(|| {
                Problem::not_ready(
                    "URL ingest needs the HttpClient port; this deployment did not \
                     configure one. Use the inline {\"text\"} form.",
                )
            })?;
            let fetched = fetch_text(http, &url).await?;
            (title, fetched, Some(url))
        }
    };

    let source_id = id_gen.ulid();
    let chunks = Chunker::default().split(&tenant_id, &source_id, &text);
    let source = SourceRow {
        id: source_id.clone(),
        tenant_id: tenant_id.clone(),
        title: clean_title(title),
        url,
        byte_len: i64::try_from(text.len()).unwrap_or(i64::MAX),
        created_at: store::iso_now(clock),
    };
    store::insert_source_with_chunks(db, &source, &chunks).await?;

    Ok((
        StatusCode::CREATED,
        Json(json!({ "source_id": source_id, "chunks": chunks.len() })),
    )
        .into_response())
}

/// Title fallback: an absent or whitespace title becomes `Untitled`,
/// which is what a result list should say rather than an empty string.
fn clean_title(raw: Option<String>) -> String {
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
fn html_to_text(html: &str) -> String {
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
/// dropped with their markup and were noise anyway.
fn decode_entities(text: &str) -> String {
    text.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
        .replace("&nbsp;", " ")
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

/// BM25 retrieval over one tenant's index: the top `k` chunks for `query`,
/// best first. The one retrieval path — `GET /search` shows its result,
/// `POST /messages` grounds the model in it — so what a tenant finds by
/// searching is exactly what an answer can cite.
///
/// A query that tokenises to nothing retrieves nothing, without touching
/// the database. A ranked id whose chunk row is gone (it cannot be today:
/// chunks and postings are written in one batch and never deleted) is
/// skipped rather than failing the request.
pub(crate) async fn retrieve(
    db: &dyn Database,
    tenant_id: &str,
    query: &str,
    k: usize,
) -> Result<Vec<Retrieved>, cratefield_core::DbError> {
    let terms = tokenize(query);
    if terms.is_empty() {
        return Ok(Vec::new());
    }

    let corpus = store::corpus_stats(db, tenant_id).await?;
    let postings = store::postings_for(db, tenant_id, &terms).await?;
    let mut ranked = bm25::rank(&terms, &postings, &corpus, &bm25::Params::default());
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
