//! HTTP handlers for `/v1/support` (paths here are relative to that
//! mount). Two credentials guard this module and never mix: the harness
//! admin token for tenant provisioning, and a tenant API key (the
//! `tenancy` crate's signed `sg_…` bearer) for everything a tenant does
//! to its own index.

use axum::extract::{Path, Query, RawQuery, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use bytes::Bytes;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use cratefield_core::{
    Action, Audience, Clock, Database, HttpClient, IdGen, Json, ModuleConfig, ModuleContext,
    Outcome, Problem, ProblemDef, RateLimit, RateLimitFailure, RateLimiter, RoutePolicy, Scope,
    Signer, Surface, SystemClock, TextModel, check_rate_limit, rate_limited, require_admin,
};

use crate::analytics;
use crate::openapi;

use crate::bm25;
use crate::chunk::{Chunker, tokenize};
use crate::connectors::{self, ConnectorConfig, Kind};
use crate::handoff::HandoffSink;
use crate::human;
use crate::messages;
use crate::store::{self, ApiKeyRow, ChunkRow, ConnectorRow, STATUS_ACTIVE, SourceRow, TenantRow};
use crate::uploads;
use crate::widget;

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

/// `limit` handling for `GET /search`: default 10, hard range 1..=50. The
/// MCP `search_sources` tool borrows both, so the two cannot diverge.
pub(crate) const DEFAULT_LIMIT: u32 = 10;
pub(crate) const MAX_LIMIT: u32 = 50;

/// `limit` handling for `GET /sources`: default 50, hard range 1..=100.
const DEFAULT_SOURCES_LIMIT: u32 = 50;
const MAX_SOURCES_LIMIT: u32 = 100;

/// `external_id` ceiling, in bytes. It is stored per source row and
/// indexed (`idx_sg_sources_tenant_external`), so a caller can give a
/// URL, a slug or a natural key — but not an unbounded one.
const MAX_EXTERNAL_ID_BYTES: usize = 512;

/// Every way key authentication can fail — no header, a malformed key, a
/// valid signature from a revoked kid, a key naming a tenant that does
/// not exist, a tenant that is not active, a key whose own id has no (or
/// another tenant's) `sg_api_keys` row — answers with this one
/// indistinguishable 401. Which of these it is would be a handout to
/// anyone probing the API; the holder of a genuine key never needs the
/// distinction, because re-minting fixes all of them the same way.
pub(crate) const UNAUTHORIZED: ProblemDef = ProblemDef {
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

/// `409` for `DELETE /keys/{kid}` on the tenant's only remaining key:
/// deleting it would leave the tenant unable to authenticate at all, so
/// the delete is refused. Mint the replacement first, then delete the old
/// key — the last key is replaceable, never removable outright.
const LAST_KEY_CONFLICT: ProblemDef = ProblemDef {
    slug: "last-api-key",
    status: StatusCode::CONFLICT,
    title: "Cannot delete the last API key",
    description: "This is the tenant's only remaining API key; deleting it would lock the \
                  tenant out of every key-guarded route. Mint its replacement first, then \
                  delete this one.",
};

pub(crate) struct ModuleState {
    pub ctx: Arc<ModuleContext>,
    /// The model `POST /messages` asks. `None` is a deployment that was
    /// not given one: the route answers `503 text-model-not-configured`
    /// rather than pretending to answer.
    pub text_model: Option<Arc<dyn TextModel>>,
    /// The handoff sink a handoff turn writes through and kicks. `None` is
    /// `Support::new()` with nothing composed: the turn still marks
    /// `needs_escalation`, and no ticket is filed.
    pub handoff: Option<Arc<dyn HandoffSink>>,
    /// The module-owned limiter the widget's per-visitor buckets run on
    /// (`crate::widget`), wired by the composition so a deployment can
    /// bound one anonymous browser separately from the tenant's own
    /// budget. `None` falls back to `ctx.ports.rate_limiter` — both
    /// buckets then share the tenant limiter — and to no limiting at all
    /// when that is absent too.
    pub visitor_rate_limiter: Option<Arc<dyn RateLimiter>>,
    /// The `OpenAPI` 3.1 document `GET /openapi.json` serves (issue #34),
    /// built once here from this module's surface plus whatever other
    /// modules' surfaces the composition injected (`Support::with_api_
    /// surface`) — a document read is not a place to rebuild a schema.
    pub openapi: Value,
}

/// Builds the module's router. `api_surfaces` are other modules' surfaces
/// the composition injected for the `OpenAPI` document (module-support
/// cannot depend on them), each paired with the module name its paths
/// mount under.
pub(crate) fn router(
    ctx: Arc<ModuleContext>,
    text_model: Option<Arc<dyn TextModel>>,
    handoff: Option<Arc<dyn HandoffSink>>,
    visitor_rate_limiter: Option<Arc<dyn RateLimiter>>,
    api_surfaces: Vec<(String, Surface)>,
) -> axum::Router {
    let mut modules = vec![(crate::MODULE_NAME.to_owned(), surface())];
    modules.extend(api_surfaces);
    let document = openapi::document(&modules);
    let state = Arc::new(ModuleState {
        ctx,
        text_model,
        handoff,
        visitor_rate_limiter,
        openapi: document,
    });
    axum::Router::new()
        .route("/admin/tenants", post(create_tenant))
        .route(
            "/admin/tenants/{tenant_id}/settings",
            put(messages::put_settings),
        )
        .route(
            "/admin/tenants/{tenant_id}/publishable-keys",
            post(widget::create_publishable_key),
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
        .route("/analytics", get(analytics))
        .route("/analytics/gaps", get(analytics_gaps))
        .route("/analytics/citations", get(analytics_citations))
        .route("/messages", post(messages::post_message))
        .route("/inbox", get(human::inbox))
        .route(
            "/conversations/{conversation_id}/takeover",
            post(human::takeover),
        )
        .route("/conversations/{conversation_id}/reply", post(human::reply))
        .route(
            "/conversations/{conversation_id}/handback",
            post(human::handback),
        )
        .route("/keys", get(list_keys).post(create_key))
        .route("/keys/{kid}", delete(delete_key))
        .route("/widget/messages", post(widget::post_widget_message))
        .route(
            "/widget/conversations/{conversation_id}",
            get(widget::get_widget_conversation),
        )
        .route("/w.js", get(widget::serve_w_js))
        .route("/mcp", post(crate::mcp::post_mcp))
        .route("/openapi.json", get(openapi))
        .with_state(state)
}

/// `GET /openapi.json` — the `OpenAPI` 3.1 document for `/v1/support/*` and
/// every module surface the composition injected (issue #34). Public on
/// purpose: it is a contract document, and a caller reads it before it
/// holds any key.
async fn openapi(State(state): State<Arc<ModuleState>>) -> Response {
    Json(state.openapi.clone()).into_response()
}

/// The module's declared surface (ADR 0010): one action per route
/// [`router`] mounts, so `GET /__surface` and the `OpenAPI` document
/// describe exactly what exists. The route split is the policy split —
/// tenant-key routes carry [`RoutePolicy::ApiKey`], admin routes
/// [`Audience::Admin`], and the widget's write is a captcha-guarded public
/// form. An input schema is declared where the handler's body is a typed
/// `JsonSchema` (the search query, the message body).
// One flat declaration per route: splitting it would scatter the mapping
// from routes to policies across helpers, which is the thing this list is
// meant to make readable at a glance.
#[allow(clippy::too_many_lines)]
pub(crate) fn surface() -> Surface {
    let key = RoutePolicy::ApiKey;
    Surface::new()
        .action(
            Action::post("create-tenant", "/admin/tenants")
                .audience(Audience::Admin)
                .outcome(Outcome::Json),
        )
        .action(
            Action::new(
                "put-tenant-settings",
                Method::PUT,
                "/admin/tenants/{tenant_id}/settings",
            )
            .audience(Audience::Admin),
        )
        .action(
            Action::post(
                "create-publishable-key",
                "/admin/tenants/{tenant_id}/publishable-keys",
            )
            .audience(Audience::Admin)
            .outcome(Outcome::Json),
        )
        .action(
            Action::post("ingest-source", "/sources")
                .policy(key)
                .outcome(Outcome::Json),
        )
        .action(
            Action::get("list-sources", "/sources")
                .audience(Audience::Public)
                .policy(key)
                .outcome(Outcome::Json),
        )
        .action(
            Action::get("get-source", "/sources/{source_id}")
                .audience(Audience::Public)
                .policy(key)
                .outcome(Outcome::Json),
        )
        .action(
            Action::new("put-source", Method::PUT, "/sources/{source_id}")
                .policy(key)
                .outcome(Outcome::Json),
        )
        .action(
            Action::delete("delete-source", "/sources/{source_id}")
                .audience(Audience::Public)
                .policy(key),
        )
        .action(
            Action::post("create-upload", "/uploads")
                .policy(key)
                .outcome(Outcome::Json),
        )
        .action(
            Action::new(
                "put-upload-part",
                Method::PUT,
                "/uploads/{upload_id}/parts/{n}",
            )
            .policy(key)
            .outcome(Outcome::Json),
        )
        .action(
            Action::post("complete-upload", "/uploads/{upload_id}/complete")
                .policy(key)
                .outcome(Outcome::Json),
        )
        .action(
            Action::get("get-upload", "/uploads/{upload_id}")
                .audience(Audience::Public)
                .policy(key)
                .outcome(Outcome::Json),
        )
        .action(
            Action::post("create-connector", "/connectors")
                .policy(key)
                .outcome(Outcome::Json),
        )
        .action(
            Action::get("search", "/search")
                .audience(Audience::Public)
                .policy(key)
                .input::<SearchQuery>()
                .outcome(Outcome::Json),
        )
        // Analytics (issue #36): read-only rollup routes.
        .action(
            Action::get("get-analytics", "/analytics")
                .audience(Audience::Public)
                .policy(key)
                .outcome(Outcome::Json),
        )
        .action(
            Action::get("get-analytics-gaps", "/analytics/gaps")
                .audience(Audience::Public)
                .policy(key)
                .outcome(Outcome::Json),
        )
        .action(
            Action::get("get-analytics-citations", "/analytics/citations")
                .audience(Audience::Public)
                .policy(key)
                .outcome(Outcome::Json),
        )
        .action(
            Action::post("post-message", "/messages")
                .policy(key)
                .input::<messages::MessageBody>()
                .outcome(Outcome::Json),
        )
        // Human in the loop (issue #35): staff-key routes.
        .action(
            Action::get("list-inbox", "/inbox")
                .audience(Audience::Public)
                .policy(key)
                .outcome(Outcome::Json),
        )
        .action(
            Action::post(
                "takeover-conversation",
                "/conversations/{conversation_id}/takeover",
            )
            .policy(key)
            .outcome(Outcome::Json),
        )
        .action(
            Action::post(
                "reply-conversation",
                "/conversations/{conversation_id}/reply",
            )
            .policy(key)
            .outcome(Outcome::Json),
        )
        .action(
            Action::post(
                "handback-conversation",
                "/conversations/{conversation_id}/handback",
            )
            .policy(key)
            .outcome(Outcome::Json),
        )
        .action(
            Action::get("list-keys", "/keys")
                .audience(Audience::Public)
                .policy(key)
                .outcome(Outcome::Json),
        )
        .action(
            Action::post("create-key", "/keys")
                .policy(key)
                .outcome(Outcome::Json),
        )
        .action(
            Action::delete("delete-key", "/keys/{kid}")
                .audience(Audience::Public)
                .policy(key),
        )
        .action(Action::post("post-widget-message", "/widget/messages").captcha())
        .action(
            Action::get(
                "get-widget-conversation",
                "/widget/conversations/{conversation_id}",
            )
            .audience(Audience::Public)
            .outcome(Outcome::Json),
        )
        .action(Action::get("w-js", "/w.js").audience(Audience::Public))
        .action(
            Action::post("mcp", "/mcp")
                .policy(key)
                .outcome(Outcome::Json),
        )
        .action(
            Action::get("openapi", "/openapi.json")
                .audience(Audience::Public)
                .outcome(Outcome::Json),
        )
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

/// Who a verified API key authenticates as: the tenant every query is
/// scoped to, and — when the key carries one — the staff member it belongs
/// to. Customer and integration keys have no `staff_id`, which is what
/// keeps the staff routes (`crate::human`) closed to them.
pub(crate) struct Principal {
    pub tenant_id: String,
    pub staff_id: Option<String>,
}

/// Verifies the `Authorization` bearer as a tenant API key and returns
/// the principal every downstream query scopes on. Revoked kids come
/// from module config (`SUPPORT_REVOKED_KIDS`, parsed by
/// [`tenancy::parse_revoked_kids`]); the tenant row must exist and be
/// active; and the verified key's own id must have a row in `sg_api_keys`
/// for that tenant — the row is the source of truth, so deleting it
/// revokes the key immediately with no config change. All failure paths
/// collapse into [`UNAUTHORIZED`].
pub(crate) async fn authenticate(
    ctx: &ModuleContext,
    headers: &HeaderMap,
) -> Result<Principal, Problem> {
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
        Ok(Some(tenant)) if tenant.status == STATUS_ACTIVE => {
            // The signature is valid, but the key only authenticates while
            // its own row exists: a key id never recorded, or one whose row
            // the tenant deleted via `DELETE /keys/{kid}`, is refused the
            // same 401 as junk. The row also carries the key's `staff_id`,
            // if it has one — that, not the signature, is what the staff
            // routes read.
            match store::find_api_key(db, &tenant.id, &tenant_key.key_id).await {
                Ok(Some(key)) => Ok(Principal {
                    tenant_id: tenant.id,
                    staff_id: key.staff_id,
                }),
                Ok(None) => Err(unauthorized()),
                Err(err) => Err(err.into()),
            }
        }
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
        Ok(principal) => principal.tenant_id,
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
/// later. Lose it and the tenant mints another with `POST /keys`, using
/// any key it still holds.
///
/// The `kid` in the response is the key's own id — the same value `GET
/// /keys` shows and `DELETE /keys/{kid}` takes — **not** the signing-key
/// generation the token was signed with. Everything key-addressed in this
/// module is per key, so `kid` means the per-key id throughout; the
/// signing generation is the `tenancy` config override
/// ([`tenancy::REVOKED_KIDS_KEY`]), a layer beneath this API.
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
            kid: minted.key_id.clone(),
            label: store::FIRST_KEY_LABEL.to_owned(),
            // The provisioning key is the tenant's own, not a staff key.
            staff_id: None,
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
            "kid": minted.key_id,
        })),
    )
        .into_response())
}

// ---------------------------------------------------------------------
// API key management: a tenant lists, mints and deletes its own keys,
// authenticating with one of them. The row in `sg_api_keys` is the source
// of truth — deleting it revokes the key on the very next request, no
// config change needed. The plaintext key is shown exactly once, at mint;
// every read shows only the key's own id, its label and its timestamp.
// ---------------------------------------------------------------------

/// The optional body of `POST /keys`: `{"label": "…", "staff_id": "…"}`.
/// An absent body is valid: it means the default label and a key that is
/// not a staff key.
#[derive(Deserialize, Default)]
struct CreateKeyBody {
    label: Option<String>,
    /// Names the staff member this key belongs to (issue #35). Present,
    /// the key may use the staff routes; absent, it is an ordinary
    /// customer or integration key.
    staff_id: Option<String>,
}

/// The label and staff id a `POST /keys` request asked for: an absent (or
/// empty) body is [`store::DEFAULT_KEY_LABEL`] and no staff id, otherwise
/// each present value is trimmed and held to the same byte ceiling a
/// tenant name gets. The guards have already run, so a malformed body is a
/// real 400 here, not a leak.
fn parse_key_body(scope: &Scope, body: &Bytes) -> Result<(String, Option<String>), Problem> {
    let bad = || {
        Problem::validation_failed(format!(
            "body: expected a JSON object with an optional string \"label\" and an optional \
             string \"staff_id\", each 1..={MAX_NAME_BYTES} bytes"
        ))
        .instance(&scope.request_id)
    };
    let parsed: CreateKeyBody = if body.is_empty() {
        CreateKeyBody::default()
    } else {
        serde_json::from_slice(body).map_err(|_| bad())?
    };
    // A value is trimmed and length-checked the same way whether it is a
    // label or a staff id; `None` when the field was absent.
    let checked = |value: Option<String>| -> Result<Option<String>, Problem> {
        match value {
            None => Ok(None),
            Some(value) => {
                let trimmed = value.trim();
                if trimmed.is_empty() || trimmed.len() > MAX_NAME_BYTES {
                    return Err(bad());
                }
                Ok(Some(trimmed.to_owned()))
            }
        }
    };
    let label = checked(parsed.label)?.unwrap_or_else(|| store::DEFAULT_KEY_LABEL.to_owned());
    let staff_id = checked(parsed.staff_id)?;
    Ok((label, staff_id))
}

/// `GET /keys` — the tenant's own API keys, oldest first, each shown as
/// `{kid, label, staff_id, created_at}` where `kid` is the key's own id
/// (the one `DELETE /keys/{kid}` takes). Nothing secret is stored, so
/// there is nothing secret to show: the key material and its MAC live only
/// in the mint response.
async fn list_keys(
    State(state): State<Arc<ModuleState>>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let auth = match authorize(&state.ctx, &headers).await {
        Authed::Ready(auth) => auth,
        Authed::Done(done) => return Ok(done),
    };
    let keys: Vec<Value> = store::list_api_keys(auth.db, &auth.tenant_id)
        .await?
        .iter()
        .map(|key| {
            json!({
                "kid": key.kid,
                "label": key.label,
                "staff_id": key.staff_id,
                "created_at": key.created_at,
            })
        })
        .collect();
    Ok(Json(json!({ "keys": keys })).into_response())
}

/// `POST /keys` — mint another API key for the authenticated tenant,
/// labelled `{"label"}` (default `"key"`). Like `POST /admin/tenants`, the
/// response carries the plaintext key **exactly once** under `api_key`,
/// alongside its `kid`, `label` and `created_at`; only the row is
/// remembered, so the response cannot be replayed later.
async fn create_key(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, Problem> {
    // The guards run before the body is parsed, exactly as on every other
    // key-guarded route: a malformed body must not earn a 4xx that would
    // tell an unauthenticated caller the route exists.
    let auth = match authorize(&state.ctx, &headers).await {
        Authed::Ready(auth) => auth,
        Authed::Done(done) => return Ok(done),
    };
    let (label, staff_id) = parse_key_body(&scope, &body)?;
    let clock: &dyn Clock = required_port(auth.ctx.ports.clock.as_deref(), "Clock")?;
    let id_gen: &dyn IdGen = required_port(auth.ctx.ports.id_gen.as_deref(), "IdGen")?;
    let signer: &dyn Signer = required_port(auth.ctx.ports.signer.as_deref(), "Signer")?;

    let minted = tenancy::mint(signer, &auth.tenant_id)
        .map_err(|_| Problem::internal().with_detail("api key minting failed"))?;
    let created_at = store::iso_now(clock);
    store::insert_api_key(
        auth.db,
        &ApiKeyRow {
            id: id_gen.ulid(),
            tenant_id: auth.tenant_id.clone(),
            kid: minted.key_id.clone(),
            label: label.clone(),
            staff_id: staff_id.clone(),
            created_at: created_at.clone(),
        },
    )
    .await?;

    Ok((
        StatusCode::CREATED,
        Json(json!({
            "kid": minted.key_id,
            "label": label,
            "staff_id": staff_id,
            "created_at": created_at,
            "api_key": minted.key,
        })),
    )
        .into_response())
}

/// `DELETE /keys/{kid}` — delete one of the tenant's own keys; `204` with
/// no body. An unknown kid, or another tenant's, is the same `404` and
/// deletes nothing. A delete that would leave the tenant with no key at
/// all is `409` ([`LAST_KEY_CONFLICT`]): locking the tenant out is never
/// what a key-management call should do, so a key is replaced by minting
/// its successor first, never removed outright while it is the last.
///
/// The last-key check and the delete are one statement
/// ([`store::delete_api_key`]) so two concurrent deletes cannot both pass
/// the check and empty the tenant; the `0`-rows case is then told apart by
/// a read (the row is gone → `404`, the row is still there → `409`).
async fn delete_key(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    Path(kid): Path<String>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let auth = match authorize(&state.ctx, &headers).await {
        Authed::Ready(auth) => auth,
        Authed::Done(done) => return Ok(done),
    };
    if store::delete_api_key(auth.db, &auth.tenant_id, &kid).await? > 0 {
        return Ok(StatusCode::NO_CONTENT.into_response());
    }
    // Nothing was removed: either the id is unknown or another tenant's
    // (`404`), or it is this tenant's last key, which the `EXISTS` guard
    // refused (`409`). Which one it is, is whether the row is still there.
    if store::find_api_key(auth.db, &auth.tenant_id, &kid)
        .await?
        .is_some()
    {
        Err(Problem::new(&LAST_KEY_CONFLICT).instance(&scope.request_id))
    } else {
        Err(Problem::not_found().instance(&scope.request_id))
    }
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
    let tenant_id = authenticate(ctx, &headers).await?.tenant_id;
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

/// One source as the source-management routes show it. `reviewed` and its
/// provenance (issue #35) name a support agent's saved correction; a
/// source indexed the ordinary way is `reviewed: false` with null
/// provenance.
fn source_json(item: &store::SourceSummary) -> Value {
    json!({
        "id": item.id,
        "title": item.title,
        "origin": if item.url.is_some() { "url" } else { "text" },
        "external_id": item.external_id,
        "bytes": item.byte_len,
        "chunk_count": item.chunk_count,
        "updated_at": item.updated_at,
        "reviewed": item.reviewed,
        "reviewed_by": item.reviewed_by,
        "reviewed_conversation_id": item.reviewed_conversation_id,
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

    upsert_source(auth.db, clock, &target, parsed).await?;
    // Read the row back rather than assemble it: a `PUT` replaces content
    // and metadata but leaves a source's `reviewed` provenance untouched,
    // so only the stored row knows what still stands.
    let summary = store::find_source_summary(auth.db, &auth.tenant_id, &source_id)
        .await?
        .ok_or_else(Problem::internal)?;
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

/// `GET /search`'s query string. `JsonSchema` so the `OpenAPI` document
/// derives its parameters from this very type (issue #34).
#[derive(Deserialize, JsonSchema)]
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
    let tenant_id = authenticate(ctx, &headers).await?.tenant_id;
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

    Ok(Json(search_body(&hits)).into_response())
}

/// The `/search` result body — the same shape the MCP `search_sources`
/// tool answers with (issue #34), so the route and the tool cannot drift.
pub(crate) fn search_body(hits: &[Retrieved]) -> Value {
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
    json!({ "results": results })
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
///
/// Shared with the analytics rollup, which normalizes a gap turn's
/// preceding question with exactly this function — a gap's terms are the
/// same terms a search would have run, so a term that appears in
/// `/analytics/gaps` is one an operator can paste into `GET /search`.
pub(crate) fn query_terms(query: &str) -> Vec<String> {
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
///
/// The ranker's top `k` is taken *after* a reviewed-source boost (issue
/// #35): [`retrieve`] keeps every candidate whose boosted score could
/// still reach the top `k`, multiplies the score of chunks whose source
/// was saved by a support agent, and re-sorts — so what a tenant finds by
/// searching is still exactly what an answer can cite, with a correction
/// preferred over an equally scored answer it replaces.
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
    // Cut to the boosted candidates before reading their rows: a chunk can
    // only reach the top `k` once boosted if its score, times
    // [`REVIEWED_BOOST`], still clears the `k`-th unboosted score, so
    // nothing below that cutoff needs its row read.
    ranked.truncate(boost_candidates(&ranked, k));

    let top: Vec<String> = ranked
        .iter()
        .map(|scored| scored.chunk_id.clone())
        .collect();
    let mut by_id: HashMap<String, ChunkRow> = store::chunks_by_id(db, tenant_id, &top)
        .await?
        .into_iter()
        .map(|chunk| (chunk.id.clone(), chunk))
        .collect();

    // The same `chunks_by_id` read carries each hit's source `reviewed`
    // flag, so the boost costs no extra query. A ranked id whose row is
    // gone needs no entry: it is dropped from the result below.
    let reviewed: HashSet<String> = by_id
        .values()
        .filter(|chunk| chunk.reviewed)
        .map(|chunk| chunk.id.clone())
        .collect();
    boost_reviewed(&mut ranked, &reviewed);
    ranked.truncate(k);

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

/// How much a chunk from a reviewed source outranks an equally scored one
/// (issue #35): a support agent's saved correction was written to be the
/// answer, so it wins a close call — but a clearly better ordinary chunk
/// still beats it.
pub const REVIEWED_BOOST: f64 = 1.5;

/// Retrieval over-fetches this many candidates at most for the reviewed
/// boost (issue #35): every ranked chunk whose boosted score could still
/// reach the top `k` is read as one `chunks_by_id` batch, and this caps
/// that read so a corpus of near-equal scores cannot make it unbounded.
/// Larger than any `k` the routes ask for, so the cutoff — not this — is
/// what usually decides the batch size.
pub const MAX_BOOST_CANDIDATES: usize = 64;

/// How many of the ranked chunks the reviewed boost must consider: the
/// prefix whose score, multiplied by [`REVIEWED_BOOST`], could still clear
/// the `k`-th unboosted score — an exact cutoff, not a fixed multiple of
/// `k`, so a reviewed chunk in a tight score band is never dropped just
/// because it sat past a fixed window. Capped at [`MAX_BOOST_CANDIDATES`].
/// Pure, so the cutoff is unit-testable.
fn boost_candidates(ranked: &[bm25::Scored], k: usize) -> usize {
    if k == 0 {
        return 0;
    }
    match ranked.get(k - 1) {
        Some(cutoff) => {
            let reach = cutoff.score / REVIEWED_BOOST;
            ranked
                .iter()
                .take_while(|scored| scored.score >= reach)
                .count()
                .min(MAX_BOOST_CANDIDATES)
        }
        // Fewer than `k` chunks ranked: every one is a candidate.
        None => ranked.len().min(MAX_BOOST_CANDIDATES),
    }
}

/// Multiplies every ranked chunk whose source is reviewed by
/// [`REVIEWED_BOOST`], then re-orders best first. Pure — no database — so
/// the rule is unit-testable directly. The sort is stable, so equal scores
/// keep the ranker's `chunk_id` order.
pub(crate) fn boost_reviewed(ranked: &mut [bm25::Scored], reviewed: &HashSet<String>) {
    for scored in ranked.iter_mut() {
        if reviewed.contains(&scored.chunk_id) {
            scored.score *= REVIEWED_BOOST;
        }
    }
    ranked.sort_by(|a, b| b.score.total_cmp(&a.score));
}

// ---------------------------------------------------------------------
// Analytics (issue #36): three read-only routes over the daily rollups
// `crate::analytics` recomputes on the daily cron. Each reads the
// rollup tables (and `sg_sources` for the never-cited list) and nothing
// else, so a dashboard's cost is the range it asks for, never the size
// of the workspace's message and ticket tables.
// ---------------------------------------------------------------------

/// `limit` handling for the gap and citation lists: default 20, hard
/// range 1..=100.
const DEFAULT_ANALYTICS_LIMIT: i64 = 20;
const MAX_ANALYTICS_LIMIT: i64 = 100;

/// The widest range `/analytics` answers, in days — a year plus the leap
/// day. A dashboard reads a trailing window; a caller asking for the
/// whole history wants an export path, and this bound keeps one request's
/// row count predictable.
const MAX_ANALYTICS_DAYS: i64 = 366;

/// The default window when `from` is omitted: `to` and the 29 days
/// before it, so a bare `GET /analytics` is the trailing 30 days.
const DEFAULT_ANALYTICS_DAYS: i64 = 30;

/// The `?from=&to=&limit=` query the three analytics routes share. Parsed
/// by hand *after* [`authorize`] (the `list_sources` pattern), so a
/// malformed value earns the same 401/429 any other request would and
/// only an authenticated, under-budget caller sees the 400.
#[derive(Deserialize, Default)]
struct AnalyticsQuery {
    from: Option<String>,
    to: Option<String>,
    limit: Option<i64>,
}

/// What the analytics routes run before they read anything: the caller
/// the guards authenticated, the inclusive UTC day range it resolved, and
/// the parsed query (for `limit`) — or the response the guards already
/// built. The [`Authed`] shape, one layer up.
enum AnalyticsAsk<'a> {
    Ready {
        auth: Authorized<'a>,
        from: String,
        to: String,
        query: AnalyticsQuery,
    },
    Done(Response),
}

/// The authorize → parse → resolve-range preamble the three analytics
/// routes share: the guarded `401`/`429` when authorization answered, or
/// the authenticated caller with its range. A malformed query or range is
/// the `400` the caller returns as a `Problem`.
async fn analytics_ask<'a>(
    ctx: &'a ModuleContext,
    scope: &Scope,
    raw: Option<&str>,
    headers: &HeaderMap,
) -> Result<AnalyticsAsk<'a>, Problem> {
    let auth = match authorize(ctx, headers).await {
        Authed::Ready(auth) => auth,
        Authed::Done(done) => return Ok(AnalyticsAsk::Done(done)),
    };
    let query = parse_analytics_query(raw, scope)?;
    let (from, to) = resolve_range(&query, &utc_today(auth.ctx), scope)?;
    Ok(AnalyticsAsk::Ready {
        auth,
        from,
        to,
        query,
    })
}

/// `GET /analytics?from=&to=` — the tenant's day-by-day deflection and
/// escalation numbers, oldest day first, plus the range's totals and the
/// deflection rate. `from` and `to` are inclusive UTC days (`to` defaults
/// to today, `from` to 29 days before it); `from > to` or a span past
/// [`MAX_ANALYTICS_DAYS`] is a `400`. Every value is read from
/// `sg_daily_stats`, which the daily rollup writes — see the module
/// README for what each column means.
async fn analytics(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    RawQuery(raw): RawQuery,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let (auth, from, to) = match analytics_ask(&state.ctx, &scope, raw.as_deref(), &headers).await?
    {
        AnalyticsAsk::Ready { auth, from, to, .. } => (auth, from, to),
        AnalyticsAsk::Done(done) => return Ok(done),
    };
    let days = analytics::day_stats(auth.db, &auth.tenant_id, &from, &to).await?;

    // The range's totals, summed here rather than in a second query: the
    // day rows are already in hand. The deflection rate is the share of
    // the range's conversations that never reached a person — `null`, not
    // `0`, when the range holds none, because "no data" and "nothing
    // deflected" are different answers a dashboard must not conflate.
    let total = |pick: fn(&analytics::DayRow) -> i64| days.iter().map(pick).sum::<i64>();
    let conversations = total(|day| day.conversations);
    let handed_off = total(|day| day.handed_off);
    // A rate is a fraction; the counters are counts. A range with more
    // than 2^53 conversations would lose a precision no ratio could show.
    #[expect(clippy::cast_precision_loss)]
    let deflection_rate =
        (conversations > 0).then(|| (conversations - handed_off) as f64 / conversations as f64);
    let totals = json!({
        "conversations": conversations,
        "answered": total(|day| day.answered),
        "clarify": total(|day| day.clarify),
        "handoff": total(|day| day.handoff),
        "handed_off": handed_off,
        "filed": total(|day| day.filed),
        "rejected": total(|day| day.rejected),
        "needs_info": total(|day| day.needs_info),
        "duplicates": total(|day| day.duplicates),
        "dead_lettered": total(|day| day.dead_lettered),
    });

    Ok(Json(json!({
        "from": from,
        "to": to,
        "days": days,
        "totals": totals,
        "deflection_rate": deflection_rate,
    }))
    .into_response())
}

/// `GET /analytics/gaps?from=&to=&limit=` — the normalized query terms
/// behind the range's unanswered turns, most hits first (`limit` default
/// 20, capped at 100). Only terms are stored and returned, never the
/// questions they came from; a term is one an operator can search for.
async fn analytics_gaps(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    RawQuery(raw): RawQuery,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let (auth, from, to, query) =
        match analytics_ask(&state.ctx, &scope, raw.as_deref(), &headers).await? {
            AnalyticsAsk::Ready {
                auth,
                from,
                to,
                query,
            } => (auth, from, to, query),
            AnalyticsAsk::Done(done) => return Ok(done),
        };
    let limit = analytics_limit(&query);
    let terms = analytics::gap_terms(auth.db, &auth.tenant_id, &from, &to, limit).await?;
    let terms: Vec<Value> = terms
        .iter()
        .map(|(term, hits)| json!({ "term": term, "hits": hits }))
        .collect();

    Ok(Json(json!({ "from": from, "to": to, "terms": terms })).into_response())
}

/// `GET /analytics/citations?from=&to=&limit=` — the range's most-cited
/// sources (most cites first, `limit` default 20, capped at 100) and the
/// tenant's current sources that earned no citation in the range.
async fn analytics_citations(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    RawQuery(raw): RawQuery,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let (auth, from, to, query) =
        match analytics_ask(&state.ctx, &scope, raw.as_deref(), &headers).await? {
            AnalyticsAsk::Ready {
                auth,
                from,
                to,
                query,
            } => (auth, from, to, query),
            AnalyticsAsk::Done(done) => return Ok(done),
        };
    let limit = analytics_limit(&query);
    let most_cited = analytics::most_cited(auth.db, &auth.tenant_id, &from, &to, limit).await?;
    let never_cited = analytics::never_cited(auth.db, &auth.tenant_id, &from, &to, limit).await?;

    Ok(Json(json!({
        "from": from,
        "to": to,
        "most_cited": most_cited,
        "never_cited": never_cited,
    }))
    .into_response())
}

/// The `limit` a gap or citation request asks for, defaulted and clamped
/// into 1..=[`MAX_ANALYTICS_LIMIT`] — the `list_sources` clamp shape, so
/// the route never answers an unbounded list.
fn analytics_limit(query: &AnalyticsQuery) -> usize {
    usize::try_from(
        query
            .limit
            .unwrap_or(DEFAULT_ANALYTICS_LIMIT)
            .clamp(1, MAX_ANALYTICS_LIMIT),
    )
    .unwrap_or(usize::MAX)
}

/// Today's UTC day from the `Clock` port — the anchor the default range
/// is measured back from, and the same `YYYY-MM-DD` the rollup buckets
/// by. `Db`, `Clock` and `IdGen` are required ports, so this is a harness
/// bug when it is somehow absent; the empty string then parses to a 400
/// rather than a panic in a Worker isolate.
fn utc_today(ctx: &ModuleContext) -> String {
    let clock = ctx
        .ports
        .clock
        .clone()
        .unwrap_or_else(|| Arc::new(SystemClock));
    store::iso_now(clock.as_ref())
        .get(..10)
        .unwrap_or_default()
        .to_owned()
}

/// Parses the raw query string into [`AnalyticsQuery`], or the 400 a
/// malformed `from`/`to`/`limit` earns.
fn parse_analytics_query(raw: Option<&str>, scope: &Scope) -> Result<AnalyticsQuery, Problem> {
    let Some(raw) = raw.filter(|raw| !raw.is_empty()) else {
        return Ok(AnalyticsQuery::default());
    };
    let bad = || {
        Problem::validation_failed(
            "query: from and to are YYYY-MM-DD UTC days and limit is an integer",
        )
        .instance(&scope.request_id)
    };
    let uri: http::Uri = format!("https://support.local/?{raw}")
        .parse()
        .map_err(|_| bad())?;
    Ok(Query::<AnalyticsQuery>::try_from_uri(&uri)
        .map_err(|_| bad())?
        .0)
}

/// Resolves the inclusive UTC day range a request asks for: `to` defaults
/// to `today`, `from` to `to` less [`DEFAULT_ANALYTICS_DAYS`] − 1; both
/// must be real `YYYY-MM-DD` days, `from` must not be after `to`, and the
/// span must not exceed [`MAX_ANALYTICS_DAYS`]. Returns the two days in
/// the exact form the rollup stores them.
fn resolve_range(
    query: &AnalyticsQuery,
    today: &str,
    scope: &Scope,
) -> Result<(String, String), Problem> {
    let bad = |detail: String| Problem::validation_failed(detail).instance(&scope.request_id);
    let today = analytics::parse_day(today)
        .map_err(|_| bad("analytics: the server clock gave no usable day".to_owned()))?;
    let to = match query.to.as_deref() {
        Some(raw) => analytics::parse_day(raw)
            .map_err(|_| bad(format!("query: to={raw:?} is not a YYYY-MM-DD UTC day")))?,
        None => today,
    };
    let from = match query.from.as_deref() {
        Some(raw) => analytics::parse_day(raw)
            .map_err(|_| bad(format!("query: from={raw:?} is not a YYYY-MM-DD UTC day")))?,
        None => to - time::Duration::days(DEFAULT_ANALYTICS_DAYS - 1),
    };
    if from > to {
        return Err(bad(format!(
            "query: from={} is after to={}",
            analytics::format_day(from).unwrap_or_default(),
            analytics::format_day(to).unwrap_or_default(),
        )));
    }
    if (to - from).whole_days() >= MAX_ANALYTICS_DAYS {
        return Err(bad(format!(
            "query: the range is longer than {MAX_ANALYTICS_DAYS} days"
        )));
    }
    let day = |date| {
        analytics::format_day(date).map_err(|err| {
            Problem::internal()
                .with_detail(err.to_string())
                .instance(&scope.request_id)
        })
    };
    Ok((day(from)?, day(to)?))
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

    fn scored(items: &[(&str, f64)]) -> Vec<bm25::Scored> {
        items
            .iter()
            .map(|(chunk_id, score)| bm25::Scored {
                chunk_id: (*chunk_id).to_owned(),
                score: *score,
            })
            .collect()
    }

    fn order(ranked: &[bm25::Scored]) -> Vec<&str> {
        ranked.iter().map(|s| s.chunk_id.as_str()).collect()
    }

    #[test]
    fn the_reviewed_boost_wins_a_close_call_but_not_a_clear_win() {
        let reviewed: HashSet<String> = ["b".to_owned()].into_iter().collect();

        // 9.0 * 1.5 = 13.5 climbs past 10.0.
        let mut close = scored(&[("a", 10.0), ("b", 9.0)]);
        boost_reviewed(&mut close, &reviewed);
        assert_eq!(order(&close), ["b", "a"]);

        // 5.0 * 1.5 = 7.5 still trails a clearly better 10.0.
        let mut clear = scored(&[("a", 10.0), ("b", 5.0)]);
        boost_reviewed(&mut clear, &reviewed);
        assert_eq!(order(&clear), ["a", "b"]);
    }

    #[test]
    fn an_unreviewed_shortlist_keeps_its_scores_and_order() {
        let mut ranked = scored(&[("a", 2.0), ("b", 1.0)]);
        boost_reviewed(&mut ranked, &HashSet::new());
        assert_eq!(order(&ranked), ["a", "b"]);
        assert_eq!(ranked[0].score.to_bits(), 2.0_f64.to_bits());
        assert_eq!(ranked[1].score.to_bits(), 1.0_f64.to_bits());
    }

    #[test]
    fn a_reviewed_chunk_past_a_fixed_window_still_reaches_the_top() {
        // Twenty chunks in a tight score band; the last is reviewed and k
        // is 4, so a fixed 4k window would have thrown it away. The exact
        // cutoff keeps every candidate whose boosted score could clear the
        // k-th unboosted score, so the boost can lift it into the top k.
        let mut ranked: Vec<bm25::Scored> = (0..20)
            .map(|i| bm25::Scored {
                chunk_id: format!("c{i}"),
                score: 10.0 - f64::from(i) * 0.01,
            })
            .collect();
        let reviewed: HashSet<String> = ["c19".to_owned()].into_iter().collect();
        let k = 4;
        assert!(
            !order(&ranked)[..k].contains(&"c19"),
            "the fixture must rank the reviewed chunk below the cut"
        );

        let candidates = boost_candidates(&ranked, k);
        assert!(
            candidates > k * 4,
            "the exact cutoff must reach past a fixed 4k window"
        );
        ranked.truncate(candidates);
        boost_reviewed(&mut ranked, &reviewed);
        ranked.truncate(k);
        assert!(order(&ranked).contains(&"c19"), "{:?}", order(&ranked));
    }

    #[test]
    fn the_candidate_cutoff_is_capped() {
        // A flat corpus of near-equal scores would otherwise name the whole
        // index as candidates; the cap bounds the row read.
        let ranked: Vec<bm25::Scored> = (0..MAX_BOOST_CANDIDATES + 50)
            .map(|i| bm25::Scored {
                chunk_id: format!("c{i}"),
                score: 1.0,
            })
            .collect();
        assert_eq!(boost_candidates(&ranked, 1), MAX_BOOST_CANDIDATES);
    }
}
