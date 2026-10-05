//! The web widget's server half: three routes a browser page calls with a
//! *publishable* key and no `Authorization` header, plus the admin route
//! that mints one.
//!
//! **Why these routes are shaped differently from the rest of the
//! module.** `w.js` runs on the customer's site but is served from the
//! API origin, and it deliberately sends CORS-*simple* requests only: a
//! `POST` whose JSON body travels as `text/plain` and a `GET` whose key
//! rides the query string. No custom header — an `Authorization` header
//! would force a preflight, and a preflight is a second origin check
//! this module would rather not depend on. The key still travels inside
//! the request, so authentication is unchanged in strength; what changes
//! is that the *origin* becomes a first-class guard here, checked by the
//! handler rather than by the harness `CorsLayer`, which never sees a
//! simple request.
//!
//! **The origin check is exact membership, not suffix matching**, against
//! the per-tenant allowlist `PUT …/admin/tenants/{id}/settings` stores
//! normalized (`widget_origins`): no allowlist, no widget. A response
//! before the check — the 401s, the origin 403 — deliberately carries no
//! `Access-Control-Allow-Origin` at all, so the browser blocks the body
//! from the page's JavaScript; every response after it carries the
//! request's own origin and `Vary: Origin`, because the answer now
//! depends on that header.
//!
//! **End-user abuse controls** — the visitor is anonymous by design, so
//! the controls key on the visitor token and the client IP rather than an
//! account: two per-visitor buckets run on the module-owned limiter — one
//! on the visitor id, one on the client IP — bounding one browser's spend
//! of the tenant's budget, and only a request that passes them reaches
//! the tenant's own budget (`guard_rate_limit`), so a flood refused at
//! the visitor gate costs the shared `support:{tenant}` key nothing.
//! After `N` turns without a
//! verified-human verdict the route demands a Turnstile token
//! (`SUPPORT_WIDGET_CAPTCHA_AFTER`, default [`DEFAULT_CAPTCHA_AFTER`]);
//! passing once latches `human` into the visitor token, so a real human
//! solves one challenge per token lifetime, not per message.
//!
//! **Visitor and conversation tokens are body payloads, not cookies.**
//! The visitor token is kept by the page and echoed in the next request
//! body rather than set as a cookie, because third-party cookies are
//! partitioned away in Safari and Firefox — a cookie set by the API
//! origin would not come back — and credentialed CORS would force
//! `Access-Control-Allow-Credentials` behind an exact-origin echo: a
//! permanently larger surface for the same identifier. A signed token the
//! page holds is the credential, the same shape the rest of this
//! workspace signs.
//!
//! The turn itself is [`crate::messages::run_turn`], the exact function
//! `POST /messages` runs; nothing here re-implements answering.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use cratefield_core::{
    Clock, Database, IdGen, Json, Kid, ModuleConfig, ModuleContext, Payload, Problem, ProblemDef,
    RateLimit, RateLimitFailure, Scope, Signer, VentureEnv, check_rate_limit, client_ip,
    deployed_env, origin_of, rate_limited, require_admin,
};

use crate::handlers::{ModuleState, UNAUTHORIZED, guard_rate_limit, required_port};
use crate::messages;
use crate::store;

/// 403: the request named no `Origin`, or one that is not on the tenant's
/// allowlist. One slug for both, so a probing page cannot learn whether
/// the tenant uses the widget at all.
const ORIGIN_NOT_ALLOWED: ProblemDef = ProblemDef {
    slug: "origin-not-allowed",
    status: StatusCode::FORBIDDEN,
    title: "Origin not allowed",
    description: "The request's Origin is missing or is not on this tenant's widget allowlist.",
};

/// 403: the visitor owes a captcha and did not present one that verified.
/// The body carries the `site_key` extension member — the one thing the
/// widget needs to render the challenge it has been asked to solve.
const WIDGET_CAPTCHA_REQUIRED: ProblemDef = ProblemDef {
    slug: "widget-captcha-required",
    status: StatusCode::FORBIDDEN,
    title: "Human verification required",
    description: "This visitor has sent enough messages to owe a human-verification token; \
                  solve the challenge and send it as captcha_token.",
};

/// Turns without a verified-human verdict before a captcha is demanded:
/// config `SUPPORT_WIDGET_CAPTCHA_AFTER`, this value when unset. A new
/// visitor gets five honest answers before the first challenge.
pub(crate) const DEFAULT_CAPTCHA_AFTER: u32 = 5;

/// How long a visitor token lives: seven days, renewed on every turn
/// (each reply mints the token with the advanced count). Long enough
/// that a visitor does not lose a thread mid-conversation; short enough
/// that a leaked token from a page's storage does not outlive the
/// incident by a month.
pub(crate) const VISITOR_TOKEN_TTL_SECS: u64 = 7 * 24 * 60 * 60;

/// How long a conversation token lives: one day. It can read one
/// transcript and nothing else, and the widget treats any refusal from
/// the transcript route as "drop this thread and start fresh" — an
/// expired token is a new conversation, not an error page.
pub(crate) const CONVERSATION_TOKEN_TTL_SECS: u64 = 24 * 60 * 60;

/// The purpose visitor tokens are signed with. Module-qualified per the
/// harness convention. The token carries an explicit `exp`
/// ([`VISITOR_TOKEN_TTL_SECS`]); the signer's per-purpose ceiling still
/// clamps it — the reference signer's 30-day default sits above it and
/// never lengthens what is asked for.
pub(crate) const VISITOR_PURPOSE: &str = "support.widget-visitor";

/// The purpose conversation tokens are signed with, lifetime
/// [`CONVERSATION_TOKEN_TTL_SECS`] as above.
pub(crate) const CONVERSATION_PURPOSE: &str = "support.widget-conversation";

/// The widget script, served verbatim at `GET /v1/support/w.js` so an
/// embedding page has one origin to trust and the integrity hash in
/// `widget/w.js.sri` is computed over exactly these bytes.
pub(crate) const W_JS: &str = include_str!("../../../widget/w.js");

/// `POST /widget/messages` body. Read as raw bytes regardless of
/// `Content-Type`: the browser sends `text/plain` to keep the request
/// CORS-simple, and rejecting that header would be rejecting the widget.
#[derive(Deserialize)]
struct WidgetMessageBody {
    key: String,
    message: String,
    conversation_id: Option<String>,
    /// The visitor token from the previous reply, when the page has one.
    visitor: Option<String>,
    /// The Turnstile token, when the last reply demanded one.
    captcha_token: Option<String>,
}

/// `GET /widget/conversations/{id}` query.
#[derive(Deserialize)]
pub(crate) struct WidgetConversationQuery {
    key: Option<String>,
    token: Option<String>,
}

/// What a visitor token carries, signed as the payload `subject` (the one
/// free-form field a [`Payload`] has — a JSON document in it rides the
/// MAC like any other subject): the tenant it belongs to, the random
/// visitor id, how many turns this visitor has completed, and whether a
/// captcha has ever passed — the latch that keeps a solved challenge
/// solved for the token's lifetime.
#[derive(Debug, Serialize, Deserialize)]
struct VisitorClaims {
    tenant: String,
    vid: String,
    /// Completed turns, as the abuse controls count them.
    n: u32,
    human: bool,
}

impl VisitorClaims {
    fn sign(&self, signer: &dyn Signer, now: u64) -> String {
        signer.sign(&Payload {
            purpose: VISITOR_PURPOSE.to_owned(),
            subject: serde_json::to_string(self).unwrap_or_default(),
            // The signer clamps a too-large request down to the purpose
            // ceiling; it never extends one.
            exp: Some(now.saturating_add(VISITOR_TOKEN_TTL_SECS)),
            kid: Kid::Cur,
        })
    }

    /// Verifies `token` and returns its claims — but only when they name
    /// *this* tenant. A token minted against another tenant's key is junk
    /// here exactly as if it were malformed: one anonymous visitor's
    /// budget must never be portable to another tenant.
    fn verify(signer: &dyn Signer, token: &str, tenant_id: &str) -> Option<Self> {
        let payload = signer.verify(token, VISITOR_PURPOSE)?;
        let claims: Self = serde_json::from_str(&payload.subject).ok()?;
        (claims.tenant == tenant_id).then_some(claims)
    }

    /// The token with this turn's count added and the human latch
    /// carried through.
    fn advanced(&self) -> Self {
        Self {
            tenant: self.tenant.clone(),
            vid: self.vid.clone(),
            n: self.n.saturating_add(1),
            human: self.human,
        }
    }
}

/// What a conversation token carries: the tenant and the one conversation
/// it may read. The transcript route demands a match on both, so a leaked
/// token reads one transcript, not a tenant's.
#[derive(Debug, Serialize, Deserialize)]
struct ConversationClaims {
    tenant: String,
    conversation_id: String,
}

impl ConversationClaims {
    fn sign(&self, signer: &dyn Signer, now: u64) -> String {
        signer.sign(&Payload {
            purpose: CONVERSATION_PURPOSE.to_owned(),
            subject: serde_json::to_string(self).unwrap_or_default(),
            exp: Some(now.saturating_add(CONVERSATION_TOKEN_TTL_SECS)),
            kid: Kid::Cur,
        })
    }

    /// `true` when the token names exactly this tenant and conversation.
    fn matches(signer: &dyn Signer, token: &str, tenant_id: &str, conversation_id: &str) -> bool {
        signer
            .verify(token, CONVERSATION_PURPOSE)
            .and_then(|payload| serde_json::from_str::<Self>(&payload.subject).ok())
            .is_some_and(|claims| {
                claims.tenant == tenant_id && claims.conversation_id == conversation_id
            })
    }
}

/// A random visitor id: the ULID's random tail, lowercased — the same
/// trick the waitlist's referral codes use, and enough entropy that two
/// visitors of one tenant do not share a rate-limit bucket.
fn fresh_vid(id_gen: &dyn IdGen) -> String {
    id_gen
        .ulid()
        .chars()
        .rev()
        .take(12)
        .collect::<String>()
        .to_lowercase()
}

/// `POST /v1/support/widget/messages` — the browser turn. The guards run
/// in cost order (bytes, MAC, allowlist, limiter, captcha) and every
/// response after the origin check is stamped with that origin, because
/// the widget can only read answers the browser lets it read.
pub(crate) async fn post_widget_message(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, Problem> {
    let ctx = &state.ctx;
    // 1. The body, before anything else: the key is in it.
    let body: WidgetMessageBody = serde_json::from_slice(&body).map_err(|_| {
        Problem::validation_failed(
            "body: expected a JSON object with string \"key\" and \"message\", and optional \
             strings \"conversation_id\", \"visitor\" and \"captcha_token\"",
        )
        .instance(&scope.request_id)
    })?;
    let message = body.message.trim();
    if message.is_empty() || message.chars().count() > messages::MAX_MESSAGE_CHARS {
        return Err(Problem::validation_failed(format!(
            "message: required, 1..={} characters",
            messages::MAX_MESSAGE_CHARS
        ))
        .instance(&scope.request_id));
    }

    // 2. The publishable key, and the tenant it names — active, or the
    //    same indistinguishable 401 the secret routes answer. A *secret*
    //    key is refused here exactly like any other junk: a page-sourced
    //    credential gets widget authority or nothing.
    let tenant_id = authenticate_widget(ctx, &body.key).await?;

    // 3. The origin allowlist. No origin, no answer; the 403 carries no
    //    ACAO, so a hostile page cannot read the refusal either.
    let origin = check_origin(ctx, &tenant_id, &headers).await?;

    // 4. The visitor, or a fresh anonymous one. Absent, malformed,
    //    expired and foreign-tenant tokens all land here — a 4xx would
    //    only teach a hostile page what a valid token looks like.
    let id_gen: &dyn IdGen = required_port(ctx.ports.id_gen.as_deref(), "IdGen")?;
    let signer: &dyn Signer = required_port(ctx.ports.signer.as_deref(), "Signer")?;
    // The turn needs the clock anyway (`run_turn` stamps every write);
    // here it also dates the two tokens minted at the end.
    let now = now_secs(ctx)?;
    let visitor = body
        .visitor
        .as_deref()
        .and_then(|token| VisitorClaims::verify(signer, token, &tenant_id))
        .unwrap_or_else(|| VisitorClaims {
            tenant: tenant_id.clone(),
            vid: fresh_vid(id_gen),
            n: 0,
            human: false,
        });

    // 5. Rate limits, cheapest first: the visitor's two buckets on the
    //    module-owned limiter, then the tenant's own budget — a flood the
    //    visitor gate refuses must not spend `support:{tenant}`, which
    //    the tenant's API callers share.
    let remote_ip = client_ip(&headers);
    if let Some(response) = guard_visitor_limit(
        &state,
        &tenant_id,
        Some(&visitor.vid),
        remote_ip.as_deref(),
        "",
        &origin,
    )
    .await
    {
        return Ok(response);
    }
    if let Some(rate_limited) = guard_rate_limit(ctx, &tenant_id).await {
        return Ok(stamp_origin(rate_limited, &origin));
    }

    // 6. The captcha gate: from the Nth unverified turn on, the visitor
    //    must prove humanity once; the verdict latches into the token.
    let mut visitor = visitor;
    if let Some(response) = require_captcha(
        &state,
        &mut visitor,
        body.captcha_token.as_deref(),
        remote_ip.as_deref(),
        &scope,
        &origin,
    )
    .await
    {
        return Ok(response);
    }

    // 7. The turn — the very function `POST /messages` runs, in the
    //    language `POST /messages` would pick: the message's own words
    //    first, the browser's Accept-Language below that.
    let lang = messages::turn_language(
        message,
        headers
            .get(header::ACCEPT_LANGUAGE)
            .and_then(|value| value.to_str().ok()),
    );
    let reply = match messages::run_turn(
        &state,
        &scope,
        &tenant_id,
        message,
        body.conversation_id.as_deref(),
        // The widget collects no contact address; only `POST /messages`
        // carries one.
        None,
        lang,
    )
    .await
    {
        Ok(reply) => reply,
        Err(failure) => return Ok(stamp_origin(failure.into_response(), &origin)),
    };

    // 8. The tokens the page keeps: the advanced visitor (n + 1, so the
    //    captcha threshold counts completed turns) and, for this
    //    conversation, the read token the transcript route checks.
    let conversation_token = ConversationClaims {
        tenant: tenant_id,
        conversation_id: reply.conversation_id.clone(),
    }
    .sign(signer, now);
    let mut body = reply.to_json();
    body["visitor"] = json!(visitor.advanced().sign(signer, now));
    body["conversation_token"] = json!(conversation_token);
    Ok(stamp_origin(Json(body).into_response(), &origin))
}

/// `GET /v1/support/widget/conversations/{id}?key=…&token=…` — one
/// conversation's transcript: what the visitor was shown and nothing
/// else (`model_answer` never leaves the store through here). `no-store`
/// because this is the poll endpoint — a cached transcript is a stale
/// one, and a URL carrying a credential must not survive in a shared
/// cache regardless.
pub(crate) async fn get_widget_conversation(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    Path(conversation_id): Path<String>,
    Query(query): Query<WidgetConversationQuery>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    // The key rides the query string, so the query *is* the credential:
    // a missing or empty one is the same 401 as a bad one.
    let unauthorized = || Problem::new(&UNAUTHORIZED);
    let Some(key) = query.key.as_deref().filter(|key| !key.is_empty()) else {
        return Err(unauthorized());
    };
    let ctx = &state.ctx;
    let tenant_id = authenticate_widget(ctx, key).await?;
    let origin = check_origin(ctx, &tenant_id, &headers).await?;

    // The token must name this tenant and this conversation, and the
    // conversation must exist for this tenant — a mismatch on either is
    // the same 404, so a probing page cannot distinguish a wrong token
    // from a conversation it has no business knowing about. (This is the
    // one widget response that is a 404 rather than a 401/403: the
    // widget treats any refusal here as "this thread is gone" and drops
    // it, so distinguishing them would only help an attacker map ids.)
    let gone = || Problem::not_found().instance(&scope.request_id);
    let Some(token) = query.token.as_deref().filter(|token| !token.is_empty()) else {
        return Err(gone());
    };
    let signer: &dyn Signer = required_port(ctx.ports.signer.as_deref(), "Signer")?;
    if !ConversationClaims::matches(signer, token, &tenant_id, &conversation_id) {
        return Err(gone());
    }

    // Polling is cheap but unbounded in time; an IP bucket (`:poll`)
    // keeps one browser's refresh loop from spending the tenant's
    // budget. It runs before any read, so a refused poll costs the
    // database nothing.
    let remote_ip = client_ip(&headers);
    if let Some(response) = guard_visitor_limit(
        &state,
        &tenant_id,
        None,
        remote_ip.as_deref(),
        ":poll",
        &origin,
    )
    .await
    {
        return Ok(response);
    }

    let db: &dyn Database = required_port(ctx.ports.db.as_deref(), "Db")?;
    let conversation = store::find_conversation_with_status(db, &tenant_id, &conversation_id)
        .await?
        .ok_or_else(gone)?;
    let messages = store::conversation_messages(db, &tenant_id, &conversation_id).await?;
    let transcript: Vec<Value> = messages
        .iter()
        .map(|message| {
            json!({
                "id": message.id,
                "role": message.role,
                "body": message.body,
                "outcome": message.outcome,
                "citations": message
                    .citations
                    .iter()
                    .map(|(chunk_id, quote)| json!({ "chunk_id": chunk_id, "quote": quote }))
                    .collect::<Vec<Value>>(),
                "created_at": message.created_at,
            })
        })
        .collect();
    let mut response = Json(json!({
        "conversation_id": conversation.id,
        "status": conversation.status,
        "needs_escalation": conversation.needs_escalation,
        "messages": transcript,
    }))
    .into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok(stamp_origin(response, &origin))
}

/// `GET /v1/support/w.js` — the widget script, served from the API origin
/// it calls. Wildcard CORS is deliberate and is not the allowlist
/// leaking: the script itself carries no authority, and `*` is what lets
/// an SRI tag with `crossorigin="anonymous"` load it from any page.
///
/// No `Cache-Control` of its own: the harness `security_headers_layer`
/// stamps `Cache-Control: no-store` over every `/v1/*` response, so any
/// value set here is overwritten before it ships. The script is re-fetched
/// per page view — a few KiB from the visitor's nearest colo — and the
/// SRI pin in `widget/w.js.sri` is what actually protects its integrity
/// between fetches.
pub(crate) async fn serve_w_js() -> Response {
    let mut response = (
        [(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        W_JS,
    )
        .into_response();
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    response
}

/// `POST /v1/support/admin/tenants/{tenant_id}/publishable-keys` — mint a
/// second-class key for the web widget, guarded by the harness admin
/// token exactly like `POST /admin/tenants`. The tenant must exist and be
/// active: a key for a tenant that cannot authenticate is junk with a
/// MAC. The key is shown once, exactly like a tenant's first key —
/// nothing about it is stored, so this response cannot be replayed.
pub(crate) async fn create_publishable_key(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    Path(tenant_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    require_admin(&*state.ctx.config, &headers)?;
    let ctx = &state.ctx;
    let signer: &dyn Signer = required_port(ctx.ports.signer.as_deref(), "Signer")?;
    let db: &dyn Database = required_port(ctx.ports.db.as_deref(), "Db")?;
    match store::find_tenant(db, &tenant_id).await? {
        Some(tenant) if tenant.status == store::STATUS_ACTIVE => {}
        // Unknown, suspended, closed: one 404, the way the settings
        // route answers the same question.
        _ => return Err(Problem::not_found().instance(&scope.request_id)),
    }
    let minted = tenancy::mint_publishable(signer, &tenant_id)
        .map_err(|_| Problem::internal().with_detail("publishable key minting failed"))?;
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "key": minted.key,
            "kid": minted.kid,
            "tenant_id": minted.tenant_id,
        })),
    )
        .into_response())
}

/// Verifies a presented publishable key and returns the tenant id every
/// downstream query filters on — the widget twin of
/// [`crate::handlers::authenticate`], with the same collapse: every
/// failure path, including a *secret* key presented at this door, is the
/// one indistinguishable 401.
async fn authenticate_widget(ctx: &ModuleContext, presented: &str) -> Result<String, Problem> {
    let unauthorized = || Problem::new(&UNAUTHORIZED);
    let Some(signer) = ctx.ports.signer.as_deref() else {
        return Err(unauthorized());
    };
    let module = ModuleConfig::new(crate::MODULE_NAME, ctx.config.as_ref());
    let revoked = tenancy::parse_revoked_kids(
        &module
            .get_opt(tenancy::REVOKED_KIDS_KEY)
            .unwrap_or_default(),
    );
    let Ok(tenant_key) = tenancy::verify_publishable(signer, presented, &revoked) else {
        return Err(unauthorized());
    };
    let db = required_port(ctx.ports.db.as_deref(), "Db")?;
    match store::find_tenant(db, &tenant_key.tenant_id).await {
        Ok(Some(tenant)) if tenant.status == store::STATUS_ACTIVE => Ok(tenant.id),
        Ok(_) => Err(unauthorized()),
        // A database outage is an infrastructure failure, not evidence
        // about the key: it gets the 500 the `DbError` carries, not the
        // 401 above.
        Err(err) => Err(err.into()),
    }
}

/// The origin allowlist check: `Origin` present, a parseable origin, and
/// exactly equal to one stored entry. The tenant's settings row is the
/// allowlist; nothing stored — or an explicitly empty list — refuses
/// every page. The `Problem` this returns carries no CORS header, so the
/// browser leaves the refusal unreadable to the page that caused it.
async fn check_origin(
    ctx: &ModuleContext,
    tenant_id: &str,
    headers: &HeaderMap,
) -> Result<String, Problem> {
    let refused = |detail: String| Problem::new(&ORIGIN_NOT_ALLOWED).with_detail(detail);
    let Some(raw) = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
    else {
        return Err(refused("the Origin header is required".to_owned()));
    };
    let origin = origin_of(raw).map_err(|_| refused(format!("{raw:?} is not a valid origin")))?;
    let db = required_port(ctx.ports.db.as_deref(), "Db")?;
    let stored = store::find_tenant_settings(db, tenant_id).await?;
    let allowed = store::parse_widget_origins(
        stored
            .as_ref()
            .map(|settings| settings.widget_origins.as_deref())
            .unwrap_or_default(),
    );
    if allowed.iter().any(|allowed| allowed == &origin) {
        Ok(origin)
    } else {
        Err(refused("not on this tenant's widget allowlist".to_owned()))
    }
}

/// Validates and normalizes the origins an admin submitted for the
/// allowlist: each must be a bare origin (`scheme://host[:port]` — no
/// path, no query, no `*`), parsed through the same RFC 6454 serializer
/// the request-time comparison uses, so what an admin writes is
/// byte-for-byte what a request must present. `https` always; plain
/// `http` only for `localhost` / `127.0.0.1` (any port), so a staging
/// page can be listed without opening cleartext origins in production.
/// Order is preserved, duplicates dropped; an empty list is legal and
/// means the widget is closed.
///
/// # Errors
///
/// A readable message naming the offending entry, for the 400 the
/// settings route renders.
pub(crate) fn normalize_widget_origins(raw: &[String]) -> Result<Vec<String>, String> {
    let mut normalized: Vec<String> = Vec::with_capacity(raw.len());
    for entry in raw {
        if entry.contains('*') {
            return Err(format!(
                "widget_origins: {entry:?} contains '*' — the allowlist is exact origins only"
            ));
        }
        let origin = origin_of(entry).map_err(|err| {
            format!(
                "widget_origins: {entry:?} is not a bare origin like \"https://support.example\" \
                 ({err})"
            )
        })?;
        // `http` is for the developer's own machine and nothing else: the
        // normalized form is `http://host[:port]`, so the host is the
        // text between the scheme and the first `:` (or the end).
        let insecure_ok = origin.strip_prefix("http://").is_some_and(|rest| {
            let host = rest.split(':').next().unwrap_or_default();
            host == "localhost" || host == "127.0.0.1"
        });
        if !origin.starts_with("https://") && !insecure_ok {
            return Err(format!(
                "widget_origins: {origin} is not https (plain http is allowed only for localhost \
                 / 127.0.0.1)"
            ));
        }
        if !normalized.contains(&origin) {
            normalized.push(origin);
        }
    }
    Ok(normalized)
}

/// The visitor limiter: the module-owned one when the composition built
/// it, else the context's — the same port the tenant budget runs on — so
/// a deployment without a second binding still gets both buckets at a
/// shared budget rather than none at all.
fn visitor_limiter(state: &ModuleState) -> Option<&Arc<dyn cratefield_core::RateLimiter>> {
    state
        .visitor_rate_limiter
        .as_ref()
        .or(state.ctx.ports.rate_limiter.as_ref())
}

/// The visitor buckets of one widget request — per visitor id, and per
/// client IP when the edge showed one, each suffixed `:poll` on the
/// transcript route so polling does not spend a message slot. Keys are
/// namespaced per tenant so two tenants sharing one limiter cannot spend
/// each other's budgets. `FailClosed` for the same reason the tenant
/// budget is: the limiter is the only thing between an anonymous visitor
/// and a paid model call.
async fn guard_visitor_limit(
    state: &ModuleState,
    tenant_id: &str,
    vid: Option<&str>,
    remote_ip: Option<&str>,
    suffix: &str,
    origin: &str,
) -> Option<Response> {
    let mut keys = Vec::new();
    if let Some(vid) = vid {
        keys.push(format!("support-widget:{tenant_id}:v:{vid}{suffix}"));
    }
    if let Some(ip) = remote_ip {
        keys.push(format!("support-widget:{tenant_id}:ip:{ip}{suffix}"));
    }
    if keys.is_empty() {
        return None;
    }
    if let RateLimit::Denied { decision } =
        check_rate_limit(visitor_limiter(state), &keys, RateLimitFailure::FailClosed).await
    {
        return Some(stamp_origin(rate_limited(&decision), origin));
    }
    None
}

/// The captcha gate. Demanded once the visitor has completed
/// [`captcha_after`] turns without ever having passed one; passing once
/// latches `human` into the visitor token, which is why this takes
/// `&mut VisitorClaims`. The posture mirrors the harness's own
/// `verify_human_form`: a present port must verify the token — a
/// non-`ok` verdict or a transport error fails closed; an absent port
/// refuses in production unless the operator recorded
/// `HARNESS_ALLOW_UNPROTECTED_WRITES`, and stands down below production
/// so a staging deploy can drive the widget without a live Turnstile.
///
/// The refusal is hand-built because `Problem` cannot carry `site_key`.
async fn require_captcha(
    state: &ModuleState,
    visitor: &mut VisitorClaims,
    captcha_token: Option<&str>,
    remote_ip: Option<&str>,
    scope: &Scope,
    origin: &str,
) -> Option<Response> {
    if visitor.human {
        return None;
    }
    if visitor.n < captcha_after(&state.ctx) {
        return None;
    }
    // No port: the production posture is the harness's own — refuse,
    // unless the operator accepted serving unprotected. Below production
    // the gate stands down (the limiter still bounds the visitor).
    let Some(captcha) = state.ctx.ports.captcha.as_ref() else {
        let env = deployed_env(state.ctx.venture.env, state.ctx.config.as_ref());
        return if env == VentureEnv::Production && !state.ctx.unprotected_writes_accepted {
            Some(captcha_refusal(state, scope, origin))
        } else {
            None
        };
    };
    // No token is a refusal — the port being wired does not soften the
    // demand, it enforces it.
    let Some(token) = captcha_token else {
        return Some(captcha_refusal(state, scope, origin));
    };
    match captcha.verify(token, remote_ip).await {
        Ok(verdict) if verdict.ok => {
            visitor.human = true;
            None
        }
        _ => Some(captcha_refusal(state, scope, origin)),
    }
}

/// The configured captcha threshold: `SUPPORT_WIDGET_CAPTCHA_AFTER`, or
/// [`DEFAULT_CAPTCHA_AFTER`].
fn captcha_after(ctx: &ModuleContext) -> u32 {
    ModuleConfig::new(crate::MODULE_NAME, ctx.config.as_ref())
        .get_u32("WIDGET_CAPTCHA_AFTER", DEFAULT_CAPTCHA_AFTER)
}

/// The current unix time in whole seconds — the unit `Payload::exp`
/// speaks. Before 1970 is nonsense for a clock and mints an
/// already-expired token: fail-closed, the same side an absent clock
/// errs on.
fn now_secs(ctx: &ModuleContext) -> Result<u64, Problem> {
    let clock: &dyn Clock = required_port(ctx.ports.clock.as_deref(), "Clock")?;
    Ok(u64::try_from(clock.now().unix_timestamp()).unwrap_or(0))
}

/// The `site_key` the widget renders the challenge with: config
/// `SUPPORT_TURNSTILE_SITE_KEY`, JSON `null` when the deployment has not
/// set one — the widget then says so in words instead of rendering a
/// challenge that can never pass.
fn turnstile_site_key(ctx: &ModuleContext) -> Option<String> {
    ModuleConfig::new(crate::MODULE_NAME, ctx.config.as_ref())
        .get_opt("TURNSTILE_SITE_KEY")
        .filter(|key| !key.is_empty())
}

/// The captcha refusal: problem+json with the `site_key` extension
/// member, stamped with the origin so the widget may read it. Built as a
/// [`Problem`] so the harness names its `type` under the serving venture's
/// own base (cratefield-core 0.7); the extension member survives that
/// re-render.
fn captcha_refusal(state: &ModuleState, scope: &Scope, origin: &str) -> Response {
    let problem = Problem::new(&WIDGET_CAPTCHA_REQUIRED)
        .with_detail(WIDGET_CAPTCHA_REQUIRED.description)
        .instance(&scope.request_id)
        .with_extension("site_key", turnstile_site_key(&state.ctx));
    stamp_origin(problem.into_response(), origin)
}

/// Stamps a response with the request's own origin and `Vary: Origin` —
/// every widget response after the allowlist check, because the body now
/// depends on that header and a cache must not hand one page's answer to
/// another origin. A member the venture `CorsLayer` already wrote (the
/// page may also be on the venture allowlist) is left alone: the value
/// is the same origin, and the member appearing twice is exactly the
/// kind of response some browsers refuse.
fn stamp_origin(mut response: Response, origin: &str) -> Response {
    if let Ok(value) = HeaderValue::from_str(origin) {
        let headers = response.headers_mut();
        let already = headers
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .is_some_and(|existing| existing == value);
        if !already {
            headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, value);
        }
    }
    // `Vary` is appended, never overwritten or duplicated: another
    // header's directives (`Vary: Accept-Encoding` from a layer below)
    // must survive, and a second `Origin` member in one field value is
    // precisely the doubled header some caches and browsers choke on.
    let varies_on_origin = response
        .headers()
        .get_all(header::VARY)
        .iter()
        .any(|value| match value.to_str() {
            Ok(value) => value
                .split(',')
                .any(|member| member.trim().eq_ignore_ascii_case("origin")),
            Err(_) => false,
        });
    if !varies_on_origin {
        response
            .headers_mut()
            .append(header::VARY, HeaderValue::from_static("Origin"));
    }
    response
}
