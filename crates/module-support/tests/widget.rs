//! Issue #33 acceptance: publishable keys, the browser widget's CORS-simple routes and
//! its per-visitor abuse controls, over every available dialect. Widget requests carry
//! a key in body or query and an `Origin` (or deliberately none) — never an
//! `Authorization` header — and nothing before the origin check is readable by a page,
//! while everything after it is stamped with exactly that origin.

use async_trait::async_trait;
use axum::body::{Body, to_bytes};
use axum::http::{HeaderMap, HeaderName, Method, Request, StatusCode as SC, header};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use cratefield_core::{
    Clock, Completion, Decision, HmacSigner, MapConfig, Module, RateLimitError, RateLimiter,
    Signer, Statement,
};
use cratefield_testing::{
    Dialect, FakeCaptcha, FakeTextModel, TEST_HARNESS_SECRET, TestHarness, TextModelMode,
};
use module_support::Support;
use serde_json::{Value, json};
use sha2::Digest;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use time::OffsetDateTime;
use tower::ServiceExt;

const ADMIN_TOKEN: &str = "test-admin-token-0123456789abcdef";
const ADMIN: &str = "/v1/support/admin/tenants";
/// The problem `type` base: since cratefield-core 0.7 every problem is named
/// under the serving venture's own `<public_url>/problems/`, and
/// `TestHarness` serves as `https://test.example`.
const PROBLEMS: &str = "https://test.example/problems/";
const MESSAGES: &str = "/v1/support/widget/messages";
const W_JS: &str = "/v1/support/w.js";
const CONV_PURPOSE: &str = "support.widget-conversation";
const VISITOR_PURPOSE: &str = "support.widget-visitor";
const QUESTION: &str = "How do I reset my password?";
const RESET_DOC: &str = "Reset your password from the settings page under Security.";
/// The origin a well-behaved page sends — deliberately *not* on the venture's own
/// CORS allowlist, so every ACAO here was written by the handler, not a layer.
const PAGE: &str = "https://shop.example";
/// The client IP every request the IP bucket cares about sends.
const IP: &str = "203.0.113.7";
/// What a downgraded turn shows instead of the model's words.
const CLARIFY_BODY: &str = "I want to give you an accurate answer rather than a fast wrong \
     one — could you rephrase the question or add a little more detail?";

/// A buffered JSON response; parsed once, eagerly, like `routes.rs`.
struct Reply {
    status: SC,
    headers: HeaderMap,
    body: Value,
}

impl Reply {
    #[rustfmt::skip]
    async fn of(response: axum::response::Response) -> Self {
        let (parts, body) = response.into_parts();
        let bytes = to_bytes(body, 1024 * 1024).await.expect("body reads");
        let body = if bytes.is_empty() { Value::Null }
            else { serde_json::from_slice(&bytes).expect("body is JSON") };
        Self { status: parts.status, headers: parts.headers, body }
    }

    /// The problem `type` URI — what every refusal assertion hangs on.
    fn problem_type(&self) -> String {
        self.body["type"].as_str().unwrap_or_default().to_owned()
    }

    fn acao(&self) -> Option<&str> {
        self.header(&header::ACCESS_CONTROL_ALLOW_ORIGIN)
    }

    fn header(&self, name: &HeaderName) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }
}

// ----- requests ---------------------------------------------------------

/// A request with arbitrary headers and a raw body — never an `Authorization` header (CORS-simple).
#[rustfmt::skip]
async fn send_raw(
    router: &axum::Router, method: Method, path: &str,
    origin: Option<&str>, ip: Option<&str>, body: Option<String>,
) -> Reply {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(origin) = origin { builder = builder.header(header::ORIGIN, origin); }
    if let Some(ip) = ip { builder = builder.header("cf-connecting-ip", ip); }
    let request = builder.body(body.map_or_else(Body::empty, Body::from)).expect("built");
    let response = router.clone().oneshot(request).await.expect("answers");
    Reply::of(response).await
}

/// `POST /widget/messages` as the widget sends it: JSON as `text/plain`, so the request stays CORS-simple.
#[rustfmt::skip]
async fn widget_post(router: &axum::Router, body: Value,
                     origin: Option<&str>, ip: Option<&str>) -> Reply {
    send_raw(router, Method::POST, MESSAGES, origin, ip, Some(body.to_string())).await
}

/// A turn from the allowed page; `ip` is the only variability left.
async fn turn(kit: &TestHarness, body: Value, ip: Option<&str>) -> Reply {
    widget_post(&kit.router, body, Some(PAGE), ip).await
}

/// A `GET` for a transcript URL, from the allowed page.
async fn poll(kit: &TestHarness, path: &str, ip: Option<&str>) -> Reply {
    send_raw(&kit.router, Method::GET, path, Some(PAGE), ip, None).await
}

/// A raw `GET` whose body is JavaScript, not JSON: full response parts.
#[rustfmt::skip]
async fn get_bytes(router: &axum::Router, path: &str) -> (SC, HeaderMap, Vec<u8>) {
    let request = Request::builder().method(Method::GET).uri(path).body(Body::empty()).expect("built");
    let response = router.clone().oneshot(request).await.expect("answers");
    let (parts, body) = response.into_parts();
    let bytes = to_bytes(body, 1024 * 1024).await.expect("body reads").to_vec();
    (parts.status, parts.headers, bytes)
}

/// A bearer-authenticated request: any of the three keys.
#[rustfmt::skip]
async fn bearer_send(router: &axum::Router, method: Method, path: &str,
                     key: &str, body: Option<Value>) -> Reply {
    let mut builder = Request::builder().method(method).uri(path)
        .header(header::AUTHORIZATION, format!("Bearer {key}"));
    if body.is_some() { builder = builder.header(header::CONTENT_TYPE, "application/json"); }
    let payload = body.map_or_else(Body::empty, |v| Body::from(v.to_string()));
    let response = router.clone().oneshot(builder.body(payload).expect("built")).await.expect("answers");
    Reply::of(response).await
}

#[rustfmt::skip]
async fn admin_send(router: &axum::Router, method: Method, path: &str,
                    body: Option<Value>) -> Reply {
    bearer_send(router, method, path, ADMIN_TOKEN, body).await
}

// ----- assertions -------------------------------------------------------

/// The shared refusal shape: `status` plus the module's problem `type`.
#[rustfmt::skip]
fn expect_problem(reply: &Reply, status: SC, slug: &str) {
    assert_eq!(reply.status, status, "{}", reply.body);
    assert_eq!(reply.problem_type(), format!("{PROBLEMS}{slug}"));
}

/// The 429 quadruple: status, slug, an origin stamp so the earning page can read it, and `Retry-After`.
#[rustfmt::skip]
fn assert_rate_limited(reply: &Reply, origin: &str, retry_after: &str) {
    expect_problem(reply, SC::TOO_MANY_REQUESTS, "rate-limited");
    assert_eq!(reply.acao(), Some(origin), "readable by the page that earned it");
    assert_eq!(reply.header(&header::RETRY_AFTER), Some(retry_after));
}

/// Percent-encodes a query value; the tokens in play (`sg_pub_…`,
/// base64url, ULIDs) are URL-safe apart from `=` padding.
#[rustfmt::skip]
fn urlencode(value: &str) -> String {
    value.replace('=', "%3D")
}

/// The claims inside a visitor token, verified against the kit's signer.
fn visitor_claims(kit: &TestHarness, token: &str) -> Value {
    let payload = kit.signer.verify(token, VISITOR_PURPOSE).expect("verifies");
    serde_json::from_str(&payload.subject).expect("claims are JSON")
}

// ----- kits -------------------------------------------------------------

#[rustfmt::skip]
fn support(kit_limiter: Option<Arc<CountingLimiter>>) -> Vec<Box<dyn Module>> {
    vec![Box::new(Support::new().visitor_rate_limiter(kit_limiter.map(|l| l as _)))]
}

/// A kit whose widget routes see `captcha` and `extra` config pairs over the admin token; model is [`clarify_mode`].
#[rustfmt::skip]
fn widget_kit(dialect: Dialect, captcha: FakeCaptcha, extra: &[(&'static str, &str)],
              visitor_limiter: Option<Arc<CountingLimiter>>) -> TestHarness {
    let model = FakeTextModel::new(clarify_mode());
    TestHarness::with_database_and_ports(support(visitor_limiter), dialect, |ports| {
        let mut pairs = vec![("ADMIN_TOKEN", ADMIN_TOKEN)];
        pairs.extend(extra.iter().copied());
        ports.config = Arc::new(MapConfig::from_pairs(pairs));
        ports.captcha = Some(Arc::new(captcha));
        ports.text_model = Some(Arc::new(model.clone()));
    })
}

fn default_kit(dialect: Dialect, visitor_limiter: Option<Arc<CountingLimiter>>) -> TestHarness {
    widget_kit(dialect, FakeCaptcha::allow_all(), &[], visitor_limiter)
}

/// No module-owned visitor limiter; `limiter` is the context port — the one the tenant budget runs on.
fn context_kit(dialect: Dialect, limiter: Arc<dyn RateLimiter>) -> TestHarness {
    TestHarness::with_database_and_ports(support(None), dialect, |ports| {
        ports.config = Arc::new(MapConfig::from_pairs([("ADMIN_TOKEN", ADMIN_TOKEN)]));
        ports.captcha = Some(Arc::new(FakeCaptcha::allow_all()));
        ports.text_model = Some(Arc::new(FakeTextModel::new(clarify_mode())));
        ports.rate_limiter = Some(limiter);
    })
}

/// The scripted answer every widget test runs on: low confidence, so every turn
/// downgrades to `clarify` — the shown-words-vs-model-words gap the transcript asserts.
#[rustfmt::skip]
fn clarify_mode() -> TextModelMode {
    let value = json!({ "answer": "the model's private guess", "citations": [], "confidence": 0.1 });
    TextModelMode::Complete(Completion::new(value.to_string(), "fake-fast").json(value))
}

#[rustfmt::skip]
fn kits() -> Vec<TestHarness> {
    Dialect::available().into_iter().map(|d| default_kit(d, None)).collect()
}

// ----- fixtures ---------------------------------------------------------

/// Everything a widget request needs to pass the guards, plus one real turn to poll.
struct Seeded {
    api_key: String,
    pub_key: String,
    tenant_id: String,
    conversation_id: String,
    conversation_token: String,
    visitor_token: String,
}

/// One `POST /admin/tenants`, asserted; returns the id and secret key.
#[rustfmt::skip]
async fn create_tenant(kit: &TestHarness, name: &str) -> (String, String) {
    let body = Some(json!({ "name": name }));
    let reply = admin_send(&kit.router, Method::POST, ADMIN, body).await;
    assert_eq!(reply.status, SC::CREATED, "{}", reply.body);
    let id = reply.body["tenant_id"].as_str().expect("tenant id").to_owned();
    let key = reply.body["api_key"].as_str().expect("api key").to_owned();
    (id, key)
}

/// Mints a publishable key through the admin route, asserting the wire shape once for all.
#[rustfmt::skip]
async fn mint_publishable(kit: &TestHarness, tenant_id: &str) -> String {
    let path = format!("{ADMIN}/{tenant_id}/publishable-keys");
    let reply = admin_send(&kit.router, Method::POST, &path, None).await;
    assert_eq!(reply.status, SC::CREATED, "{}", reply.body);
    let key = reply.body["key"].as_str().expect("key").to_owned();
    assert!(key.starts_with("sg_pub_"), "publishable prefix: {key}");
    assert_eq!(reply.body["tenant_id"], tenant_id);
    assert!(reply.body["kid"].as_str().is_some_and(|kid| !kid.is_empty()));
    key
}

async fn put_settings(kit: &TestHarness, tenant_id: &str, body: Value) -> Reply {
    let path = format!("{ADMIN}/{tenant_id}/settings");
    admin_send(&kit.router, Method::PUT, &path, Some(body)).await
}

/// Allowlists `origins`, asserted; normalization is the server's job — callers may hand it cruft.
async fn allow_origins(kit: &TestHarness, tenant_id: &str, origins: &[&str]) -> Reply {
    let reply = put_settings(kit, tenant_id, json!({ "widget_origins": origins })).await;
    assert_eq!(reply.status, SC::OK, "{}", reply.body);
    reply
}

/// A second tenant on the same kit, same allowed page: id and key.
async fn twin_tenant(kit: &TestHarness) -> (String, String) {
    let (other_id, _key) = create_tenant(kit, "Other").await;
    let other_pub = mint_publishable(kit, &other_id).await;
    allow_origins(kit, &other_id, &[PAGE]).await;
    (other_id, other_pub)
}

#[rustfmt::skip]
async fn seed(kit: &TestHarness) -> Seeded {
    let (tenant_id, api_key) = create_tenant(kit, "Acme Support").await;
    let body = json!({ "title": "Help", "text": RESET_DOC });
    let ingest = bearer_send(&kit.router, Method::POST, "/v1/support/sources", &api_key, Some(body)).await;
    assert_eq!(ingest.status, SC::CREATED, "{}", ingest.body);
    let pub_key = mint_publishable(kit, &tenant_id).await;
    allow_origins(kit, &tenant_id, &[PAGE]).await;
    let first = turn(kit, json!({ "key": pub_key, "message": QUESTION }), Some(IP)).await;
    assert_eq!(first.status, SC::OK, "{}", first.body);
    Seeded {
        api_key, pub_key, tenant_id,
        conversation_id: first.body["conversation_id"].as_str().expect("id").to_owned(),
        conversation_token: first.body["conversation_token"].as_str().expect("token").to_owned(),
        visitor_token: first.body["visitor"].as_str().expect("visitor").to_owned(),
    }
}

/// The transcript URL for a seeded conversation: key and token ride the query string.
#[rustfmt::skip]
fn transcript_path(seeded: &Seeded) -> String {
    format!("/v1/support/widget/conversations/{}?key={}&token={}",
        seeded.conversation_id, urlencode(&seeded.pub_key), urlencode(&seeded.conversation_token))
}

// ----- limiters ---------------------------------------------------------

/// A `RateLimiter` that allows `visitor_max` per visitor-key and `ip_max` per
/// IP-key, then denies — the widget's buckets made observable.
struct CountingLimiter {
    visitor_max: u32,
    ip_max: u32,
    calls: Mutex<HashMap<String, u32>>,
}

impl CountingLimiter {
    #[rustfmt::skip]
    fn new(visitor_max: u32, ip_max: u32) -> Arc<Self> {
        Arc::new(Self { visitor_max, ip_max, calls: Mutex::new(HashMap::new()) })
    }

    #[rustfmt::skip]
    fn seen(&self, key: &str) -> u32 {
        self.calls.lock().expect("lock").get(key).copied().unwrap_or(0)
    }
}

#[async_trait]
impl RateLimiter for CountingLimiter {
    #[rustfmt::skip]
    async fn limit(&self, key: &str) -> Result<Decision, RateLimitError> {
        let max = if key.contains(":ip:") { self.ip_max } else { self.visitor_max };
        let mut calls = self.calls.lock().expect("lock");
        let count = calls.entry(key.to_owned()).or_insert(0);
        *count += 1;
        let ok = *count <= max;
        drop(calls);
        Ok(Decision { ok, retry_after: (!ok).then(|| Duration::from_secs(30)), quota: None })
    }
}

/// Denies exactly the keys `matches` picks (after `retry` seconds): the budget key, the `:poll` suffix.
struct Deny(fn(&str) -> bool, u64);

#[async_trait]
impl RateLimiter for Deny {
    #[rustfmt::skip]
    async fn limit(&self, key: &str) -> Result<Decision, RateLimitError> {
        let ok = !(self.0)(key);
        Ok(Decision { ok, retry_after: (!ok).then(|| Duration::from_secs(self.1)), quota: None })
    }
}

#[pollster::test]
async fn a_publishable_key_opens_no_secret_or_admin_route() {
    for kit in kits() {
        let seeded = seed(&kit).await;
        let (key, tenant_id) = (seeded.pub_key.clone(), seeded.tenant_id.clone());

        // Every secret-key route plus the admin routes; a publishable key opens none
        // of them. The 403 (not 401): a bearer was presented, it is simply the wrong one.
        let sources = "/v1/support/sources";
        let doc = || Some(json!({ "title": "t", "text": "x" }));
        let msg = Some(json!({ "message": "hi" }));
        let keys_path = format!("{ADMIN}/{tenant_id}/publishable-keys");
        let settings_path = format!("{ADMIN}/{tenant_id}/settings");
        #[rustfmt::skip]
        let routes: Vec<(Method, String, SC, &str, Option<Value>)> = vec![
            (Method::POST, sources.into(), SC::UNAUTHORIZED, "unauthorized", doc()),
            (Method::GET, sources.into(), SC::UNAUTHORIZED, "unauthorized", None),
            (Method::GET, format!("{sources}/some-id"), SC::UNAUTHORIZED, "unauthorized", None),
            (Method::PUT, format!("{sources}/some-id"), SC::UNAUTHORIZED, "unauthorized", doc()),
            (Method::DELETE, format!("{sources}/some-id"), SC::UNAUTHORIZED, "unauthorized", None),
            (Method::GET, "/v1/support/search?q=reset".into(), SC::UNAUTHORIZED, "unauthorized", None),
            (Method::POST, "/v1/support/messages".into(), SC::UNAUTHORIZED, "unauthorized", msg),
            // Key management (#37): a page-readable key can neither list, mint nor
            // revoke the tenant's secret keys.
            (Method::GET, "/v1/support/keys".into(), SC::UNAUTHORIZED, "unauthorized", None),
            (Method::POST, "/v1/support/keys".into(), SC::UNAUTHORIZED, "unauthorized", Some(json!({}))),
            (Method::DELETE, "/v1/support/keys/some-kid".into(), SC::UNAUTHORIZED, "unauthorized", None),
            (Method::POST, ADMIN.into(), SC::FORBIDDEN, "admin-forbidden", Some(json!({ "name": "X" }))),
            (Method::PUT, settings_path, SC::FORBIDDEN, "admin-forbidden", None),
            (Method::POST, keys_path, SC::FORBIDDEN, "admin-forbidden", None),
        ];
        for (method, path, expected, slug, body) in routes {
            let label = format!("{method} {path}");
            let reply = bearer_send(&kit.router, method, &path, &key, body).await;
            assert_eq!(reply.status, expected, "{label}: {}", reply.body);
            assert_eq!(reply.problem_type(), format!("{PROBLEMS}{slug}"), "{label}");
        }

        // The other face of the wall: the secret key is equally worthless at the
        // widget door, refused before any origin check — no CORS header to read.
        let body = json!({ "key": seeded.api_key, "message": QUESTION });
        let secret_at_widget = turn(&kit, body, None).await;
        expect_problem(&secret_at_widget, SC::UNAUTHORIZED, "unauthorized");
        assert!(secret_at_widget.acao().is_none());

        // Minting itself: no bearer is a 401 (the admin guard answers before the
        // tenant lookup), and an unknown tenant is a 404, not a 400 — the id is never
        // valid input, so it must not read as "wrong shape".
        let path = format!("{ADMIN}/x/publishable-keys");
        let unauth = send_raw(&kit.router, Method::POST, &path, None, None, None).await;
        assert_eq!(unauth.status, SC::UNAUTHORIZED);
        let path = format!("{ADMIN}/does-not-exist/publishable-keys");
        let missing = admin_send(&kit.router, Method::POST, &path, None).await;
        assert_eq!(missing.status, SC::NOT_FOUND, "{}", missing.body);
    }
}

#[pollster::test]
async fn the_origin_allowlist_gates_the_widget_and_stamps_every_answer() {
    for kit in kits() {
        let (tenant_id, _key) = create_tenant(&kit, "Acme Support").await;
        let pub_key = mint_publishable(&kit, &tenant_id).await;
        let body = || json!({ "key": pub_key, "message": QUESTION });

        // No allowlist yet: closed whichever way the origin is wrong, unreadable (no ACAO).
        for origin in [Some(PAGE), Some("https://evil.example"), None] {
            let reply = widget_post(&kit.router, body(), origin, None).await;
            expect_problem(&reply, SC::FORBIDDEN, "origin-not-allowed");
            assert!(reply.acao().is_none(), "origin {origin:?} gets no ACAO");
        }

        // Listing origins opens the widget for exactly those; written cruft is normalized.
        let origins = [PAGE, "https://other.example:443/some/path"];
        let put = allow_origins(&kit, &tenant_id, &origins).await;
        #[rustfmt::skip]
        assert_eq!(put.body["widget_origins"], json!([PAGE, "https://other.example"]));
        assert_eq!(put.body["answer_threshold"], 0.6, "the documented default");

        let ok = turn(&kit, body(), None).await;
        assert_eq!(ok.status, SC::OK, "{}", ok.body);
        assert_eq!(ok.acao(), Some(PAGE));
        assert_eq!(ok.header(&header::VARY), Some("Origin"));

        let evil = widget_post(&kit.router, body(), Some("https://evil.example"), None).await;
        expect_problem(&evil, SC::FORBIDDEN, "origin-not-allowed");
        assert!(evil.acao().is_none());

        // Normalization runs both ways: stored cruft collapses to the RFC 6454 form,
        // and a request origin needing the same treatment matches its entry, answered
        // with the normalized stamp. `Origin: null` is refused like any non-member.
        let stored = allow_origins(&kit, &tenant_id, &["HTTPS://Shop.Example:443/page"]).await;
        #[rustfmt::skip]
        assert_eq!(stored.body["widget_origins"], json!([PAGE]), "{}", stored.body);
        let ok = widget_post(&kit.router, body(), Some("HTTPS://Shop.Example:443"), None).await;
        assert_eq!(ok.status, SC::OK, "{}", ok.body);
        assert_eq!(ok.acao(), Some(PAGE), "the stamp is the normalized form");
        let nul = widget_post(&kit.router, body(), Some("null"), None).await;
        expect_problem(&nul, SC::FORBIDDEN, "origin-not-allowed");
        assert!(nul.acao().is_none(), "null must be unreadable to the page");
    }
}

#[pollster::test]
async fn a_widget_turn_answers_with_tokens_and_continues_the_conversation() {
    for kit in kits() {
        let seeded = seed(&kit).await;

        // The first reply carried both tokens; the visitor token counts this turn.
        let first = visitor_claims(&kit, &seeded.visitor_token);
        assert_eq!(first["tenant"], seeded.tenant_id);
        assert_eq!(first["n"], 1);
        assert_eq!(first["human"], false);

        // The same visitor (token echoed) continues; the count advances, vid stays.
        #[rustfmt::skip]
        let body = json!({ "key": seeded.pub_key, "message": "and via email?",
            "conversation_id": seeded.conversation_id, "visitor": seeded.visitor_token });
        let second = turn(&kit, body, Some(IP)).await;
        assert_eq!(second.status, SC::OK, "{}", second.body);
        assert_eq!(second.body["conversation_id"], seeded.conversation_id);
        assert!(second.body["answer"].is_string());
        assert!(second.body["citations"].is_array());
        let token = second.body["visitor"].as_str().expect("visitor");
        let again = visitor_claims(&kit, token);
        assert_eq!(again["vid"], first["vid"]);
        assert_eq!(again["n"], 2);

        // The conversation token names exactly this conversation.
        #[rustfmt::skip]
        let payload = kit.signer.verify(&seeded.conversation_token, CONV_PURPOSE)
            .expect("verifies");
        let claims: Value = serde_json::from_str(&payload.subject).unwrap();
        assert_eq!(claims["tenant"], seeded.tenant_id);
        assert_eq!(claims["conversation_id"], seeded.conversation_id);

        // A junk visitor token is not an error: anonymous again, the turn still answers.
        let junk = json!({ "key": seeded.pub_key, "message": QUESTION, "visitor": "not-a-token" });
        assert_eq!(turn(&kit, junk, None).await.status, SC::OK);
    }
}

#[pollster::test]
async fn visitor_buckets_are_per_visitor_and_per_ip() {
    for dialect in Dialect::available() {
        // One turn per visitor id, two per client IP; seed's turn spent A's slot and
        // one of the IP's two, so the assertions below count only the test's requests.
        let limiter = CountingLimiter::new(1, 2);
        let kit = default_kit(dialect.clone(), Some(limiter.clone()));
        let seeded = seed(&kit).await;
        let as_a = || json!({ "key": seeded.pub_key, "message": QUESTION, "visitor": seeded.visitor_token });
        let anonymous = || json!({ "key": seeded.pub_key, "message": QUESTION });

        // A again: the per-visitor bucket is spent — 429, readable by its page.
        assert_rate_limited(&turn(&kit, as_a(), Some(IP)).await, PAGE, "30");

        // B, same page and IP, new vid: allowed — per visitor; spends the IP's last slot.
        let b = turn(&kit, anonymous(), Some(IP)).await;
        assert_eq!(b.status, SC::OK, "{}", b.body);
        // A third visitor on the same IP now meets the IP bucket.
        let third = turn(&kit, anonymous(), Some(IP)).await;
        assert_eq!(third.status, SC::TOO_MANY_REQUESTS);

        // Both bucket shapes were consulted, tenant-namespaced.
        let claims = visitor_claims(&kit, &seeded.visitor_token);
        let vid = claims["vid"].as_str().expect("vid");
        let v_key = format!("support-widget:{}:v:{vid}", seeded.tenant_id);
        let ip_key = format!("support-widget:{}:ip:{IP}", seeded.tenant_id);
        assert!(limiter.seen(&v_key) >= 2, "per-visitor key consulted");
        assert!(limiter.seen(&ip_key) >= 3, "per-IP key consulted");

        // Polling runs in `:poll` buckets of its own, so a refresh loop never spends
        // a message slot: three polls hit the IP's poll bucket thrice, messages never.
        let poll_limiter = CountingLimiter::new(1000, 1000);
        let kit = default_kit(dialect, Some(poll_limiter.clone()));
        let seeded = seed(&kit).await;
        let path = transcript_path(&seeded);
        for _ in 0..3 {
            assert_eq!(poll(&kit, &path, Some(IP)).await.status, SC::OK);
        }
        let tenant = &seeded.tenant_id;
        let poll_key = format!("support-widget:{tenant}:ip:{IP}:poll");
        assert_eq!(poll_limiter.seen(&poll_key), 3);
        let msg_key = format!("support-widget:{tenant}:ip:{IP}");
        assert_eq!(poll_limiter.seen(&msg_key), 1);
    }
}

/// The context limiter still gates the widget at both doors: the tenant budget
/// runs in front of the turn — the same 429 the API routes answer — and a denied
/// `:poll` bucket runs in front of the transcript read, so nothing is served. Both
/// refusals are origin-stamped, readable by the page that earned them.
#[pollster::test]
async fn the_context_limiter_gates_turn_and_transcript() {
    for dialect in Dialect::available() {
        let budget = context_kit(
            dialect.clone(),
            Arc::new(Deny(|k| k.starts_with("support:"), 9)),
        );
        let (tenant_id, _key) = create_tenant(&budget, "Acme Support").await;
        let pub_key = mint_publishable(&budget, &tenant_id).await;
        allow_origins(&budget, &tenant_id, &[PAGE]).await;
        let body = json!({ "key": pub_key, "message": QUESTION });
        assert_rate_limited(&turn(&budget, body, Some(IP)).await, PAGE, "9");

        let polls = context_kit(dialect, Arc::new(Deny(|k| k.ends_with(":poll"), 5)));
        let seeded = seed(&polls).await;
        let reply = poll(&polls, &transcript_path(&seeded), Some(IP)).await;
        assert_rate_limited(&reply, PAGE, "5");
    }
}

#[pollster::test]
async fn captcha_is_demanded_after_n_turns_and_latches_once_passed() {
    for dialect in Dialect::available() {
        // Full config keys: `MapConfig` reads through the module prefix; suffixes invisible.
        let config = [
            ("SUPPORT_WIDGET_CAPTCHA_AFTER", "2"),
            ("SUPPORT_TURNSTILE_SITE_KEY", "site-key-1"),
        ];
        let kit = widget_kit(dialect, FakeCaptcha::with_tokens(["good"]), &config, None);
        let mut seeded = seed(&kit).await;
        #[rustfmt::skip]
        let owed_body = |token: &str| json!({ "key": seeded.pub_key, "message": QUESTION, "visitor": token });

        // Turn two is still under the threshold.
        let second = turn(&kit, owed_body(&seeded.visitor_token), Some(IP)).await;
        assert_eq!(second.status, SC::OK, "{}", second.body);
        seeded.visitor_token = second.body["visitor"].as_str().expect("visitor").to_owned();

        // Turn three, no token offered: 403 with the site key, readable by the page.
        let owed = turn(&kit, owed_body(&seeded.visitor_token), Some(IP)).await;
        expect_problem(&owed, SC::FORBIDDEN, "widget-captcha-required");
        assert_eq!(owed.body["site_key"], "site-key-1");
        assert_eq!(owed.acao(), Some(PAGE));

        // A wrong token does not pass either.
        let mut wrong = owed_body(&seeded.visitor_token);
        wrong["captcha_token"] = json!("bad");
        assert_eq!(turn(&kit, wrong, Some(IP)).await.status, SC::FORBIDDEN);

        // The right token passes and latches: the reply's token is human, next turn free.
        let mut solved_body = owed_body(&seeded.visitor_token);
        solved_body["captcha_token"] = json!("good");
        let solved = turn(&kit, solved_body, Some(IP)).await;
        assert_eq!(solved.status, SC::OK, "{}", solved.body);
        let token = solved.body["visitor"].as_str().expect("visitor");
        assert_eq!(visitor_claims(&kit, token)["human"], true);
        let body = json!({ "key": seeded.pub_key, "message": QUESTION, "visitor": token });
        assert_eq!(turn(&kit, body, Some(IP)).await.status, SC::OK);
    }
}

/// Visitor buckets are tenant-scoped, two ways. A token minted by tenant A is
/// not portable: on B's widget it is junk exactly like a malformed one, so the
/// visitor starts fresh (new vid, count reset). And on one shared limiter the
/// bucket keys name the tenant, so A's spend never fills B's.
#[pollster::test]
async fn visitor_buckets_are_tenant_scoped() {
    for dialect in Dialect::available() {
        let limiter = CountingLimiter::new(1, 1);
        let kit = default_kit(dialect, Some(limiter.clone()));
        let seeded = seed(&kit).await; // fills A's visitor slot and A's one IP slot

        // A, new visitor, same IP: A's IP bucket is full — 429.
        let body = json!({ "key": seeded.pub_key, "message": QUESTION });
        assert_rate_limited(&turn(&kit, body, Some(IP)).await, PAGE, "30");

        // B, same limiter, same IP, same page: B's buckets are empty.
        let (other_id, other_pub) = twin_tenant(&kit).await;
        let body = json!({ "key": other_pub, "message": QUESTION });
        assert_eq!(turn(&kit, body, Some(IP)).await.status, SC::OK);

        // And the ledger agrees: one key per tenant, no spillover.
        let a_key = format!("support-widget:{}:ip:{IP}", seeded.tenant_id);
        assert_eq!(limiter.seen(&a_key), 2);
        let b_key = format!("support-widget:{other_id}:ip:{IP}");
        assert_eq!(limiter.seen(&b_key), 1);

        // A's token replayed on B's widget (fresh IP: B spent its own one IP slot
        // above): junk like any malformed token, answered 200 with a fresh visitor.
        let old = visitor_claims(&kit, &seeded.visitor_token);
        let body =
            json!({ "key": other_pub, "message": QUESTION, "visitor": seeded.visitor_token });
        let reply = turn(&kit, body, Some("203.0.113.8")).await;
        assert_eq!(reply.status, SC::OK, "{}", reply.body);
        let token = reply.body["visitor"].as_str().expect("visitor");
        let fresh = visitor_claims(&kit, token);
        assert_eq!(fresh["tenant"], other_id, "the token belongs to B");
        assert_eq!(fresh["n"], 1, "B counts this turn on an empty bucket");
        assert_ne!(fresh["vid"], old["vid"], "a fresh vid, not A's");
    }
}

#[pollster::test]
async fn the_transcript_shows_what_the_visitor_was_shown_and_nothing_else() {
    for kit in kits() {
        let seeded = seed(&kit).await;
        let reply = poll(&kit, &transcript_path(&seeded), Some(IP)).await;
        assert_eq!(reply.status, SC::OK, "{}", reply.body);
        // Never served stale; a credential-bearing URL must not sit in a shared cache.
        assert_eq!(reply.header(&header::CACHE_CONTROL), Some("no-store"));
        assert_eq!(reply.acao(), Some(PAGE));
        assert_eq!(reply.body["conversation_id"], seeded.conversation_id);
        assert_eq!(reply.body["status"], "open");
        assert_eq!(reply.body["needs_escalation"], false);
        let messages = reply.body["messages"].as_array().expect("messages");
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["role"], "user");
        assert_eq!(messages[0]["body"], QUESTION);
        assert_eq!(messages[1]["role"], "assistant");
        assert_eq!(messages[1]["outcome"], "clarify");
        // The canned clarify message was shown; the model's words stay in the store.
        assert_eq!(messages[1]["body"], CLARIFY_BODY);
        let rendered = reply.body.to_string();
        #[rustfmt::skip]
        assert!(!rendered.contains("model_answer") && !rendered.contains("private guess"),
            "model_answer must never leave the store: {rendered}");

        // A wrong conversation token reads as "no such conversation", so no refusal
        // maps ids for a probing page; a missing key alone is the one 401.
        let id = &seeded.conversation_id;
        let key = urlencode(&seeded.pub_key);
        let token = urlencode(&seeded.conversation_token);
        #[rustfmt::skip]
        let probes = vec![
            (format!("key={key}&token=not-the-token"), SC::NOT_FOUND),
            (format!("key={key}&token="), SC::NOT_FOUND),
            (format!("token={token}"), SC::UNAUTHORIZED),
        ];
        for (query, expected) in probes {
            let path = format!("/v1/support/widget/conversations/{id}?{query}");
            #[rustfmt::skip]
            assert_eq!(poll(&kit, &path, None).await.status, expected, "query {query}");
        }

        // A key from another tenant — given the same allowlist, so the origin check
        // passes — still cannot read the conversation: the token names *this* tenant,
        // and the mismatch is the same 404 as a wrong token.
        let (_other_id, other_pub) = twin_tenant(&kit).await;
        let other_key = urlencode(&other_pub);
        let path = format!("/v1/support/widget/conversations/{id}?key={other_key}&token={token}");
        assert_eq!(poll(&kit, &path, None).await.status, SC::NOT_FOUND);
    }
}

/// A wall clock the test can move — shared by the signer (which checks `exp`)
/// and the module (which dates the tokens it mints). Starts an hour behind the
/// real clock so ULID timestamps (ULIDs read the real clock) stay behind `exp`.
struct TickClock(Mutex<OffsetDateTime>);

impl Clock for TickClock {
    fn now(&self) -> OffsetDateTime {
        *self.0.lock().expect("lock")
    }
}

#[pollster::test]
async fn an_expired_token_is_a_404_and_a_fresh_visitor() {
    for dialect in Dialect::available() {
        let start = OffsetDateTime::now_utc() - time::Duration::hours(1);
        let clock = Arc::new(TickClock(Mutex::new(start)));
        let kit = TestHarness::with_database_and_ports(support(None), dialect, |ports| {
            ports.config = Arc::new(MapConfig::from_pairs([("ADMIN_TOKEN", ADMIN_TOKEN)]));
            ports.captcha = Some(Arc::new(FakeCaptcha::allow_all()));
            ports.text_model = Some(Arc::new(FakeTextModel::new(clarify_mode())));
            let signer = HmacSigner::new(TEST_HARNESS_SECRET, None).expect("secret is long enough");
            ports.signer = Some(Arc::new(signer.with_clock(clock.clone())));
            ports.clock = Some(clock.clone());
        });
        let seeded = seed(&kit).await;
        let stale_vid = visitor_claims(&kit, &seeded.visitor_token)["vid"].clone();

        // The conversation token's day is gone: the poll route answers the same 404
        // a wrong token gets — the widget's cue to drop this thread and start fresh.
        *clock.0.lock().expect("lock") += time::Duration::hours(25);
        let expired = poll(&kit, &transcript_path(&seeded), Some(IP)).await;
        expect_problem(&expired, SC::NOT_FOUND, "not-found");

        // The visitor outlives its conversation: the day-old token still verifies
        // (its week is not up), the turn lands in the *same* visitor's bucket, and
        // the reply's conversation token — minted now — reads the new thread.
        let body =
            json!({ "key": seeded.pub_key, "message": QUESTION, "visitor": seeded.visitor_token });
        let mid = turn(&kit, body, None).await;
        assert_eq!(mid.status, SC::OK, "{}", mid.body);
        let token = mid.body["visitor"].as_str().expect("visitor");
        #[rustfmt::skip]
        assert_eq!(visitor_claims(&kit, token)["vid"], stale_vid, "visitor not expired");
        let id = mid.body["conversation_id"].as_str().expect("id");
        let conv_token = urlencode(mid.body["conversation_token"].as_str().expect("token"));
        let key_q = urlencode(&seeded.pub_key);
        let path = format!("/v1/support/widget/conversations/{id}?key={key_q}&token={conv_token}");
        assert_eq!(poll(&kit, &path, None).await.status, SC::OK);

        // Past the week the visitor token is junk too: a fresh anonymous visitor.
        *clock.0.lock().expect("lock") += time::Duration::days(8);
        let body = json!({ "key": seeded.pub_key, "message": QUESTION, "visitor": token });
        let reply = turn(&kit, body, None).await;
        assert_eq!(reply.status, SC::OK, "{}", reply.body);
        let token = reply.body["visitor"].as_str().expect("visitor");
        let fresh = visitor_claims(&kit, token);
        assert_eq!(fresh["n"], 1, "a brand-new visitor's first turn");
        #[rustfmt::skip]
        assert_ne!(fresh["vid"], stale_vid, "a fresh vid, not the expired one's");
    }
}

#[pollster::test]
async fn the_script_is_served_with_its_integrity_pin_intact() {
    for kit in kits() {
        let (status, headers, bytes) = get_bytes(&kit.router, W_JS).await;
        assert_eq!(status, SC::OK);
        #[rustfmt::skip]
        let value = |name: &HeaderName| headers.get(name).and_then(|v| v.to_str().ok());
        let content_type = value(&header::CONTENT_TYPE);
        assert_eq!(content_type, Some("application/javascript; charset=utf-8"));
        // `no-store` and `*` are the layers' choices; the SRI pin is the real guarantee.
        assert_eq!(value(&header::CACHE_CONTROL), Some("no-store"));
        assert_eq!(value(&header::ACCESS_CONTROL_ALLOW_ORIGIN), Some("*"));

        // The served bytes are the committed script, and the committed SRI pin is
        // exactly the hash a browser computes over them. If this fails, `widget/w.js`
        // changed: regenerate `widget/w.js.sri` (`openssl dgst -sha384 -binary widget/w.js | openssl base64 -A`).
        let mut digest = sha2::Sha384::new();
        digest.update(&bytes);
        let pin = format!("sha384-{}", STANDARD.encode(digest.finalize()));
        assert_eq!(include_str!("../../../widget/w.js.sri").trim(), pin);
        assert_eq!(bytes, include_str!("../../../widget/w.js").as_bytes());
    }
}

#[pollster::test]
async fn settings_merge_each_field_without_disturbing_the_other() {
    for kit in kits() {
        let (tenant_id, _key) = create_tenant(&kit, "Acme Support").await;

        // Threshold alone: the widget stays closed, shown as an empty allowlist.
        let first = put_settings(&kit, &tenant_id, json!({ "answer_threshold": 0.9 })).await;
        assert_eq!(first.status, SC::OK, "{}", first.body);
        assert_eq!(first.body["answer_threshold"], 0.9);
        assert_eq!(first.body["widget_origins"], json!([]));

        // Origins alone: the threshold is untouched; plain http is a dev machine only.
        let body = json!({ "widget_origins": ["http://localhost:3000", PAGE] });
        let second = put_settings(&kit, &tenant_id, body).await;
        assert_eq!(second.status, SC::OK, "{}", second.body);
        assert_eq!(second.body["answer_threshold"], 0.9);
        #[rustfmt::skip]
        assert_eq!(second.body["widget_origins"], json!(["http://localhost:3000", PAGE]));

        // Validation rejects what a request-time comparison could never match. (A path
        // on an otherwise-fine origin is no error: the RFC 6454 serializer strips it.)
        #[rustfmt::skip]
        let rejects = [
            (json!({}), "at least one"),
            (json!({ "answer_threshold": 1.5 }), "0.0..=1.0"),
            (json!({ "widget_origins": ["http://shop.example"] }), "https"),
            (json!({ "widget_origins": ["https://*.example"] }), "'*'"),
            (json!({ "widget_origins": ["not an origin"] }), "bare origin"),
        ];
        for (body, fragment) in rejects {
            let bad = put_settings(&kit, &tenant_id, body.clone()).await;
            expect_problem(&bad, SC::BAD_REQUEST, "validation-failed");
            let detail = bad.body["detail"].as_str().unwrap_or_default();
            assert!(detail.contains(fragment), "{body} -> {}", bad.body);
        }

        // The rejected writes touched nothing.
        let after = put_settings(&kit, &tenant_id, json!({ "answer_threshold": 0.7 })).await;
        assert_eq!(after.status, SC::OK, "{}", after.body);
        #[rustfmt::skip]
        assert_eq!(after.body["widget_origins"], json!(["http://localhost:3000", PAGE]),
            "survived the rejected writes");

        // A path is not rejected — it is stripped: stored bytes are what requests present.
        let body = json!({ "widget_origins": ["https://third.example/a/b?q=1"] });
        let stripped = put_settings(&kit, &tenant_id, body).await;
        assert_eq!(stripped.status, SC::OK, "{}", stripped.body);
        #[rustfmt::skip]
        assert_eq!(stripped.body["widget_origins"], json!(["https://third.example"]));

        // An unknown tenant is a 404.
        let body = json!({ "answer_threshold": 0.5 });
        let missing = put_settings(&kit, "does-not-exist", body).await;
        assert_eq!(missing.status, SC::NOT_FOUND);

        // The stored threshold is whole percent, and it is what the turn path reads.
        #[rustfmt::skip]
        let sql = format!("SELECT answer_threshold_pct AS v FROM sg_tenant_settings \
            WHERE tenant_id = '{tenant_id}'");
        #[rustfmt::skip]
        let rows = kit.db.query(&Statement::new(sql)).await.expect("settings row reads");
        #[rustfmt::skip]
        let pct = rows.rows.first().and_then(|row| row.get::<i64>("v")).expect("row exists");
        assert_eq!(pct, 70);
    }
}

/// A direct database probe: a never-written settings row leaves `widget_origins`
/// NULL, and a threshold-only write keeps it NULL — the state the origin check
/// reads as "widget closed", and the field the atomic upsert must not clobber.
#[pollster::test]
async fn an_unconfigured_allowlist_is_null_and_closes_the_widget() {
    for kit in kits() {
        let (tenant_id, _key) = create_tenant(&kit, "Acme Support").await;
        let stored = widget_origins_column(&kit, &tenant_id).await;
        assert_eq!(stored, None, "no settings row before any settings write");

        // Threshold-only write creates the row, column still NULL; `[]` is an array, not NULL.
        put_settings(&kit, &tenant_id, json!({ "answer_threshold": 0.5 })).await;
        let stored = widget_origins_column(&kit, &tenant_id).await;
        assert_eq!(
            stored,
            Some(None),
            "widget_origins stays SQL NULL until set"
        );

        put_settings(&kit, &tenant_id, json!({ "widget_origins": [] })).await;
        let stored = widget_origins_column(&kit, &tenant_id).await;
        assert_eq!(stored, Some(Some("[]".to_owned())));
    }
}

/// The raw column as three facts: `None` (no row), `Some(None)` (NULL), `Some(Some(json))`.
async fn widget_origins_column(kit: &TestHarness, tenant_id: &str) -> Option<Option<String>> {
    #[rustfmt::skip]
    let sql = format!("SELECT widget_origins AS v FROM sg_tenant_settings WHERE tenant_id = '{tenant_id}'");
    #[rustfmt::skip]
    let rows = kit.db.query(&Statement::new(sql)).await.expect("query runs");
    let row = rows.rows.first()?;
    // `Row::get::<Option<String>>` answers `Some(None)` for a NULL, `Some(Some(text))`
    // for a value; the column exists past the migration, so the outer `None` cannot.
    Some(row.get::<Option<String>>("v").unwrap_or_default())
}
