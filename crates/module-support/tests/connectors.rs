//! Issue #29 acceptance, over every available dialect: the connector
//! route (201 + one batch, 400 problems, 503 without the `HttpClient`
//! port), the three crawl roots, the caps, the allowlist, conditional
//! GETs, and the delete-on-404 path.
//!
//! The crawls run through the real `POST /connectors` route and the real
//! `Module::scheduled` hook. Fetches go to a URL-routed fake `HttpClient`
//! (`cratefield_testing::FakeHttpClient` is order-based; a connector's
//! crawl order is an implementation detail, so these tests route by URL
//! and let the runner pick the order), and execution is driven the way
//! production drives it: the `Defer` port's drain after a connector is
//! created, and the scheduled re-sync afterwards.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode, header};
use bytes::Bytes;
use cratefield_core::{HttpClient, HttpError, MapConfig, Module, Ports, Statement};
use cratefield_testing::{Dialect, MemoryBlob, TestHarness};
use http::Response;
use module_support::Support;
use serde_json::{Value, json};
use tower::ServiceExt;

mod stats_check;
use stats_check::{assert_stats_exact, df_of};

const ADMIN_TOKEN: &str = "test-admin-token-0123456789abcdef";
const ADMIN: &str = "/v1/support/admin/tenants";
const CONNECTORS: &str = "/v1/support/connectors";
const SEARCH: &str = "/v1/support/search";

/// A URL-routed fake `HttpClient`: each URL maps to a queue of scripted
/// responses served in order (the last one repeats, so a re-sync hits the
/// same route again without re-scripting it), and every request is
/// recorded with its headers — what the conditional-GET and Bearer
/// assertions read.
#[derive(Clone)]
struct RoutedHttpClient {
    inner: Arc<RoutedInner>,
}

/// One URL's scripted responses, served in order (the last repeats).
type ScriptedResponses = std::collections::VecDeque<Result<Response<Bytes>, HttpError>>;

struct RoutedInner {
    routes: Mutex<HashMap<String, ScriptedResponses>>,
    captured: Mutex<Vec<Captured>>,
}

#[derive(Clone)]
struct Captured {
    uri: String,
    headers: Vec<(String, String)>,
}

impl RoutedHttpClient {
    fn new() -> Self {
        Self {
            inner: Arc::new(RoutedInner {
                routes: Mutex::new(HashMap::new()),
                captured: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Scripts one response for `url`, replacing anything queued there.
    fn route(&self, url: &str, response: Result<Response<Bytes>, HttpError>) {
        self.route_many(url, vec![response]);
    }

    /// Scripts a queue of responses for `url`: the last one repeats.
    fn route_many(&self, url: &str, responses: Vec<Result<Response<Bytes>, HttpError>>) {
        assert!(!responses.is_empty(), "a route needs at least one response");
        self.inner
            .routes
            .lock()
            .expect("route lock")
            .insert(url.to_owned(), responses.into_iter().collect());
    }

    fn captured(&self) -> Vec<Captured> {
        self.inner.captured.lock().expect("capture lock").clone()
    }

    /// Every request URI the client saw.
    fn requested(&self) -> Vec<String> {
        self.captured().into_iter().map(|c| c.uri).collect()
    }

    fn requested_count(&self, url: &str) -> usize {
        self.requested()
            .into_iter()
            .filter(|uri| uri == url)
            .count()
    }

    /// The header value the *last* request to `url` sent — assertions
    /// read the re-sync's conditional GET, not the first fetch.
    fn sent_header(&self, url: &str, name: &str) -> Option<String> {
        self.captured()
            .into_iter()
            .rfind(|c| c.uri == url)
            .and_then(|c| {
                c.headers
                    .into_iter()
                    .find(|(key, _)| key.eq_ignore_ascii_case(name))
                    .map(|(_, value)| value)
            })
    }
}

impl HttpClient for RoutedHttpClient {
    // `HttpClient` is an `async_trait`; this is its expansion, written out
    // so the test needs no extra dev-dependency (same as routes.rs).
    fn send<'life0, 'async_trait>(
        &'life0 self,
        request: http::Request<Bytes>,
    ) -> Pin<Box<dyn Future<Output = Result<http::Response<Bytes>, HttpError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let (parts, _) = request.into_parts();
            let uri = parts.uri.to_string();
            let headers = parts
                .headers
                .iter()
                .map(|(name, value)| {
                    (
                        name.as_str().to_owned(),
                        value.to_str().unwrap_or_default().to_owned(),
                    )
                })
                .collect();
            self.inner
                .captured
                .lock()
                .expect("capture lock")
                .push(Captured {
                    uri: uri.clone(),
                    headers,
                });
            let response = self
                .inner
                .routes
                .lock()
                .expect("route lock")
                .get_mut(&uri)
                .and_then(std::collections::VecDeque::pop_front);
            match response {
                Some(response) => {
                    // Repeat the last scripted response so re-syncs need
                    // no re-scripting.
                    self.inner
                        .routes
                        .lock()
                        .expect("route lock")
                        .entry(uri)
                        .or_default()
                        .push_back(response.clone());
                    response
                }
                None => Err(HttpError::Transport(format!("no scripted route for {uri}"))),
            }
        })
    }
}

fn ok(status: u16, headers: &[(&str, &str)], body: &str) -> Result<Response<Bytes>, HttpError> {
    let mut builder = Response::builder().status(status);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    builder
        .body(Bytes::from(body.to_owned()))
        .map_err(|err| HttpError::Transport(err.to_string()))
}

fn sitemap(locs: &[&str]) -> String {
    let entries: Vec<String> = locs
        .iter()
        .map(|loc| format!("<url><loc>{loc}</loc></url>"))
        .collect();
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{}</urlset>"#,
        entries.concat()
    )
}

/// A sitemap *index*: its `<loc>` children are sitemap URLs, fetched with
/// the sitemap role — unlike a plain `urlset`, whose locs are pages.
fn sitemap_index(locs: &[&str]) -> String {
    let entries: Vec<String> = locs
        .iter()
        .map(|loc| format!("<sitemap><loc>{loc}</loc></sitemap>"))
        .collect();
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{}</sitemapindex>"#,
        entries.concat()
    )
}

// -- Kit and route helpers ------------------------------------------------

struct Reply {
    status: StatusCode,
    body: Value,
}

async fn send(
    router: &axum::Router,
    method: Method,
    path: &str,
    bearer: Option<&str>,
    json_body: Option<&str>,
) -> Reply {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(key) = bearer {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {key}"));
    }
    let body = match json_body {
        Some(payload) => {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
            Body::from(payload.to_owned())
        }
        None => Body::empty(),
    };
    let response = router
        .clone()
        .oneshot(builder.body(body).expect("request builds"))
        .await
        .expect("router answers");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("body reads");
    Reply {
        status,
        body: serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    }
}

fn support() -> Vec<Box<dyn Module>> {
    vec![Box::new(Support::new())]
}

fn connector_kit(
    dialect: Dialect,
    http: &RoutedHttpClient,
    extra: &[(&'static str, &str)],
) -> TestHarness {
    TestHarness::with_database_and_ports(support(), dialect, |ports| {
        let mut pairs = vec![("ADMIN_TOKEN", ADMIN_TOKEN)];
        pairs.extend(extra.iter().copied());
        ports.config = Arc::new(MapConfig::from_pairs(pairs));
        ports.http = Some(Arc::new(http.clone()));
    })
}

async fn mint_tenant(kit: &TestHarness) -> Value {
    let reply = send(
        &kit.router,
        Method::POST,
        ADMIN,
        Some(ADMIN_TOKEN),
        Some(r#"{"name":"Docs"}"#),
    )
    .await;
    assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
    reply.body
}

async fn create_connector(kit: &TestHarness, key: &str, body: Value) -> Reply {
    send(
        &kit.router,
        Method::POST,
        CONNECTORS,
        Some(key),
        Some(&body.to_string()),
    )
    .await
}

/// Runs the deferred sweep a connector creation (or a fetch's
/// discoveries) queued — the same execution opportunity production gets.
async fn drain(kit: &TestHarness) {
    kit.defer.drain().await;
}

/// The scheduled re-sync, through the public `Module::scheduled` hook
/// with a context built the way a runtime builds one: the kit's own
/// database, clock and defer, the routed fake as the `HttpClient` port, and
/// the given config pairs (the GitHub tests pass the token here).
async fn resync(kit: &TestHarness, http: &RoutedHttpClient, extra: &[(&'static str, &str)]) {
    resync_at(kit, http, kit.clock.0, extra).await;
}

/// `resync` at an explicit instant — the retry test needs the clock to
/// have moved past a recorded backoff.
async fn resync_at(
    kit: &TestHarness,
    http: &RoutedHttpClient,
    at: time::OffsetDateTime,
    extra: &[(&'static str, &str)],
) {
    let module = Support::new();
    let mut pairs = vec![("ADMIN_TOKEN", ADMIN_TOKEN)];
    pairs.extend(extra.iter().copied());
    let mut ports = Ports::with_config(Arc::new(MapConfig::from_pairs(pairs)));
    ports.db = Some(kit.db.clone());
    ports.http = Some(Arc::new(http.clone()));
    ports.clock = Some(Arc::new(cratefield_testing::FixedClock(at)));
    ports.id_gen = Some(Arc::new(cratefield_core::UlidIdGen));
    ports.defer = Some(Arc::new(kit.defer.clone()));
    ports.signer = Some(kit.signer.clone());
    let ctx = kit.harness.module_context(&module, &ports);
    module
        .scheduled(&ctx, "23 4 * * *")
        .await
        .expect("scheduled resync runs");
}

fn count_of(kit: &TestHarness, table: &str) -> usize {
    let rows = pollster::block_on(kit.db.query(&Statement::new(format!(
        "SELECT COUNT(*) AS n FROM {table}"
    ))))
    .expect("count query runs");
    usize::try_from(
        rows.rows
            .first()
            .and_then(|row| row.get::<i64>("n"))
            .expect("aggregate row"),
    )
    .expect("count is non-negative")
}

fn text_column(kit: &TestHarness, sql: &str) -> Vec<String> {
    let rows = pollster::block_on(kit.db.query(&Statement::new(sql))).expect("query runs");
    rows.rows
        .iter()
        .map(|row| row.get::<String>("v").expect("text value"))
        .collect()
}

fn option_column(kit: &TestHarness, sql: &str) -> Vec<Option<String>> {
    let rows = pollster::block_on(kit.db.query(&Statement::new(sql))).expect("query runs");
    rows.rows
        .iter()
        .filter_map(|row| row.get::<Option<String>>("v"))
        .collect()
}

async fn search_hits(kit: &TestHarness, key: &str, query: &str) -> Vec<Value> {
    let reply = send(
        &kit.router,
        Method::GET,
        &format!("{SEARCH}?q={query}"),
        Some(key),
        None,
    )
    .await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    reply.body["results"]
        .as_array()
        .expect("results array")
        .clone()
}

// -- The connector route --------------------------------------------------

#[pollster::test]
async fn creating_a_connector_inserts_the_row_and_seed_job_and_crawls_through_defer() {
    for dialect in Dialect::available() {
        let http = RoutedHttpClient::new();
        http.route(
            "https://docs.example/sitemap.xml",
            ok(
                200,
                &[("ETAG", "\"s1\"")],
                &sitemap(&[
                    "https://docs.example/a.html",
                    "https://docs.example/b.html",
                    "https://docs.example/c.html",
                ]),
            ),
        );
        for (page, word) in [
            ("https://docs.example/a.html", "aardvark"),
            ("https://docs.example/b.html", "burrow"),
            ("https://docs.example/c.html", "crest"),
        ] {
            http.route(
                page,
                ok(
                    200,
                    &[("CONTENT-TYPE", "text/plain")],
                    &format!("the {word} page"),
                ),
            );
        }
        let kit = connector_kit(dialect, &http, &[]);
        let tenant = mint_tenant(&kit).await;
        let key = tenant["api_key"].as_str().expect("api key");

        let reply = create_connector(
            &kit,
            key,
            json!({ "kind": "sitemap", "url": "https://docs.example/sitemap.xml" }),
        )
        .await;
        assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
        assert_eq!(reply.body["kind"], "sitemap");
        assert_eq!(reply.body["seed_url"], "https://docs.example/sitemap.xml");
        assert_eq!(reply.body["max_pages"], 200);
        assert_eq!(reply.body["max_bytes"], 1024 * 1024);
        assert_eq!(reply.body["max_depth"], 3);
        let connector_id = reply.body["connector_id"].as_str().expect("id");

        // The row and its seed job landed together, before any fetch.
        assert_eq!(count_of(&kit, "sg_connectors"), 1);
        assert!(count_of(&kit, "sg_ingest_outbox") >= 1);
        assert_eq!(
            text_column(
                &kit,
                &format!("SELECT kind AS v FROM sg_connectors WHERE id = '{connector_id}'")
            ),
            ["sitemap"]
        );

        // The deferred sweep runs the seed, discovers the three pages,
        // and cascades until the crawl is done.
        drain(&kit).await;
        assert_eq!(count_of(&kit, "sg_sources"), 3, "three pages indexed");
        assert_eq!(count_of(&kit, "sg_ingest_pages"), 4, "sitemap plus pages");
        assert_eq!(count_of(&kit, "sg_ingest_outbox"), 0, "all jobs retired");
        assert_eq!(
            text_column(&kit, "SELECT title AS v FROM sg_sources ORDER BY title"),
            [
                "https://docs.example/a.html",
                "https://docs.example/b.html",
                "https://docs.example/c.html",
            ]
        );

        // The crawled pages are searchable through the normal route.
        let key = tenant["api_key"].as_str().expect("api key");
        let hits = search_hits(&kit, key, "aardvark").await;
        assert_eq!(hits.len(), 1, "the aardvark page is found");
        assert_eq!(hits[0]["title"], "https://docs.example/a.html");
    }
}

#[pollster::test]
async fn the_connector_route_requires_a_key_and_validates_its_body() {
    for dialect in Dialect::available() {
        let http = RoutedHttpClient::new();
        let kit = connector_kit(dialect, &http, &[]);
        let tenant = mint_tenant(&kit).await;
        let key = tenant["api_key"].as_str().expect("api key");

        // No key, junk key: the same 401 every tenant route answers.
        for bearer in [None, Some("junk")] {
            let reply = send(
                &kit.router,
                Method::POST,
                CONNECTORS,
                bearer,
                Some(r#"{"kind":"sitemap","url":"https://docs.example/sitemap.xml"}"#),
            )
            .await;
            assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{}", reply.body);
        }
        assert_eq!(count_of(&kit, "sg_connectors"), 0);

        // Bad bodies: 400 naming the field, nothing written.
        for bad in [
            json!({}).to_string(),
            json!({ "kind": "rss", "url": "https://docs.example/feed.xml" }).to_string(),
            json!({ "kind": "sitemap" }).to_string(),
            json!({ "kind": "sitemap", "url": "not a url" }).to_string(),
            json!({ "kind": "sitemap", "url": "ftp://docs.example/sitemap.xml" }).to_string(),
            json!({ "kind": "github", "url": "https://docs.example" }).to_string(),
            json!({ "kind": "github", "owner": "acme" }).to_string(),
            json!({ "kind": "github", "owner": "ac/me", "repo": "widgets" }).to_string(),
            json!({ "kind": "github", "owner": "acme", "repo": "widgets", "ref": "" }).to_string(),
            json!({ "kind": "github", "owner": "acme", "repo": "widgets", "ref": "  " })
                .to_string(),
            json!({ "kind": "github", "owner": "acme", "repo": "widgets", "ref": "?" }).to_string(),
            json!({ "kind": "github", "owner": "acme", "repo": "widgets", "ref": "a#b" })
                .to_string(),
            json!({ "kind": "github", "owner": "acme", "repo": "widgets", "ref": "a..b" })
                .to_string(),
            json!({ "kind": "github", "owner": "acme", "repo": "widgets", "ref": "a b" })
                .to_string(),
            json!({ "kind": "github", "owner": "acme", "repo": "widgets", "ref": "a\u{7f}b" })
                .to_string(),
            "{oops".to_owned(),
        ] {
            let reply = send(&kit.router, Method::POST, CONNECTORS, Some(key), Some(&bad)).await;
            assert_eq!(
                reply.status,
                StatusCode::BAD_REQUEST,
                "{bad}: {}",
                reply.body
            );
        }
        assert_eq!(count_of(&kit, "sg_connectors"), 0, "nothing written");
        assert_eq!(http.captured().len(), 0, "nothing fetched");
    }
}

#[pollster::test]
async fn creating_a_connector_without_an_http_client_answers_503_and_writes_nothing() {
    for dialect in Dialect::available() {
        let kit = TestHarness::with_database_and_ports(support(), dialect, |ports| {
            ports.config = Arc::new(MapConfig::from_pairs([("ADMIN_TOKEN", ADMIN_TOKEN)]));
            ports.http = None;
        });
        let tenant = mint_tenant(&kit).await;
        let key = tenant["api_key"].as_str().expect("api key");
        let reply = create_connector(
            &kit,
            key,
            json!({ "kind": "sitemap", "url": "https://docs.example/sitemap.xml" }),
        )
        .await;
        assert_eq!(
            reply.status,
            StatusCode::SERVICE_UNAVAILABLE,
            "{}",
            reply.body
        );
        assert_eq!(count_of(&kit, "sg_connectors"), 0);
        assert_eq!(count_of(&kit, "sg_ingest_outbox"), 0);
    }
}

#[pollster::test]
async fn caps_are_clamped_to_the_documented_maxima() {
    for dialect in Dialect::available() {
        let http = RoutedHttpClient::new();
        let kit = connector_kit(dialect, &http, &[]);
        let tenant = mint_tenant(&kit).await;
        let key = tenant["api_key"].as_str().expect("api key");
        let reply = create_connector(
            &kit,
            key,
            json!({
                "kind": "url_prefix",
                "url": "https://docs.example/docs",
                "max_pages": 99_999,
                "max_bytes": 99_999_999,
                "max_depth": 99,
            }),
        )
        .await;
        assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
        assert_eq!(reply.body["max_pages"], 2000);
        assert_eq!(reply.body["max_bytes"], 4 * 1024 * 1024);
        assert_eq!(reply.body["max_depth"], 5);

        // Defaults when absent (asserted on a second connector).
        let reply = create_connector(
            &kit,
            key,
            json!({ "kind": "url_prefix", "url": "https://docs.example/guide" }),
        )
        .await;
        assert_eq!(reply.body["max_pages"], 200);
        assert_eq!(reply.body["max_bytes"], 1024 * 1024);
        assert_eq!(reply.body["max_depth"], 3);
    }
}

#[pollster::test]
async fn the_page_cap_stops_a_sitemap_at_its_budget() {
    for dialect in Dialect::available() {
        let http = RoutedHttpClient::new();
        http.route(
            "https://docs.example/sitemap.xml",
            ok(
                200,
                &[],
                &sitemap(&[
                    "https://docs.example/1.html",
                    "https://docs.example/2.html",
                    "https://docs.example/3.html",
                    "https://docs.example/4.html",
                ]),
            ),
        );
        for page in ["1", "2", "3", "4"] {
            http.route(
                &format!("https://docs.example/{page}.html"),
                ok(
                    200,
                    &[("CONTENT-TYPE", "text/plain")],
                    &format!("page {page}"),
                ),
            );
        }
        let kit = connector_kit(dialect, &http, &[]);
        let tenant = mint_tenant(&kit).await;
        let key = tenant["api_key"].as_str().expect("api key");

        let reply = create_connector(
            &kit,
            key,
            json!({
                "kind": "sitemap",
                "url": "https://docs.example/sitemap.xml",
                "max_pages": 3,
            }),
        )
        .await;
        assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
        assert_eq!(reply.body["max_pages"], 3);
        drain(&kit).await;

        // The sitemap's own row spends one slot of the three; two pages
        // remain for sources.
        assert_eq!(count_of(&kit, "sg_ingest_pages"), 3, "the cap holds");
        assert_eq!(count_of(&kit, "sg_sources"), 2, "the cap holds");
        assert_eq!(
            text_column(&kit, "SELECT title AS v FROM sg_sources ORDER BY title"),
            ["https://docs.example/1.html", "https://docs.example/2.html"],
        );
        // Over-budget URLs are never even attempted.
        for page in ["3", "4"] {
            assert_eq!(
                http.requested_count(&format!("https://docs.example/{page}.html")),
                0,
                "page {page} is never fetched"
            );
        }
    }
}

/// The cap counts navigation rows too: a sitemap *index* (a sitemap of
/// sitemaps) holds one row per child sitemap it enqueues, so its breadth
/// is bounded by the same `max_pages` — a child sitemap that starts when
/// the rows are spent never fetches a single page.
#[pollster::test]
async fn a_sitemap_index_breadth_spends_the_page_cap_too() {
    for dialect in Dialect::available() {
        let http = RoutedHttpClient::new();
        http.route(
            "https://docs.example/sitemap.xml",
            ok(
                200,
                &[],
                &sitemap_index(&["https://docs.example/a.xml", "https://docs.example/b.xml"]),
            ),
        );
        for child in ["a", "b"] {
            http.route(
                &format!("https://docs.example/{child}.xml"),
                ok(
                    200,
                    &[],
                    &sitemap(&[
                        &format!("https://docs.example/{child}1.html"),
                        &format!("https://docs.example/{child}2.html"),
                    ]),
                ),
            );
        }
        for page in ["a1", "a2", "b1", "b2"] {
            http.route(
                &format!("https://docs.example/{page}.html"),
                ok(
                    200,
                    &[("CONTENT-TYPE", "text/plain")],
                    &format!("page {page}"),
                ),
            );
        }
        let kit = connector_kit(dialect, &http, &[]);
        let tenant = mint_tenant(&kit).await;
        let key = tenant["api_key"].as_str().expect("api key");

        // Five rows: the index, two child sitemaps, and room for one
        // page. `b.xml` starts third, sees the rows spent, and stops.
        let reply = create_connector(
            &kit,
            key,
            json!({
                "kind": "sitemap",
                "url": "https://docs.example/sitemap.xml",
                "max_pages": 5,
            }),
        )
        .await;
        assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
        drain(&kit).await;

        assert_eq!(
            count_of(&kit, "sg_ingest_pages"),
            5,
            "the cap holds: index, a.xml, b.xml, a1, a2"
        );
        assert_eq!(
            text_column(&kit, "SELECT title AS v FROM sg_sources ORDER BY title"),
            [
                "https://docs.example/a1.html",
                "https://docs.example/a2.html"
            ],
        );
        // `b.xml`'s pages are never attempted — by the time their jobs
        // run, the budget is spent on the rows above them.
        for page in ["b1", "b2"] {
            assert_eq!(
                http.requested_count(&format!("https://docs.example/{page}.html")),
                0,
                "page {page} is never fetched"
            );
        }
    }
}

/// Two re-sync ticks without a drain in between leave exactly one pending
/// outbox row per URL: the second tick's enqueues dedup against the
/// first's still-unretired rows (in flight, or waiting out a backoff),
/// instead of stacking duplicates that each carry a fresh retry budget.
#[pollster::test]
async fn two_resync_ticks_without_draining_leave_one_row_per_url() {
    for dialect in Dialect::available() {
        let http = RoutedHttpClient::new();
        http.route(
            "https://docs.example/sitemap.xml",
            ok(
                200,
                &[],
                &sitemap(&["https://docs.example/1.html", "https://docs.example/2.html"]),
            ),
        );
        // Every page fetch answers 503, so nothing completes: after tick
        // one both URLs sit in the outbox on backoff.
        for page in ["1", "2"] {
            http.route(
                &format!("https://docs.example/{page}.html"),
                ok(503, &[], "overloaded"),
            );
        }
        let kit = connector_kit(dialect, &http, &[]);
        let tenant = mint_tenant(&kit).await;
        let key = tenant["api_key"].as_str().expect("api key");

        let reply = create_connector(
            &kit,
            key,
            json!({"kind": "sitemap", "url": "https://docs.example/sitemap.xml"}),
        )
        .await;
        assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
        let outbox_rows = || count_of(&kit, "sg_ingest_outbox");
        let attempts = || {
            option_column(
                &kit,
                "SELECT CAST(attempts AS TEXT) AS v FROM sg_ingest_outbox",
            )
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
        };

        // Tick one — the deferred seed sweep is never drained, so the
        // scheduled hook does all the work: the seed job runs, the two
        // pages are enqueued, and each 503s into a backoff.
        resync(&kit, &http, &[]).await;
        assert_eq!(outbox_rows(), 2, "one row per page URL");
        assert_eq!(attempts(), ["1", "1"], "one retry each, nothing stacked");
        assert_eq!(
            http.requested_count("https://docs.example/1.html"),
            1,
            "fetched exactly once"
        );

        // Tick two, still without draining: the same URLs are pending, so
        // they must not stack duplicate rows — a duplicate would be due
        // immediately and show up here as a second fetch and a fresh
        // retry budget.
        resync(&kit, &http, &[]).await;
        assert_eq!(outbox_rows(), 2, "still one row per page URL");
        assert_eq!(attempts(), ["1", "1"], "no fresh retry budgets");
        assert_eq!(
            http.requested_count("https://docs.example/1.html"),
            1,
            "the backoff row is neither re-enqueued nor re-fetched"
        );
    }
}

#[pollster::test]
async fn the_depth_cap_stops_a_prefix_crawl_at_its_budget() {
    for dialect in Dialect::available() {
        let http = RoutedHttpClient::new();
        // Root-relative from the seed (its URL is the prefix itself, no
        // trailing slash, so a same-directory relative link would resolve
        // one level *above* the prefix); then document-relative from the
        // child, which lives under the prefix.
        let seed_html = r#"<html><body><a href="/docs/child.html">next</a></body></html>"#;
        let child_html = r#"<html><body><a href="grandchild.html">next</a></body></html>"#;
        http.route(
            "https://docs.example/docs",
            ok(200, &[("CONTENT-TYPE", "text/html")], seed_html),
        );
        http.route(
            "https://docs.example/docs/child.html",
            ok(200, &[("CONTENT-TYPE", "text/html")], child_html),
        );
        http.route(
            "https://docs.example/docs/grandchild.html",
            ok(200, &[("CONTENT-TYPE", "text/plain")], "too deep"),
        );
        let kit = connector_kit(dialect, &http, &[]);
        let tenant = mint_tenant(&kit).await;
        let key = tenant["api_key"].as_str().expect("api key");

        let reply = create_connector(
            &kit,
            key,
            json!({
                "kind": "url_prefix",
                "url": "https://docs.example/docs",
                "max_depth": 1,
            }),
        )
        .await;
        assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
        drain(&kit).await;

        assert_eq!(count_of(&kit, "sg_sources"), 2, "seed plus one hop");
        assert_eq!(
            http.requested_count("https://docs.example/docs/grandchild.html"),
            0,
            "the second hop is beyond the cap and never fetched"
        );
    }
}

#[pollster::test]
async fn an_oversized_response_is_dropped_not_indexed() {
    for dialect in Dialect::available() {
        let http = RoutedHttpClient::new();
        let big = "x".repeat(4096);
        http.route(
            "https://docs.example/big",
            ok(200, &[("CONTENT-TYPE", "text/plain")], &big),
        );
        let kit = connector_kit(dialect, &http, &[]);
        let tenant = mint_tenant(&kit).await;
        let key = tenant["api_key"].as_str().expect("api key");

        let reply = create_connector(
            &kit,
            key,
            json!({
                "kind": "url_prefix",
                "url": "https://docs.example/big",
                "max_bytes": 1024,
            }),
        )
        .await;
        assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
        drain(&kit).await;

        assert_eq!(count_of(&kit, "sg_sources"), 0, "the body was too big");
        assert_eq!(count_of(&kit, "sg_ingest_pages"), 0, "no validators either");
        assert_eq!(count_of(&kit, "sg_ingest_outbox"), 0, "the job was retired");
    }
}

#[pollster::test]
async fn off_allowlist_urls_are_never_fetched() {
    for dialect in Dialect::available() {
        let http = RoutedHttpClient::new();
        // A sitemap naming another host, a scheme downgrade of its own
        // host, and two of its own.
        http.route(
            "https://docs.example/sitemap.xml",
            ok(
                200,
                &[],
                &sitemap(&[
                    "https://docs.example/mine.html",
                    "https://docs.example/also-mine.html",
                    "https://evil.example/steal.html",
                    "http://docs.example/downgrade.html",
                    "https://docs.example.evil.com/spoof.html",
                ]),
            ),
        );
        for page in ["mine", "also-mine"] {
            http.route(
                &format!("https://docs.example/{page}.html"),
                ok(
                    200,
                    &[("CONTENT-TYPE", "text/plain")],
                    format!("page {page}").as_str(),
                ),
            );
        }
        let kit = connector_kit(dialect, &http, &[]);
        let tenant = mint_tenant(&kit).await;
        let key = tenant["api_key"].as_str().expect("api key");

        let reply = create_connector(
            &kit,
            key,
            json!({ "kind": "sitemap", "url": "https://docs.example/sitemap.xml" }),
        )
        .await;
        assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
        drain(&kit).await;

        assert_eq!(count_of(&kit, "sg_sources"), 2, "only the sitemap's host");
        let requested = http.requested().join("\n");
        for hostile in [
            "https://evil.example/steal.html",
            "http://docs.example/downgrade.html",
            "https://docs.example.evil.com/spoof.html",
        ] {
            assert!(!requested.contains(hostile), "{hostile} was fetched");
        }
    }
}

// -- Conditional GET and the scheduled re-sync ----------------------------

#[pollster::test]
async fn a_resync_sends_validators_and_a_304_writes_nothing() {
    for dialect in Dialect::available() {
        let http = RoutedHttpClient::new();
        // First sync answers 200 with validators; the re-sync gets 304s.
        http.route_many(
            "https://docs.example/sitemap.xml",
            vec![
                ok(
                    200,
                    &[
                        ("ETAG", "\"s1\""),
                        ("LAST-MODIFIED", "Wed, 01 Jan 2025 00:00:00 GMT"),
                    ],
                    &sitemap(&["https://docs.example/a.html", "https://docs.example/b.html"]),
                ),
                ok(304, &[], ""),
            ],
        );
        for (page, word) in [
            ("https://docs.example/a.html", "aardvark"),
            ("https://docs.example/b.html", "burrow"),
        ] {
            http.route_many(
                page,
                vec![
                    ok(
                        200,
                        &[("CONTENT-TYPE", "text/plain"), ("ETAG", "\"p1\"")],
                        &format!("the {word} page"),
                    ),
                    ok(304, &[], ""),
                ],
            );
        }
        let kit = connector_kit(dialect, &http, &[]);
        let tenant = mint_tenant(&kit).await;
        let key = tenant["api_key"].as_str().expect("api key");

        let reply = create_connector(
            &kit,
            key,
            json!({ "kind": "sitemap", "url": "https://docs.example/sitemap.xml" }),
        )
        .await;
        assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
        drain(&kit).await;
        let sources_before = count_of(&kit, "sg_sources");
        let chunks_before = count_of(&kit, "sg_chunks");
        assert_eq!(sources_before, 2);

        // The validators were recorded (order by url: a, b, sitemap).
        assert_eq!(
            option_column(&kit, "SELECT etag AS v FROM sg_ingest_pages ORDER BY url"),
            [
                Some("\"p1\"".to_owned()),
                Some("\"p1\"".to_owned()),
                Some("\"s1\"".to_owned()),
            ],
            "one validator per fetched URL"
        );

        // The scheduled re-sync: conditional GETs, 304s, nothing written.
        resync(&kit, &http, &[]).await;
        for (url, expected) in [
            ("https://docs.example/sitemap.xml", Some("\"s1\"")),
            ("https://docs.example/a.html", Some("\"p1\"")),
            ("https://docs.example/b.html", Some("\"p1\"")),
        ] {
            assert_eq!(
                http.sent_header(url, "if-none-match").as_deref(),
                expected,
                "{url} sent its stored ETag"
            );
        }
        assert_eq!(count_of(&kit, "sg_sources"), sources_before, "no re-index");
        assert_eq!(count_of(&kit, "sg_chunks"), chunks_before, "no re-chunking");
        assert_eq!(count_of(&kit, "sg_ingest_outbox"), 0, "all jobs retired");
    }
}

#[pollster::test]
async fn a_page_that_answers_404_on_resync_deletes_its_source() {
    for dialect in Dialect::available() {
        let http = RoutedHttpClient::new();
        // Second sync: the sitemap no longer lists the page — a sitemap
        // that still listed it would legitimately re-add the URL within
        // the same sweep (a listed URL keeps re-fetching through its
        // validators until it answers 404; see `Runner::resync`).
        http.route_many(
            "https://docs.example/sitemap.xml",
            vec![
                ok(
                    200,
                    &[("ETAG", "\"s1\"")],
                    &sitemap(&["https://docs.example/gone.html"]),
                ),
                ok(200, &[("ETAG", "\"s2\"")], &sitemap(&[])),
            ],
        );
        http.route_many(
            "https://docs.example/gone.html",
            vec![
                ok(
                    200,
                    &[("CONTENT-TYPE", "text/plain"), ("ETAG", "\"p1\"")],
                    "the vanish aardvark page",
                ),
                ok(404, &[], ""),
            ],
        );
        let kit = connector_kit(dialect, &http, &[]);
        let tenant = mint_tenant(&kit).await;
        let key = tenant["api_key"].as_str().expect("api key");

        let reply = create_connector(
            &kit,
            key,
            json!({ "kind": "sitemap", "url": "https://docs.example/sitemap.xml" }),
        )
        .await;
        assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
        drain(&kit).await;
        assert_eq!(count_of(&kit, "sg_sources"), 1);
        let source_id = text_column(&kit, "SELECT id AS v FROM sg_sources")[0].clone();
        assert!(count_of(&kit, "sg_chunks") > 0);
        assert!(count_of(&kit, "sg_postings") > 0);

        // The vanished page's own term finds it — then finds nothing.
        let hits = search_hits(&kit, key, "aardvark").await;
        assert_eq!(hits.len(), 1);
        assert!(assert_stats_exact(&kit, "after the connector's first index") > 0);

        resync(&kit, &http, &[]).await;

        assert_eq!(
            assert_stats_exact(&kit, "after the 404 delete"),
            0,
            "the page's statistics left with its source"
        );
        assert_eq!(count_of(&kit, "sg_tenant_stats"), 0);
        assert_eq!(count_of(&kit, "sg_sources"), 0, "the source is gone");
        assert_eq!(count_of(&kit, "sg_chunks"), 0);
        assert_eq!(count_of(&kit, "sg_postings"), 0);
        // Its page row went too; the sitemap's row stays.
        assert_eq!(count_of(&kit, "sg_ingest_pages"), 1);
        assert_eq!(
            count_of(
                &kit,
                &format!("sg_ingest_pages WHERE source_id = '{source_id}'")
            ),
            0
        );
        let hits = search_hits(&kit, key, "aardvark").await;
        assert_eq!(
            hits,
            Vec::<Value>::new(),
            "the deleted source is unsearchable"
        );
    }
}

// -- The GitHub connector -------------------------------------------------

fn tree_response(paths: &[(&str, &str)]) -> String {
    let entries: Vec<Value> = paths
        .iter()
        .map(|(path, kind)| json!({ "path": path, "type": kind }))
        .collect();
    json!({ "sha": "abc", "tree": entries, "truncated": false }).to_string()
}

#[pollster::test]
async fn a_github_connector_ingests_only_glob_matches_and_sends_the_bearer_token() {
    for dialect in Dialect::available() {
        let http = RoutedHttpClient::new();
        let tree = "https://api.github.com/repos/acme/widgets/git/trees/main?recursive=1";
        http.route(
            tree,
            ok(
                200,
                &[("CONTENT-TYPE", "application/json")],
                &tree_response(&[
                    ("README.md", "blob"),
                    ("docs/a.md", "blob"),
                    ("docs/sub/b.md", "blob"),
                    ("docs/img.png", "blob"),
                    ("src/lib.rs", "blob"),
                ]),
            ),
        );
        http.route(
            "https://api.github.com/repos/acme/widgets/contents/docs/a.md?ref=main",
            ok(200, &[("CONTENT-TYPE", "text/plain")], "# A doc quokka"),
        );
        http.route(
            "https://api.github.com/repos/acme/widgets/contents/docs/sub/b.md?ref=main",
            ok(200, &[("CONTENT-TYPE", "text/plain")], "# B doc quokka"),
        );
        let kit = connector_kit(dialect, &http, &[("GITHUB_TOKEN", "tok-123")]);
        let tenant = mint_tenant(&kit).await;
        let key = tenant["api_key"].as_str().expect("api key");

        let reply = create_connector(
            &kit,
            key,
            json!({
                "kind": "github",
                "owner": "acme",
                "repo": "widgets",
                "path_glob": "docs/**/*.md",
                "ref": "main",
                "credential_ref": "GITHUB_TOKEN",
            }),
        )
        .await;
        assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
        assert_eq!(
            reply.body["seed_url"],
            "https://api.github.com/repos/acme/widgets/git/trees/main?recursive=1"
        );
        drain(&kit).await;

        assert_eq!(count_of(&kit, "sg_sources"), 2, "only the two glob matches");
        assert_eq!(
            option_column(
                &kit,
                "SELECT external_id AS v FROM sg_sources ORDER BY external_id"
            ),
            [
                Some("github:acme/widgets:docs/a.md".to_owned()),
                Some("github:acme/widgets:docs/sub/b.md".to_owned()),
            ]
        );
        // The two matches are searchable.
        let hits = search_hits(&kit, key, "quokka").await;
        assert_eq!(hits.len(), 2);

        // Only tree + the two files were ever requested.
        let requested = http.requested();
        assert_eq!(requested.len(), 3, "{requested:?}");
        for url in [
            "https://api.github.com/repos/acme/widgets/contents/README.md?ref=main",
            "https://api.github.com/repos/acme/widgets/contents/docs/img.png?ref=main",
            "https://api.github.com/repos/acme/widgets/contents/src/lib.rs?ref=main",
        ] {
            assert!(!requested.contains(&url.to_owned()), "{url} fetched");
        }

        // GitHub requests are identifiable and authenticated: the tree
        // asks for JSON, the files for raw, both carry the Bearer token
        // resolved from the Config port by the stored reference.
        assert_eq!(
            http.sent_header(tree, "authorization").as_deref(),
            Some("Bearer tok-123")
        );
        assert_eq!(
            http.sent_header(tree, "accept").as_deref(),
            Some("application/vnd.github+json")
        );
        let file_url = "https://api.github.com/repos/acme/widgets/contents/docs/a.md?ref=main";
        assert_eq!(
            http.sent_header(file_url, "authorization").as_deref(),
            Some("Bearer tok-123")
        );
        assert_eq!(
            http.sent_header(file_url, "accept").as_deref(),
            Some("application/vnd.github.raw")
        );
        assert!(
            http.sent_header(file_url, "user-agent")
                .is_some_and(|ua| ua.starts_with("supportgenius/")),
            "GitHub refuses requests without a User-Agent"
        );
    }
}

#[pollster::test]
async fn a_github_file_that_answers_404_deletes_by_external_id() {
    for dialect in Dialect::available() {
        let http = RoutedHttpClient::new();
        let tree = "https://api.github.com/repos/acme/widgets/git/trees/main?recursive=1";
        http.route_many(
            tree,
            vec![
                ok(200, &[], &tree_response(&[("docs/a.md", "blob")])),
                ok(304, &[], ""),
            ],
        );
        http.route_many(
            "https://api.github.com/repos/acme/widgets/contents/docs/a.md?ref=main",
            vec![
                ok(
                    200,
                    &[("CONTENT-TYPE", "text/plain"), ("ETAG", "\"f1\"")],
                    "aardvark",
                ),
                ok(404, &[], ""),
            ],
        );
        let kit = connector_kit(dialect, &http, &[]);
        let tenant = mint_tenant(&kit).await;
        let key = tenant["api_key"].as_str().expect("api key");

        let reply = create_connector(
            &kit,
            key,
            json!({
                "kind": "github",
                "owner": "acme",
                "repo": "widgets",
                "path_glob": "docs/*.md",
                "ref": "main",
            }),
        )
        .await;
        assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
        drain(&kit).await;
        assert_eq!(count_of(&kit, "sg_sources"), 1, "the file indexed once");

        // The re-sync refetches the tree (304: unchanged) and the file,
        // which is gone: the source keyed `github:acme/widgets:docs/a.md`
        // — not by its fetch URL — must be the thing deleted.
        assert!(assert_stats_exact(&kit, "after the GitHub file indexed") > 0);
        resync(&kit, &http, &[]).await;
        assert_eq!(count_of(&kit, "sg_sources"), 0);
        assert_eq!(count_of(&kit, "sg_chunks"), 0);
        assert_eq!(count_of(&kit, "sg_ingest_pages"), 1, "only the tree row");
        assert_eq!(assert_stats_exact(&kit, "after the GitHub 404 delete"), 0);
    }
}

#[pollster::test]
async fn a_url_prefix_crawl_stays_under_its_prefix() {
    for dialect in Dialect::available() {
        let http = RoutedHttpClient::new();
        http.route(
            "https://docs.example/docs",
            ok(
                200,
                &[("CONTENT-TYPE", "text/html")],
                r#"<html><body>
                    <a href="/docs/intro.html">in</a>
                    <a href="/blog/post.html">out</a>
                    <a href="https://other.example/x">away</a>
                </body></html>"#,
            ),
        );
        http.route(
            "https://docs.example/docs/intro.html",
            ok(200, &[("CONTENT-TYPE", "text/plain")], "the intro pad"),
        );
        http.route(
            "https://docs.example/blog/post.html",
            ok(200, &[("CONTENT-TYPE", "text/plain")], "off prefix pad"),
        );
        let kit = connector_kit(dialect, &http, &[]);
        let tenant = mint_tenant(&kit).await;
        let key = tenant["api_key"].as_str().expect("api key");

        let reply = create_connector(
            &kit,
            key,
            json!({ "kind": "url_prefix", "url": "https://docs.example/docs" }),
        )
        .await;
        assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
        drain(&kit).await;

        assert_eq!(count_of(&kit, "sg_sources"), 2, "seed plus in-prefix link");
        let requested = http.requested().join("\n");
        assert!(
            !requested.contains("https://docs.example/blog/post.html"),
            "a same-host page off the prefix is not fetched"
        );
        assert!(
            !requested.contains("other.example"),
            "off-host is not fetched"
        );
    }
}

#[pollster::test]
async fn an_off_host_sitemap_child_is_skipped_before_any_fetch() {
    for dialect in Dialect::available() {
        let http = RoutedHttpClient::new();
        http.route(
            "https://docs.example/sitemap.xml",
            ok(200, &[], &sitemap(&["https://evil.example/pwn.html"])),
        );
        http.route(
            "https://evil.example/pwn.html",
            ok(200, &[("CONTENT-TYPE", "text/plain")], "stolen pad"),
        );
        let kit = connector_kit(dialect, &http, &[]);
        let tenant = mint_tenant(&kit).await;
        let key = tenant["api_key"].as_str().expect("api key");

        let reply = create_connector(
            &kit,
            key,
            json!({ "kind": "sitemap", "url": "https://docs.example/sitemap.xml" }),
        )
        .await;
        assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
        drain(&kit).await;

        assert_eq!(count_of(&kit, "sg_sources"), 0);
        assert_eq!(http.requested(), ["https://docs.example/sitemap.xml"]);
    }
}

#[pollster::test]
async fn a_transient_upstream_outage_retries_with_backoff_and_survives() {
    for dialect in Dialect::available() {
        let http = RoutedHttpClient::new();
        http.route(
            "https://docs.example/sitemap.xml",
            ok(200, &[], &sitemap(&["https://docs.example/a.html"])),
        );
        http.route_many(
            "https://docs.example/a.html",
            vec![
                ok(503, &[], "overloaded"),
                ok(200, &[("CONTENT-TYPE", "text/plain")], "the aardvark page"),
            ],
        );
        let kit = connector_kit(dialect, &http, &[]);
        let tenant = mint_tenant(&kit).await;
        let key = tenant["api_key"].as_str().expect("api key");

        let reply = create_connector(
            &kit,
            key,
            json!({ "kind": "sitemap", "url": "https://docs.example/sitemap.xml" }),
        )
        .await;
        assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
        drain(&kit).await;

        // The 503 was retried, not dropped: the row waits on the backoff
        // schedule (30 s base step) with its lease released — the frozen
        // clock has not reached it, so nothing more ran.
        assert_eq!(count_of(&kit, "sg_sources"), 0, "the retry is not yet due");
        assert_eq!(count_of(&kit, "sg_ingest_outbox"), 1, "one row awaits");
        let next_at =
            text_column(&kit, "SELECT next_attempt_at AS v FROM sg_ingest_outbox")[0].clone();
        assert_eq!(
            next_at,
            rfc3339(kit.clock.0 + time::Duration::seconds(30)),
            "the first retry waits the 30 s base step"
        );
        let locked_until = option_column(&kit, "SELECT locked_until AS v FROM sg_ingest_outbox");
        assert_eq!(locked_until, [Option::<String>::None], "lease released");

        // Advance the clock past the backoff and re-sync: the retry runs
        // and succeeds.
        resync_at(&kit, &http, kit.clock.0 + time::Duration::seconds(60), &[]).await;
        assert_eq!(count_of(&kit, "sg_sources"), 1, "the retry succeeded");
        assert_eq!(count_of(&kit, "sg_ingest_outbox"), 0, "both rows retired");
    }
}

/// `rfc3339` is `connectors.rs`'s private timestamp helper; the retry
/// assertion only needs its shape.
fn rfc3339(at: time::OffsetDateTime) -> String {
    let formatted = at
        .format(&time::format_description::well_known::Rfc3339)
        .expect("timestamp formats");
    formatted.split('.').next().unwrap_or(&formatted).to_owned()
}

// -- What main gained since the branch: source management, bigrams, the
// -- combined cron hook, and the migration sequence -------------------------

/// `Module::scheduled` over the kit's database with the routed fake as
/// the `HttpClient` and, when given, a `MemoryBlob` — the port the upload
/// sweep needs — returning the tick's result instead of asserting it.
async fn tick(
    kit: &TestHarness,
    http: &RoutedHttpClient,
    at: time::OffsetDateTime,
    blob: Option<&MemoryBlob>,
) -> Result<(), cratefield_core::AnyError> {
    let module = Support::new();
    let mut ports = Ports::with_config(Arc::new(MapConfig::from_pairs([(
        "ADMIN_TOKEN",
        ADMIN_TOKEN,
    )])));
    ports.db = Some(kit.db.clone());
    ports.http = Some(Arc::new(http.clone()));
    ports.clock = Some(Arc::new(cratefield_testing::FixedClock(at)));
    ports.id_gen = Some(Arc::new(cratefield_core::UlidIdGen));
    ports.defer = Some(Arc::new(kit.defer.clone()));
    ports.signer = Some(kit.signer.clone());
    ports.blob = blob.map(|blob| Arc::new(blob.clone()) as Arc<dyn cratefield_core::Blob>);
    let ctx = kit.harness.module_context(&module, &ports);
    module.scheduled(&ctx, "23 4 * * *").await
}

/// One sitemap naming one page, the page answering `bodies` in order.
fn one_page_site(http: &RoutedHttpClient, page: &str, bodies: &[(&str, &str)]) {
    http.route(
        "https://docs.example/sitemap.xml",
        ok(200, &[], &sitemap(&[page])),
    );
    http.route_many(
        page,
        bodies
            .iter()
            .map(|(etag, body)| ok(200, &[("CONTENT-TYPE", "text/plain"), ("ETAG", etag)], body))
            .collect(),
    );
}

async fn crawl_sitemap(kit: &TestHarness, key: &str) {
    let reply = create_connector(
        kit,
        key,
        json!({ "kind": "sitemap", "url": "https://docs.example/sitemap.xml" }),
    )
    .await;
    assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
    drain(kit).await;
}

#[pollster::test]
async fn connector_sources_list_with_their_url_as_external_id_and_can_be_deleted() {
    for dialect in Dialect::available() {
        let http = RoutedHttpClient::new();
        let page = "https://docs.example/returns.html";
        let text = "returns are accepted within thirty days";
        one_page_site(&http, page, &[("\"p1\"", text)]);
        let kit = connector_kit(dialect, &http, &[]);
        let tenant = mint_tenant(&kit).await;
        let key = tenant["api_key"].as_str().expect("api key");
        crawl_sitemap(&kit, key).await;

        let listed = send(
            &kit.router,
            Method::GET,
            "/v1/support/sources",
            Some(key),
            None,
        )
        .await;
        assert_eq!(listed.status, StatusCode::OK, "{}", listed.body);
        let sources = listed.body["sources"].as_array().expect("sources").clone();
        assert_eq!(sources.len(), 1, "{}", listed.body);
        let source = &sources[0];
        assert_eq!(source["external_id"], page, "the URL is the upsert key");
        assert_eq!(source["origin"], "url");
        assert_eq!(source["title"], page);
        assert_eq!(source["bytes"], text.len());
        assert_eq!(source["updated_at"], rfc3339(kit.clock.0));
        assert_eq!(
            text_column(&kit, "SELECT created_at AS v FROM sg_sources"),
            text_column(&kit, "SELECT updated_at AS v FROM sg_sources"),
            "a first index is created and updated at once"
        );

        let id = source["id"].as_str().expect("source id");
        let deleted = send(
            &kit.router,
            Method::DELETE,
            &format!("/v1/support/sources/{id}"),
            Some(key),
            None,
        )
        .await;
        assert_eq!(deleted.status, StatusCode::NO_CONTENT);
        assert_eq!(count_of(&kit, "sg_sources"), 0);
        assert_eq!(count_of(&kit, "sg_chunks"), 0);
        assert_eq!(count_of(&kit, "sg_postings"), 0);
        assert_eq!(search_hits(&kit, key, "thirty").await, Vec::<Value>::new());
    }
}

#[pollster::test]
async fn a_changed_page_replaces_its_source_in_place_and_bumps_updated_at() {
    for dialect in Dialect::available() {
        let http = RoutedHttpClient::new();
        let page = "https://docs.example/shipping.html";
        one_page_site(
            &http,
            page,
            &[
                ("\"p1\"", "parcels leave the rotterdam depot"),
                ("\"p2\"", "parcels leave the antwerp depot"),
            ],
        );
        let kit = connector_kit(dialect, &http, &[]);
        let tenant = mint_tenant(&kit).await;
        let key = tenant["api_key"].as_str().expect("api key");
        crawl_sitemap(&kit, key).await;
        let id_before = text_column(&kit, "SELECT id AS v FROM sg_sources");
        let created = text_column(&kit, "SELECT created_at AS v FROM sg_sources");
        let tenant_id = tenant["tenant_id"].as_str().expect("tenant id");
        assert!(assert_stats_exact(&kit, "after the first crawl") > 0);
        assert_eq!(df_of(&kit, tenant_id, "rotterdam"), Some(1));

        let later = kit.clock.0 + time::Duration::hours(24);
        tick(&kit, &http, later, None).await.expect("tick runs");

        assert_eq!(
            text_column(&kit, "SELECT id AS v FROM sg_sources"),
            id_before,
            "replaced in place: same source id, no second copy"
        );
        assert_eq!(
            text_column(&kit, "SELECT created_at AS v FROM sg_sources"),
            created,
            "the first index date survives"
        );
        assert_eq!(
            text_column(&kit, "SELECT updated_at AS v FROM sg_sources"),
            [rfc3339(later)]
        );
        assert_eq!(search_hits(&kit, key, "antwerp").await.len(), 1);
        assert_eq!(
            search_hits(&kit, key, "rotterdam").await,
            Vec::<Value>::new()
        );
        // The connector's replace diffed the statistics like the manual
        // PUT does: the vanished window's terms out, the new one's in.
        assert_stats_exact(&kit, "after the connector re-index");
        assert_eq!(df_of(&kit, tenant_id, "rotterdam"), None);
        assert_eq!(df_of(&kit, tenant_id, "antwerp"), Some(1));
        assert_eq!(df_of(&kit, tenant_id, "parcels"), Some(1));
    }
}

#[pollster::test]
async fn a_hand_indexed_source_under_the_page_url_is_adopted_not_duplicated() {
    for dialect in Dialect::available() {
        let http = RoutedHttpClient::new();
        let page = "https://docs.example/faq.html";
        one_page_site(&http, page, &[("\"p1\"", "the warranty covers two years")]);
        let kit = connector_kit(dialect, &http, &[]);
        let tenant = mint_tenant(&kit).await;
        let key = tenant["api_key"].as_str().expect("api key");

        let manual = send(
            &kit.router,
            Method::POST,
            "/v1/support/sources",
            Some(key),
            Some(
                &json!({ "title": "FAQ", "text": "the warranty covers one year", "external_id": page })
                    .to_string(),
            ),
        )
        .await;
        assert_eq!(manual.status, StatusCode::CREATED, "{}", manual.body);
        let manual_id = manual.body["source_id"].as_str().expect("id").to_owned();

        // An anonymous source stays untouched by any connector.
        let anonymous = send(
            &kit.router,
            Method::POST,
            "/v1/support/sources",
            Some(key),
            Some(&json!({ "title": "Other", "text": "unrelated gazebo notes" }).to_string()),
        )
        .await;
        assert_eq!(anonymous.status, StatusCode::CREATED, "{}", anonymous.body);

        crawl_sitemap(&kit, key).await;

        assert_eq!(count_of(&kit, "sg_sources"), 2, "adopted, not duplicated");
        assert_eq!(
            text_column(
                &kit,
                &format!("SELECT id AS v FROM sg_sources WHERE external_id = '{page}'")
            ),
            std::slice::from_ref(&manual_id)
        );
        assert_eq!(
            text_column(
                &kit,
                &format!("SELECT source_id AS v FROM sg_ingest_pages WHERE url = '{page}'")
            ),
            [manual_id],
            "the page row points at the adopted source"
        );
        assert_eq!(search_hits(&kit, key, "two").await.len(), 1);
        assert_eq!(search_hits(&kit, key, "gazebo").await.len(), 1);
        // Adoption is a replace of the hand-indexed source: its "one"
        // left the statistics, the crawled "two" entered.
        assert_stats_exact(&kit, "after adopting a hand-indexed source");
        let tenant_id = tenant["tenant_id"].as_str().expect("tenant id");
        assert_eq!(df_of(&kit, tenant_id, "one"), None);
        assert_eq!(df_of(&kit, tenant_id, "warranty"), Some(1));
    }
}

#[pollster::test]
async fn a_crawled_japanese_page_is_found_by_bigram_search() {
    for dialect in Dialect::available() {
        let http = RoutedHttpClient::new();
        one_page_site(
            &http,
            "https://docs.example/ja/returns.html",
            &[("\"p1\"", "返品は三十日以内に受け付けます")],
        );
        let kit = connector_kit(dialect, &http, &[]);
        let tenant = mint_tenant(&kit).await;
        let key = tenant["api_key"].as_str().expect("api key");
        crawl_sitemap(&kit, key).await;

        // "返品" (returns), percent-encoded for the query string.
        let hits = search_hits(&kit, key, "%E8%BF%94%E5%93%81").await;
        assert_eq!(hits.len(), 1, "a CJK bigram finds the crawled page");
    }
}

#[pollster::test]
async fn one_cron_tick_runs_every_sweep_even_when_one_fails() {
    for dialect in Dialect::available() {
        let http = RoutedHttpClient::new();
        let page = "https://docs.example/hours.html";
        one_page_site(
            &http,
            page,
            &[
                ("\"p1\"", "the desk opens at nine"),
                ("\"p2\"", "the desk opens at eight"),
            ],
        );
        let kit = connector_kit(dialect, &http, &[]);
        let tenant = mint_tenant(&kit).await;
        let key = tenant["api_key"].as_str().expect("api key");
        crawl_sitemap(&kit, key).await;

        // Re-index work: every chunk stamped as an older tokenizer's.
        pollster::block_on(kit.db.execute(&Statement::new(
            "UPDATE sg_chunks SET tokenizer_version = 1",
        )))
        .expect("stale stamp written");
        // Upload-sweep failure: its outbox is gone, so its claim errors.
        pollster::block_on(
            kit.db
                .execute(&Statement::new("DROP TABLE sg_support_outbox")),
        )
        .expect("outbox dropped");

        let blob = MemoryBlob::new();
        let later = kit.clock.0 + time::Duration::hours(24);
        let result = tick(&kit, &http, later, Some(&blob)).await;

        assert!(result.is_err(), "the upload sweep's error is reported");
        // The re-index before it ran: the page's chunks were rewritten,
        // then replaced by the re-sync with current-tokenizer rows.
        assert_eq!(
            count_of(&kit, "sg_chunks WHERE tokenizer_version = 1"),
            0,
            "the re-index sweep ran"
        );
        // The connector re-sync after it ran too.
        assert_eq!(search_hits(&kit, key, "eight").await.len(), 1);
        assert_eq!(search_hits(&kit, key, "nine").await, Vec::<Value>::new());
        assert_eq!(count_of(&kit, "sg_ingest_outbox"), 0, "all jobs retired");
        // The re-index sweep and the re-sync's replace each moved the
        // statistics with the rows, in one tick.
        assert!(assert_stats_exact(&kit, "after the re-index and the re-sync") > 0);
    }
}

#[test]
fn the_connector_migration_is_support_0006() {
    let migrations = Support::new().migrations();
    let ids: Vec<(&str, &str)> = migrations
        .sqlite
        .iter()
        .map(|migration| (migration.id, migration.name))
        .collect();
    assert_eq!(
        ids,
        [
            ("0001", "init"),
            ("0002", "conversations"),
            ("0003", "source_management"),
            ("0004", "internationalization"),
            ("0005", "uploads"),
            ("0006", "connectors"),
            ("0007", "search_stats"),
            ("0008", "widget_settings"),
            ("0009", "human_handoff"),
        ],
        "connectors is support/0006, after main's 0003/0004 and the uploads 0005"
    );
}
