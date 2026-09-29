//! Issue #2 acceptance, over every available dialect: tenant
//! provisioning, ingest chunk/posting bookkeeping, the 48 KiB ceiling,
//! URL ingest, BM25 ranking, per-tenant isolation, the
//! indistinguishable-401 auth matrix, and the search edge cases.
//!
//! Issue #3 acceptance (`POST /messages` and the tenant settings route)
//! follows, from "Issue #3" below. The model is `cratefield_testing`'s
//! `FakeTextModel`, so every outcome is mode-scripted, not stochastic;
//! content is seeded through the real `/sources` ingest, so every cited
//! chunk id is one BM25 genuinely retrieves.

use axum::body::{Body, to_bytes};
use axum::http::{HeaderMap, Method, Request, StatusCode, header};
use cratefield_core::{
    Completion, Database, Decision, MapConfig, ModelTier, Module, Prompt, Statement, TextModel,
    TextModelError,
};
use cratefield_testing::{
    Dialect, FakeHttpClient, FakeRateLimiter, FakeTextModel, TestHarness, TextModelMode,
};
use module_support::Support;
use module_support::chunk::Chunker;
use serde_json::{Value, json};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tower::ServiceExt;

const ADMIN_TOKEN: &str = "test-admin-token-0123456789abcdef";
const ADMIN: &str = "/v1/support/admin/tenants";
const SOURCES: &str = "/v1/support/sources";
const SEARCH: &str = "/v1/support/search";
const MESSAGES: &str = "/v1/support/messages";
const PROBLEMS: &str = "https://factory0.ventures/problems/";

/// A buffered response: every route here answers JSON (success or
/// problem+json), so the body is parsed once, eagerly.
struct Reply {
    status: StatusCode,
    headers: HeaderMap,
    body: Value,
}

impl Reply {
    async fn of(response: axum::response::Response) -> Self {
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("response body reads");
        // A 204 carries no body, which parses as null rather than as an
        // error — every other response here is JSON.
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).expect("response body is JSON")
        };
        Self {
            status,
            headers,
            body,
        }
    }

    /// The problem `type` URI: what the auth-matrix equality hangs on.
    fn problem_type(&self) -> &str {
        self.body["type"].as_str().unwrap_or_default()
    }
}

/// `cratefield_testing::request` sends no headers and every route here
/// needs an `Authorization` bearer, so the kit's oneshot pattern is
/// reproduced here with a header slot.
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
    Reply::of(response).await
}

fn support() -> Vec<Box<dyn Module>> {
    vec![Box::new(Support::new())]
}

/// One kit per available dialect, configured with the admin token.
fn kit_with(dialect: Dialect, extra: &[(&'static str, &str)]) -> TestHarness {
    TestHarness::with_database_and_ports(support(), dialect, |ports| {
        let mut pairs = vec![("ADMIN_TOKEN", ADMIN_TOKEN)];
        pairs.extend(extra.iter().copied());
        ports.config = Arc::new(MapConfig::from_pairs(pairs));
    })
}

fn kits() -> Vec<TestHarness> {
    Dialect::available()
        .into_iter()
        .map(|dialect| kit_with(dialect, &[]))
        .collect()
}

async fn mint_tenant(kit: &TestHarness, name: &str) -> Value {
    let body = json!({ "name": name }).to_string();
    let reply = send(
        &kit.router,
        Method::POST,
        ADMIN,
        Some(ADMIN_TOKEN),
        Some(&body),
    )
    .await;
    assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
    reply.body
}

async fn ingest(kit: &TestHarness, key: &str, body: Value) -> Reply {
    send(
        &kit.router,
        Method::POST,
        SOURCES,
        Some(key),
        Some(&body.to_string()),
    )
    .await
}

async fn search(kit: &TestHarness, key: &str, query: &str) -> Reply {
    send(
        &kit.router,
        Method::GET,
        &format!("{SEARCH}?{query}"),
        Some(key),
        None,
    )
    .await
}

fn count_of(kit: &TestHarness, table: &str) -> usize {
    let rows = pollster::block_on(kit.db.query(&Statement::new(format!(
        "SELECT COUNT(*) AS n FROM {table}"
    ))))
    .expect("count query runs");
    let n = rows
        .rows
        .first()
        .and_then(|row| row.get::<i64>("n"))
        .expect("aggregate row");
    usize::try_from(n).expect("count is non-negative")
}

/// The single text column of each row, aliased `v`, in query order.
fn text_column(kit: &TestHarness, sql: &str) -> Vec<String> {
    let rows = pollster::block_on(kit.db.query(&Statement::new(sql))).expect("query runs");
    rows.rows
        .iter()
        .map(|row| row.get::<String>("v").expect("text value"))
        .collect()
}

fn body_str(body: &Value, field: &str) -> String {
    body[field].as_str().expect(field).to_owned()
}

#[pollster::test]
async fn provisioning_mints_a_usable_key_and_records_both_rows() {
    for kit in kits() {
        let tenant = mint_tenant(&kit, "Acme Support").await;
        let tenant_id = body_str(&tenant, "tenant_id");
        let api_key = body_str(&tenant, "api_key");
        let kid = body_str(&tenant, "kid");
        assert_eq!(tenant["name"], "Acme Support");
        assert!(api_key.starts_with("sg_"), "key shape: {api_key}");
        assert!(!kid.is_empty());

        assert_eq!(count_of(&kit, "sg_tenants"), 1);
        assert_eq!(count_of(&kit, "sg_api_keys"), 1);
        assert_eq!(
            text_column(
                &kit,
                &format!("SELECT status AS v FROM sg_tenants WHERE id = '{tenant_id}'")
            ),
            ["active"]
        );
        assert_eq!(
            text_column(
                &kit,
                &format!("SELECT kid AS v FROM sg_api_keys WHERE tenant_id = '{tenant_id}'")
            ),
            [kid]
        );

        // The minted key is usable, exactly as the response claims.
        let reply = ingest(
            &kit,
            &api_key,
            json!({ "title": "Policy", "text": "the refund window is thirty days" }),
        )
        .await;
        assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
    }
}

#[pollster::test]
async fn admin_route_answers_401_without_a_token_and_403_with_the_wrong_token() {
    for kit in kits() {
        let body = Some(r#"{"name":"Acme"}"#);
        let absent = send(&kit.router, Method::POST, ADMIN, None, body).await;
        assert_eq!(absent.status, StatusCode::UNAUTHORIZED);
        assert_eq!(
            absent.problem_type(),
            format!("{PROBLEMS}admin-unauthorized")
        );

        let wrong = send(
            &kit.router,
            Method::POST,
            ADMIN,
            Some("not-the-token"),
            body,
        )
        .await;
        assert_eq!(wrong.status, StatusCode::FORBIDDEN);
        assert_eq!(wrong.problem_type(), format!("{PROBLEMS}admin-forbidden"));
    }
}

#[pollster::test]
async fn unauthenticated_malformed_body_gets_the_guards_401_not_the_extractors_4xx() {
    for kit in kits() {
        let reply = send(
            &kit.router,
            Method::POST,
            ADMIN,
            None,
            Some("this is not json"),
        )
        .await;
        assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{}", reply.body);
        assert_eq!(
            reply.problem_type(),
            format!("{PROBLEMS}admin-unauthorized")
        );

        // The same malformed body under a valid token is the 400 it
        // always was — the guard only reorders who answers first.
        let authenticated = send(
            &kit.router,
            Method::POST,
            ADMIN,
            Some(ADMIN_TOKEN),
            Some("{oops"),
        )
        .await;
        assert_eq!(authenticated.status, StatusCode::BAD_REQUEST);
        assert_eq!(
            authenticated.problem_type(),
            format!("{PROBLEMS}validation-failed")
        );
    }
}

#[pollster::test]
async fn unauthenticated_malformed_sources_body_gets_the_auth_401_not_the_extractors_4xx() {
    for kit in kits() {
        // `POST /sources` used to take a `Json<SourceIngest>` extractor,
        // which parsed the body before the handler could authenticate:
        // an unauthenticated caller with a malformed body got the
        // extractor's 4xx instead of the one indistinguishable 401,
        // leaking that auth was never reached.
        let reply = send(&kit.router, Method::POST, SOURCES, None, Some("{oops")).await;
        assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{}", reply.body);
        assert_eq!(reply.problem_type(), format!("{PROBLEMS}unauthorized"));

        // The same malformed body under a valid key is the 400 it always
        // was — authentication only reorders who answers first.
        let tenant = mint_tenant(&kit, "Malformed").await;
        let authenticated = send(
            &kit.router,
            Method::POST,
            SOURCES,
            Some(&body_str(&tenant, "api_key")),
            Some("{oops"),
        )
        .await;
        assert_eq!(
            authenticated.status,
            StatusCode::BAD_REQUEST,
            "{}",
            authenticated.body
        );
        assert_eq!(
            authenticated.problem_type(),
            format!("{PROBLEMS}validation-failed")
        );
    }
}

#[pollster::test]
async fn ingest_writes_chunks_and_postings_exactly_as_the_chunker_produces_them() {
    for kit in kits() {
        let tenant = mint_tenant(&kit, "Ingest").await;
        let api_key = body_str(&tenant, "api_key");
        let tenant_id = body_str(&tenant, "tenant_id");
        let text: String = (0..400)
            .map(|i| format!("word{i:03}"))
            .collect::<Vec<_>>()
            .join(" ");

        let reply = ingest(&kit, &api_key, json!({ "title": "Policy", "text": text })).await;
        assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
        let source_id = body_str(&reply.body, "source_id");

        let expected = Chunker::default().split(&tenant_id, &source_id, &text);
        assert!(expected.len() > 1, "fixture must span several chunks");
        assert_eq!(reply.body["chunks"].as_u64(), Some(expected.len() as u64));

        assert_eq!(count_of(&kit, "sg_chunks"), expected.len());
        let expected_postings: usize = expected.iter().map(|chunk| chunk.terms.len()).sum();
        assert!(expected_postings > expected.len());
        assert_eq!(count_of(&kit, "sg_postings"), expected_postings);

        let stored_ids = text_column(
            &kit,
            &format!(
                "SELECT id AS v FROM sg_chunks WHERE source_id = '{source_id}' ORDER BY ordinal"
            ),
        );
        let expected_ids: Vec<&str> = expected.iter().map(|chunk| chunk.id.as_str()).collect();
        assert_eq!(stored_ids, expected_ids);
    }
}

#[pollster::test]
async fn stored_chunk_ids_are_content_addresses_and_a_resplit_of_the_same_text_is_identical() {
    for kit in kits() {
        let tenant = mint_tenant(&kit, "Stable").await;
        let api_key = body_str(&tenant, "api_key");
        let tenant_id = body_str(&tenant, "tenant_id");
        let text = "ferritin stores iron inside a protein shell".to_owned();

        let reply = ingest(&kit, &api_key, json!({ "text": text })).await;
        let source_id = body_str(&reply.body, "source_id");

        // The stable-id guarantee, end to end: what the DB carries is
        // exactly the chunker's content address of
        // (tenant, source, text), and re-splitting the same text under
        // the same pair yields the same ids again.
        let stored = text_column(
            &kit,
            &format!(
                "SELECT id AS v FROM sg_chunks WHERE source_id = '{source_id}' ORDER BY ordinal"
            ),
        );
        let first_split = Chunker::default().split(&tenant_id, &source_id, &text);
        let resplit = Chunker::default().split(&tenant_id, &source_id, &text);
        let expected: Vec<&str> = first_split.iter().map(|chunk| chunk.id.as_str()).collect();
        assert_eq!(stored, expected);
        let again: Vec<&str> = resplit.iter().map(|chunk| chunk.id.as_str()).collect();
        assert_eq!(expected, again);
    }
}

#[pollster::test]
async fn text_at_the_48_kib_ceiling_is_accepted_and_one_byte_over_is_rejected() {
    for kit in kits() {
        let tenant = mint_tenant(&kit, "Ceiling").await;
        let api_key = body_str(&tenant, "api_key");

        let at_limit = "lorem ipsum ".repeat(4096);
        assert_eq!(at_limit.len(), 48 * 1024);
        let ok = ingest(&kit, &api_key, json!({ "text": at_limit })).await;
        assert_eq!(ok.status, StatusCode::CREATED, "{}", ok.body);

        let one_over = format!("{at_limit}x");
        let rejected = ingest(&kit, &api_key, json!({ "text": one_over })).await;
        assert_eq!(
            rejected.status,
            StatusCode::BAD_REQUEST,
            "{}",
            rejected.body
        );
        assert_eq!(
            rejected.problem_type(),
            format!("{PROBLEMS}validation-failed")
        );
        let detail = rejected.body["detail"].as_str().expect("detail present");
        assert!(
            detail.contains("49152"),
            "detail must name the limit: {detail}"
        );
    }
}

#[pollster::test]
async fn url_ingest_fetches_through_the_http_port_and_records_the_source_url() {
    for dialect in Dialect::available() {
        let url = "https://docs.example/handbook";
        let fake = FakeHttpClient::scripted(vec![
            http::Response::builder()
                .status(200)
                .header(header::CONTENT_TYPE, "text/plain")
                .body(bytes::Bytes::from_static(
                    b"the handbook covers refunds and the handbook covers shipping",
                ))
                .map_err(|err| cratefield_core::HttpError::Transport(err.to_string())),
        ]);
        let handle = fake.clone();
        let kit = TestHarness::with_database_and_ports(support(), dialect, |ports| {
            ports.config = Arc::new(MapConfig::from_pairs([("ADMIN_TOKEN", ADMIN_TOKEN)]));
            ports.http = Some(Arc::new(handle));
        });

        let tenant = mint_tenant(&kit, "Fetched").await;
        let api_key = body_str(&tenant, "api_key");
        let reply = ingest(&kit, &api_key, json!({ "url": url, "title": "Handbook" })).await;
        assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
        let source_id = body_str(&reply.body, "source_id");
        assert!(reply.body["chunks"].as_u64().unwrap_or(0) > 0);

        // The requested URL reached the client port and landed on the
        // source row.
        assert!(
            fake.captured()
                .iter()
                .any(|(_method, uri, _body)| uri == url)
        );
        assert_eq!(
            text_column(
                &kit,
                &format!("SELECT url AS v FROM sg_sources WHERE id = '{source_id}'")
            ),
            [url]
        );
    }
}

#[pollster::test]
async fn url_ingest_without_an_http_client_port_answers_503_not_ready() {
    for dialect in Dialect::available() {
        let kit = TestHarness::with_database_and_ports(support(), dialect, |ports| {
            ports.config = Arc::new(MapConfig::from_pairs([("ADMIN_TOKEN", ADMIN_TOKEN)]));
            ports.http = None;
        });
        let tenant = mint_tenant(&kit, "NoFetch").await;
        let api_key = body_str(&tenant, "api_key");
        let reply = ingest(
            &kit,
            &api_key,
            json!({ "url": "https://docs.example/handbook" }),
        )
        .await;
        assert_eq!(
            reply.status,
            StatusCode::SERVICE_UNAVAILABLE,
            "{}",
            reply.body
        );
        assert_eq!(reply.problem_type(), format!("{PROBLEMS}not-ready"));
    }
}

#[pollster::test]
async fn search_ranks_the_better_matching_document_first_with_strictly_ordered_scores() {
    for kit in kits() {
        let tenant = mint_tenant(&kit, "Ranked").await;
        let api_key = body_str(&tenant, "api_key");

        // "quokka" occurs in one document only; "kangaroo" in both; the
        // pad word keeps document lengths comparable.
        let a = ingest(
            &kit,
            &api_key,
            json!({ "title": "Quokka", "text": "quokka quokka quokka kangaroo pad" }),
        )
        .await;
        let b = ingest(
            &kit,
            &api_key,
            json!({ "title": "Kangaroo", "text": "kangaroo kangaroo kangaroo pad" }),
        )
        .await;
        let (a_id, b_id) = (
            body_str(&a.body, "source_id"),
            body_str(&b.body, "source_id"),
        );

        // A matches both query terms, B only one: A must rank first and
        // the scores must be strictly ordered.
        let both = search(&kit, &api_key, "q=quokka+kangaroo").await;
        assert_eq!(both.status, StatusCode::OK, "{}", both.body);
        let results = both.body["results"].as_array().expect("results array");
        assert_eq!(results.len(), 2, "both documents match: {}", both.body);
        assert_eq!(results[0]["source_id"], a_id, "A outranks B: {}", both.body);
        assert_eq!(results[1]["source_id"], b_id);
        let top = results[0]["score"].as_f64().expect("score");
        let second = results[1]["score"].as_f64().expect("score");
        assert!(top > second, "scores strictly ordered: {top} vs {second}");

        // A term appearing in only one document outranks one appearing
        // in both: same tf (3) and comparable length, so the gap is the
        // IDF of quokka (1 of 2 documents) versus kangaroo (2 of 2).
        let quokka = search(&kit, &api_key, "q=quokka").await;
        let kangaroo = search(&kit, &api_key, "q=kangaroo").await;
        let quokka_top = quokka.body["results"][0]["score"].as_f64().expect("score");
        let kangaroo_top = kangaroo.body["results"][0]["score"]
            .as_f64()
            .expect("score");
        assert!(
            quokka_top > kangaroo_top,
            "rare term outranks common one: {quokka_top} vs {kangaroo_top}"
        );
    }
}

#[pollster::test]
async fn tenant_b_never_sees_tenant_a_documents_in_search_results() {
    for kit in kits() {
        let a = mint_tenant(&kit, "Tenant A").await;
        let b = mint_tenant(&kit, "Tenant B").await;
        let (a_key, b_key) = (body_str(&a, "api_key"), body_str(&b, "api_key"));

        let a_source = body_str(
            &ingest(
                &kit,
                &a_key,
                json!({ "text": "ferritin vault stores the quokka archive" }),
            )
            .await
            .body,
            "source_id",
        );
        let b_source = body_str(
            &ingest(&kit, &b_key, json!({ "text": "quokka habitat notes" }))
                .await
                .body,
            "source_id",
        );
        let a_chunk_ids = text_column(
            &kit,
            &format!(
                "SELECT id AS v FROM sg_chunks WHERE tenant_id = '{}'",
                body_str(&a, "tenant_id")
            ),
        );
        assert!(!a_chunk_ids.is_empty());

        // A's distinctive term finds nothing for B: zero results, so
        // none of A's ids can leak either.
        let distinctive = search(&kit, &b_key, "q=ferritin").await;
        assert_eq!(distinctive.status, StatusCode::OK, "{}", distinctive.body);
        assert_eq!(
            distinctive.body["results"]
                .as_array()
                .expect("results")
                .len(),
            0,
            "B must see none of A's documents: {}",
            distinctive.body
        );

        // Even on the term both tenants indexed, B sees only B's own
        // source, and no A id appears anywhere in the response.
        let shared = search(&kit, &b_key, "q=quokka").await;
        assert_eq!(shared.status, StatusCode::OK, "{}", shared.body);
        let results = shared.body["results"].as_array().expect("results");
        assert!(!results.is_empty(), "B still sees its own document");
        for result in results {
            assert_eq!(
                result["source_id"], b_source,
                "only B's source: {}",
                shared.body
            );
        }
        let response = shared.body.to_string();
        assert!(
            !response.contains(&a_source),
            "A's source id leaked: {response}"
        );
        for chunk_id in &a_chunk_ids {
            assert!(
                !response.contains(chunk_id),
                "A's chunk id leaked: {response}"
            );
        }
    }
}

#[pollster::test]
async fn every_key_failure_answers_the_same_indistinguishable_401() {
    for dialect in Dialect::available() {
        let kit = kit_with(dialect.clone(), &[]);
        let tenant = mint_tenant(&kit, "Matrix").await;
        let api_key = body_str(&tenant, "api_key");

        let absent = send(&kit.router, Method::GET, SEARCH, None, None).await;
        let junk = send(&kit.router, Method::GET, SEARCH, Some("junk"), None).await;
        let mut tampered = api_key.clone().into_bytes();
        let last = tampered.len() - 1;
        tampered[last] = if tampered[last] == b'A' { b'B' } else { b'A' };
        let flipped_mac = send(
            &kit.router,
            Method::GET,
            SEARCH,
            Some(std::str::from_utf8(&tampered).expect("still utf-8")),
            None,
        )
        .await;

        // The kit's ring is derived from one fixed test secret, so every
        // kit's minted key carries the same kid; revoking that kid in a
        // fresh kit's config makes its freshly minted key fail.
        let kid = body_str(&tenant, "kid");
        let revoked_kit = kit_with(dialect, &[("SUPPORT_REVOKED_KIDS", &kid)]);
        let re_minted = mint_tenant(&revoked_kit, "Matrix").await;
        let revoked = send(
            &revoked_kit.router,
            Method::GET,
            SEARCH,
            Some(&body_str(&re_minted, "api_key")),
            None,
        )
        .await;

        let expected_type = absent.problem_type().to_owned();
        assert_eq!(absent.status, StatusCode::UNAUTHORIZED);
        assert_eq!(junk.status, StatusCode::UNAUTHORIZED);
        assert_eq!(flipped_mac.status, StatusCode::UNAUTHORIZED);
        assert_eq!(revoked.status, StatusCode::UNAUTHORIZED);
        for reply in [&junk, &flipped_mac, &revoked] {
            assert_eq!(
                reply.problem_type(),
                expected_type,
                "all key failures share one problem type: {}",
                reply.body
            );
        }
        assert_eq!(expected_type, format!("{PROBLEMS}unauthorized"));
    }
}

#[pollster::test]
async fn empty_query_answers_empty_results_and_limit_clamps_to_the_hard_range() {
    for kit in kits() {
        let tenant = mint_tenant(&kit, "Edges").await;
        let api_key = body_str(&tenant, "api_key");

        // ~72 chunks, every one of them containing the marker word, so
        // a q=zz match set is bigger than any limit under test.
        let words: Vec<String> = (0..5400)
            .flat_map(|i| vec![format!("t{i:04}"), "zz".to_owned()])
            .collect();
        let reply = ingest(&kit, &api_key, json!({ "text": words.join(" ") })).await;
        assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
        assert!(reply.body["chunks"].as_u64().unwrap_or(0) > 50);

        let result_count = |reply: &Reply| -> usize {
            assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
            reply.body["results"].as_array().expect("results").len()
        };

        assert_eq!(result_count(&search(&kit, &api_key, "q=zz").await), 10);
        assert_eq!(
            result_count(&search(&kit, &api_key, "q=zz&limit=0").await),
            1
        );
        assert_eq!(
            result_count(&search(&kit, &api_key, "q=zz&limit=1").await),
            1
        );
        assert_eq!(
            result_count(&search(&kit, &api_key, "q=zz&limit=1000").await),
            50
        );

        let empty_text = search(&kit, &api_key, "q=").await;
        assert_eq!(empty_text.status, StatusCode::OK);
        assert_eq!(
            empty_text.body["results"]
                .as_array()
                .expect("results")
                .len(),
            0
        );
        let absent_q = send(&kit.router, Method::GET, SEARCH, Some(&api_key), None).await;
        assert_eq!(absent_q.status, StatusCode::OK);
        assert_eq!(
            absent_q.body["results"].as_array().expect("results").len(),
            0
        );
    }
}

// ---------------------------------------------------------------------
// Issue #3: `POST /messages` and `PUT /admin/tenants/{id}/settings`.
// ---------------------------------------------------------------------

const QUESTION: &str = "How do I reset my password?";
const RESET_DOC: &str = "Reset your password from the settings page under Security.";
const BILLING_DOC: &str = "Billing plans are changed on the billing page by an owner.";

/// A kit whose `Support` asks `model`. The fake starts unconfigured: a
/// test sets its mode once ingest has minted the chunk ids the reply
/// cites.
fn kit_with_model(dialect: Dialect, model: &FakeTextModel) -> TestHarness {
    TestHarness::with_database_and_ports(vec![Box::new(Support::new())], dialect, |ports| {
        ports.config = Arc::new(MapConfig::from_pairs([("ADMIN_TOKEN", ADMIN_TOKEN)]));
        ports.text_model = Some(Arc::new(model.clone()));
    })
}

/// One `(kit, model)` pair per available dialect, each with its own fake.
fn model_kits() -> Vec<(TestHarness, FakeTextModel)> {
    Dialect::available()
        .into_iter()
        .map(|dialect| {
            let model = FakeTextModel::new(TextModelMode::NotConfigured);
            (kit_with_model(dialect, &model), model)
        })
        .collect()
}

/// A structured model reply, the way an adapter hands one back: the
/// parsed value in `json`, the same value as text. The mode `Complete`
/// answers it to every turn that asks, until the test switches modes.
fn reply(answer: &str, citations: &[(&str, &str)], confidence: f64) -> TextModelMode {
    let citations: Vec<Value> = citations
        .iter()
        .map(|(chunk_id, quote)| json!({ "chunk_id": chunk_id, "quote": quote }))
        .collect();
    let value = json!({ "answer": answer, "citations": citations, "confidence": confidence });
    TextModelMode::Complete(Completion::new(value.to_string(), "fake-fast").json(value))
}

/// A tenant with one ingested document, and the id of its only chunk.
struct Seeded {
    api_key: String,
    tenant_id: String,
    chunk_id: String,
}

async fn seed(kit: &TestHarness, name: &str, text: &str) -> Seeded {
    let tenant = mint_tenant(kit, name).await;
    let api_key = body_str(&tenant, "api_key");
    let chunk_id = ingest_one(kit, &api_key, text).await;
    Seeded {
        api_key,
        tenant_id: body_str(&tenant, "tenant_id"),
        chunk_id,
    }
}

/// Ingests a one-chunk document and returns that chunk's id.
async fn ingest_one(kit: &TestHarness, api_key: &str, text: &str) -> String {
    let reply = ingest(kit, api_key, json!({ "title": "Help", "text": text })).await;
    assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
    assert_eq!(reply.body["chunks"], 1, "fixture documents are one chunk");
    let source_id = body_str(&reply.body, "source_id");
    let ids = text_column(
        kit,
        &format!("SELECT id AS v FROM sg_chunks WHERE source_id = '{source_id}'"),
    );
    ids.into_iter().next().expect("one chunk")
}

async fn post_message(
    kit: &TestHarness,
    api_key: &str,
    message: &str,
    conversation_id: Option<&str>,
) -> Reply {
    let mut body = json!({ "message": message });
    if let Some(id) = conversation_id {
        body["conversation_id"] = json!(id);
    }
    send(
        &kit.router,
        Method::POST,
        MESSAGES,
        Some(api_key),
        Some(&body.to_string()),
    )
    .await
}

/// `post_message`, asserting a 200.
async fn turn(
    kit: &TestHarness,
    api_key: &str,
    message: &str,
    conversation_id: Option<&str>,
) -> Value {
    let reply = post_message(kit, api_key, message, conversation_id).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    reply.body
}

fn settings_path(tenant_id: &str) -> String {
    format!("{ADMIN}/{tenant_id}/settings")
}

fn count_where(kit: &TestHarness, table: &str, predicate: &str) -> usize {
    count_of(kit, &format!("{table} WHERE {predicate}"))
}

/// Every conversation row in full, for the "consumed nothing" checks:
/// row equality (status, flag, both timestamps), not just a count.
fn conversation_rows(kit: &TestHarness) -> Vec<(String, String, i64, String, String)> {
    let rows = pollster::block_on(kit.db.query(&Statement::new(
        "SELECT id, status, needs_escalation, created_at, updated_at \
         FROM sg_conversations ORDER BY id",
    )))
    .expect("conversation query runs");
    rows.rows
        .iter()
        .map(|row| {
            (
                row.get::<String>("id").expect("id"),
                row.get::<String>("status").expect("status"),
                row.get::<i64>("needs_escalation").expect("flag"),
                row.get::<String>("created_at").expect("created_at"),
                row.get::<String>("updated_at").expect("updated_at"),
            )
        })
        .collect()
}

/// `(status, needs_escalation)` of the one conversation.
fn conversation_state(kit: &TestHarness) -> (String, i64) {
    let rows = conversation_rows(kit);
    assert_eq!(rows.len(), 1, "exactly one conversation");
    let (_, status, flag, _, _) = rows.into_iter().next().expect("one row");
    (status, flag)
}

fn assistant_column(kit: &TestHarness, column: &str) -> Vec<String> {
    text_column(
        kit,
        &format!(
            "SELECT {column} AS v FROM sg_messages WHERE role = 'assistant' ORDER BY conversation_id, seq"
        ),
    )
}

fn citation_count(body: &Value) -> usize {
    body["citations"].as_array().expect("citations array").len()
}

fn conversation_id(body: &Value) -> String {
    body_str(body, "conversation_id")
}

#[pollster::test]
async fn an_answered_turn_persists_the_conversation_and_both_messages() {
    for (kit, model) in model_kits() {
        let seeded = seed(&kit, "Answered", RESET_DOC).await;
        model.set_mode(reply(
            "Reset it from Settings > Security.",
            &[(
                &seeded.chunk_id,
                "Reset your password from the settings page",
            )],
            0.9,
        ));

        let body = turn(&kit, &seeded.api_key, QUESTION, None).await;
        assert_eq!(body["outcome"], "answered");
        assert_eq!(body["answer"], "Reset it from Settings > Security.");
        assert_eq!(citation_count(&body), 1);
        assert_eq!(body["citations"][0]["chunk_id"], seeded.chunk_id.as_str());
        assert_eq!(
            body["citations"][0]["quote"],
            "Reset your password from the settings page"
        );
        assert_eq!(body["confidence"], 0.9);
        assert_eq!(body["needs_escalation"], false);
        assert!(!conversation_id(&body).is_empty());
        assert!(!body_str(&body, "message_id").is_empty());

        assert_eq!(count_of(&kit, "sg_conversations"), 1);
        assert_eq!(count_of(&kit, "sg_messages"), 2);
        assert_eq!(conversation_state(&kit), ("open".to_owned(), 0));
        assert_eq!(
            text_column(
                &kit,
                "SELECT body AS v FROM sg_messages WHERE role = 'user'"
            ),
            [QUESTION]
        );
        assert_eq!(assistant_column(&kit, "outcome"), ["answered"]);
        assert_eq!(
            assistant_column(&kit, "id"),
            [body_str(&body, "message_id")],
            "messageId is the assistant message"
        );
        // The raw citations are stored in the model's snake_case shape.
        assert_eq!(
            assistant_column(&kit, "citations"),
            [format!(
                r#"[{{"chunk_id":"{}","quote":"Reset your password from the settings page"}}]"#,
                seeded.chunk_id
            )]
        );
        assert_eq!(
            count_where(
                &kit,
                "sg_messages",
                "role = 'assistant' AND confidence_pct = 90"
            ),
            1
        );
    }
}

#[pollster::test]
async fn a_low_confidence_reply_clarifies_and_returns_no_citations() {
    for (kit, model) in model_kits() {
        let seeded = seed(&kit, "Low", RESET_DOC).await;
        model.set_mode(reply(
            "Have you tried turning it off and on again?",
            &[(&seeded.chunk_id, "Reset your password")],
            0.2,
        ));

        let body = turn(&kit, &seeded.api_key, QUESTION, None).await;
        assert_eq!(body["outcome"], "clarify");
        assert_eq!(citation_count(&body), 0);
        assert_eq!(body["needs_escalation"], false);
        let answer = body_str(&body, "answer");
        assert!(!answer.contains("off and on again"), "{answer}");
        assert!(answer.contains("could you rephrase"), "{answer}");
        assert_eq!(assistant_column(&kit, "outcome"), ["clarify"]);
    }
}

/// The headline rule: a citation that names a chunk not retrieved for this
/// request is decoration, so the answer is downgraded even at very high
/// confidence, and the response carries no citations at all.
#[pollster::test]
async fn a_citation_to_an_unretrieved_chunk_downgrades_answered_to_clarify() {
    for (kit, model) in model_kits() {
        let seeded = seed(&kit, "Fabricated", RESET_DOC).await;
        model.set_mode(reply(
            "Reset it from Settings > Security.",
            &[
                (&seeded.chunk_id, "Reset your password"),
                ("c-fabricated", "invented"),
            ],
            0.99,
        ));

        let body = turn(&kit, &seeded.api_key, QUESTION, None).await;
        assert_eq!(body["outcome"], "clarify", "the downgrade");
        assert_eq!(citation_count(&body), 0);
        let answer = body_str(&body, "answer");
        assert!(!answer.contains("Settings > Security"), "{answer}");
    }
}

/// A chunk that exists, but is another tenant's, was not retrieved for
/// this request either: the rule is "retrieved for THIS request", not
/// "exists somewhere".
#[pollster::test]
async fn a_citation_to_another_tenants_chunk_is_downgraded() {
    for (kit, model) in model_kits() {
        let owner = seed(&kit, "Owner", RESET_DOC).await;
        let asker = seed(
            &kit,
            "Asker",
            "Reset your password by emailing the helpdesk.",
        )
        .await;
        assert_ne!(owner.chunk_id, asker.chunk_id);

        // Citing only the foreign chunk, and citing it alongside the
        // asker's own: both are downgraded.
        model.set_mode(reply(
            "Reset it from Settings > Security.",
            &[(
                &owner.chunk_id,
                "Reset your password from the settings page",
            )],
            0.99,
        ));
        let body = turn(&kit, &asker.api_key, QUESTION, None).await;
        assert_eq!(body["outcome"], "clarify");
        assert_eq!(citation_count(&body), 0);

        model.set_mode(reply(
            "Reset it from Settings > Security.",
            &[
                (&asker.chunk_id, "Reset your password"),
                (
                    &owner.chunk_id,
                    "Reset your password from the settings page",
                ),
            ],
            0.99,
        ));
        let body = turn(&kit, &asker.api_key, QUESTION, None).await;
        assert_eq!(body["outcome"], "clarify");
        assert_eq!(citation_count(&body), 0);
    }
}

/// `body` (what the user was shown) and `model_answer` (what the model
/// said) deliberately differ on a downgraded turn.
#[pollster::test]
async fn a_downgraded_turn_stores_the_models_raw_answer() {
    for (kit, model) in model_kits() {
        let seeded = seed(&kit, "Audit", RESET_DOC).await;
        model.set_mode(reply(
            "Reset it from Settings > Security.",
            &[("c-fabricated", "invented")],
            0.99,
        ));

        let body = turn(&kit, &seeded.api_key, QUESTION, None).await;
        assert_eq!(body["outcome"], "clarify");
        assert_eq!(
            assistant_column(&kit, "model_answer"),
            ["Reset it from Settings > Security."]
        );
        let shown = assistant_column(&kit, "body");
        assert_eq!(shown, [body_str(&body, "answer")]);
        assert!(shown[0].contains("could you rephrase"), "{shown:?}");
        // The raw citations are kept even though none were published.
        assert_eq!(
            assistant_column(&kit, "citations"),
            [r#"[{"chunk_id":"c-fabricated","quote":"invented"}]"#]
        );
        assert_eq!(
            count_where(
                &kit,
                "sg_messages",
                "role = 'user' AND model_answer IS NULL"
            ),
            1,
            "model_answer is assistant-only"
        );
    }
}

/// `/__health` is where the module's ports surface: `TextModel` is
/// declared **optional** — retrieval and ingest run without it, and the
/// messages route degrades to `503 text-model-not-configured` — while the
/// ports the routes cannot answer without are under `requires`.
#[pollster::test]
async fn health_lists_text_model_among_the_optional_ports() {
    let kit = TestHarness::new(support());
    let health = send(&kit.router, Method::GET, "/__health", None, None).await;
    assert_eq!(health.status, StatusCode::OK);
    let module = health.body["modules"]
        .as_array()
        .expect("modules array")
        .iter()
        .find(|module| module["name"] == "support")
        .expect("support is listed");
    let listed = |field: &str| -> Vec<String> {
        module[field]
            .as_array()
            .unwrap_or_else(|| panic!("{field} is an array"))
            .iter()
            .map(|port| port.as_str().expect("a port name").to_owned())
            .collect()
    };
    let optional = listed("optional");
    assert!(
        optional.contains(&"TextModel".to_owned()),
        "the text model is declared optional: {optional:?}"
    );
    assert!(
        optional.contains(&"HttpClient".to_owned()) && optional.contains(&"RateLimiter".to_owned()),
        "the other degrading ports stay optional too: {optional:?}"
    );
    assert!(
        !listed("requires").contains(&"TextModel".to_owned()),
        "support boots — degraded — without a model, so it is not required"
    );
}

#[pollster::test]
async fn an_unconfigured_model_answers_503_and_writes_nothing() {
    for dialect in Dialect::available() {
        // Both shapes of "no model": the runtime providing no port at
        // all, and a configured model that reports NotConfigured.
        let reporting = FakeTextModel::new(TextModelMode::NotConfigured);
        let absent = TestHarness::with_database_and_ports(support(), dialect.clone(), |ports| {
            ports.config = Arc::new(MapConfig::from_pairs([("ADMIN_TOKEN", ADMIN_TOKEN)]));
            ports.text_model = None;
        });
        for kit in [absent, kit_with_model(dialect, &reporting)] {
            let seeded = seed(&kit, "Unconfigured", RESET_DOC).await;
            let reply = post_message(&kit, &seeded.api_key, QUESTION, None).await;
            assert_eq!(
                reply.status,
                StatusCode::SERVICE_UNAVAILABLE,
                "{}",
                reply.body
            );
            assert_eq!(
                reply.problem_type(),
                format!("{PROBLEMS}text-model-not-configured")
            );
            assert!(!reply.headers.contains_key(header::RETRY_AFTER));
            assert_eq!(count_of(&kit, "sg_conversations"), 0);
            assert_eq!(count_of(&kit, "sg_messages"), 0);
        }
    }
}

#[pollster::test]
async fn a_transient_failure_is_retryable_writes_nothing_and_a_retry_succeeds() {
    for (kit, model) in model_kits() {
        let seeded = seed(&kit, "Transient", RESET_DOC).await;
        // A provider pause is passed through, rounded up to whole seconds
        // and never below 1; no pause is the 2s default.
        for (pause, expected) in [
            (Some(Duration::from_secs(7)), "7"),
            (Some(Duration::from_millis(1500)), "2"),
            (Some(Duration::from_millis(1)), "1"),
            (Some(Duration::ZERO), "1"),
            (None, "2"),
        ] {
            model.set_mode(TextModelMode::Transient { retry_after: pause });
            let reply = post_message(&kit, &seeded.api_key, QUESTION, None).await;
            assert_eq!(
                reply.status,
                StatusCode::SERVICE_UNAVAILABLE,
                "{}",
                reply.body
            );
            assert_eq!(
                reply.problem_type(),
                format!("{PROBLEMS}text-model-unavailable")
            );
            assert_eq!(
                reply
                    .headers
                    .get(header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok()),
                Some(expected)
            );
            assert_eq!(count_of(&kit, "sg_conversations"), 0);
            assert_eq!(count_of(&kit, "sg_messages"), 0);
        }

        model.set_mode(reply(
            "From settings.",
            &[(&seeded.chunk_id, "settings page")],
            0.9,
        ));

        let body = turn(&kit, &seeded.api_key, QUESTION, None).await;
        assert_eq!(body["outcome"], "answered");
        assert_eq!(count_of(&kit, "sg_conversations"), 1);
        assert_eq!(count_of(&kit, "sg_messages"), 2);
    }
}

/// The failure above only proves nothing is written on the
/// conversation-*creating* path. This one fails on an existing
/// conversation that has already spent a clarify, and shows the row, the
/// messages and the clarify budget all untouched.
#[pollster::test]
async fn a_transient_failure_on_an_existing_conversation_consumes_nothing() {
    for (kit, model) in model_kits() {
        let seeded = seed(&kit, "Budget", RESET_DOC).await;
        let unsure = || reply("Maybe this?", &[(seeded.chunk_id.as_str(), "q")], 0.3);
        model.set_mode(unsure());

        let first = turn(&kit, &seeded.api_key, QUESTION, None).await;
        assert_eq!(first["outcome"], "clarify");
        let conversation = conversation_id(&first);
        let before = conversation_rows(&kit);
        assert_eq!(count_of(&kit, "sg_messages"), 2);

        model.set_mode(TextModelMode::Transient { retry_after: None });
        let failed = post_message(
            &kit,
            &seeded.api_key,
            "password still broken",
            Some(&conversation),
        )
        .await;
        assert_eq!(failed.status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(failed.headers.contains_key(header::RETRY_AFTER));

        model.set_mode(unsure());
        assert_eq!(
            conversation_rows(&kit),
            before,
            "the row is byte-for-byte unchanged"
        );
        assert_eq!(count_of(&kit, "sg_messages"), 2);

        // The failed turn spent no clarify: with one of two spent, the
        // retry still clarifies rather than handing off.
        let retried = turn(
            &kit,
            &seeded.api_key,
            "password still broken",
            Some(&conversation),
        )
        .await;
        assert_eq!(retried["outcome"], "clarify");
        assert_eq!(retried["conversation_id"], conversation.as_str());
        assert_eq!(count_of(&kit, "sg_messages"), 4);
    }
}

#[pollster::test]
async fn unparseable_wrong_shape_and_refused_replies_are_502s_that_write_nothing() {
    let unusable: Vec<(&str, Result<Completion, TextModelError>)> = vec![
        (
            "not JSON",
            Ok(Completion::new(
                "I am sorry, I cannot help with that.",
                "fake-fast",
            )),
        ),
        (
            "JSON of the wrong shape",
            Ok(Completion::new("{}", "fake-fast")
                .json(json!({ "verdict": "maybe", "sources": [] }))),
        ),
        (
            "camelCase chunkId",
            Ok(Completion::new(
                r#"{"answer":"a","citations":[{"chunkId":"c1","quote":"q"}],"confidence":0.9}"#,
                "fake-fast",
            )),
        ),
        (
            "confidence out of range",
            Ok(Completion::new(
                r#"{"answer":"a","citations":[],"confidence":1.4}"#,
                "fake-fast",
            )),
        ),
        (
            "refused",
            Err(TextModelError::Rejected("schema refused".to_owned())),
        ),
        (
            "lost in transport",
            Err(TextModelError::Transport("reset".to_owned())),
        ),
    ];
    for dialect in Dialect::available() {
        for (why, scripted) in &unusable {
            let mode = match scripted {
                Ok(completion) => TextModelMode::Complete(completion.clone()),
                Err(error) => TextModelMode::Error(error.clone()),
            };
            let model = FakeTextModel::new(mode);
            let kit = kit_with_model(dialect.clone(), &model);
            let seeded = seed(&kit, "Garbage", RESET_DOC).await;

            let reply = post_message(&kit, &seeded.api_key, QUESTION, None).await;
            assert_eq!(
                reply.status,
                StatusCode::BAD_GATEWAY,
                "{why}: {}",
                reply.body
            );
            assert_eq!(
                reply.problem_type(),
                format!("{PROBLEMS}text-model-invalid-answer"),
                "{why}"
            );
            assert_eq!(count_of(&kit, "sg_conversations"), 0, "{why}");
            assert_eq!(count_of(&kit, "sg_messages"), 0, "{why}");
        }
    }
}

#[pollster::test]
async fn the_tenant_threshold_overrides_the_documented_default() {
    assert!((module_support::DEFAULT_ANSWER_THRESHOLD - 0.60).abs() < f32::EPSILON);
    for (kit, model) in model_kits() {
        let seeded = seed(&kit, "Threshold", RESET_DOC).await;
        let confident = || {
            reply(
                "From settings.",
                &[(seeded.chunk_id.as_str(), "settings page")],
                0.7,
            )
        };
        model.set_mode(confident());

        // Under the 0.60 default, 0.7 answers.
        let first = turn(&kit, &seeded.api_key, QUESTION, None).await;
        assert_eq!(first["outcome"], "answered");

        let put = send(
            &kit.router,
            Method::PUT,
            &settings_path(&seeded.tenant_id),
            Some(ADMIN_TOKEN),
            Some(r#"{"answer_threshold":0.9}"#),
        )
        .await;
        assert_eq!(put.status, StatusCode::OK, "{}", put.body);
        assert_eq!(put.body["answer_threshold"], 0.9);
        assert_eq!(put.body["tenant_id"], seeded.tenant_id.as_str());
        assert_eq!(
            count_where(&kit, "sg_tenant_settings", "answer_threshold_pct = 90"),
            1
        );

        // The same confidence now clarifies: the bar is the tenant's.
        let second = turn(
            &kit,
            &seeded.api_key,
            QUESTION,
            Some(&conversation_id(&first)),
        )
        .await;
        assert_eq!(second["outcome"], "clarify");
        assert_eq!(citation_count(&second), 0);

        // A second PUT updates the one row rather than adding another.
        let lowered = send(
            &kit.router,
            Method::PUT,
            &settings_path(&seeded.tenant_id),
            Some(ADMIN_TOKEN),
            Some(r#"{"answer_threshold":0.5}"#),
        )
        .await;
        assert_eq!(lowered.status, StatusCode::OK, "{}", lowered.body);
        assert_eq!(count_of(&kit, "sg_tenant_settings"), 1);
        assert_eq!(
            count_where(&kit, "sg_tenant_settings", "answer_threshold_pct = 50"),
            1
        );
    }
}

#[pollster::test]
async fn the_settings_route_is_guarded_like_the_other_admin_route() {
    for (kit, _model) in model_kits() {
        let seeded = seed(&kit, "Guarded", RESET_DOC).await;
        let path = settings_path(&seeded.tenant_id);

        // No token, a wrong token, and a tenant API key: refused before the
        // body is parsed (a malformed body gets the guard's answer too).
        for body in [r#"{"answer_threshold":0.9}"#, "{oops"] {
            let absent = send(&kit.router, Method::PUT, &path, None, Some(body)).await;
            assert_eq!(absent.status, StatusCode::UNAUTHORIZED, "{}", absent.body);
            assert_eq!(
                absent.problem_type(),
                format!("{PROBLEMS}admin-unauthorized")
            );
            for wrong in ["not-the-token", seeded.api_key.as_str()] {
                let refused = send(&kit.router, Method::PUT, &path, Some(wrong), Some(body)).await;
                assert_eq!(refused.status, StatusCode::FORBIDDEN, "{}", refused.body);
                assert_eq!(refused.problem_type(), format!("{PROBLEMS}admin-forbidden"));
            }
        }
        assert_eq!(count_of(&kit, "sg_tenant_settings"), 0);

        // Under the token: out of range, wrong type and malformed bodies are
        // validation problems, and an unknown tenant is a 404.
        for bad in [
            r#"{"answer_threshold":1.5}"#,
            r#"{"answer_threshold":-0.1}"#,
            r#"{"answer_threshold":"high"}"#,
            // Bodies are snake_case like the rest of the module; the
            // camelCase spelling is an unknown field, so the key is missing.
            r#"{"answerThreshold":0.9}"#,
            "{oops",
        ] {
            let reply = send(
                &kit.router,
                Method::PUT,
                &path,
                Some(ADMIN_TOKEN),
                Some(bad),
            )
            .await;
            assert_eq!(
                reply.status,
                StatusCode::BAD_REQUEST,
                "{bad}: {}",
                reply.body
            );
            assert_eq!(reply.problem_type(), format!("{PROBLEMS}validation-failed"));
        }
        let unknown = send(
            &kit.router,
            Method::PUT,
            &settings_path("no-such-tenant"),
            Some(ADMIN_TOKEN),
            Some(r#"{"answer_threshold":0.9}"#),
        )
        .await;
        assert_eq!(unknown.status, StatusCode::NOT_FOUND, "{}", unknown.body);
        assert_eq!(count_of(&kit, "sg_tenant_settings"), 0);
    }
}

#[pollster::test]
async fn nothing_retrieved_hands_off_and_marks_the_conversation_escalated() {
    for (kit, model) in model_kits() {
        // A document that shares no term with the question.
        let seeded = seed(&kit, "Empty", BILLING_DOC).await;
        model.set_mode(reply("Guessing freely.", &[("nowhere", "nothing")], 0.99));

        let body = turn(&kit, &seeded.api_key, "reset password", None).await;
        assert_eq!(body["outcome"], "handoff");
        assert_eq!(body["needs_escalation"], true);
        assert_eq!(citation_count(&body), 0);
        assert!(body_str(&body, "answer").contains("passed your question to a person"));
        assert_eq!(conversation_state(&kit), ("escalated".to_owned(), 1));
        assert_eq!(assistant_column(&kit, "outcome"), ["handoff"]);
    }
}

#[pollster::test]
async fn two_clarifies_then_a_handoff() {
    for (kit, model) in model_kits() {
        let seeded = seed(&kit, "Clarify", RESET_DOC).await;
        let mut answers = ["Maybe this?", "Or this?", "Still guessing."].into_iter();
        let mut next_answer = || {
            let answer = answers.next().expect("one scripted answer per turn");
            reply(answer, &[(&seeded.chunk_id, "q")], 0.3)
        };
        model.set_mode(next_answer());

        let first = turn(&kit, &seeded.api_key, QUESTION, None).await;
        assert_eq!(first["outcome"], "clarify");
        let conversation = conversation_id(&first);
        model.set_mode(next_answer());
        let second = turn(
            &kit,
            &seeded.api_key,
            "my password still fails",
            Some(&conversation),
        )
        .await;
        assert_eq!(second["outcome"], "clarify");
        assert_eq!(second["needs_escalation"], false);
        assert_eq!(conversation_state(&kit), ("open".to_owned(), 0));

        model.set_mode(next_answer());
        let third = turn(
            &kit,
            &seeded.api_key,
            "password please",
            Some(&conversation),
        )
        .await;
        assert_eq!(
            third["outcome"], "handoff",
            "a third non-answer stops guessing"
        );
        assert_eq!(third["needs_escalation"], true);
        assert_eq!(conversation_state(&kit), ("escalated".to_owned(), 1));
        assert_eq!(
            assistant_column(&kit, "outcome"),
            ["clarify", "clarify", "handoff"]
        );
        // `seq` orders the conversation: user, assistant, user, … from 0,
        // though every message here shares one fixed-clock instant.
        assert_eq!(
            text_column(
                &kit,
                "SELECT role || ':' || CAST(seq AS TEXT) AS v FROM sg_messages ORDER BY seq"
            ),
            [
                "user:0",
                "assistant:1",
                "user:2",
                "assistant:3",
                "user:4",
                "assistant:5"
            ]
        );
        assert_eq!(
            text_column(
                &kit,
                "SELECT body AS v FROM sg_messages WHERE role = 'user' ORDER BY seq"
            ),
            [QUESTION, "my password still fails", "password please"]
        );
    }
}

/// An answered follow-up does not un-escalate a conversation a person is
/// already picking up.
#[pollster::test]
async fn escalation_is_sticky_across_an_answered_follow_up() {
    for (kit, model) in model_kits() {
        let seeded = seed(&kit, "Sticky", RESET_DOC).await;
        model.set_mode(reply("Guessing.", &[("nowhere", "nothing")], 0.99));

        // Nothing retrieved for this wording: handoff.
        let handoff = turn(&kit, &seeded.api_key, "billing owner", None).await;
        assert_eq!(handoff["outcome"], "handoff");
        let conversation = conversation_id(&handoff);

        model.set_mode(reply(
            "From settings.",
            &[(&seeded.chunk_id, "settings page")],
            0.9,
        ));
        let follow_up = turn(&kit, &seeded.api_key, QUESTION, Some(&conversation)).await;
        assert_eq!(follow_up["outcome"], "answered");
        assert_eq!(citation_count(&follow_up), 1);
        assert_eq!(
            follow_up["needs_escalation"], true,
            "answered, but the ticket behind it stays escalated"
        );
        assert_eq!(conversation_state(&kit), ("escalated".to_owned(), 1));
    }
}

/// A `TextModel` that runs one SQL statement against the kit's database
/// while the model call is in flight, then answers from its fake: a
/// concurrent request landing between a turn's reads and its write.
struct WritesDuringCall {
    inner: FakeTextModel,
    db: OnceLock<Arc<dyn Database>>,
    pending: Mutex<Option<String>>,
}

// `TextModel` is an `async_trait`; this is its expansion, written out so
// the test needs no extra dev-dependency.
impl TextModel for WritesDuringCall {
    fn complete<'life0, 'life1, 'async_trait>(
        &'life0 self,
        prompt: &'life1 Prompt,
    ) -> Pin<Box<dyn Future<Output = Result<Completion, TextModelError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let pending = self.pending.lock().expect("pending lock").take();
            if let Some(sql) = pending {
                let db = self.db.get().expect("db is set before the first turn");
                db.execute(&Statement::new(sql))
                    .await
                    .expect("the concurrent write runs");
            }
            self.inner.complete(prompt).await
        })
    }
}

/// A handoff that commits while a parallel answered turn is waiting on the
/// model must survive that turn's write: only an escalating turn writes
/// the flag, so a non-escalating one cannot clear it from a stale read.
#[pollster::test]
async fn a_concurrent_answered_turn_cannot_clear_an_escalation() {
    for dialect in Dialect::available() {
        let model = Arc::new(WritesDuringCall {
            inner: FakeTextModel::new(TextModelMode::NotConfigured),
            db: OnceLock::new(),
            pending: Mutex::new(None),
        });
        let kit = TestHarness::with_database_and_ports(
            vec![Box::new(Support::new())],
            dialect,
            |ports| {
                ports.config = Arc::new(MapConfig::from_pairs([("ADMIN_TOKEN", ADMIN_TOKEN)]));
                ports.text_model = Some(model.clone());
            },
        );
        assert!(model.db.set(kit.db.clone()).is_ok(), "db set once");
        let seeded = seed(&kit, "Race", RESET_DOC).await;
        model.inner.set_mode(reply(
            "From settings.",
            &[(&seeded.chunk_id, "settings page")],
            0.9,
        ));

        let first = turn(&kit, &seeded.api_key, QUESTION, None).await;
        assert_eq!(first["outcome"], "answered");
        let conversation = conversation_id(&first);
        assert_eq!(conversation_state(&kit), ("open".to_owned(), 0));

        // The second turn reads the conversation as open; a parallel
        // handoff escalates it before this turn writes.
        *model.pending.lock().expect("pending lock") = Some(format!(
            "UPDATE sg_conversations SET status = 'escalated', needs_escalation = 1 \
             WHERE id = '{conversation}'"
        ));
        let second = turn(&kit, &seeded.api_key, QUESTION, Some(&conversation)).await;
        assert_eq!(second["outcome"], "answered");
        assert_eq!(
            conversation_state(&kit),
            ("escalated".to_owned(), 1),
            "the answered turn's write leaves the parallel escalation alone"
        );
        assert_eq!(count_of(&kit, "sg_messages"), 4);
    }
}

#[pollster::test]
async fn the_model_is_asked_for_the_fast_tier_and_sees_chunk_ids_and_the_snake_case_schema() {
    for (kit, model) in model_kits() {
        let seeded = seed(&kit, "Prompt", RESET_DOC).await;
        let second = ingest_one(
            &kit,
            &seeded.api_key,
            "Password rules: twelve characters minimum.",
        )
        .await;
        model.set_mode(reply(
            "From settings.",
            &[(&seeded.chunk_id, "settings page")],
            0.9,
        ));

        let body = turn(&kit, &seeded.api_key, QUESTION, None).await;
        assert_eq!(body["outcome"], "answered");

        let prompts = model.prompts();
        assert_eq!(prompts.len(), 1);
        let prompt = &prompts[0];
        assert_eq!(prompt.tier, ModelTier::Fast, "a tier, never a vendor");
        let user: String = prompt
            .messages
            .iter()
            .map(|message| message.content.as_str())
            .collect();
        for id in [&seeded.chunk_id, &second] {
            assert!(
                user.contains(&format!("[{id}] ")),
                "missing [{id}] in {user}"
            );
        }
        assert!(user.contains(QUESTION));
        assert!(
            prompt
                .system
                .as_deref()
                .is_some_and(|system| system.contains("Cite only chunk ids")),
            "the system prompt carries the grounding rule"
        );
        let schema = prompt.json_schema.as_ref().expect("a schema is sent");
        let citation = &schema["properties"]["citations"]["items"];
        assert!(citation["properties"].get("chunk_id").is_some(), "{schema}");
        assert!(citation["properties"].get("chunkId").is_none(), "{schema}");
        assert_eq!(citation["required"], json!(["chunk_id", "quote"]));
        assert_eq!(
            schema["required"],
            json!(["answer", "citations", "confidence"])
        );
    }
}

#[pollster::test]
async fn an_unknown_or_foreign_conversation_is_a_404_without_calling_the_model() {
    for (kit, model) in model_kits() {
        let owner = seed(&kit, "Owner", RESET_DOC).await;
        let other = seed(&kit, "Other", RESET_DOC).await;
        model.set_mode(reply(
            "From settings.",
            &[(&owner.chunk_id, "settings page")],
            0.9,
        ));
        let first_turn = turn(&kit, &owner.api_key, QUESTION, None).await;
        let foreign = conversation_id(&first_turn);

        for id in ["no-such-conversation", foreign.as_str()] {
            let reply = post_message(&kit, &other.api_key, QUESTION, Some(id)).await;
            assert_eq!(reply.status, StatusCode::NOT_FOUND, "{id}: {}", reply.body);
        }
        assert_eq!(
            model.prompts().len(),
            1,
            "only the owner's turn reached the model"
        );
        assert_eq!(count_of(&kit, "sg_conversations"), 1);
        assert_eq!(count_of(&kit, "sg_messages"), 2);
    }
}

#[pollster::test]
async fn empty_and_oversized_messages_are_validation_problems() {
    for (kit, model) in model_kits() {
        let seeded = seed(&kit, "Validation", RESET_DOC).await;
        let at_cap = "é".repeat(4000);
        let over_cap = "x".repeat(4001);
        for (body, why) in [
            (json!({ "message": "   " }).to_string(), "whitespace only"),
            (
                json!({ "message": over_cap }).to_string(),
                "one character over the cap",
            ),
            (json!({ "text": "hello" }).to_string(), "no message field"),
            ("{oops".to_owned(), "not JSON"),
        ] {
            let reply = send(
                &kit.router,
                Method::POST,
                MESSAGES,
                Some(&seeded.api_key),
                Some(&body),
            )
            .await;
            assert_eq!(
                reply.status,
                StatusCode::BAD_REQUEST,
                "{why}: {}",
                reply.body
            );
            assert_eq!(
                reply.problem_type(),
                format!("{PROBLEMS}validation-failed"),
                "{why}"
            );
        }
        assert!(
            model.prompts().is_empty(),
            "validation happens before the model"
        );

        // The cap counts characters, not bytes: 4000 two-byte characters
        // are accepted.
        model.set_mode(reply("Guess.", &[], 0.1));
        let reply = post_message(&kit, &seeded.api_key, &at_cap, None).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        assert_eq!(count_of(&kit, "sg_messages"), 2);
    }
}

#[pollster::test]
async fn messages_needs_a_valid_api_key() {
    for (kit, model) in model_kits() {
        mint_tenant(&kit, "Auth").await;
        let body = json!({ "message": QUESTION }).to_string();
        for key in [None, Some("sg_not-a-key"), Some(ADMIN_TOKEN)] {
            let reply = send(&kit.router, Method::POST, MESSAGES, key, Some(&body)).await;
            assert_eq!(
                reply.status,
                StatusCode::UNAUTHORIZED,
                "{key:?}: {}",
                reply.body
            );
            assert_eq!(reply.problem_type(), format!("{PROBLEMS}unauthorized"));
        }
        // A malformed body without a key is the same 401, not a 400.
        let malformed = send(&kit.router, Method::POST, MESSAGES, None, Some("{oops")).await;
        assert_eq!(malformed.status, StatusCode::UNAUTHORIZED);
        assert!(model.prompts().is_empty());
        assert_eq!(count_of(&kit, "sg_messages"), 0);
    }
}

// ---------------------------------------------------------------------
// Issue #28: source management — `GET /sources`, `GET /sources/{id}`,
// `PUT /sources/{id}`, `DELETE /sources/{id}`, and the replace-in-place
// ingest for a repeated `external_id`.
// ---------------------------------------------------------------------

fn source_path(source_id: &str) -> String {
    format!("{SOURCES}/{source_id}")
}

/// `id|created_at` for every chunk of one source, in ordinal order: a
/// row that survived a replace keeps both, a re-inserted row cannot.
fn chunk_rows_of(kit: &TestHarness, source_id: &str) -> Vec<String> {
    text_column(
        kit,
        &format!(
            "SELECT id || '|' || created_at AS v FROM sg_chunks \
             WHERE source_id = '{source_id}' ORDER BY ordinal"
        ),
    )
}

async fn get_source(kit: &TestHarness, key: &str, path: &str) -> Reply {
    send(&kit.router, Method::GET, path, Some(key), None).await
}

#[pollster::test]
async fn deleting_a_source_removes_it_from_the_index_and_the_corpus() {
    for kit in kits() {
        let tenant = mint_tenant(&kit, "Deleted").await;
        let api_key = body_str(&tenant, "api_key");

        // A and B share the term "quokka", C does not; each document is
        // one chunk, so B's quokka score is scored against df = 2 of
        // N = 3 — and, once A is gone, against df = 1 of N = 2.
        let a = ingest(
            &kit,
            &api_key,
            json!({ "external_id": "a", "text": "quokka quokka ferritin" }),
        )
        .await;
        let b = ingest(
            &kit,
            &api_key,
            json!({ "external_id": "b", "text": "quokka quokka kangaroo pad" }),
        )
        .await;
        let c = ingest(
            &kit,
            &api_key,
            json!({ "external_id": "c", "text": "billing plans change on the page" }),
        )
        .await;
        for reply in [&a, &b, &c] {
            assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
        }
        let (a_id, b_id) = (
            body_str(&a.body, "source_id"),
            body_str(&b.body, "source_id"),
        );
        let a_chunk = text_column(
            &kit,
            &format!("SELECT id AS v FROM sg_chunks WHERE source_id = '{a_id}'"),
        );
        assert_eq!(a_chunk.len(), 1, "fixture documents are one chunk");

        let score_of_b = |reply: &Reply| -> f64 {
            reply.body["results"]
                .as_array()
                .expect("results")
                .iter()
                .find(|hit| hit["source_id"] == b_id.as_str())
                .map(|hit| hit["score"].as_f64().expect("score"))
                .expect("B matches quokka")
        };
        let before = search(&kit, &api_key, "q=quokka").await;
        assert_eq!(before.body["results"].as_array().expect("r").len(), 2);
        let score_before = score_of_b(&before);

        let deleted = send(
            &kit.router,
            Method::DELETE,
            &source_path(&a_id),
            Some(&api_key),
            None,
        )
        .await;
        assert_eq!(deleted.status, StatusCode::NO_CONTENT, "{}", deleted.body);

        // The row, its chunk and its postings are all gone.
        assert_eq!(
            count_where(&kit, "sg_sources", &format!("id = '{a_id}'")),
            0
        );
        assert_eq!(
            count_where(&kit, "sg_chunks", &format!("source_id = '{a_id}'")),
            0
        );
        assert_eq!(
            count_where(&kit, "sg_postings", &format!("chunk_id = '{}'", a_chunk[0])),
            0
        );

        // A term only A indexed retrieves nothing now, and the corpus
        // shrank to B and C.
        let ferritin = search(&kit, &api_key, "q=ferritin").await;
        assert_eq!(
            ferritin.body["results"].as_array().expect("r").len(),
            0,
            "{}",
            ferritin.body
        );
        let after = search(&kit, &api_key, "q=quokka").await;
        let results = after.body["results"].as_array().expect("r");
        assert_eq!(results.len(), 1, "{}", after.body);
        assert_eq!(results[0]["source_id"], b_id.as_str());
        let score_after = score_of_b(&after);

        // The rise is exactly BM25's df correction: in a fresh tenant
        // holding only B and C — the corpus the original one now is — B
        // scores precisely what it scores here, and more than before,
        // because quokka went from df 2 of N 3 to df 1 of N 2.
        let fresh = mint_tenant(&kit, "Fresh").await;
        let fresh_key = body_str(&fresh, "api_key");
        ingest(
            &kit,
            &fresh_key,
            json!({ "text": "quokka quokka kangaroo pad" }),
        )
        .await;
        ingest(
            &kit,
            &fresh_key,
            json!({ "text": "billing plans change on the page" }),
        )
        .await;
        let alone = search(&kit, &fresh_key, "q=quokka").await;
        let score_alone = alone.body["results"][0]["score"].as_f64().expect("score");
        assert!(
            score_after > score_before,
            "df dropped, idf rose: {score_after} vs {score_before}"
        );
        // Bit-exact by construction, not by luck: a single-term query over
        // one posting row per chunk has one float expression with one
        // evaluation order, so the same inputs must produce the same bits.
        #[allow(clippy::float_cmp)]
        {
            assert_eq!(
                score_after, score_alone,
                "B scores exactly what it scores in the equivalent fresh corpus"
            );
        }

        // B and C were untouched by A's deletion.
        assert_eq!(
            count_where(&kit, "sg_sources", &format!("id = '{b_id}'")),
            1
        );
        assert_eq!(
            count_where(
                &kit,
                "sg_chunks",
                &format!("source_id = '{}'", body_str(&c.body, "source_id"))
            ),
            1
        );
    }
}

#[pollster::test]
async fn replacing_a_source_keeps_the_surviving_windows_and_diffs_the_index() {
    for kit in kits() {
        let tenant = mint_tenant(&kit, "Replaced").await;
        let api_key = body_str(&tenant, "api_key");
        let tenant_id = body_str(&tenant, "tenant_id");
        let words: Vec<String> = (0..400).map(|i| format!("word{i:03}")).collect();
        let original = words.join(" ");
        let created = ingest(
            &kit,
            &api_key,
            json!({ "title": "Before", "external_id": "doc", "text": original }),
        )
        .await;
        assert_eq!(created.status, StatusCode::CREATED, "{}", created.body);
        let source_id = body_str(&created.body, "source_id");

        let before_ids = text_column(
            &kit,
            &format!(
                "SELECT id AS v FROM sg_chunks WHERE source_id = '{source_id}' ORDER BY ordinal"
            ),
        );
        let before_rows = chunk_rows_of(&kit, &source_id);

        // One paragraph — words 200..210 — changes.
        let mut edited = words.clone();
        for (i, word) in edited.iter_mut().enumerate() {
            if (200..210).contains(&i) {
                *word = format!("mark{i:03}");
            }
        }
        let updated = edited.join(" ");
        let put = send(
            &kit.router,
            Method::PUT,
            &source_path(&source_id),
            Some(&api_key),
            Some(&json!({ "title": "After", "external_id": "doc", "text": updated }).to_string()),
        )
        .await;
        assert_eq!(put.status, StatusCode::OK, "{}", put.body);
        assert_eq!(put.body["id"], source_id.as_str());
        assert_eq!(put.body["title"], "After");
        assert_eq!(put.body["origin"], "text");
        assert_eq!(put.body["external_id"], "doc");
        assert_eq!(put.body["bytes"], updated.len());
        assert_eq!(put.body["origin"], "text");

        // What is stored is exactly what the chunker produces for the
        // edited text under the SAME source id — so the windows that did
        // not overlap the edit kept their content-addressed ids, and the
        // edit is confined to the window that spans words 200..210.
        let expected = Chunker::default().split(&tenant_id, &source_id, &updated);
        let after_ids: Vec<&str> = expected.iter().map(|c| c.id.as_str()).collect();
        assert!(after_ids.len() > 1, "fixture must span several chunks");
        let stored_ids = text_column(
            &kit,
            &format!(
                "SELECT id AS v FROM sg_chunks WHERE source_id = '{source_id}' ORDER BY ordinal"
            ),
        );
        assert_eq!(stored_ids, after_ids);

        let kept: std::collections::HashSet<&str> = before_ids
            .iter()
            .filter(|id| after_ids.contains(&id.as_str()))
            .map(String::as_str)
            .collect();
        let removed: Vec<&String> = before_ids
            .iter()
            .filter(|id| !after_ids.contains(&id.as_str()))
            .collect();
        assert!(!kept.is_empty(), "unchanged windows must keep their ids");
        assert_eq!(
            removed.len(),
            1,
            "exactly the window overlapping words 200..210 is re-chunked"
        );

        // Vanished ids have neither chunk rows nor postings left.
        for id in &removed {
            assert_eq!(count_where(&kit, "sg_chunks", &format!("id = '{id}'")), 0);
            assert_eq!(
                count_where(&kit, "sg_postings", &format!("chunk_id = '{id}'")),
                0
            );
        }

        // Surviving rows were not re-inserted: same row identity (id and
        // created_at, ordinal order), and the index as a whole matches a
        // fresh ingest of the edited text.
        let after_rows = chunk_rows_of(&kit, &source_id);
        let kept_rows: Vec<&String> = after_rows
            .iter()
            .filter(|row| kept.contains(row.split('|').next().expect("id|ts")))
            .collect();
        assert_eq!(
            kept_rows.len(),
            kept.len(),
            "every kept window is still one row"
        );
        for row in kept_rows {
            assert!(
                before_rows.contains(row),
                "{row} must be the pre-replace row, created_at included"
            );
        }
        let expected_postings: usize = expected.iter().map(|chunk| chunk.terms.len()).sum();
        assert_eq!(
            count_where(
                &kit,
                "sg_postings",
                &format!("chunk_id IN (SELECT id FROM sg_chunks WHERE source_id = '{source_id}')")
            ),
            expected_postings
        );

        // A body external_id another source of the tenant already holds
        // is a 409 before anything is written — not a bare unique-index
        // 500 — and it changes neither source.
        let other = ingest(
            &kit,
            &api_key,
            json!({ "title": "Twin", "external_id": "twin", "text": "twin text" }),
        )
        .await;
        assert_eq!(other.status, StatusCode::CREATED, "{}", other.body);
        let clashed = send(
            &kit.router,
            Method::PUT,
            &source_path(&source_id),
            Some(&api_key),
            Some(r#"{"text":"clashed","external_id":"twin"}"#),
        )
        .await;
        assert_eq!(clashed.status, StatusCode::CONFLICT, "{}", clashed.body);
        assert_eq!(
            clashed.problem_type(),
            format!("{PROBLEMS}source-external-id-conflict")
        );
        assert_eq!(count_of(&kit, "sg_sources"), 2);
        assert_eq!(
            text_column(
                &kit,
                &format!("SELECT id AS v FROM sg_chunks WHERE source_id = '{source_id}'")
            )
            .len(),
            after_ids.len(),
            "a clashed replace writes nothing"
        );
    }
}

#[pollster::test]
async fn reingesting_an_external_id_replaces_the_source_in_place() {
    for kit in kits() {
        let tenant = mint_tenant(&kit, "Upserted").await;
        let api_key = body_str(&tenant, "api_key");
        let tenant_id = body_str(&tenant, "tenant_id");
        let v1 = "the refund window is thirty days";
        let first = ingest(
            &kit,
            &api_key,
            json!({ "external_id": "handbook", "text": v1 }),
        )
        .await;
        assert_eq!(first.status, StatusCode::CREATED, "{}", first.body);
        let source_id = body_str(&first.body, "source_id");

        let v2 = "the refund window is now forty five days and covers shipping";
        let second = ingest(
            &kit,
            &api_key,
            json!({ "external_id": "handbook", "title": "Handbook", "text": v2 }),
        )
        .await;
        assert_eq!(second.status, StatusCode::OK, "{}", second.body);
        assert_eq!(second.body["source_id"], source_id.as_str(), "same id");

        // One source row, and the chunk/posting set of a fresh v2.
        assert_eq!(count_of(&kit, "sg_sources"), 1);
        let expected = Chunker::default().split(&tenant_id, &source_id, v2);
        assert_eq!(expected.len(), 1, "fixture documents are one chunk");
        assert_eq!(
            count_where(&kit, "sg_chunks", &format!("source_id = '{source_id}'")),
            expected.len()
        );
        let stored_ids = text_column(
            &kit,
            &format!(
                "SELECT id AS v FROM sg_chunks WHERE source_id = '{source_id}' ORDER BY ordinal"
            ),
        );
        let expected_ids: Vec<&str> = expected.iter().map(|chunk| chunk.id.as_str()).collect();
        assert_eq!(stored_ids, expected_ids);
        assert_eq!(
            count_where(
                &kit,
                "sg_postings",
                &format!("chunk_id = '{}'", expected_ids[0])
            ),
            expected[0].terms.len(),
            "the old version's postings are gone, the new version's are in"
        );

        // The index serves the new text: the v2-only word retrieves, the
        // v1-only word does not.
        let forty = search(&kit, &api_key, "q=forty").await;
        assert_eq!(
            forty.body["results"].as_array().expect("r").len(),
            1,
            "{}",
            forty.body
        );
        let thirty = search(&kit, &api_key, "q=thirty").await;
        assert_eq!(
            thirty.body["results"].as_array().expect("r").len(),
            0,
            "{}",
            thirty.body
        );

        // The uniqueness is per tenant: another workspace's "handbook" is
        // its own source, created fresh.
        let other = mint_tenant(&kit, "Other").await;
        let theirs = ingest(
            &kit,
            &body_str(&other, "api_key"),
            json!({ "external_id": "handbook", "text": v1 }),
        )
        .await;
        assert_eq!(theirs.status, StatusCode::CREATED, "{}", theirs.body);
        assert_ne!(theirs.body["source_id"], source_id.as_str());
        assert_eq!(count_of(&kit, "sg_sources"), 2);
    }
}

#[pollster::test]
async fn url_ingest_defaults_the_external_id_to_the_url_so_reposts_replace() {
    for dialect in Dialect::available() {
        let url = "https://docs.example/handbook";
        let plain = |body: &'static str| {
            http::Response::builder()
                .status(200)
                .header(header::CONTENT_TYPE, "text/plain")
                .body(bytes::Bytes::from_static(body.as_bytes()))
                .map_err(|err| cratefield_core::HttpError::Transport(err.to_string()))
        };
        let fake = FakeHttpClient::scripted(vec![
            plain("quokka habitat notes"),
            plain("quokka habitat notes, revised"),
        ]);
        let kit = TestHarness::with_database_and_ports(support(), dialect, |ports| {
            ports.config = Arc::new(MapConfig::from_pairs([("ADMIN_TOKEN", ADMIN_TOKEN)]));
            ports.http = Some(Arc::new(fake.clone()));
        });
        let tenant = mint_tenant(&kit, "Fetched twice").await;
        let api_key = body_str(&tenant, "api_key");

        let first = ingest(&kit, &api_key, json!({ "url": url })).await;
        assert_eq!(first.status, StatusCode::CREATED, "{}", first.body);
        let source_id = body_str(&first.body, "source_id");

        let second = ingest(&kit, &api_key, json!({ "url": url })).await;
        assert_eq!(second.status, StatusCode::OK, "{}", second.body);
        assert_eq!(second.body["source_id"], source_id.as_str());
        assert_eq!(count_of(&kit, "sg_sources"), 1);

        // The derived identity is visible on the source, along with its
        // url origin.
        let got = get_source(&kit, &api_key, &source_path(&source_id)).await;
        assert_eq!(got.status, StatusCode::OK, "{}", got.body);
        assert_eq!(got.body["origin"], "url");
        assert_eq!(got.body["external_id"], url);
    }
}

#[pollster::test]
async fn sources_list_get_and_foreign_ids_are_tenant_scoped() {
    for kit in kits() {
        let a = mint_tenant(&kit, "Lister A").await;
        let b = mint_tenant(&kit, "Lister B").await;
        let (a_key, b_key) = (body_str(&a, "api_key"), body_str(&b, "api_key"));
        let a_id = body_str(&a, "tenant_id");

        let docs = [
            ("Alpha", Some("alpha"), "alpha covers refunds"),
            ("Beta", None, "beta covers shipping and returns"),
            ("Gamma", Some("gamma"), "gamma covers billing plans"),
        ];
        for (title, external_id, text) in docs {
            let mut body = json!({ "title": title, "text": text });
            if let Some(external_id) = external_id {
                body["external_id"] = json!(external_id);
            }
            let reply = ingest(&kit, &a_key, body).await;
            assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
        }

        let list = get_source(&kit, &a_key, SOURCES).await;
        assert_eq!(list.status, StatusCode::OK, "{}", list.body);
        let items = list.body["sources"].as_array().expect("sources");
        assert_eq!(items.len(), 3, "{}", list.body);
        assert_eq!(list.body["next"], Value::Null, "{}", list.body);
        let ids: Vec<&str> = items
            .iter()
            .map(|item| item["id"].as_str().expect("id"))
            .collect();
        assert!(
            ids.windows(2).all(|pair| pair[0] < pair[1]),
            "keyset order is id order: {ids:?}"
        );

        // The item shape, on the first item; then spot checks matched by
        // title — id order is not ingest order within one millisecond.
        let by_title = |probe: &str| {
            items
                .iter()
                .find(|item| item["title"] == probe)
                .expect("every listed source is one of the fixtures")
        };
        let item = &items[0];
        for key in [
            "id",
            "title",
            "origin",
            "external_id",
            "bytes",
            "chunk_count",
            "updated_at",
        ] {
            assert!(item.get(key).is_some(), "missing {key}: {item}");
        }
        assert_eq!(by_title("Alpha")["external_id"], "alpha");
        assert_eq!(by_title("Alpha")["bytes"], "alpha covers refunds".len());
        assert_eq!(by_title("Alpha")["chunk_count"], 1);
        assert!(by_title("Beta")["external_id"].is_null());
        assert!(
            by_title("Gamma")["updated_at"]
                .as_str()
                .is_some_and(|ts| !ts.is_empty()),
            "{}",
            by_title("Gamma")
        );

        // Pagination: two pages over the same keyset order.
        let page1 = get_source(&kit, &a_key, &format!("{SOURCES}?limit=2")).await;
        assert_eq!(page1.body["sources"].as_array().expect("s").len(), 2);
        assert_eq!(page1.body["next"], ids[1], "{}", page1.body);
        let page2 = get_source(&kit, &a_key, &format!("{SOURCES}?limit=2&after={}", ids[1])).await;
        let rest = page2.body["sources"].as_array().expect("s");
        assert_eq!(rest.len(), 1, "{}", page2.body);
        assert_eq!(rest[0]["id"], ids[2]);
        assert_eq!(page2.body["next"], Value::Null, "{}", page2.body);
        // The clamps: 0 becomes 1, 1000 becomes 100 (and 3 < 100 anyway).
        let clamped = get_source(&kit, &a_key, &format!("{SOURCES}?limit=0")).await;
        assert_eq!(clamped.body["sources"].as_array().expect("s").len(), 1);

        // GET one matches the list item exactly.
        let one = get_source(&kit, &a_key, &source_path(ids[0])).await;
        assert_eq!(one.status, StatusCode::OK, "{}", one.body);
        assert_eq!(one.body, items[0]);
        let unknown = get_source(&kit, &a_key, &source_path("no-such-source")).await;
        assert_eq!(unknown.status, StatusCode::NOT_FOUND, "{}", unknown.body);
        assert_eq!(unknown.problem_type(), format!("{PROBLEMS}not-found"));

        // Another tenant's ids do not exist here: not on GET, not on PUT,
        // not on DELETE — and nothing of A's is touched either way.
        let a_rows_before = chunk_rows_of(&kit, ids[0]);
        let a_counts = || {
            (
                count_where(&kit, "sg_sources", &format!("tenant_id = '{a_id}'")),
                count_where(&kit, "sg_chunks", &format!("tenant_id = '{a_id}'")),
                count_where(&kit, "sg_postings", &format!("tenant_id = '{a_id}'")),
            )
        };
        let counts_before = a_counts();
        let foreign_get = get_source(&kit, &b_key, &source_path(ids[0])).await;
        assert_eq!(foreign_get.status, StatusCode::NOT_FOUND);
        let foreign_put = send(
            &kit.router,
            Method::PUT,
            &source_path(ids[0]),
            Some(&b_key),
            Some(r#"{"text":"stolen contents"}"#),
        )
        .await;
        assert_eq!(
            foreign_put.status,
            StatusCode::NOT_FOUND,
            "{}",
            foreign_put.body
        );
        let foreign_delete = send(
            &kit.router,
            Method::DELETE,
            &source_path(ids[0]),
            Some(&b_key),
            None,
        )
        .await;
        assert_eq!(foreign_delete.status, StatusCode::NOT_FOUND);
        assert_eq!(a_counts(), counts_before, "a foreign id deletes nothing");
        assert_eq!(chunk_rows_of(&kit, ids[0]), a_rows_before);

        // B's own list is empty: listing is tenant-scoped too.
        let b_list = get_source(&kit, &b_key, SOURCES).await;
        assert_eq!(b_list.body["sources"].as_array().expect("s").len(), 0);
        assert_eq!(b_list.body["next"], Value::Null);
    }
}

#[pollster::test]
async fn every_source_route_rate_limits_before_touching_the_store() {
    for dialect in Dialect::available() {
        // Minting the tenant goes through the limiter too, so the first
        // decision lets it through; every source route after it is refused.
        let limiter = FakeRateLimiter::scripted(
            vec![Decision {
                ok: true,
                retry_after: None,
                quota: None,
            }],
            Decision {
                ok: false,
                retry_after: Some(Duration::from_secs(3)),
                quota: None,
            },
        );
        let kit = TestHarness::with_database_and_ports(support(), dialect, |ports| {
            ports.config = Arc::new(MapConfig::from_pairs([("ADMIN_TOKEN", ADMIN_TOKEN)]));
            ports.rate_limiter = Some(Arc::new(limiter.clone()));
        });
        let tenant = mint_tenant(&kit, "Limited").await;
        let minted = limiter.calls();
        let api_key = body_str(&tenant, "api_key");
        let missing = source_path("no-such-source");
        let malformed = format!("{SOURCES}?limit=abc");

        // Each route answers 429 before anything else — including before
        // the 404s the unknown ids would earn, and before the query
        // string is parsed, so a malformed `limit` is a 429 here too.
        for (method, path, body) in [
            (Method::POST, SOURCES, Some(r#"{"text":"hello world"}"#)),
            (Method::GET, SOURCES, None),
            (Method::GET, malformed.as_str(), None),
            (Method::GET, missing.as_str(), None),
            (Method::PUT, missing.as_str(), Some(r#"{"text":"hello"}"#)),
            (Method::DELETE, missing.as_str(), None),
        ] {
            let reply = send(&kit.router, method.clone(), path, Some(&api_key), body).await;
            assert_eq!(
                reply.status,
                StatusCode::TOO_MANY_REQUESTS,
                "{method} {path}: {}",
                reply.body
            );
            assert_eq!(
                reply
                    .headers
                    .get(header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok()),
                Some("3"),
                "{method} {path}"
            );
        }
        assert_eq!(
            limiter.calls() - minted,
            6,
            "one limiter check per request, reached every time"
        );
        assert_eq!(count_of(&kit, "sg_sources"), 0, "nothing was written");
    }
}

// Issue #32: unspaced-script retrieval (overlapping bigrams and the
// scheduled re-index that re-claims an existing index) and the per-turn
// language (`respond_in`, the `lang` column, the localized canned texts).
// ---------------------------------------------------------------------

/// Percent-encodes a query value the way a browser would, so a Japanese
/// or Thai `q` survives `Request::builder().uri(...)` — the `Query`
/// extractor on the other side decodes it back.
fn encoded(query: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    for byte in query.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char);
            }
            other => {
                let _ = write!(out, "%{other:02X}");
            }
        }
    }
    out
}

#[pollster::test]
async fn japanese_documents_are_found_by_japanese_queries_in_ranked_order() {
    for kit in kits() {
        let tenant = mint_tenant(&kit, "日本語").await;
        let api_key = body_str(&tenant, "api_key");

        // Two documents that share no bigram with each other's topic:
        // password reset versus billing.
        let a = ingest(
            &kit,
            &api_key,
            json!({ "title": "パスワード", "text": "パスワードをリセットするには、設定ページからアカウントにログインしてください。" }),
        )
        .await;
        let b = ingest(
            &kit,
            &api_key,
            json!({ "title": "請求", "text": "請求書の支払い方法は、アカウントの請求セクションで変更できます。" }),
        )
        .await;
        let (a_id, b_id) = (
            body_str(&a.body, "source_id"),
            body_str(&b.body, "source_id"),
        );
        assert_eq!(a.body["chunks"], 1, "{}", a.body);
        assert_eq!(b.body["chunks"], 1, "{}", b.body);

        // Each query names one document's bigrams only: that document
        // ranks first, with strictly ordered scores — bigram retrieval,
        // end to end, through the one tokenizer both sides share.
        for (query, expected) in [
            ("パスワードをリセット", a_id.as_str()),
            ("請求書の支払い方法", b_id.as_str()),
        ] {
            let reply = search(&kit, &api_key, &format!("q={}", encoded(query))).await;
            assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
            let results = reply.body["results"].as_array().expect("results array");
            assert_eq!(results.len(), 1, "{query}: {}", reply.body);
            assert_eq!(results[0]["source_id"], expected, "{query}");
        }
    }
}

#[pollster::test]
async fn the_scheduled_reindex_reclaims_a_v1_index_for_japanese_queries() {
    for kit in kits() {
        let seeded = seed(
            &kit,
            "Reindexed",
            "パスワードのリセットは設定ページから行えます",
        )
        .await;

        // Simulate what the v1 tokenizer wrote: one unsegmented token for
        // the whole run, stamped with version 1. (The text is one
        // whitespace-delimited word, so v1 indexed exactly this term.)
        let v1_term = "パスワードのリセットは設定ページから行えます";
        for sql in [
            format!(
                "UPDATE sg_chunks SET tokenizer_version = 1, term_count = 1 \
                 WHERE id = '{}'",
                seeded.chunk_id
            ),
            format!(
                "DELETE FROM sg_postings WHERE chunk_id = '{}'",
                seeded.chunk_id
            ),
            format!(
                "INSERT INTO sg_postings (tenant_id, term, chunk_id, tf) VALUES \
                 ('{}', '{v1_term}', '{}', 1)",
                seeded.tenant_id, seeded.chunk_id
            ),
        ] {
            pollster::block_on(kit.db.execute(&Statement::new(sql))).expect("v1 simulation runs");
        }

        // The natural query misses: v1 matches only the exact full run.
        let before = search(
            &kit,
            &seeded.api_key,
            "q=%E3%83%91%E3%82%B9%E3%83%AF%E3%83%BC%E3%83%89",
        )
        .await;
        assert_eq!(
            before.body["results"]
                .as_array()
                .expect("results array")
                .len(),
            0,
            "a v1 index does not answer a natural query: {}",
            before.body
        );

        // The sweep re-tokenizes the stale chunk from its stored text.
        let reindexed = module_support::reindex_stale_chunks(kit.db.as_ref(), 100)
            .await
            .expect("re-index runs");
        assert_eq!(reindexed, 1);
        assert_eq!(
            count_where(&kit, "sg_chunks", "tokenizer_version = 2"),
            1,
            "the chunk is restamped"
        );
        // The term count is the re-tokenized corpus length the ranker
        // reads: the sum of the new postings' frequencies.
        let (term_count, posting_sum): (i64, i64) = pollster::block_on(async {
            let rows = kit
                .db
                .query(&Statement::new(format!(
                    "SELECT (SELECT term_count FROM sg_chunks WHERE id = '{}') AS n, \
                     (SELECT COALESCE(SUM(tf), 0) FROM sg_postings WHERE chunk_id = '{}') AS s",
                    seeded.chunk_id, seeded.chunk_id
                )))
                .await
                .expect("stats query runs");
            let row = rows.rows.first().expect("one stats row");
            (row.get("n").expect("n"), row.get("s").expect("s"))
        });
        assert!(term_count > 1, "bigrams were written: {term_count}");
        assert_eq!(term_count, posting_sum, "term_count matches the postings");

        // And the same query now finds the document.
        let after = search(
            &kit,
            &seeded.api_key,
            "q=%E3%83%91%E3%82%B9%E3%83%AF%E3%83%BC%E3%83%89",
        )
        .await;
        let results = after.body["results"].as_array().expect("results array");
        assert_eq!(results.len(), 1, "{}", after.body);
        assert_eq!(results[0]["chunk_id"], seeded.chunk_id.as_str());

        // A current index is left alone: the sweep answers 0 and a
        // re-ingest of unchanged text is not churned by the stamp.
        assert_eq!(
            module_support::reindex_stale_chunks(kit.db.as_ref(), 100)
                .await
                .expect("re-index runs"),
            0,
            "nothing is stale any more"
        );
    }
}

#[pollster::test]
async fn a_german_turn_records_the_language_and_asks_the_model_to_answer_in_it() {
    for (kit, model) in model_kits() {
        let seeded = seed(
            &kit,
            "Deutsch",
            "Sie können Ihr Passwort auf der Einstellungsseite unter Sicherheit zurücksetzen.",
        )
        .await;
        let german = "Auf der Einstellungsseite unter Sicherheit.";
        model.set_mode(reply(
            german,
            &[(&seeded.chunk_id, "Passwort auf der Einstellungsseite")],
            0.9,
        ));

        // whatlang answers reliably for this question (verified: deu,
        // confidence ~0.97), so no Accept-Language header is needed.
        let body = turn(
            &kit,
            &seeded.api_key,
            "Wie kann ich mein Passwort zurücksetzen?",
            None,
        )
        .await;
        assert_eq!(body["outcome"], "answered");
        assert_eq!(body["answer"], german);

        let prompts = model.prompts();
        assert_eq!(prompts.len(), 1);
        let user: String = prompts[0]
            .messages
            .iter()
            .map(|message| message.content.as_str())
            .collect();
        assert!(
            user.contains("respond_in: de"),
            "the turn's language is named for the model: {user}"
        );
        assert!(
            prompts[0]
                .system
                .as_deref()
                .is_some_and(|system| system.contains("respond_in")),
            "the system prompt tells the model what respond_in means"
        );

        // Both messages of the turn carry the language.
        assert_eq!(
            text_column(&kit, "SELECT lang AS v FROM sg_messages ORDER BY seq"),
            ["de", "de"]
        );
    }
}

#[pollster::test]
async fn an_undetectable_language_falls_back_to_accept_language_or_none() {
    for (kit, model) in model_kits() {
        let seeded = seed(&kit, "Ambiguous", RESET_DOC).await;
        model.set_mode(reply(
            "From settings.",
            &[(&seeded.chunk_id, "settings page")],
            0.9,
        ));

        // "How do I reset my password?" is genuinely undecidable from
        // trigrams alone (whatlang calls it unreliable), so the header
        // decides — its first entry, quality values respected.
        let request = Request::builder()
            .method(Method::POST)
            .uri(MESSAGES)
            .header(header::AUTHORIZATION, format!("Bearer {}", seeded.api_key))
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ACCEPT_LANGUAGE, "ja, en;q=0.8")
            .body(Body::from(json!({ "message": QUESTION }).to_string()))
            .expect("request builds");
        let replied = Reply::of(
            kit.router
                .clone()
                .oneshot(request)
                .await
                .expect("router answers"),
        )
        .await;
        assert_eq!(replied.status, StatusCode::OK, "{}", replied.body);
        assert!(
            model
                .prompts()
                .last()
                .expect("one turn")
                .messages
                .iter()
                .map(|message| message.content.as_str())
                .collect::<String>()
                .contains("respond_in: ja"),
            "Accept-Language's first entry names the language"
        );

        // With no header either, the turn has no language and the prompt
        // carries no respond_in line.
        let bare = turn(&kit, &seeded.api_key, QUESTION, None).await;
        assert_eq!(bare["outcome"], "answered");
        assert!(
            !model
                .prompts()
                .last()
                .expect("second turn")
                .messages
                .iter()
                .map(|message| message.content.as_str())
                .collect::<String>()
                .contains("respond_in"),
            "no language, no respond_in line"
        );
        // Both turns wrote both messages; the first named ja, the second
        // had no signal at all.
        assert_eq!(count_where(&kit, "sg_messages", "lang = 'ja'"), 2);
        assert_eq!(count_where(&kit, "sg_messages", "lang IS NULL"), 2);
    }
}

#[pollster::test]
async fn a_german_clarify_is_rendered_in_german() {
    for (kit, model) in model_kits() {
        // A retrieval that finds the document but a model answer below
        // the threshold: the turn downgrades to the canned clarify, in
        // the question's language. (An empty retrieval would hand off
        // instead — see `nothing_retrieved_hands_off…`.)
        let seeded = seed(
            &kit,
            "Klarstellung",
            "Sie können Ihr Passwort auf der Einstellungsseite unter Sicherheit zurücksetzen.",
        )
        .await;
        model.set_mode(reply(
            "Rate ich einfach.",
            &[(&seeded.chunk_id, "Passwort")],
            0.3,
        ));

        let body = turn(
            &kit,
            &seeded.api_key,
            "Wie kann ich mein Passwort zurücksetzen?",
            None,
        )
        .await;
        assert_eq!(body["outcome"], "clarify");
        assert_eq!(citation_count(&body), 0);
        assert_eq!(
            body_str(&body, "answer"),
            "Ich möchte Ihnen eine fundierte Antwort geben statt einer schnellen falschen — \
             könnten Sie die Frage umformulieren oder ein paar Einzelheiten ergänzen?"
        );
    }
}
