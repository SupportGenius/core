//! Issue #2 acceptance, over every available dialect: tenant
//! provisioning, ingest chunk/posting bookkeeping, the 48 KiB ceiling,
//! URL ingest, BM25 ranking, per-tenant isolation, the
//! indistinguishable-401 auth matrix, and the search edge cases.
//!
//! Issue #3 acceptance (`POST /messages` and the tenant settings route)
//! follows, from "Issue #3" below. The model is `text_model`'s
//! `FakeTextModel`, so every outcome is scripted, not stochastic; content
//! is seeded through the real `/sources` ingest, so every cited chunk id is
//! one BM25 genuinely retrieves.

use axum::body::{Body, to_bytes};
use axum::http::{HeaderMap, Method, Request, StatusCode, header};
use cratefield_core::{Database, MapConfig, Module, Statement};
use cratefield_testing::{Dialect, FakeHttpClient, TestHarness};
use module_support::Support;
use module_support::chunk::Chunker;
use serde_json::{Value, json};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use text_model::testing::FakeTextModel;
use text_model::{Completion, ModelTier, Prompt, TextModel, TextModelError};
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
        Self {
            status,
            headers,
            body: serde_json::from_slice(&bytes).expect("response body is JSON"),
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

/// A kit whose `Support` asks `model`. The fake starts with an empty
/// script: a test pushes the replies once ingest has minted the chunk ids
/// they cite.
fn kit_with_model(dialect: Dialect, model: &FakeTextModel) -> TestHarness {
    TestHarness::with_database_and_ports(
        vec![Box::new(Support::new().text_model(Arc::new(model.clone())))],
        dialect,
        |ports| {
            ports.config = Arc::new(MapConfig::from_pairs([("ADMIN_TOKEN", ADMIN_TOKEN)]));
        },
    )
}

/// One `(kit, model)` pair per available dialect, each with its own fake.
fn model_kits() -> Vec<(TestHarness, FakeTextModel)> {
    Dialect::available()
        .into_iter()
        .map(|dialect| {
            let model = FakeTextModel::scripted(Vec::new());
            (kit_with_model(dialect, &model), model)
        })
        .collect()
}

/// A structured model reply, the way an adapter hands one back: the
/// parsed value in `json`, the same value as text. A `Result` because that
/// is what `FakeTextModel::push` scripts.
#[allow(clippy::unnecessary_wraps)]
fn reply(
    answer: &str,
    citations: &[(&str, &str)],
    confidence: f64,
) -> Result<Completion, TextModelError> {
    let citations: Vec<Value> = citations
        .iter()
        .map(|(chunk_id, quote)| json!({ "chunk_id": chunk_id, "quote": quote }))
        .collect();
    let value = json!({ "answer": answer, "citations": citations, "confidence": confidence });
    Ok(Completion::new(value.to_string(), "fake-fast").json(value))
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
        model.push(reply(
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
        model.push(reply(
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
        model.push(reply(
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
        model.push(reply(
            "Reset it from Settings > Security.",
            &[(
                &owner.chunk_id,
                "Reset your password from the settings page",
            )],
            0.99,
        ));
        model.push(reply(
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
        for _ in 0..2 {
            let body = turn(&kit, &asker.api_key, QUESTION, None).await;
            assert_eq!(body["outcome"], "clarify");
            assert_eq!(citation_count(&body), 0);
        }
    }
}

/// `body` (what the user was shown) and `model_answer` (what the model
/// said) deliberately differ on a downgraded turn.
#[pollster::test]
async fn a_downgraded_turn_stores_the_models_raw_answer() {
    for (kit, model) in model_kits() {
        let seeded = seed(&kit, "Audit", RESET_DOC).await;
        model.push(reply(
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

#[pollster::test]
async fn an_unconfigured_model_answers_503_and_writes_nothing() {
    for dialect in Dialect::available() {
        // Both shapes of "no model": none given to the builder, and one
        // that reports NotConfigured.
        let reporting = FakeTextModel::scripted(vec![Err(TextModelError::NotConfigured)]);
        for kit in [
            kit_with(dialect.clone(), &[]),
            kit_with_model(dialect, &reporting),
        ] {
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
        for pause in [
            Duration::from_secs(7),
            Duration::from_millis(1500),
            Duration::from_millis(1),
            Duration::ZERO,
        ] {
            model.push(Err(TextModelError::Transient {
                retry_after: Some(pause),
            }));
        }
        model.push(Err(TextModelError::Transient { retry_after: None }));
        model.push(reply(
            "From settings.",
            &[(&seeded.chunk_id, "settings page")],
            0.9,
        ));

        for expected in ["7", "2", "1", "1", "2"] {
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
        model.push(unsure());
        model.push(Err(TextModelError::Transient { retry_after: None }));
        model.push(unsure());

        let first = turn(&kit, &seeded.api_key, QUESTION, None).await;
        assert_eq!(first["outcome"], "clarify");
        let conversation = conversation_id(&first);
        let before = conversation_rows(&kit);
        assert_eq!(count_of(&kit, "sg_messages"), 2);

        let failed = post_message(
            &kit,
            &seeded.api_key,
            "password still broken",
            Some(&conversation),
        )
        .await;
        assert_eq!(failed.status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(failed.headers.contains_key(header::RETRY_AFTER));
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
            let model = FakeTextModel::scripted(vec![scripted.clone()]);
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
        model.push(confident());
        model.push(confident());

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
        model.push(reply("Guessing freely.", &[("nowhere", "nothing")], 0.99));

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
        for answer in ["Maybe this?", "Or this?", "Still guessing."] {
            model.push(reply(answer, &[(&seeded.chunk_id, "q")], 0.3));
        }

        let first = turn(&kit, &seeded.api_key, QUESTION, None).await;
        assert_eq!(first["outcome"], "clarify");
        let conversation = conversation_id(&first);
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
        model.push(reply("Guessing.", &[("nowhere", "nothing")], 0.99));
        model.push(reply(
            "From settings.",
            &[(&seeded.chunk_id, "settings page")],
            0.9,
        ));

        // Nothing retrieved for this wording: handoff.
        let handoff = turn(&kit, &seeded.api_key, "billing owner", None).await;
        assert_eq!(handoff["outcome"], "handoff");
        let conversation = conversation_id(&handoff);

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
            inner: FakeTextModel::scripted(Vec::new()),
            db: OnceLock::new(),
            pending: Mutex::new(None),
        });
        let kit = TestHarness::with_database_and_ports(
            vec![Box::new(Support::new().text_model(model.clone()))],
            dialect,
            |ports| {
                ports.config = Arc::new(MapConfig::from_pairs([("ADMIN_TOKEN", ADMIN_TOKEN)]));
            },
        );
        assert!(model.db.set(kit.db.clone()).is_ok(), "db set once");
        let seeded = seed(&kit, "Race", RESET_DOC).await;
        for _ in 0..2 {
            model.inner.push(reply(
                "From settings.",
                &[(&seeded.chunk_id, "settings page")],
                0.9,
            ));
        }

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
        model.push(reply(
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
        model.push(reply(
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
        model.push(reply("Guess.", &[], 0.1));
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
