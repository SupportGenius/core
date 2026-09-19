//! Issue #2 acceptance, over every available dialect: tenant
//! provisioning, ingest chunk/posting bookkeeping, the 48 KiB ceiling,
//! URL ingest, BM25 ranking, per-tenant isolation, the
//! indistinguishable-401 auth matrix, and the search edge cases.

use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode, header};
use cratefield_core::{MapConfig, Module, Statement};
use cratefield_testing::{Dialect, FakeHttpClient, TestHarness};
use module_support::Support;
use module_support::chunk::Chunker;
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;

const ADMIN_TOKEN: &str = "test-admin-token-0123456789abcdef";
const ADMIN: &str = "/v1/support/admin/tenants";
const SOURCES: &str = "/v1/support/sources";
const SEARCH: &str = "/v1/support/search";
const PROBLEMS: &str = "https://factory0.ventures/problems/";

/// A buffered response: every route here answers JSON (success or
/// problem+json), so the body is parsed once, eagerly.
struct Reply {
    status: StatusCode,
    body: Value,
}

impl Reply {
    async fn of(response: axum::response::Response) -> Self {
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("response body reads");
        Self {
            status,
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
