//! Issue #30 acceptance, over every available dialect: a 300 KiB
//! document uploaded in parts is searchable once the extract job has run
//! (inline via the deferred drain, or from cron when nothing drains it),
//! a PDF yields its text, the per-tenant quota and the size ceilings
//! hold, a foreign tenant sees nothing, and cron collects what nobody
//! finished.
//!
//! The module is exercised through the same real router and real
//! in-memory database as `routes.rs`; the blob is `cratefield_testing`'s
//! `MemoryBlob`, whose object count doubles as the assertion that part
//! bytes are actually dropped once an upload is terminal or collected.

use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode, header};
use cratefield_core::{MapConfig, Module, Ports, Statement, UlidIdGen};
use cratefield_testing::{Dialect, MemoryBlob, TestHarness};
use module_support::Support;
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;

mod stats_check;
use stats_check::{assert_stats_exact, df_of};

const ADMIN_TOKEN: &str = "test-admin-token-0123456789abcdef";
const ADMIN: &str = "/v1/support/admin/tenants";
const SEARCH: &str = "/v1/support/search";
const PROBLEMS: &str = "https://factory0.ventures/problems/";

/// A buffered JSON response, as in `routes.rs`.
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

    fn problem_type(&self) -> &str {
        self.body["type"].as_str().unwrap_or_default()
    }

    fn str_field(&self, field: &str) -> String {
        self.body[field].as_str().expect(field).to_owned()
    }
}

/// Sends a request with an explicit content type and raw bytes — the
/// upload part `PUT` carries neither JSON nor a JSON content type.
async fn send_raw(
    router: &axum::Router,
    method: Method,
    path: &str,
    bearer: &str,
    content_type: &str,
    body: Vec<u8>,
) -> Reply {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header(header::AUTHORIZATION, format!("Bearer {bearer}"))
                .header(header::CONTENT_TYPE, content_type)
                .body(Body::from(body))
                .expect("request builds"),
        )
        .await
        .expect("router answers");
    Reply::of(response).await
}

async fn send_json(
    router: &axum::Router,
    method: Method,
    path: &str,
    bearer: Option<&str>,
    body: Value,
) -> Reply {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(key) = bearer {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {key}"));
    }
    let response = router
        .clone()
        .oneshot(
            builder
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .expect("request builds"),
        )
        .await
        .expect("router answers");
    Reply::of(response).await
}

fn support() -> Vec<Box<dyn Module>> {
    vec![Box::new(Support::new())]
}

/// One kit per dialect, with a `MemoryBlob` mounted for the upload
/// routes and a handle kept for assertions. `extra` carries module
/// config (`SUPPORT_UPLOAD_QUOTA_BYTES` for the quota test).
fn kit_with(dialect: Dialect, extra: &[(&'static str, &str)]) -> (TestHarness, MemoryBlob) {
    let blob = MemoryBlob::new();
    let handle = blob.clone();
    let kit = TestHarness::with_database_and_ports(support(), dialect, |ports| {
        let mut pairs = vec![("ADMIN_TOKEN", ADMIN_TOKEN)];
        pairs.extend(extra.iter().copied());
        ports.config = Arc::new(MapConfig::from_pairs(pairs));
        ports.blob = Some(Arc::new(blob));
    });
    (kit, handle)
}

fn kits() -> Vec<(TestHarness, MemoryBlob)> {
    Dialect::available()
        .into_iter()
        .map(|dialect| kit_with(dialect, &[]))
        .collect()
}

async fn mint_tenant(kit: &TestHarness) -> String {
    let reply = send_json(
        &kit.router,
        Method::POST,
        ADMIN,
        Some(ADMIN_TOKEN),
        json!({ "name": "Acme Support" }),
    )
    .await;
    assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
    reply.str_field("api_key")
}

/// `POST /uploads` with one JSON body.
async fn open_upload(
    kit: &TestHarness,
    key: &str,
    filename: &str,
    content_type: &str,
    bytes: usize,
) -> Reply {
    send_json(
        &kit.router,
        Method::POST,
        "/v1/support/uploads",
        Some(key),
        json!({ "filename": filename, "content_type": content_type, "bytes": bytes }),
    )
    .await
}

/// `PUT /uploads/{id}/parts/{n}` with raw bytes.
async fn put_part(
    kit: &TestHarness,
    key: &str,
    upload_id: &str,
    n: usize,
    bytes: Vec<u8>,
) -> Reply {
    send_raw(
        &kit.router,
        Method::PUT,
        &format!("/v1/support/uploads/{upload_id}/parts/{n}"),
        key,
        "application/octet-stream",
        bytes,
    )
    .await
}

async fn complete(kit: &TestHarness, key: &str, upload_id: &str) -> Reply {
    send_json(
        &kit.router,
        Method::POST,
        &format!("/v1/support/uploads/{upload_id}/complete"),
        Some(key),
        json!({}),
    )
    .await
}

async fn get_upload(kit: &TestHarness, key: &str, upload_id: &str) -> Reply {
    send_raw(
        &kit.router,
        Method::GET,
        &format!("/v1/support/uploads/{upload_id}"),
        key,
        "application/json",
        Vec::new(),
    )
    .await
}

async fn search(kit: &TestHarness, key: &str, query: &str) -> Reply {
    send_raw(
        &kit.router,
        Method::GET,
        &format!("{SEARCH}?q={query}"),
        key,
        "application/json",
        Vec::new(),
    )
    .await
}

/// Uploads `document` in [`module_support`] part-sized chunks and
/// completes, returning the upload id. The whole point of the API is
/// that the caller needs nothing but the response's `part_bytes`.
async fn upload_in_parts(
    kit: &TestHarness,
    key: &str,
    filename: &str,
    content_type: &str,
    document: Vec<u8>,
) -> String {
    let opened = open_upload(kit, key, filename, content_type, document.len()).await;
    assert_eq!(opened.status, StatusCode::CREATED, "{}", opened.body);
    let upload_id = opened.str_field("id");
    let part_bytes = usize::try_from(opened.body["part_bytes"].as_u64().expect("part_bytes"))
        .expect("part_bytes fits usize");
    assert_eq!(part_bytes, 48 * 1024, "the documented part ceiling");
    for (n, chunk) in document.chunks(part_bytes).enumerate() {
        let part = put_part(kit, key, &upload_id, n, chunk.to_vec()).await;
        assert_eq!(part.status, StatusCode::OK, "{}", part.body);
    }
    let done = complete(kit, key, &upload_id).await;
    assert_eq!(done.status, StatusCode::ACCEPTED, "{}", done.body);
    upload_id
}

/// The ports `Module::scheduled` needs, over the kit's own database,
/// blob handle and fixed clock — the same shape the conformance suite
/// builds, minus everything the scheduled path never touches.
fn scheduled_ports(kit: &TestHarness, blob: &MemoryBlob) -> Ports {
    let mut ports = Ports::empty();
    ports.db = Some(kit.db.clone());
    ports.blob = Some(Arc::new(blob.clone()));
    ports.clock = Some(Arc::new(kit.clock.clone()));
    ports.id_gen = Some(Arc::new(UlidIdGen));
    ports
}

/// Cron, by hand: `Module::scheduled` driven with a context built over
/// the kit's own ports.
async fn run_scheduled(kit: &TestHarness, blob: &MemoryBlob) {
    let ports = scheduled_ports(kit, blob);
    let ctx = kit.harness.module_context(kit.modules[0].as_ref(), &ports);
    kit.modules[0]
        .scheduled(&ctx, "23 4 * * *")
        .await
        .expect("scheduled runs");
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

/// A document of repeated filler with one marker phrase at the very end,
/// sized past `min_bytes` — the marker lands in the final part, so a
/// hit proves every part made it into the index.
fn long_document(min_bytes: usize, marker: &str) -> Vec<u8> {
    let filler = b"the support team answers questions about billing, setup and \
                   account recovery for workspace administrators. ";
    let mut text = Vec::with_capacity(min_bytes + 1024);
    while text.len() < min_bytes {
        text.extend_from_slice(filler);
    }
    text.extend_from_slice(marker.as_bytes());
    text
}

/// The single text column `v` of the first row of `sql`.
fn text_of(kit: &TestHarness, sql: &str) -> String {
    let rows = pollster::block_on(kit.db.query(&Statement::new(sql))).expect("query runs");
    rows.rows
        .first()
        .and_then(|row| row.get::<String>("v"))
        .expect("one text row")
}

/// A one-page PDF whose page shows `text`, built with the same lopdf
/// API the extractor reads back — no committed binary fixture, the
/// document is generated here. (The helper cannot live in the module:
/// `extract` is private, so the integration suite keeps its own copy.)
fn one_page_pdf(text: &str) -> Vec<u8> {
    use lopdf::content::Content;
    // The macro must be imported, not path-invoked: lopdf's
    // trailing-comma arm recurses as a bare `dictionary!`, which only
    // resolves when the macro is in scope.
    use lopdf::dictionary;

    let operations = vec![lopdf::content::Operation::new(
        "Tj",
        vec![lopdf::Object::string_literal(text)],
    )];
    let content = Content { operations };
    let mut doc = lopdf::Document::with_version("1.4");
    let font_id = doc.add_object(dictionary! {
        "Type" => "Font",
        "Subtype" => "Type1",
        "BaseFont" => "Helvetica",
    });
    let resources_id = doc.add_object(dictionary! {
        "Font" => dictionary! { "F1" => font_id },
    });
    let pages_id = doc.add_object(dictionary! {
        "Type" => "Pages",
        "Kids" => lopdf::Object::Array(Vec::new()),
        "Count" => 1,
    });
    let content_id = doc.add_object(lopdf::Stream::new(
        dictionary! {},
        content.encode().expect("content encodes"),
    ));
    let shown_id = doc.add_object(dictionary! {
        "Type" => "Page",
        "Parent" => pages_id,
        "Contents" => content_id,
        "Resources" => resources_id,
    });
    if let lopdf::Object::Dictionary(kids) = doc.objects.get_mut(&pages_id).expect("pages") {
        kids.set(
            "Kids",
            lopdf::Object::Array(vec![lopdf::Object::Reference(shown_id)]),
        );
    }
    let catalog_id = doc.add_object(dictionary! {
        "Type" => "Catalog",
        "Pages" => pages_id,
    });
    doc.trailer.set("Root", catalog_id);
    doc.compress();
    let mut bytes = Vec::new();
    doc.save_to(&mut bytes).expect("serialises");
    bytes
}

#[pollster::test]
async fn a_large_text_upload_is_searchable_once_extracted() {
    for (kit, blob) in kits() {
        let key = mint_tenant(&kit).await;
        let marker = "the quokka husbandry manual explains enclosure humidity";
        let document = long_document(300 * 1024, marker);
        assert!(document.len() > 300 * 1024, "{} bytes", document.len());

        let upload_id =
            upload_in_parts(&kit, &key, "quokka-manual.txt", "text/plain", document).await;
        // Parts were stored, and the upload is complete but not yet
        // indexed before the deferred drain runs.
        assert!(!blob.is_empty(), "parts are in blob storage");
        let status = get_upload(&kit, &key, &upload_id).await;
        assert_eq!(status.status, StatusCode::OK, "{}", status.body);
        assert_eq!(status.str_field("status"), "complete");

        kit.defer.drain().await;

        let status = get_upload(&kit, &key, &upload_id).await;
        assert_eq!(status.str_field("status"), "extracted", "{}", status.body);
        let source_id = status.str_field("source_id");
        assert!(!source_id.is_empty());
        // Terminal: the part bytes are gone, only the index remains.
        assert_eq!(blob.len(), 0, "part blobs are dropped after extraction");

        let hits = search(&kit, &key, "quokka").await;
        assert_eq!(hits.status, StatusCode::OK, "{}", hits.body);
        let results = hits.body["results"].as_array().expect("results");
        assert!(
            results
                .iter()
                .any(|hit| hit["source_id"].as_str() == Some(source_id.as_str())),
            "the uploaded document is searchable: {}",
            hits.body
        );
    }
}

#[pollster::test]
async fn a_pdf_upload_yields_searchable_text() {
    for (kit, blob) in kits() {
        let key = mint_tenant(&kit).await;
        let marker = "the harvest quota for cloudberries is twelve crates";
        let pdf = one_page_pdf(marker);

        let upload_id =
            upload_in_parts(&kit, &key, "harvest-rules.pdf", "application/pdf", pdf).await;
        kit.defer.drain().await;

        let status = get_upload(&kit, &key, &upload_id).await;
        assert_eq!(status.str_field("status"), "extracted", "{}", status.body);
        let source_id = status.str_field("source_id");
        assert_eq!(blob.len(), 0, "part blobs are dropped after extraction");

        let hits = search(&kit, &key, "cloudberries").await;
        let results = hits.body["results"].as_array().expect("results");
        assert!(
            results
                .iter()
                .any(|hit| hit["source_id"].as_str() == Some(source_id.as_str())),
            "the PDF's text is searchable: {}",
            hits.body
        );
    }
}

#[pollster::test]
async fn cron_extracts_what_the_deferred_drain_did_not() {
    for (kit, blob) in kits() {
        let key = mint_tenant(&kit).await;
        let marker = "escalation paths are documented in the on-call runbook";
        let document = long_document(64 * 1024, marker);

        // `complete` enqueues the job; the kit's FakeDefer collects the
        // deferred drain without running it, so the only thing that can
        // make the document searchable is cron.
        let upload_id = upload_in_parts(&kit, &key, "runbook.txt", "text/plain", document).await;
        assert_eq!(
            get_upload(&kit, &key, &upload_id).await.str_field("status"),
            "complete"
        );

        run_scheduled(&kit, &blob).await;

        let status = get_upload(&kit, &key, &upload_id).await;
        assert_eq!(status.str_field("status"), "extracted", "{}", status.body);
        let source_id = status.str_field("source_id");
        let hits = search(&kit, &key, "runbook").await;
        let results = hits.body["results"].as_array().expect("results");
        assert!(
            results
                .iter()
                .any(|hit| hit["source_id"].as_str() == Some(source_id.as_str())),
            "cron's extract made the document searchable: {}",
            hits.body
        );
    }
}

#[pollster::test]
async fn the_quota_is_enforced_and_returned_by_extraction() {
    for (kit, blob) in kits_with_quota() {
        let key = mint_tenant(&kit).await;
        let document = long_document(700, "first");

        // 700 bytes fits the 1 KiB budget; a second open upload would
        // not, and is refused before any row or part exists.
        let upload_id = upload_in_parts(&kit, &key, "first.txt", "text/plain", document).await;
        assert!(!blob.is_empty());
        let refused = open_upload(&kit, &key, "second.txt", "text/plain", 700).await;
        assert_eq!(refused.status, StatusCode::FORBIDDEN, "{}", refused.body);
        assert_eq!(
            refused.problem_type(),
            format!("{PROBLEMS}upload-quota-exceeded")
        );

        // Extraction frees the budget: the parts are gone, only the
        // index remains.
        kit.defer.drain().await;
        assert_eq!(
            get_upload(&kit, &key, &upload_id).await.str_field("status"),
            "extracted"
        );
        let accepted = open_upload(&kit, &key, "third.txt", "text/plain", 700).await;
        assert_eq!(accepted.status, StatusCode::CREATED, "{}", accepted.body);
    }
}

fn kits_with_quota() -> Vec<(TestHarness, MemoryBlob)> {
    Dialect::available()
        .into_iter()
        .map(|dialect| kit_with(dialect, &[("SUPPORT_UPLOAD_QUOTA_BYTES", "1024")]))
        .collect()
}

#[pollster::test]
async fn a_declaration_past_the_maximum_is_refused() {
    for (kit, _blob) in kits() {
        let key = mint_tenant(&kit).await;
        let refused = open_upload(
            &kit,
            &key,
            "big.pdf",
            "application/pdf",
            4 * 1024 * 1024 + 1,
        )
        .await;
        assert_eq!(
            refused.status,
            StatusCode::PAYLOAD_TOO_LARGE,
            "{}",
            refused.body
        );
        assert_eq!(
            refused.problem_type(),
            format!("{PROBLEMS}upload-too-large")
        );
        assert_eq!(
            count_of(&kit, "sg_uploads"),
            0,
            "no row for a refused upload"
        );
    }
}

#[pollster::test]
async fn a_part_past_the_ceiling_is_refused() {
    for (kit, _blob) in kits() {
        let key = mint_tenant(&kit).await;
        let opened = open_upload(&kit, &key, "doc.txt", "text/plain", 64 * 1024).await;
        let upload_id = opened.str_field("id");
        let part = put_part(&kit, &key, &upload_id, 0, vec![b'x'; 48 * 1024 + 1]).await;
        assert_eq!(part.status, StatusCode::BAD_REQUEST, "{}", part.body);
        assert_eq!(part.problem_type(), format!("{PROBLEMS}validation-failed"));
    }
}

#[pollster::test]
async fn complete_requires_the_declared_parts() {
    for (kit, _blob) in kits() {
        let key = mint_tenant(&kit).await;
        let opened = open_upload(&kit, &key, "doc.txt", "text/plain", 150).await;
        let upload_id = opened.str_field("id");

        // Missing tail: one part of 50 bytes against 150 declared.
        put_part(&kit, &key, &upload_id, 0, vec![b'a'; 50]).await;
        let short = complete(&kit, &key, &upload_id).await;
        assert_eq!(short.status, StatusCode::CONFLICT, "{}", short.body);
        assert_eq!(short.problem_type(), format!("{PROBLEMS}upload-incomplete"));

        // A gap: parts 0 and 2 of three, missing 1 — still short even
        // before the contiguity check bites.
        put_part(&kit, &key, &upload_id, 2, vec![b'a'; 50]).await;
        let gapped = complete(&kit, &key, &upload_id).await;
        assert_eq!(gapped.status, StatusCode::CONFLICT, "{}", gapped.body);
        assert_eq!(
            gapped.problem_type(),
            format!("{PROBLEMS}upload-incomplete")
        );

        // Filling the gap completes.
        put_part(&kit, &key, &upload_id, 1, vec![b'a'; 50]).await;
        let done = complete(&kit, &key, &upload_id).await;
        assert_eq!(done.status, StatusCode::ACCEPTED, "{}", done.body);
    }
}

#[pollster::test]
async fn a_foreign_tenant_sees_no_upload() {
    for (kit, _blob) in kits() {
        let alice = mint_tenant(&kit).await;
        let mallory = mint_tenant(&kit).await;
        let opened = open_upload(&kit, &alice, "doc.txt", "text/plain", 10).await;
        let upload_id = opened.str_field("id");

        // Mallory can put a part into Alice's upload id? No — the 404 is
        // the same for a foreign id and an unknown one, so nothing about
        // Alice's uploads is even confirmed to exist.
        let put = put_part(&kit, &mallory, &upload_id, 0, vec![b'x'; 10]).await;
        assert_eq!(put.status, StatusCode::NOT_FOUND, "{}", put.body);
        assert_eq!(put.problem_type(), format!("{PROBLEMS}not-found"));

        let done = complete(&kit, &mallory, &upload_id).await;
        assert_eq!(done.status, StatusCode::NOT_FOUND, "{}", done.body);

        let read = get_upload(&kit, &mallory, &upload_id).await;
        assert_eq!(read.status, StatusCode::NOT_FOUND, "{}", read.body);

        // And nothing Mallory did reached Alice's upload: it still has
        // zero parts, so her own complete still says incomplete.
        let still_open = complete(&kit, &alice, &upload_id).await;
        assert_eq!(
            still_open.status,
            StatusCode::CONFLICT,
            "{}",
            still_open.body
        );
    }
}

#[pollster::test]
async fn a_closed_upload_accepts_no_parts_and_no_second_complete() {
    for (kit, _blob) in kits() {
        let key = mint_tenant(&kit).await;
        let opened = open_upload(&kit, &key, "doc.txt", "text/plain", 5).await;
        let upload_id = opened.str_field("id");
        put_part(&kit, &key, &upload_id, 0, vec![b'x'; 5]).await;
        assert_eq!(
            complete(&kit, &key, &upload_id).await.status,
            StatusCode::ACCEPTED
        );

        let late_part = put_part(&kit, &key, &upload_id, 0, vec![b'x'; 5]).await;
        assert_eq!(late_part.status, StatusCode::CONFLICT, "{}", late_part.body);
        assert_eq!(late_part.problem_type(), format!("{PROBLEMS}upload-closed"));

        let again = complete(&kit, &key, &upload_id).await;
        assert_eq!(again.status, StatusCode::CONFLICT, "{}", again.body);
        assert_eq!(again.problem_type(), format!("{PROBLEMS}upload-closed"));
    }
}

#[pollster::test]
async fn a_reput_replaces_the_part() {
    for (kit, blob) in kits() {
        let key = mint_tenant(&kit).await;
        let opened = open_upload(&kit, &key, "doc.txt", "text/plain", 12).await;
        let upload_id = opened.str_field("id");

        let first = put_part(&kit, &key, &upload_id, 0, vec![b'a'; 10]).await;
        assert_eq!(first.status, StatusCode::OK, "{}", first.body);
        assert_eq!(first.body["received_bytes"], 10);
        // Same ordinal, different bytes: the total swaps the old part
        // out rather than adding on top.
        let second = put_part(&kit, &key, &upload_id, 0, vec![b'b'; 12]).await;
        assert_eq!(second.status, StatusCode::OK, "{}", second.body);
        assert_eq!(second.body["received_bytes"], 12);

        assert_eq!(blob.len(), 1, "the replacement replaced the object");
        let done = complete(&kit, &key, &upload_id).await;
        assert_eq!(done.status, StatusCode::ACCEPTED, "{}", done.body);
        let status = get_upload(&kit, &key, &upload_id).await;
        assert_eq!(status.body["received_bytes"], 12, "{}", status.body);
    }
}

#[pollster::test]
async fn upload_routes_answer_not_ready_without_the_blob_port() {
    for dialect in Dialect::available() {
        let kit = TestHarness::with_database_and_ports(support(), dialect, |ports| {
            ports.config = Arc::new(MapConfig::from_pairs([("ADMIN_TOKEN", ADMIN_TOKEN)]));
            // No blob: the deployment that never configured object
            // storage. Everything else is the standard fakes.
        });
        let key = mint_tenant(&kit).await;
        let opened = open_upload(&kit, &key, "doc.txt", "text/plain", 10).await;
        assert_eq!(
            opened.status,
            StatusCode::SERVICE_UNAVAILABLE,
            "{}",
            opened.body
        );
        assert_eq!(opened.problem_type(), format!("{PROBLEMS}not-ready"));
        assert_eq!(count_of(&kit, "sg_uploads"), 0, "no row without storage");

        // The rest of the module is untouched: inline ingest still works.
        let reply = send_json(
            &kit.router,
            Method::POST,
            "/v1/support/sources",
            Some(&key),
            json!({ "title": "Policy", "text": "the refund window is thirty days" }),
        )
        .await;
        assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
    }
}

#[pollster::test]
async fn a_bad_document_fails_the_upload_and_drops_the_parts() {
    for (kit, blob) in kits() {
        let key = mint_tenant(&kit).await;
        let opened = open_upload(&kit, &key, "broken.pdf", "application/pdf", 10).await;
        let upload_id = opened.str_field("id");
        put_part(&kit, &key, &upload_id, 0, b"not a pdf\n".to_vec()).await;
        assert_eq!(
            complete(&kit, &key, &upload_id).await.status,
            StatusCode::ACCEPTED
        );

        kit.defer.drain().await;

        let status = get_upload(&kit, &key, &upload_id).await;
        assert_eq!(status.str_field("status"), "failed", "{}", status.body);
        assert!(
            status.str_field("error").contains("unreadable PDF"),
            "{}",
            status.body
        );
        assert_eq!(blob.len(), 0, "the failed upload's parts are dropped");

        // A failed upload freed its budget too.
        let next = open_upload(&kit, &key, "next.txt", "text/plain", 10).await;
        assert_eq!(next.status, StatusCode::CREATED, "{}", next.body);
    }
}

/// Plain text is the identity extractor, so 3 MiB of it — inside every
/// transport limit, `complete` accepted — extracts to 3 MiB of text,
/// past the 2 MiB ceiling. The extraction-only bound fails the upload
/// with the reason, drops the parts and frees the budget.
#[pollster::test]
async fn text_expanding_past_the_extraction_ceiling_fails_the_upload() {
    for (kit, blob) in kits() {
        let key = mint_tenant(&kit).await;
        let document = long_document(3 * 1024 * 1024 + 1, "past the extraction ceiling");
        let upload_id = upload_in_parts(&kit, &key, "big.txt", "text/plain", document).await;

        kit.defer.drain().await;

        let status = get_upload(&kit, &key, &upload_id).await;
        assert_eq!(status.str_field("status"), "failed", "{}", status.body);
        assert!(
            status.str_field("error").contains("ceiling"),
            "{}",
            status.body
        );
        assert_eq!(blob.len(), 0, "the failed upload's parts are dropped");

        // The budget was freed with the parts.
        let next = open_upload(&kit, &key, "next.txt", "text/plain", 10).await;
        assert_eq!(next.status, StatusCode::CREATED, "{}", next.body);
    }
}

#[pollster::test]
async fn cron_collects_an_abandoned_upload() {
    for (kit, blob) in kits() {
        let key = mint_tenant(&kit).await;

        // The upload that will look abandoned: opened now, parts stored,
        // never completed.
        let abandoned = open_upload(&kit, &key, "stale.txt", "text/plain", 10).await;
        let stale_id = abandoned.str_field("id");
        put_part(&kit, &key, &stale_id, 0, vec![b'x'; 10]).await;
        assert!(!blob.is_empty());

        // A second upload opened *after* the clock has moved past the
        // TTL must survive the same sweep.
        let opened_at = kit.clock.0;
        let ttl = time::Duration::hours(24);
        kit.db
            .execute(&Statement::new(format!(
                "UPDATE sg_uploads SET created_at = '{}'",
                (opened_at - ttl - time::Duration::hours(1))
                    .format(&time::format_description::well_known::Rfc3339)
                    .expect("cutoff formats")
            )))
            .await
            .expect("backdating runs");
        let fresh = open_upload(&kit, &key, "fresh.txt", "text/plain", 10).await;
        let fresh_id = fresh.str_field("id");
        put_part(&kit, &key, &fresh_id, 0, vec![b'y'; 10]).await;

        run_scheduled(&kit, &blob).await;

        // The abandoned upload: rows and part blobs gone, and the route
        // now answers 404 — the upload no longer exists.
        assert_eq!(blob.len(), 1, "only the fresh upload's part remains");
        let gone = get_upload(&kit, &key, &stale_id).await;
        assert_eq!(gone.status, StatusCode::NOT_FOUND, "{}", gone.body);
        assert_eq!(gone.problem_type(), format!("{PROBLEMS}not-found"));
        assert_eq!(count_of(&kit, "sg_uploads"), 1);
        assert_eq!(count_of(&kit, "sg_upload_parts"), 1);

        // The fresh upload is untouched and still usable.
        let alive = get_upload(&kit, &key, &fresh_id).await;
        assert_eq!(alive.status, StatusCode::OK, "{}", alive.body);
        assert_eq!(alive.str_field("status"), "open");
    }
}

// ---------------------------------------------------------------------
// Where uploads meet what main grew beside them: source management
// (#45 — list, read, replace, delete, `external_id`), the bigram
// tokenizer with its `tokenizer_version` stamp and scheduled re-index
// (#49), and the one `scheduled` hook both of them now share.
// ---------------------------------------------------------------------

/// Sends a bodiless request and answers the status alone — `DELETE
/// /sources/{id}` answers `204` with no JSON to parse.
async fn send_status(kit: &TestHarness, method: Method, path: &str, key: &str) -> StatusCode {
    kit.router
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header(header::AUTHORIZATION, format!("Bearer {key}"))
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("router answers")
        .status()
}

fn count_where(kit: &TestHarness, table: &str, predicate: &str) -> usize {
    let rows = pollster::block_on(kit.db.query(&Statement::new(format!(
        "SELECT COUNT(*) AS n FROM {table} WHERE {predicate}"
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

/// Percent-encodes a query value, as `routes.rs` does, so a Japanese
/// `q` survives the request URI.
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
async fn an_extracted_upload_is_an_ordinary_managed_source() {
    for (kit, blob) in kits() {
        let key = mint_tenant(&kit).await;
        let marker = "warranty claims need the original receipt";
        let document = long_document(60 * 1024, marker);
        let text_len = document.len();
        let upload_id =
            upload_in_parts(&kit, &key, "  warranty.md  ", "text/markdown", document).await;
        run_scheduled(&kit, &blob).await;
        let status = get_upload(&kit, &key, &upload_id).await;
        assert_eq!(status.str_field("status"), "extracted", "{}", status.body);
        let source_id = status.str_field("source_id");

        // Listed and readable through the source-management routes, in
        // their item shape: a text source with no caller identity, the
        // trimmed filename as its title, and the indexed text's size.
        let listed = send_raw(
            &kit.router,
            Method::GET,
            "/v1/support/sources",
            &key,
            "application/json",
            Vec::new(),
        )
        .await;
        assert_eq!(listed.status, StatusCode::OK, "{}", listed.body);
        let sources = listed.body["sources"].as_array().expect("sources array");
        assert_eq!(sources.len(), 1, "{}", listed.body);
        let item = &sources[0];
        assert_eq!(item["id"], source_id.as_str());
        assert_eq!(item["title"], "warranty.md");
        assert_eq!(item["origin"], "text");
        assert!(item["external_id"].is_null(), "{item}");
        assert_eq!(item["bytes"], json!(text_len));
        assert!(item["chunk_count"].as_i64().expect("chunk_count") > 1);
        assert!(item["updated_at"].is_string(), "{item}");

        // The extract job's batch wrote the search statistics with the
        // source it produced (issue #31).
        assert!(assert_stats_exact(&kit, "after the upload was extracted") > 1);
        let tenant_id = text_of(
            &kit,
            &format!("SELECT tenant_id AS v FROM sg_sources WHERE id = '{source_id}'"),
        );
        assert_eq!(df_of(&kit, &tenant_id, "warranty"), Some(1));

        // Every chunk it wrote carries the current tokenizer stamp, so the
        // scheduled re-index has nothing to redo for it.
        assert_eq!(
            count_where(
                &kit,
                "sg_chunks",
                &format!(
                    "source_id = '{source_id}' AND tokenizer_version <> {}",
                    module_support::chunk::TOKENIZER_VERSION
                ),
            ),
            0,
            "extracted chunks are stamped current"
        );

        // Deleting it is the ordinary delete: the index goes, search
        // stops finding it, and the upload row stays as bookkeeping.
        let deleted = send_status(
            &kit,
            Method::DELETE,
            &format!("/v1/support/sources/{source_id}"),
            &key,
        )
        .await;
        assert_eq!(deleted, StatusCode::NO_CONTENT);
        let hits = search(&kit, &key, "warranty").await;
        assert_eq!(
            hits.body["results"].as_array().expect("results").len(),
            0,
            "{}",
            hits.body
        );
        assert_eq!(
            count_where(&kit, "sg_chunks", &format!("source_id = '{source_id}'")),
            0
        );
        assert_eq!(
            assert_stats_exact(&kit, "after the extracted source was deleted"),
            0,
            "the tenant's only source is gone, and its statistics with it"
        );
        let after = get_upload(&kit, &key, &upload_id).await;
        assert_eq!(after.str_field("status"), "extracted", "{}", after.body);
        assert_eq!(after.str_field("source_id"), source_id);
    }
}

#[pollster::test]
async fn a_japanese_upload_is_found_by_a_japanese_query() {
    for (kit, blob) in kits() {
        let key = mint_tenant(&kit).await;
        // Past one part, with the target phrase in the last one, so the
        // bigram tokenizer runs over the assembled document, not a part.
        let mut document = Vec::new();
        while document.len() < 50 * 1024 {
            document.extend_from_slice(
                "請求書の支払い方法は、アカウントの請求セクションで変更できます。".as_bytes(),
            );
        }
        document.extend_from_slice("パスワードをリセットするには設定ページを開きます。".as_bytes());
        let upload_id = upload_in_parts(&kit, &key, "manual-ja.txt", "text/plain", document).await;
        run_scheduled(&kit, &blob).await;
        let status = get_upload(&kit, &key, &upload_id).await;
        assert_eq!(status.str_field("status"), "extracted", "{}", status.body);
        let source_id = status.str_field("source_id");

        let hits = search(&kit, &key, &encoded("パスワードをリセット")).await;
        assert_eq!(hits.status, StatusCode::OK, "{}", hits.body);
        let results = hits.body["results"].as_array().expect("results");
        assert!(!results.is_empty(), "{}", hits.body);
        assert_eq!(results[0]["source_id"], source_id.as_str(), "{}", hits.body);
        assert!(assert_stats_exact(&kit, "after a Japanese upload") > 1);
    }
}

#[pollster::test]
async fn one_cron_tick_runs_both_the_reindex_and_the_upload_sweeps() {
    for (kit, blob) in kits() {
        let key = mint_tenant(&kit).await;

        // An inline source whose chunks are made to look like an older
        // tokenizer wrote them — the re-index sweep's work.
        let inline = send_json(
            &kit.router,
            Method::POST,
            "/v1/support/sources",
            Some(&key),
            json!({ "title": "Inline", "text": "refunds are issued within five business days" }),
        )
        .await;
        assert_eq!(inline.status, StatusCode::CREATED, "{}", inline.body);
        let inline_id = inline.str_field("source_id");
        pollster::block_on(kit.db.execute(&Statement::new(format!(
            "UPDATE sg_chunks SET tokenizer_version = 1 WHERE source_id = '{inline_id}'"
        ))))
        .expect("stale stamp written");

        // A completed upload nobody drained inline — the extract sweep's.
        let upload_id = upload_in_parts(
            &kit,
            &key,
            "shipping.txt",
            "text/plain",
            long_document(50 * 1024, "parcels ship from the rotterdam depot"),
        )
        .await;

        run_scheduled(&kit, &blob).await;

        assert_eq!(
            count_where(&kit, "sg_chunks", "tokenizer_version = 1"),
            0,
            "the re-index sweep ran"
        );
        let status = get_upload(&kit, &key, &upload_id).await;
        assert_eq!(
            status.str_field("status"),
            "extracted",
            "the upload sweep ran in the same tick: {}",
            status.body
        );
        assert_eq!(count_where(&kit, "sg_support_outbox", "1 = 1"), 0);
        // Both sweeps of the tick kept the statistics exact: the re-index
        // rewrote the stale chunk's contribution, the extract added the
        // upload's.
        assert!(assert_stats_exact(&kit, "after one cron tick of both sweeps") > 1);
    }
}

#[test]
fn the_upload_migration_follows_internationalization() {
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
        ],
        "uploads is support/0005, after the two migrations main gained (connectors follow it)"
    );
}
