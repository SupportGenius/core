//! Fixtures shared by the composed-venture tests (`handoff.rs`, `mcp.rs`,
//! `cron.rs`): the composed kit, the tenant/request plumbing every test
//! drives the router with, the tracker credential pair, the model
//! completions both modules script, and the row-count helpers.
//!
//! The completion builders are shared because both modules read the **same**
//! object: support parses `answer`/`citations`/`confidence`, escalation's
//! draft stage parses the ticket fields, and each `Deserialize`s past the
//! other's — so one fast-tier completion scripts both a handoff answer and
//! the ticket it becomes.

#![allow(dead_code)] // each test binary uses the helpers it needs

use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode, header};
use cratefield_core::{Completion, Destination, MapConfig, ModelTier, Statement};
use cratefield_testing::{TestHarness, TextModelMode};
use serde_json::{Value, json};
use tower::ServiceExt;

use module_escalation::store;

/// The harness admin token every admin route is guarded by.
pub(crate) const ADMIN_TOKEN: &str = "test-admin-token-0123456789abcdef";
/// `POST /v1/support/admin/tenants` — mints a tenant and its first key.
pub(crate) const ADMIN: &str = "/v1/support/admin/tenants";
/// `POST /v1/support/messages` — one support turn.
pub(crate) const MESSAGES: &str = "/v1/support/messages";
/// A question the (empty) workspace can answer nothing from, so the turn
/// always decides `handoff`.
pub(crate) const MESSAGE: &str = "reset password";

/// A buffered response, parsed as JSON (every route answers JSON or
/// problem+json).
pub(crate) struct Reply {
    pub(crate) status: StatusCode,
    pub(crate) body: Value,
}

impl Reply {
    pub(crate) async fn of(response: axum::response::Response) -> Self {
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("response body reads");
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(Value::Null)
        };
        Self { status, body }
    }
}

/// `cratefield_testing::request` sends no headers and the support routes
/// need an `Authorization` bearer, so this is the kit's oneshot pattern
/// with a header slot.
pub(crate) async fn send(
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
    Reply::of(
        router
            .clone()
            .oneshot(builder.body(body).expect("request builds"))
            .await
            .expect("router answers"),
    )
    .await
}

/// The composition's own modules over the published doubles, with the
/// admin token and the tracker credential in config and both model tiers
/// scripted.
pub(crate) fn kit() -> TestHarness {
    let kit = TestHarness::with_ports(
        vec![
            Box::new(supportgenius_composition::support()),
            Box::new(supportgenius_composition::escalation()),
        ],
        |ports| {
            ports.config = Arc::new(MapConfig::from_pairs([
                ("ADMIN_TOKEN", ADMIN_TOKEN),
                (CREDENTIAL_REF, CREDENTIAL_SECRET),
            ]));
        },
    );
    kit.text_model
        .set_mode_for(ModelTier::Fast, TextModelMode::Complete(fast_completion()));
    kit.text_model.set_mode_for(
        ModelTier::Strong,
        TextModelMode::Complete(file_completion()),
    );
    kit
}

/// Mints one tenant and returns `(tenant_id, api_key)`.
pub(crate) async fn mint_tenant(kit: &TestHarness) -> (String, String) {
    let reply = send(
        &kit.router,
        Method::POST,
        ADMIN,
        Some(ADMIN_TOKEN),
        Some(&json!({ "name": "Acme" }).to_string()),
    )
    .await;
    assert_eq!(reply.status, StatusCode::CREATED, "{:?}", reply.body);
    let tenant_id = reply.body["tenant_id"]
        .as_str()
        .expect("tenant_id")
        .to_owned();
    let api_key = reply.body["api_key"].as_str().expect("api_key").to_owned();
    (tenant_id, api_key)
}

/// Seeds the tenant's tracker destination — a GitHub repo with the
/// credential *reference* the file stage resolves through `Config`.
pub(crate) fn seed_destination(kit: &TestHarness, tenant_id: &str) {
    let stmt = store::put_destination_stmt(
        tenant_id,
        &Destination::GitHub {
            owner: "acme".to_owned(),
            repo: "api".to_owned(),
        },
        CREDENTIAL_REF,
        "2027-01-15T00:00:00Z",
    );
    pollster::block_on(kit.db.batch_atomic(&[stmt])).expect("destination seeds");
}

/// The single text column of one row, aliased `v`.
fn column_of(kit: &TestHarness, table: &str, column: &str) -> Option<String> {
    let rows = pollster::block_on(kit.db.query(&Statement::new(format!(
        "SELECT {column} AS v FROM {table}"
    ))))
    .expect("column query runs");
    rows.rows.first().and_then(|row| row.get::<String>("v"))
}

/// The single text column of the one `sg_tickets` row.
pub(crate) fn ticket_column(kit: &TestHarness, column: &str) -> Option<String> {
    column_of(kit, "sg_tickets", column)
}

/// The one `sg_conversations` row's `status` (`'open'` or `'escalated'`).
pub(crate) fn conversation_status(kit: &TestHarness) -> Option<String> {
    column_of(kit, "sg_conversations", "status")
}

/// The `Config` key the escalation file stage resolves the tracker
/// credential under (never the secret itself, which lives here only
/// because this is a test).
pub(crate) const CREDENTIAL_REF: &str = "ESCALATION_TRACKER_CREDENTIAL";
/// The secret behind [`CREDENTIAL_REF`].
pub(crate) const CREDENTIAL_SECRET: &str = "token-1";

/// The fast-tier answer. Support parses `answer`/`citations`/`confidence`
/// from it; escalation's draft stage parses the same object as its draft
/// (both `Deserialize` and ignore each other's fields), so one completion
/// scripts both a handoff answer and the ticket it becomes.
pub(crate) fn fast_completion() -> Completion {
    let payload = json!({
        "answer": "I could not find that in this workspace's documents.",
        "citations": [],
        "confidence": 0.2,
        "title": "Customer cannot reset their password",
        "repro_steps": ["Open the reset page", "Submit the address"],
        "expected": "A reset link arrives",
        "actual": "Nothing arrives",
        "environment": "production",
        "severity": "error",
    });
    Completion::new(payload.to_string(), "fake-fast").json(payload)
}

/// The strong-tier judge's `file` verdict.
pub(crate) fn file_completion() -> Completion {
    let payload = json!({
        "is_defect": true,
        "reproducible": true,
        "severity_ok": true,
        "pii_clean": true,
        "verdict": "file",
        "reasons": ["the steps name a real failure"],
    });
    Completion::new(payload.to_string(), "fake-strong").json(payload)
}

/// The `n` column of a single `SELECT COUNT(*) AS n ...` query. Shared so a
/// row-count helper below is the query it asks, not the extraction.
pub(crate) fn count(kit: &TestHarness, sql: &str) -> i64 {
    let rows = pollster::block_on(kit.db.query(&Statement::new(sql))).expect("count query runs");
    rows.rows
        .first()
        .and_then(|row| row.get::<i64>("n"))
        .expect("aggregate row")
}

/// How many rows `table` holds.
pub(crate) fn count_of(kit: &TestHarness, table: &str) -> i64 {
    count(kit, &format!("SELECT COUNT(*) AS n FROM {table}"))
}
