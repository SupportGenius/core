//! Issue #21, part A: a support handoff and the escalation ticket behind
//! it are one atomic write, and escalation is kicked to run immediately
//! instead of waiting for its cron.
//!
//! These drive the **composed** router, not either module alone: the point
//! of the seam is that `module-support` calls a port it does not implement
//! and `module-escalation` implements it, with only this crate — which
//! depends on both — knowing the two are connected.

mod common;

use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode, header};
use cratefield_core::{Destination, MapConfig, ModelTier, Statement};
use cratefield_testing::{TestHarness, TextModelMode};
use serde_json::{Value, json};
use tower::ServiceExt;

use module_escalation::store;

use common::{CREDENTIAL_REF, CREDENTIAL_SECRET, count_of, fast_completion, file_completion};

const ADMIN_TOKEN: &str = "test-admin-token-0123456789abcdef";
const ADMIN: &str = "/v1/support/admin/tenants";
const MESSAGES: &str = "/v1/support/messages";

/// A question the (empty) workspace can answer nothing from, so the turn
/// always hands off — retrieval finds no chunk to ground an answer on.
const MESSAGE: &str = "reset password";

/// A buffered response, parsed as JSON (every route here answers JSON or
/// problem+json).
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
fn kit() -> TestHarness {
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
async fn mint_tenant(kit: &TestHarness) -> (String, String) {
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
fn seed_destination(kit: &TestHarness, tenant_id: &str) {
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

/// One `POST /messages` turn.
async fn turn(kit: &TestHarness, api_key: &str) -> Reply {
    send(
        &kit.router,
        Method::POST,
        MESSAGES,
        Some(api_key),
        Some(&json!({ "message": MESSAGE }).to_string()),
    )
    .await
}

/// The single text column of the one ticket row, aliased `v`.
fn ticket_column(kit: &TestHarness, column: &str) -> Option<String> {
    let rows = pollster::block_on(kit.db.query(&Statement::new(format!(
        "SELECT {column} AS v FROM sg_tickets"
    ))))
    .expect("ticket query runs");
    rows.rows.first().and_then(|row| row.get::<String>("v"))
}

#[pollster::test]
async fn handoff_files_a_ticket_through_the_deferred_drain() {
    let kit = kit();
    let (tenant_id, api_key) = mint_tenant(&kit).await;
    seed_destination(&kit, &tenant_id);

    let reply = turn(&kit, &api_key).await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    assert_eq!(reply.body["outcome"], "handoff");
    assert_eq!(reply.body["needs_escalation"], true);

    // The handoff and its ticket committed together: a ticket row exists
    // as soon as the turn answered, still at intake with nothing filed.
    assert_eq!(
        count_of(&kit, "sg_tickets"),
        1,
        "the turn staged one ticket"
    );
    assert_eq!(ticket_column(&kit, "external_id"), None, "not filed yet");
    let transcript = ticket_column(&kit, "transcript").expect("transcript column");
    assert!(
        transcript.contains(MESSAGE),
        "the ticket carries the turn's transcript: {transcript}"
    );

    // The kick deferred the escalation run; draining runs it to the end.
    kit.defer.drain().await;
    assert_eq!(
        ticket_column(&kit, "external_id").as_deref(),
        Some("fake-0"),
        "the deferred run filed the ticket"
    );
}

#[pollster::test]
async fn no_turn_rows_when_the_ticket_half_of_the_batch_fails() {
    // The sink is wired but the escalation module is not composed, so its
    // tables do not exist and the sink's statements fail inside the turn's
    // one batch. The support rows are in that same batch and must roll
    // back with it: no turn answered "escalated" whose ticket half failed.
    let kit = TestHarness::with_ports(
        vec![Box::new(supportgenius_composition::support())],
        |ports| {
            ports.config = Arc::new(MapConfig::from_pairs([
                ("ADMIN_TOKEN", ADMIN_TOKEN),
                (CREDENTIAL_REF, CREDENTIAL_SECRET),
            ]));
        },
    );
    kit.text_model
        .set_mode_for(ModelTier::Fast, TextModelMode::Complete(fast_completion()));
    let (_tenant_id, api_key) = mint_tenant(&kit).await;

    let reply = turn(&kit, &api_key).await;
    assert!(
        reply.status.is_server_error(),
        "the failed batch is a server error, got {} {:?}",
        reply.status,
        reply.body
    );
    assert_eq!(
        count_of(&kit, "sg_conversations"),
        0,
        "no conversation leaked"
    );
    assert_eq!(count_of(&kit, "sg_messages"), 0, "no message leaked");
}

#[pollster::test]
async fn no_ticket_row_when_the_model_fails() {
    let kit = kit();
    let (tenant_id, api_key) = mint_tenant(&kit).await;
    seed_destination(&kit, &tenant_id);
    // The model answers nothing: the turn fails before its batch, so
    // neither the turn nor a ticket is written. The fast tier is what
    // `POST /messages` asks, and a per-tier mode overrides the global one.
    kit.text_model
        .set_mode_for(ModelTier::Fast, TextModelMode::NotConfigured);

    let reply = turn(&kit, &api_key).await;
    assert_eq!(
        reply.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "{:?}",
        reply.body
    );
    assert_eq!(
        count_of(&kit, "sg_tickets"),
        0,
        "no ticket for a failed turn"
    );
    assert_eq!(
        count_of(&kit, "sg_messages"),
        0,
        "no message for a failed turn"
    );
}
