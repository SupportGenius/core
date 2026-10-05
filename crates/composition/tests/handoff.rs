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

use axum::http::{Method, StatusCode};
use cratefield_core::{MapConfig, ModelTier, Statement};
use cratefield_testing::{TestHarness, TextModelMode};
use serde_json::json;

use common::{
    ADMIN_TOKEN, CREDENTIAL_REF, CREDENTIAL_SECRET, MESSAGE, MESSAGES, Reply, count_of,
    fast_completion, kit, mint_tenant, seed_destination, send, ticket_column,
};

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

/// Issue #26: a `contact` on the turn is remembered for the escalation
/// notify stage, in the turn's own atomic batch, so a filed ticket has
/// somewhere to send.
#[pollster::test]
async fn a_contact_on_the_turn_is_stored_for_the_notify_stage() {
    let kit = kit();
    let (tenant_id, api_key) = mint_tenant(&kit).await;
    seed_destination(&kit, &tenant_id);

    let reply = send(
        &kit.router,
        Method::POST,
        MESSAGES,
        Some(&api_key),
        Some(&json!({ "message": MESSAGE, "contact": { "email": "jane@acme.test" } }).to_string()),
    )
    .await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    let conversation_id = reply.body["conversation_id"]
        .as_str()
        .expect("conversation_id")
        .to_owned();

    let rows = pollster::block_on(kit.db.query(&Statement::new(
        "SELECT tenant_id, conversation_id, email FROM sg_contacts",
    )))
    .expect("contact query runs");
    assert_eq!(rows.rows.len(), 1, "one contact row");
    let row = &rows.rows[0];
    assert_eq!(
        row.get::<String>("tenant_id").expect("tenant_id"),
        tenant_id
    );
    assert_eq!(
        row.get::<String>("conversation_id")
            .expect("conversation_id"),
        conversation_id,
        "the address is stored against the conversation"
    );
    assert_eq!(row.get::<String>("email").expect("email"), "jane@acme.test");
}

/// An address that is not one is a 400 before anything is written: no
/// conversation, no message, no ticket, no contact.
#[pollster::test]
async fn an_invalid_contact_is_rejected_without_writing_anything() {
    let kit = kit();
    let (_tenant_id, api_key) = mint_tenant(&kit).await;

    for bad in [
        "",
        "not-an-email",
        "two@@acme.test",
        "jane@ac me.test",
        "@acme.test",
    ] {
        let reply = send(
            &kit.router,
            Method::POST,
            MESSAGES,
            Some(&api_key),
            Some(&json!({ "message": MESSAGE, "contact": { "email": bad } }).to_string()),
        )
        .await;
        assert_eq!(
            reply.status,
            StatusCode::BAD_REQUEST,
            "`{bad}` is not an address: {:?}",
            reply.body
        );
    }

    assert_eq!(count_of(&kit, "sg_conversations"), 0, "no conversation");
    assert_eq!(count_of(&kit, "sg_messages"), 0, "no message");
    assert_eq!(count_of(&kit, "sg_tickets"), 0, "no ticket");
    assert_eq!(count_of(&kit, "sg_contacts"), 0, "no contact");
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
