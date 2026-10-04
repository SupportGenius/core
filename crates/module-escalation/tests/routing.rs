//! Issue #24: ticket kinds and per-kind routing. A drafter classifies the
//! conversation as a defect, a support case or a lead; a tenant may route
//! each kind to its own tracker destination; a kind with no route files
//! into the module's own built-in ticketing rather than dead-lettering;
//! and a kind the judge cannot confirm is parked for more information
//! instead of being filed.

mod support;

use std::collections::BTreeMap;

use cratefield_core::{Destination, ModelTier};
use cratefield_testing::{FakeTracker, TextModelMode, TrackerMode};
use module_escalation::model::{EventKind, Kind, Status};
use module_escalation::store::RouteTarget;
use serde_json::json;

/// The three route destinations the tests fan one tenant's kinds out to.
fn github() -> Destination {
    Destination::GitHub {
        owner: "acme".to_owned(),
        repo: "api".to_owned(),
    }
}

fn zendesk() -> Destination {
    Destination::Zendesk {
        subdomain: "acme".to_owned(),
    }
}

fn salesforce() -> Destination {
    Destination::Salesforce {
        instance: "acme.my.salesforce.com".to_owned(),
    }
}

/// (Re-)scripts the fast tier to answer one kind's draft — the seam the
/// single-tenant, three-kind test drives one ticket at a time.
fn script_draft(fixture: &support::Fixture, drafted: serde_json::Value) {
    fixture.model.set_mode_for(
        ModelTier::Fast,
        TextModelMode::Complete(support::completion(ModelTier::Fast, drafted)),
    );
}

/// Issue #24's headline: one tenant, three kinds, three routes — each
/// ticket reaches its own tracker, and a route's priority map rides along.
#[pollster::test]
async fn each_kind_files_to_its_own_route() {
    let priority = BTreeMap::from([("error".to_owned(), "high".to_owned())]);
    let fixture = support::fixture_routed(
        |db| {
            support::seed_route(
                db,
                support::TENANT,
                Kind::Defect,
                &RouteTarget::Tracker(github()),
                support::CREDENTIAL_REF,
                &priority,
            );
            support::seed_route(
                db,
                support::TENANT,
                Kind::SupportCase,
                &RouteTarget::Tracker(zendesk()),
                support::CREDENTIAL_REF,
                &BTreeMap::new(),
            );
            support::seed_route(
                db,
                support::TENANT,
                Kind::Lead,
                &RouteTarget::Tracker(salesforce()),
                support::CREDENTIAL_REF,
                &BTreeMap::new(),
            );
        },
        support::scripted_model(support::drafted_json(), support::file_judgment()),
        FakeTracker::new(TrackerMode::FileOk),
    );
    let pipeline = fixture.pipeline();

    // Defect: the fixture's own handoff, drafted from the bug transcript.
    support::drain_all(&pipeline);
    let defect = support::ticket(&fixture);
    assert_eq!(defect.kind, Kind::Defect);
    assert_eq!(defect.status, Status::Filed, "the defect files");

    // Support case: a how-to that needs a person.
    script_draft(
        &fixture,
        support::drafted_json_kind(
            Kind::SupportCase,
            "How do I add a seat to my plan?",
            &json!({
                "summary": "Billing question about adding a seat",
                "customer_ask": "How do I add a seat to my plan?",
            }),
        ),
    );
    let support_id = support::add_ticket(
        &fixture,
        "conv-support",
        "customer: how do I add a seat to my plan?",
    );
    support::drain_all(&pipeline);

    // Lead: buying intent.
    script_draft(
        &fixture,
        support::drafted_json_kind(
            Kind::Lead,
            "Acme wants 50 seats",
            &json!({
                "company": "Acme",
                "seats": 50,
                "intent": "wants to buy the enterprise plan",
            }),
        ),
    );
    let lead_id = support::add_ticket(
        &fixture,
        "conv-lead",
        "customer: we are Acme and want to buy 50 seats",
    );
    support::drain_all(&pipeline);

    let support_ticket = support::ticket_by_id(&fixture.db, &support_id);
    assert_eq!(support_ticket.kind, Kind::SupportCase);
    assert_eq!(support_ticket.status, Status::Filed);
    let lead_ticket = support::ticket_by_id(&fixture.db, &lead_id);
    assert_eq!(lead_ticket.kind, Kind::Lead);
    assert_eq!(lead_ticket.status, Status::Filed);

    // Each ticket reached the destination its kind routes to, in order.
    let filed = fixture.tracker.filed();
    assert_eq!(filed.len(), 3, "one file per kind: {filed:?}");
    assert_eq!(filed[0].dest, github(), "the defect goes to GitHub");
    assert_eq!(filed[1].dest, zendesk(), "the support case goes to Zendesk");
    assert_eq!(filed[2].dest, salesforce(), "the lead goes to Salesforce");

    // The defect's route maps `error` to `high`, so its labels carry it.
    assert!(
        filed[0].draft.labels.contains(&"priority:high".to_owned()),
        "the priority map rides on the labels: {:?}",
        filed[0].draft.labels
    );
    assert!(filed[0].draft.labels.contains(&"bug".to_owned()));
    // A route with no priority map adds no priority label.
    assert!(
        !filed[1]
            .draft
            .labels
            .iter()
            .any(|l| l.starts_with("priority:")),
        "no priority map, no priority label: {:?}",
        filed[1].draft.labels
    );
    assert!(
        filed[1]
            .draft
            .labels
            .contains(&"kind:support_case".to_owned())
    );

    // The customer-facing update names the team the kind went to, not
    // always engineering: the notify stage records it (no mailer is wired).
    let notified = support::events_for(&fixture.db, &support_id)
        .into_iter()
        .find(|event| event.kind == EventKind::NotifySkipped)
        .expect("the support case reaches the notify stage");
    let message = notified
        .detail
        .as_ref()
        .and_then(|detail| detail["message"].as_str())
        .unwrap_or_default();
    assert!(
        message.contains("the support team"),
        "a support case is announced as the support team's: {message}"
    );
}

/// A kind with no route files into the built-in ticketing — `filed` with a
/// `local:<id>` reference and no tracker call — never a dead-letter.
#[pollster::test]
async fn a_kind_with_no_route_files_locally() {
    // Only the defect has a route; the ticket drafted below is a lead.
    let fixture = support::fixture_routed(
        |db| {
            support::seed_route(
                db,
                support::TENANT,
                Kind::Defect,
                &RouteTarget::Tracker(github()),
                support::CREDENTIAL_REF,
                &BTreeMap::new(),
            );
        },
        support::scripted_model(
            support::drafted_json_kind(
                Kind::Lead,
                "Acme wants 50 seats",
                &json!({
                    "company": "Acme",
                    "seats": 50,
                    "intent": "wants to buy the enterprise plan",
                }),
            ),
            support::file_judgment(),
        ),
        FakeTracker::new(TrackerMode::FileOk),
    );
    let pipeline = fixture.pipeline();
    support::drain_all(&pipeline);

    let ticket = support::ticket(&fixture);
    assert_eq!(ticket.kind, Kind::Lead);
    assert_eq!(
        ticket.status,
        Status::Filed,
        "an unrouted kind still files, as `{}`",
        ticket.status.as_str()
    );
    let expected_external_id = format!("local:{}", fixture.ticket_id);
    assert_eq!(
        ticket.external_id.as_deref(),
        Some(expected_external_id.as_str()),
        "the built-in reference marks where it landed"
    );
    assert_eq!(ticket.external_url, None, "there is no URL to invent");
    assert!(
        fixture.tracker.filed().is_empty(),
        "no route, no tracker call"
    );
    assert!(
        !support::events(&fixture)
            .iter()
            .any(|event| event.kind == EventKind::FileDeadLettered),
        "an unrouted kind is not a dead-letter"
    );
}

/// A judge that cannot confirm the kind (`kind_ok: false`) parks the
/// ticket for more information rather than filing it.
#[pollster::test]
async fn a_kind_ok_false_judgment_parks_for_info() {
    let fixture = support::fixture_routed(
        |db| {
            support::seed_route(
                db,
                support::TENANT,
                Kind::Defect,
                &RouteTarget::Tracker(github()),
                support::CREDENTIAL_REF,
                &BTreeMap::new(),
            );
        },
        support::scripted_model(
            support::drafted_json(),
            json!({
                "is_defect": true,
                "kind_ok": false,
                "reproducible": true,
                "severity_ok": true,
                "pii_clean": true,
                "verdict": "file",
                "reasons": ["the draft does not look like a complete defect"],
            }),
        ),
        FakeTracker::new(TrackerMode::FileOk),
    );
    let pipeline = fixture.pipeline();
    support::drain_all(&pipeline);

    let ticket = support::ticket(&fixture);
    assert_eq!(
        ticket.status,
        Status::NeedsInfo,
        "a kind the judge cannot confirm is parked, not filed"
    );
    assert!(
        fixture.tracker.filed().is_empty(),
        "the tracker is never touched"
    );
    assert!(
        support::events(&fixture)
            .iter()
            .any(|event| event.kind == EventKind::NeedsInfo),
        "the ticket carries a question"
    );
}

/// A draft missing a field its kind requires is parked at the draft stage,
/// deterministically — the judge is never even asked.
#[pollster::test]
async fn a_draft_missing_its_kinds_fields_parks_for_info() {
    // A support case with no `customer_ask`: the draft gate catches it.
    let fixture = support::fixture_routed(
        |db| {
            support::seed_route(
                db,
                support::TENANT,
                Kind::SupportCase,
                &RouteTarget::Tracker(zendesk()),
                support::CREDENTIAL_REF,
                &BTreeMap::new(),
            );
        },
        support::scripted_model(
            support::drafted_json_kind(
                Kind::SupportCase,
                "How do I add a seat?",
                &json!({ "summary": "Billing question" }),
            ),
            support::file_judgment(),
        ),
        FakeTracker::new(TrackerMode::FileOk),
    );
    let pipeline = fixture.pipeline();
    support::drain_all(&pipeline);

    let ticket = support::ticket(&fixture);
    assert_eq!(ticket.status, Status::NeedsInfo);
    assert!(fixture.tracker.filed().is_empty());
    let events = support::events(&fixture);
    assert!(
        events
            .iter()
            .any(|event| event.kind == EventKind::NeedsInfo),
        "the missing field is asked for"
    );
    assert!(
        !events
            .iter()
            .any(|event| event.kind == EventKind::JudgeCompleted),
        "the judge is never reached for an incomplete draft"
    );
}
