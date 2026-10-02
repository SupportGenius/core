//! Issue #27: duplicate detection. The judge is shown the tenant's
//! already-filed tickets that might match the draft and links to one
//! instead of filing a second time; a `duplicate_of` it was not shown is
//! ignored and the report files rather than being lost; candidates are
//! scoped to the tenant.

mod support;

use std::sync::Arc;

use cratefield_adapter_sqlite::SqliteDatabase;
use cratefield_core::{ModelTier, UlidIdGen};
use cratefield_testing::{FakeTextModel, FakeTracker, TextModelMode, TrackerMode};
use module_escalation::Pipeline;
use module_escalation::model::{EventKind, Status, Verdict};
use module_escalation::testing::{FakeConfig, SettableClock};
use serde_json::json;

/// A world for the multi-conversation duplicate tests: the migrated
/// database, a clock and scriptable fakes. The single-ticket `Fixture`
/// cannot hold two escalations, so these tests assemble the pieces.
struct World {
    db: Arc<SqliteDatabase>,
    clock: Arc<SettableClock>,
    model: FakeTextModel,
    tracker: FakeTracker,
}

fn world(model: FakeTextModel, tracker: FakeTracker) -> World {
    World {
        db: support::migrated_db(),
        clock: Arc::new(SettableClock::at_unix(support::EPOCH)),
        model,
        tracker,
    }
}

/// A pipeline over `world`, with no mailer and no defer — the same shape
/// the support fixture builds, minus the defer port (drains drive every
/// stage here).
fn pipeline(world: &World) -> Pipeline {
    Pipeline::new(
        world.db.clone(),
        Arc::new(world.model.clone()),
        Arc::new(world.tracker.clone()),
        None,
        Arc::new(FakeConfig::new().with(support::CREDENTIAL_REF, support::CREDENTIAL_SECRET)),
        world.clock.clone(),
        Arc::new(UlidIdGen),
        None,
    )
}

/// Re-scripts the strong tier, so the second conversation's judge can
/// name the first ticket's id — which only exists at runtime.
fn script_judge(model: &FakeTextModel, judgment: serde_json::Value) {
    model.set_mode_for(
        ModelTier::Strong,
        TextModelMode::Complete(support::completion(ModelTier::Strong, judgment)),
    );
}

/// The brief of the last judge prompt the model was asked.
fn last_judge_brief(model: &FakeTextModel) -> String {
    model
        .prompts()
        .into_iter()
        .rev()
        .find(|prompt| prompt.tier == ModelTier::Strong)
        .expect("a judge prompt was asked")
        .messages
        .last()
        .expect("the brief is the last message")
        .content
        .clone()
}

/// A second report of the same defect is linked to the first filed
/// ticket, not filed again: one tracker file, a `sg_ticket_links` row, a
/// bumped `match_count`, a `Duplicate` status, and a judge brief that
/// named the first ticket.
#[pollster::test]
async fn a_second_report_of_the_same_defect_links_instead_of_filing() {
    let world = world(
        support::scripted_model(support::drafted_json(), support::file_judgment()),
        FakeTracker::new(TrackerMode::FileOk),
    );
    support::seed_destination(&world.db, &support::github_destination());
    let pipeline = pipeline(&world);

    let first = support::commit_handoff(
        &world.db,
        &world.clock,
        support::TENANT,
        "conv-first",
        support::TRANSCRIPT,
    );
    support::drain_all(&pipeline);
    let first_ticket = support::ticket_by_id(&world.db, &first);
    assert_eq!(first_ticket.status, Status::Filed);
    assert_eq!(world.tracker.filed().len(), 1);

    // The second conversation duplicates the first, so its judge names
    // the first ticket's id — now known.
    script_judge(&world.model, support::duplicate_judgment(&first));
    let second = support::commit_handoff(
        &world.db,
        &world.clock,
        support::TENANT,
        "conv-second",
        support::TRANSCRIPT,
    );
    support::drain_all(&pipeline);

    // One escalation, one tracker file: the duplicate linked instead.
    assert_eq!(
        world.tracker.filed().len(),
        1,
        "a duplicate is never filed: {:?}",
        world.tracker.filed()
    );

    let second_ticket = support::ticket_by_id(&world.db, &second);
    assert_eq!(
        second_ticket.status,
        Status::Duplicate,
        "not `{}`",
        second_ticket.status.as_str()
    );
    assert_eq!(second_ticket.verdict, Some(Verdict::Duplicate));
    // The duplicate carries the existing ticket's tracker reference, so
    // the notify stage can name and link it.
    assert_eq!(second_ticket.external_id, first_ticket.external_id);
    assert_eq!(second_ticket.external_url, first_ticket.external_url);

    let events = support::events_for(&world.db, &second);
    let linked = support::event_of_kind(&events, EventKind::Linked);
    assert_eq!(
        linked
            .detail
            .as_ref()
            .expect("a linked event carries its target")["duplicate_of"],
        json!(first)
    );

    // The existing ticket counted the match.
    assert_eq!(support::ticket_by_id(&world.db, &first).match_count, 1);

    // And the link row ties the second conversation to the first ticket.
    assert_eq!(
        support::ticket_links(&world.db),
        vec![(first.clone(), "conv-second".to_owned(), second.clone())]
    );

    // The second judge was shown the first ticket as a candidate, in the
    // brief's `[id] title` shape, and the `judge_completed` row records
    // the same ids — the set `duplicate_of` is validated against.
    let brief = last_judge_brief(&world.model);
    assert!(
        brief.contains(&format!("[{first}] {}", support::DRAFT_TITLE)),
        "the judge brief listed the candidate: {brief}"
    );
    let second_events = support::events_for(&world.db, &second);
    let judge_completed = support::event_of_kind(&second_events, EventKind::JudgeCompleted);
    assert_eq!(
        judge_completed
            .detail
            .as_ref()
            .expect("a judge row records its candidates")["candidates"],
        json!([first])
    );

    // A third report names an id it was never shown *while the first
    // ticket is on offer*: the name is ignored and recorded, and the
    // report files rather than being lost.
    script_judge(
        &world.model,
        support::duplicate_judgment("01JNOTSHOWNATALL"),
    );
    let third = support::commit_handoff(
        &world.db,
        &world.clock,
        support::TENANT,
        "conv-third",
        support::TRANSCRIPT,
    );
    support::drain_all(&pipeline);

    assert_eq!(
        world.tracker.filed().len(),
        2,
        "the ignored duplicate filed: {:?}",
        world.tracker.filed()
    );
    assert_eq!(
        support::ticket_by_id(&world.db, &third).status,
        Status::Filed
    );
    let third_events = support::events_for(&world.db, &third);
    let ignored = support::event_of_kind(&third_events, EventKind::DuplicateIgnored);
    assert_eq!(
        ignored.detail.as_ref().expect("detail")["duplicate_of"],
        json!("01JNOTSHOWNATALL")
    );
    assert_eq!(
        ignored.detail.as_ref().expect("detail")["candidates"],
        json!([first]),
        "the ignored row shows what actually was on offer"
    );
}

/// A `duplicate_of` naming an id the judge was never shown is ignored —
/// recorded as a `duplicate_ignored` event — and the report files, since
/// filing is safer than losing a defect on a bad id.
#[pollster::test]
async fn a_duplicate_of_that_was_not_shown_is_ignored_and_the_ticket_files() {
    let world = world(
        support::scripted_model(
            support::drafted_json(),
            support::duplicate_judgment("01JNOTSHOWNATALL"),
        ),
        FakeTracker::new(TrackerMode::FileOk),
    );
    support::seed_destination(&world.db, &support::github_destination());
    let pipeline = pipeline(&world);

    let ticket_id = support::commit_handoff(
        &world.db,
        &world.clock,
        support::TENANT,
        "conv-only",
        support::TRANSCRIPT,
    );
    support::drain_all(&pipeline);

    assert_eq!(world.tracker.filed().len(), 1, "the report still files");
    assert_eq!(
        support::ticket_by_id(&world.db, &ticket_id).status,
        Status::Filed
    );

    let events = support::events_for(&world.db, &ticket_id);
    let ignored = support::event_of_kind(&events, EventKind::DuplicateIgnored);
    let detail = ignored
        .detail
        .as_ref()
        .expect("an ignored event records what was rejected");
    assert_eq!(detail["duplicate_of"], json!("01JNOTSHOWNATALL"));
    assert_eq!(
        detail["candidates"],
        json!([]),
        "no candidate was on offer, so none was shown"
    );
}

/// Candidates are scoped to the tenant: a filed ticket in one tenant is
/// not a candidate for another tenant's conversation, so its id is not
/// shown and a judge naming it is ignored — the second tenant files.
#[pollster::test]
async fn candidates_are_scoped_to_the_tenant() {
    let world = world(
        support::scripted_model(support::drafted_json(), support::file_judgment()),
        FakeTracker::new(TrackerMode::FileOk),
    );
    support::seed_destination_for(&world.db, "tenant-a", &support::github_destination());
    support::seed_destination_for(&world.db, "tenant-b", &support::github_destination());
    let pipeline = pipeline(&world);

    let a = support::commit_handoff(
        &world.db,
        &world.clock,
        "tenant-a",
        "conv-a",
        support::TRANSCRIPT,
    );
    support::drain_all(&pipeline);
    assert_eq!(support::ticket_by_id(&world.db, &a).status, Status::Filed);

    // Tenant B's judge points at tenant A's ticket — which B was never
    // shown, because candidate lookup is tenant-scoped.
    script_judge(&world.model, support::duplicate_judgment(&a));
    let b = support::commit_handoff(
        &world.db,
        &world.clock,
        "tenant-b",
        "conv-b",
        support::TRANSCRIPT,
    );
    support::drain_all(&pipeline);

    assert_eq!(
        world.tracker.filed().len(),
        2,
        "both tenants filed: no cross-tenant link"
    );
    assert_eq!(support::ticket_by_id(&world.db, &b).status, Status::Filed);

    let events = support::events_for(&world.db, &b);
    let ignored = support::event_of_kind(&events, EventKind::DuplicateIgnored);
    assert_eq!(
        ignored.detail.as_ref().expect("detail")["duplicate_of"],
        json!(a)
    );

    // And A's id never reached B's judge brief.
    let brief = last_judge_brief(&world.model);
    assert!(
        !brief.contains(&a),
        "another tenant's ticket is not a candidate: {brief}"
    );
}
