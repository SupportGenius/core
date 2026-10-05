//! Issue #36's escalation side: [`ticket_counts_for_day`], the one query
//! the support analytics rollup reads escalation's tickets through. The
//! rollup itself lives in `module-support`; what matters here is the
//! mapping from stored event kinds to the five counts, and the join that
//! recovers a ticket's tenant (the events table has no `tenant_id`).

mod support;

use cratefield_core::{Database, Statement};
use cratefield_testing::{FakeTracker, TrackerMode};
use module_escalation::model::EventKind;
use module_escalation::store::{DayCounts, ticket_counts_for_day};
use time::format_description::well_known::Rfc3339;

/// The UTC `YYYY-MM-DD` the fixture's clock sits on.
fn day_of(unix: i64) -> String {
    time::OffsetDateTime::from_unix_timestamp(unix)
        .expect("a real unix timestamp")
        .format(&Rfc3339)
        .expect("RFC 3339 renders")
        .get(..10)
        .expect("a date part")
        .to_owned()
}

/// A finished happy path files one ticket, and the day's count says so —
/// for the tenant the ticket belongs to, which the query has to recover
/// from `sg_tickets` because `sg_ticket_events` does not hold it.
#[pollster::test]
async fn a_filed_ticket_counts_as_filed_for_its_tenant_and_day() {
    let fixture = support::fixture(
        support::happy_model(),
        FakeTracker::new(TrackerMode::FileOk),
    );
    support::drain_all(&fixture.pipeline());

    let day = day_of(support::EPOCH);
    let counts = ticket_counts_for_day(fixture.db.as_ref(), &day)
        .await
        .expect("counts");
    assert_eq!(
        counts,
        vec![DayCounts {
            tenant_id: support::TENANT.to_owned(),
            filed: 1,
            rejected: 0,
            needs_info: 0,
            duplicates: 0,
            dead_lettered: 0,
        }]
    );

    // A day with no events yields no tenant at all — absent, not a row of
    // zeros the rollup would then have to tell apart from a quiet day.
    assert!(
        ticket_counts_for_day(fixture.db.as_ref(), "1999-01-01")
            .await
            .expect("counts")
            .is_empty()
    );
}

/// The decision worth pinning: `duplicates` counts `linked` — a duplicate
/// the pipeline recognised and linked to an existing ticket — and not
/// `duplicate_ignored`, which records the opposite outcome (a
/// `duplicate_of` the judge named but was never shown, so nothing was
/// linked). Counting the latter would report duplicates the pipeline
/// refused to act on.
#[pollster::test]
async fn duplicate_ignored_is_not_a_duplicate() {
    let fixture = support::fixture(
        support::happy_model(),
        FakeTracker::new(TrackerMode::FileOk),
    );
    support::drain_all(&fixture.pipeline());
    let day = day_of(support::EPOCH);

    let event = |id: &str, kind: EventKind| {
        Statement::new(format!(
            "INSERT INTO sg_ticket_events (id, ticket_id, seq, at, stage, kind, detail) \
             VALUES ('{id}', '{}', 900, '{day}T00:00:00+00:00', 'judge', '{}', NULL)",
            fixture.ticket_id,
            kind.as_str(),
        ))
    };

    fixture
        .db
        .execute(&event("ev-ignored", EventKind::DuplicateIgnored))
        .await
        .expect("the ignored-duplicate event lands");
    let ignored = ticket_counts_for_day(fixture.db.as_ref(), &day)
        .await
        .expect("counts");
    assert_eq!(
        ignored[0].duplicates, 0,
        "an ignored name is not a duplicate"
    );
    assert_eq!(ignored[0].filed, 1, "and it did not land in another column");

    fixture
        .db
        .execute(&event("ev-linked", EventKind::Linked))
        .await
        .expect("the linked event lands");
    let linked = ticket_counts_for_day(fixture.db.as_ref(), &day)
        .await
        .expect("counts");
    assert_eq!(linked[0].duplicates, 1, "a linked duplicate is one");
}
