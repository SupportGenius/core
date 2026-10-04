//! The follow stage (issue #26): a filed ticket is polled until it closes,
//! and the customer is mailed once per tracker transition. Unlike the other
//! stages the follow row reschedules itself and runs again, so these tests
//! advance the clock past each poll's due time and read the state change
//! back off the audit trail.

mod support;

use std::sync::Arc;

use cratefield_core::{Clock as _, TicketState};
use cratefield_testing::{FakeMailer, FakeTracker, MailerMode, TrackerMode};
use module_escalation::Pipeline;
use module_escalation::model::{EventKind, Status};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// A filed ticket whose tracker reports `open`: filing leaves one `follow`
/// row due fifteen minutes later.
fn filed() -> support::Fixture {
    let fixture = support::fixture(
        support::happy_model(),
        FakeTracker::new(TrackerMode::FileOk),
    );
    support::drain_all(&fixture.pipeline());
    assert_eq!(support::ticket(&fixture).status, Status::Filed);
    fixture
}

/// [`filed`] with a `Mailer` wired and a contact seeded, so every
/// notification is a real send. Returns the mailer for `sent()` assertions.
fn filed_mailing() -> (support::Fixture, Arc<FakeMailer>) {
    use cratefield_core::Database as _;

    let mut fixture = support::fixture(
        support::happy_model(),
        FakeTracker::new(TrackerMode::FileOk),
    );
    let mailer = Arc::new(FakeMailer::new(MailerMode::SendOk));
    fixture.mailer = Some(mailer.clone());
    let contact = module_escalation::store::upsert_contact_stmt(
        support::TENANT,
        support::CONVERSATION,
        "jane@acme.test",
        &support::format_at(fixture.clock.now()),
    );
    pollster::block_on(fixture.db.batch_atomic(&[contact])).expect("contact seeds");
    support::drain_all(&fixture.pipeline());
    assert_eq!(support::ticket(&fixture).status, Status::Filed);
    (fixture, mailer)
}

/// The follow row's `next_attempt_at`, as an instant.
fn follow_due(fixture: &support::Fixture) -> OffsetDateTime {
    let (_, next_at) = support::outbox_row(fixture, "follow").expect("the follow row is queued");
    OffsetDateTime::parse(&next_at, &Rfc3339).expect("RFC 3339 next_attempt_at")
}

/// The issue's "Done when", end to end: filing mails the customer once, each
/// tracker transition mails exactly once more, a redrain sends nothing, the
/// poll stops at `Closed`, and a settled ticket is not polled again.
#[pollster::test]
async fn a_filed_ticket_is_mailed_once_per_transition_and_stops_at_closed() {
    let (fixture, mailer) = filed_mailing();
    let pipeline = fixture.pipeline();

    // Filing told the customer once.
    assert_eq!(mailer.sent().len(), 1, "the filing notice");

    // A tracker transition: the poll falls due and mails again.
    fixture.tracker.set_state(TicketState::InProgress);
    fixture.clock.advance_seconds(900);
    support::drain_all(&pipeline);
    assert_eq!(mailer.sent().len(), 2, "the in-progress update");

    // Draining again with the clock unmoved reclaims nothing: no second
    // poll, no second mail.
    support::drain_all(&pipeline);
    assert_eq!(mailer.sent().len(), 2, "a redrain sends nothing");
    assert_eq!(
        fixture.tracker.statused().len(),
        1,
        "one status call so far"
    );

    // `Closed` is the last word.
    fixture.tracker.set_state(TicketState::Closed);
    fixture.clock.advance_seconds(3600);
    support::drain_all(&pipeline);
    assert_eq!(mailer.sent().len(), 3, "the closed update");

    let events = support::events(&fixture);
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::Notified)
            .count(),
        3,
        "one send per transition"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::StatusChanged)
            .count(),
        2,
        "two state changes"
    );
    assert_eq!(
        support::outbox_count_of(&fixture, "follow"),
        0,
        "a closed ticket needs no further polling"
    );

    // A day later there is nothing left to poll.
    let polls = fixture.tracker.statused().len();
    fixture.clock.advance_seconds(86_400);
    support::drain_all(&pipeline);
    assert_eq!(fixture.tracker.statused().len(), polls, "no further polls");
}

/// The poll's own age sets the wait — hourly for the first day, daily after —
/// and a state the port cannot name is nothing to report.
#[pollster::test]
async fn the_poll_backs_off_from_hourly_to_daily() {
    let fixture = filed();
    let pipeline = fixture.pipeline();
    let before = support::events(&fixture).len();

    // An unnamed state writes nothing and reschedules an hour out.
    fixture.tracker.set_state(TicketState::Unknown);
    fixture.clock.advance_seconds(900);
    support::drain_all(&pipeline);
    assert_eq!(
        support::events(&fixture).len(),
        before,
        "no transition, no audit row"
    );
    assert_eq!(
        (follow_due(&fixture) - fixture.clock.now()).whole_seconds(),
        3600,
        "the short interval, one hour out"
    );

    // A day on, the poll slows to a daily check.
    fixture.clock.advance_seconds(25 * 3600);
    support::drain_all(&pipeline);
    assert_eq!(
        (follow_due(&fixture) - fixture.clock.now()).whole_seconds(),
        86_400,
        "the long interval, one day out"
    );
}

/// A concurrent poll that recorded the same transition holds its claim key,
/// so this run's batch — the claim and the writes together — conflicts and
/// rolls back: it writes nothing, reschedules, and never dead-letters.
#[pollster::test]
async fn a_lost_claim_writes_nothing_and_reschedules() {
    use cratefield_core::Inbox;

    let fixture = filed();
    let pipeline = fixture.pipeline();
    let before = support::events(&fixture).len();

    // A concurrent drain that won this transition holds `<ticket>:follow:0`.
    let held = pollster::block_on(Inbox::new(Pipeline::INBOX_TABLE).claim(
        &*fixture.db,
        &format!("{}:follow:0", fixture.ticket_id),
        &support::format_at(fixture.clock.now()),
    ))
    .expect("the claim reads");
    assert!(held, "the key starts free");

    fixture.tracker.set_state(TicketState::InProgress);
    fixture.clock.advance_seconds(900);
    support::drain_all(&pipeline);

    assert_eq!(
        support::events(&fixture).len(),
        before,
        "a lost claim writes no audit row"
    );
    assert_eq!(
        support::outbox_count_of(&fixture, "notify"),
        0,
        "and enqueues no status update"
    );
    assert_eq!(
        support::outbox_count_of(&fixture, "follow"),
        1,
        "the poll row comes back"
    );
}

/// A tracker failure reschedules the poll and never dead-letters: a ticket
/// that cannot be polled is not a ticket that needs a human.
#[pollster::test]
async fn a_tracker_failure_reschedules_and_never_dead_letters() {
    let fixture = filed();
    let pipeline = fixture.pipeline();

    fixture.tracker.set_mode(TrackerMode::Transient);
    fixture.clock.advance_seconds(900);
    support::drain_all(&pipeline);

    let ticket = support::ticket(&fixture);
    assert_eq!(
        ticket.status,
        Status::Filed,
        "a failed poll leaves the ticket filed, not dead-lettered"
    );
    assert_eq!(
        support::outbox_count_of(&fixture, "follow"),
        1,
        "the poll row comes back"
    );
    assert!(
        support::events(&fixture)
            .iter()
            .all(|event| event.kind != EventKind::FollowFailed
                && event.kind != EventKind::FileDeadLettered),
        "no dead-letter and no fail event was written"
    );
}
