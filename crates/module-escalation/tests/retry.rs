//! Issue #4's failure boxes: a tracker `Transient` is retried under the
//! bounded [`RetryPolicy`] (the backoff asserted on the stored outbox
//! row, not hoped for); transient failures past the attempt budget
//! dead-letter; and a tracker `Rejected` is terminal on the first call.

mod support;

use std::time::Duration;

use cratefield_core::{Clock as _, Inbox};
use module_escalation::RetryPolicy;
use module_escalation::model::{EventKind, Status};
use module_escalation::ports::text_model::ModelTier;
use module_escalation::ports::tracker::TrackerError;
use module_escalation::testing::{FakeTextModel, FakeTracker};
use time::format_description::well_known::Rfc3339;

/// Box 4a: a tracker `Transient` is retried once, exactly one policy step
/// out (read off the outbox row), and then the ticket files.
#[pollster::test]
async fn a_transient_tracker_failure_is_retried_at_the_policy_backoff_and_then_files() {
    let fixture = support::fixture(
        FakeTextModel::scripted(vec![
            Ok(support::completion(
                ModelTier::Fast,
                support::drafted_json(),
            )),
            Ok(support::completion(
                ModelTier::Strong,
                support::file_judgment(),
            )),
        ]),
        FakeTracker::scripted(vec![Err(support::transient()), Ok(support::filed())]),
    );
    // A non-default base, so the assertion below proves the policy the
    // test installed is the one scheduling the retry.
    let pipeline = fixture.pipeline_with_policy(RetryPolicy::new().base(Duration::from_secs(45)));

    // Up to and including the file stage's first, failing attempt.
    support::drain_all(&pipeline);

    let events = support::events(&fixture);
    let retry = support::event_of_kind(&events, EventKind::FileRetryScheduled);
    let detail = retry
        .detail
        .as_ref()
        .expect("the retry event carries its reason");
    assert_eq!(detail["attempt"], serde_json::json!(1));
    assert!(
        detail["reason"]
            .as_str()
            .expect("the reason is text")
            .contains("retryable"),
        "the transient failure is what the audit names: {detail}"
    );

    let ticket = support::ticket(&fixture);
    assert_ne!(
        ticket.status,
        Status::Filed,
        "the failed attempt did not file (status `{}`)",
        ticket.status.as_str()
    );
    assert_eq!(ticket.external_id, None);

    // The row comes back with one attempt spent, due exactly one base step
    // in the future — bounded backoff asserted on the stored row itself.
    let (attempts, next_at) =
        support::outbox_row(&fixture, "file").expect("the file row is still queued");
    assert_eq!(attempts, 1, "one failed attempt recorded on the row");
    let now = fixture.clock.now();
    let next = time::OffsetDateTime::parse(&next_at, &Rfc3339).expect("RFC 3339 next_attempt_at");
    let delta = next - now;
    assert!(
        delta > time::Duration::ZERO,
        "the retry moved into the future: now={now} next={next_at}"
    );
    assert_eq!(
        delta.whole_seconds(),
        45,
        "exactly one base step out: now={now} next={next_at}"
    );

    // The failed attempt released its inbox claim in the retry batch, so
    // the retry is free to re-run (claiming a free key answers true).
    let claim = pollster::block_on(Inbox::new(module_escalation::Pipeline::INBOX_TABLE).claim(
        &*fixture.db,
        &format!("{}:file", fixture.ticket_id),
        &support::format_at(fixture.clock.now()),
    ))
    .expect("claim read");
    assert!(claim, "the failed attempt released the ticket:file claim");

    // Past the retry boundary, the next drain files — the second call.
    fixture.clock.advance_seconds(46);
    support::drain_all(&pipeline);

    assert_eq!(
        fixture.tracker.filed().len(),
        2,
        "one failed attempt plus one success: {:?}",
        fixture.tracker.filed()
    );
    assert_eq!(support::ticket(&fixture).status, Status::Filed);
    assert_eq!(
        support::ticket(&fixture).external_id.as_deref(),
        Some("acme/api#7")
    );
}

/// Box 4b: transient failures past `max_attempts` stop — the ticket
/// dead-letters, the row completes, and `retry_later` is never used again.
#[pollster::test]
async fn transient_failures_past_the_attempt_budget_dead_letter() {
    let fixture = support::fixture(
        FakeTextModel::scripted(vec![
            Ok(support::completion(
                ModelTier::Fast,
                support::drafted_json(),
            )),
            Ok(support::completion(
                ModelTier::Strong,
                support::file_judgment(),
            )),
        ]),
        FakeTracker::scripted(vec![
            Err(support::transient()),
            Err(support::transient()),
            Err(support::transient()),
        ]),
    );
    let pipeline = fixture.pipeline_with_policy(
        RetryPolicy::new()
            .base(Duration::from_secs(45))
            .max_attempts(2),
    );

    // First attempt: inside the budget, rescheduled.
    support::drain_all(&pipeline);
    assert_eq!(
        support::outbox_row(&fixture, "file")
            .expect("still queued")
            .0,
        1,
        "one failed attempt so far"
    );
    assert_ne!(
        support::ticket(&fixture).status,
        Status::DeadLetter,
        "the first failure stays within the budget"
    );

    // Second attempt: the budget is spent; the row completes instead of
    // being rescheduled again.
    fixture.clock.advance_seconds(60);
    support::drain_all(&pipeline);

    let ticket = support::ticket(&fixture);
    assert_eq!(
        ticket.status,
        Status::DeadLetter,
        "not `{}`",
        ticket.status.as_str()
    );
    assert_eq!(
        fixture.tracker.filed().len(),
        2,
        "both attempts reached the tracker: {:?}",
        fixture.tracker.filed()
    );

    let dead = {
        let events = support::events(&fixture);
        support::event_of_kind(&events, EventKind::FileDeadLettered).clone()
    };
    let detail = dead
        .detail
        .as_ref()
        .expect("the dead letter carries its reason");
    assert_eq!(detail["outcome"], serde_json::json!("dead_letter"));
    assert_eq!(detail["attempt"], serde_json::json!(2));
    assert!(
        detail["reason"]
            .as_str()
            .expect("the reason is text")
            .contains("retryable"),
        "the spent budget's reason is the transient failure: {detail}"
    );

    // Terminal: nothing left in the outbox, and it stays that way however
    // long the clock runs.
    assert_eq!(
        support::outbox_count(&fixture),
        0,
        "a dead-lettered stage is never retry_later-ed"
    );
    fixture.clock.advance_seconds(10_000);
    support::drain_all(&pipeline);
    assert_eq!(support::outbox_count(&fixture), 0);
    assert_eq!(support::ticket(&fixture).status, Status::DeadLetter);
    assert_eq!(
        fixture.tracker.filed().len(),
        2,
        "nothing re-files a dead ticket"
    );
}

/// Box 4c: a tracker `Rejected` is terminal — one call, dead-letter at
/// once, with the rejection reason carried into the audit row.
#[pollster::test]
async fn a_rejected_tracker_answer_dead_letters_immediately_with_its_reason() {
    let fixture = support::fixture(
        FakeTextModel::scripted(vec![
            Ok(support::completion(
                ModelTier::Fast,
                support::drafted_json(),
            )),
            Ok(support::completion(
                ModelTier::Strong,
                support::file_judgment(),
            )),
        ]),
        FakeTracker::scripted(vec![Err(TrackerError::Rejected(
            "draft title exceeds the tracker limit".to_owned(),
        ))]),
    );
    let pipeline = fixture.pipeline();
    support::drain_all(&pipeline);

    assert_eq!(
        fixture.tracker.filed().len(),
        1,
        "one call, no retry: {:?}",
        fixture.tracker.filed()
    );
    assert_eq!(
        support::ticket(&fixture).status,
        Status::DeadLetter,
        "a rejection dead-letters on its first attempt"
    );

    let dead = {
        let events = support::events(&fixture);
        support::event_of_kind(&events, EventKind::FileDeadLettered).clone()
    };
    let detail = dead
        .detail
        .as_ref()
        .expect("the dead letter carries its reason");
    assert_eq!(detail["outcome"], serde_json::json!("dead_letter"));
    assert_eq!(detail["attempt"], serde_json::json!(1));
    let reason = detail["reason"].as_str().expect("the reason is text");
    assert!(reason.contains("tracker rejected the request"), "{reason}");
    assert!(
        reason.contains("exceeds the tracker limit"),
        "the tracker's own words reach the audit: {reason}"
    );

    assert_eq!(
        support::outbox_count(&fixture),
        0,
        "a rejection is never rescheduled"
    );
}
