//! Issue #4's box 5: draining twice files once. A second drain after
//! success sees nothing, so the real proof is the at-least-once case: the
//! same `file`-stage row re-inserted by hand is retired without a second
//! tracker call. The companion test is the crash window — a held claim
//! whose stage never committed re-runs instead of dropping the work.

mod support;

use module_escalation::model::{EventKind, Stage, Status};
use module_escalation::ports::text_model::ModelTier;
use module_escalation::testing::{FakeTextModel, FakeTracker};

/// Drafted, judged `file`, tracker accepts once.
fn happy() -> support::Fixture {
    support::fixture(
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
        FakeTracker::accepting(support::filed()),
    )
}

fn filed_event_count(fixture: &support::Fixture) -> usize {
    support::events(fixture)
        .iter()
        .filter(|event| event.kind == EventKind::Filed)
        .count()
}

/// Box 5: the stage already committed, and the same `file` row arrives
/// again (at-least-once delivery) — no second tracker file, no duplicate
/// audit row, and the redelivered row is completed.
#[pollster::test]
async fn a_redelivered_file_row_files_once_and_audits_once() {
    let fixture = happy();
    let pipeline = fixture.pipeline();
    support::drain_all(&pipeline);

    assert_eq!(support::ticket(&fixture).status, Status::Filed);
    assert_eq!(
        fixture.tracker.filed().len(),
        1,
        "the first delivery filed once"
    );

    // Re-deliver the same stage's work: fresh job id, same ticket, same
    // tenant — exactly the row a redelivery would replay.
    support::requeue_file_stage(&fixture);
    assert_eq!(
        support::outbox_count(&fixture),
        1,
        "the redelivered row is queued"
    );

    support::drain_all(&pipeline);

    assert_eq!(
        fixture.tracker.filed().len(),
        1,
        "the Inbox retires finished work: {:?}",
        fixture.tracker.filed()
    );
    assert_eq!(support::ticket(&fixture).status, Status::Filed);
    assert_eq!(
        support::outbox_count(&fixture),
        0,
        "the redelivered row was completed, not retried"
    );
    assert_eq!(
        filed_event_count(&fixture),
        1,
        "no duplicate `filed` audit row"
    );
}

/// Box 5's crash window: the `ticket:file` claim is held but the stage
/// never committed — the drain re-runs the stage on the held claim
/// instead of dropping the work forever.
#[pollster::test]
async fn a_held_claim_with_uncommitted_work_re_runs_the_stage() {
    let fixture = happy();

    // A previous attempt claimed the key and died before its batch: the
    // key is held, and the ticket has not advanced past the file stage.
    let held = support::hold_inbox_key(&fixture, Stage::File);
    assert!(held, "the test takes the claim fresh");

    let pipeline = fixture.pipeline();
    support::drain_all(&pipeline);

    let ticket = support::ticket(&fixture);
    assert_eq!(
        ticket.status,
        Status::Filed,
        "the stage re-ran on the held claim, not `{}`",
        ticket.status.as_str()
    );
    assert_eq!(
        fixture.tracker.filed().len(),
        1,
        "exactly one file: {:?}",
        fixture.tracker.filed()
    );
    assert_eq!(
        support::outbox_count(&fixture),
        0,
        "the row completed once the re-run committed"
    );
    assert_eq!(
        filed_event_count(&fixture),
        1,
        "the re-run's single commit audited once"
    );
}
