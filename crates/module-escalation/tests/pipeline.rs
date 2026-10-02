//! Issue #4's "Done when" boxes for the pipeline's product behaviour: a
//! handoff becomes a filed ticket with an external id; the judge's
//! `reject` and `needs_info` verdicts keep the tracker out of it; the
//! notify stage records its v0 skip with an explicit reason; and
//! `sg_ticket_events` reads back as a plain audit of every stage,
//! including the judge's reasons.

mod support;

use std::sync::Arc;

use cratefield_core::{Destination, ModelTier, Prompt, Severity};
use cratefield_testing::{FakeMailer, FakeTracker, MailerMode, TrackerMode};
use module_escalation::model::{Drafted, EventKind, Judgment, Status, Verdict};
use support::Fixture;

/// The scripted fakes of the happy path: a fast-tier draft, a strong-tier
/// `file` verdict, then a tracker that accepts.
fn happy() -> Fixture {
    support::fixture(
        support::happy_model(),
        FakeTracker::new(TrackerMode::FileOk),
    )
}

/// Box 1: with `FakeTextModel`, `FakeTracker` and `FakeDefer::drain()`, a
/// handoff becomes a filed ticket with an external id.
#[pollster::test]
async fn a_handoff_becomes_a_filed_ticket_with_an_external_id() {
    let fixture = happy();
    let pipeline = fixture.pipeline();

    // The first sweep runs the one row the handoff enqueued (the draft);
    // every later stage is reached through the deferred self-drain, the
    // way production advances without waiting for cron.
    let processed = pipeline.drain(10).await.expect("first sweep");
    assert_eq!(processed, 1, "the handoff left exactly one row: the draft");
    fixture.defer.drain().await;

    let ticket = support::ticket(&fixture);
    assert_eq!(
        ticket.status,
        Status::Filed,
        "the ticket filed, not `{}`",
        ticket.status.as_str()
    );
    assert_eq!(
        ticket.external_id.as_deref(),
        Some(support::FILED_EXTERNAL_ID)
    );
    assert_eq!(
        ticket.external_url.as_deref(),
        Some(support::FILED_EXTERNAL_URL)
    );

    // Exactly one file, under the key derived from the ticket id, with the
    // draft's content and the escalation labels.
    let filed = fixture.tracker.filed();
    assert_eq!(
        filed.len(),
        1,
        "one escalation, one tracker file: {filed:?}"
    );
    let call = &filed[0];
    assert_eq!(
        call.draft.idempotency_key,
        format!("escalation:{}", fixture.ticket_id)
    );
    assert_eq!(call.draft.title, support::DRAFT_TITLE);
    assert_eq!(call.draft.severity, Severity::Error);
    assert_eq!(
        call.draft.labels,
        ["escalated".to_owned(), "error".to_owned()]
    );
    assert_eq!(call.draft.environment.as_deref(), Some("production"));
    assert_eq!(
        call.dest,
        Destination::GitHub {
            owner: "acme".to_owned(),
            repo: "api".to_owned(),
        }
    );

    // The right tier asked at each stage, each carrying its own schema,
    // and the draft drafted from the transcript itself.
    let prompts: Vec<Prompt> = fixture.model.prompts();
    assert_eq!(prompts.len(), 2);
    assert_eq!(prompts[0].tier, ModelTier::Fast);
    assert_eq!(prompts[1].tier, ModelTier::Strong);
    assert_eq!(
        prompts[0].json_schema.as_ref(),
        Some(&Drafted::json_schema())
    );
    assert_eq!(
        prompts[1].json_schema.as_ref(),
        Some(&Judgment::json_schema())
    );
    assert_eq!(prompts[0].messages.len(), 1);
    assert_eq!(prompts[0].messages[0].content, support::TRANSCRIPT);
}

/// Box 2: `verdict: reject` never calls the tracker, and the `rejected`
/// event records why — the judge's reasons and every flag it set.
#[pollster::test]
async fn a_reject_verdict_never_calls_the_tracker_and_records_why() {
    let fixture = support::fixture(
        support::scripted_model(support::drafted_json(), support::reject_judgment()),
        // An accepting tracker: if the pipeline wrongly reached it, the
        // file would succeed and the status assert below would catch it.
        FakeTracker::new(TrackerMode::FileOk),
    );
    let pipeline = fixture.pipeline();
    support::drain_all(&pipeline);

    assert!(
        fixture.tracker.filed().is_empty(),
        "a rejected ticket is never filed: {:?}",
        fixture.tracker.filed()
    );

    let ticket = support::ticket(&fixture);
    assert_eq!(
        ticket.status,
        Status::Rejected,
        "not `{}`",
        ticket.status.as_str()
    );
    assert_eq!(ticket.verdict, Some(Verdict::Reject));
    assert_eq!(ticket.external_id, None);

    let events = support::events(&fixture);
    let rejected = support::event_of_kind(&events, EventKind::Rejected);
    let detail = rejected
        .detail
        .as_ref()
        .expect("a rejected event carries its detail");
    assert_eq!(
        detail["reasons"],
        serde_json::json!([support::REJECT_REASON]),
        "the audit carries the judge's own words"
    );
    assert_eq!(detail["flags"]["is_defect"], serde_json::json!(false));
    assert_eq!(detail["flags"]["reproducible"], serde_json::json!(false));
    assert_eq!(detail["flags"]["pii_clean"], serde_json::json!(true));
    assert_eq!(
        detail["flags"]["duplicate_of"],
        serde_json::Value::Null,
        "no duplicate was named"
    );

    // A rejection ends here: nothing enqueued, so no `file` row remains.
    assert_eq!(
        support::outbox_count(&fixture),
        0,
        "a rejection enqueues nothing"
    );
}

/// The done-when box for issue #37: a `file` verdict the judge found
/// unclean (`pii_clean: false`) must never reach `Tracker::file`. The
/// ticket parks exactly as a rejection does, and the audit says it was
/// blocked for PII rather than rejected as a non-defect.
#[pollster::test]
async fn a_file_verdict_with_pii_is_never_filed_and_records_the_block() {
    let fixture = support::fixture(
        support::scripted_model(support::drafted_json(), support::pii_file_judgment()),
        // An accepting tracker: were the gate missing, the file would
        // succeed and the `filed().is_empty()` assert below would fail.
        FakeTracker::new(TrackerMode::FileOk),
    );
    let pipeline = fixture.pipeline();
    support::drain_all(&pipeline);

    assert!(
        fixture.tracker.filed().is_empty(),
        "a PII-flagged draft is never filed: {:?}",
        fixture.tracker.filed()
    );

    let ticket = support::ticket(&fixture);
    assert_eq!(
        ticket.status,
        Status::Rejected,
        "not `{}`",
        ticket.status.as_str()
    );
    assert_eq!(ticket.verdict, Some(Verdict::File));
    assert_eq!(ticket.external_id, None);

    let events = support::events(&fixture);
    let blocked = support::event_of_kind(&events, EventKind::Rejected);
    let detail = blocked
        .detail
        .as_ref()
        .expect("a blocked event carries its detail");
    assert_eq!(
        detail["blocked_for_pii"],
        serde_json::json!(true),
        "the audit says the block was for PII"
    );
    assert!(
        detail["reason"]
            .as_str()
            .expect("the block carries a reason")
            .contains("PII"),
        "the reason names PII: {detail:?}"
    );

    // Blocked means nothing enqueued: no file row lingers to run later.
    assert_eq!(
        support::outbox_count(&fixture),
        0,
        "a PII block enqueues no file stage"
    );
}

/// The transcript is customer-authored text: `scrub_text` redacts the
/// email it carries before either the drafter or the judge sees it, so
/// the raw address never leaves for a model.
#[pollster::test]
async fn an_email_in_the_transcript_is_scrubbed_before_either_model_call() {
    let transcript =
        "customer: reach me at jane.doe@example.com and the checkout 500s on a gift card";
    let fixture = support::fixture_with_transcript(
        support::happy_model(),
        FakeTracker::new(TrackerMode::FileOk),
        transcript,
    );
    let pipeline = fixture.pipeline();
    support::drain_all(&pipeline);

    // Both the draft (fast) and judge (strong) prompts carry the
    // transcript; neither may carry the address verbatim.
    let prompts: Vec<Prompt> = fixture.model.prompts();
    assert_eq!(prompts.len(), 2, "the drafter and the judge both ran");
    for prompt in &prompts {
        for message in &prompt.messages {
            assert!(
                !message.content.contains("jane.doe@example.com"),
                "the address reached the {:?} prompt: {}",
                prompt.tier,
                message.content
            );
            assert!(
                message.content.contains("[subject_hash:"),
                "the address is redacted, not dropped silently: {}",
                message.content
            );
        }
    }
}

/// Box 3: `verdict: needs_info` writes a customer-facing question instead
/// of filing.
#[pollster::test]
async fn a_needs_info_verdict_writes_a_question_instead_of_filing() {
    let fixture = support::fixture(
        support::scripted_model(support::drafted_json(), support::needs_info_judgment()),
        FakeTracker::new(TrackerMode::FileOk),
    );
    let pipeline = fixture.pipeline();
    support::drain_all(&pipeline);

    assert!(
        fixture.tracker.filed().is_empty(),
        "an under-specified ticket is never filed: {:?}",
        fixture.tracker.filed()
    );

    let ticket = support::ticket(&fixture);
    assert_eq!(
        ticket.status,
        Status::NeedsInfo,
        "not `{}`",
        ticket.status.as_str()
    );
    let question = ticket
        .customer_question
        .as_deref()
        .expect("a question is stored on the ticket");
    assert!(
        question.contains(support::NEEDS_INFO_REASON),
        "the question quotes the judge's reason: {question}"
    );
    assert_eq!(ticket.external_id, None);

    let events = support::events(&fixture);
    let asked = support::event_of_kind(&events, EventKind::NeedsInfo);
    let detail = asked
        .detail
        .as_ref()
        .expect("a needs_info event carries its detail");
    assert_eq!(detail["question"], serde_json::json!(question));
    assert_eq!(
        detail["reasons"],
        serde_json::json!([support::NEEDS_INFO_REASON])
    );
}

/// Box 6: `sg_ticket_events` is a readable audit of every stage — exactly
/// the ordered sequence the happy path emits, with the judge's reasons
/// verbatim and a detail worth reading at every step.
#[pollster::test]
async fn the_event_trail_reads_as_an_audit_of_every_stage() {
    let fixture = happy();
    let pipeline = fixture.pipeline();
    support::drain_all(&pipeline);

    let events = support::events(&fixture);
    assert_eq!(
        support::stage_and_kind(&events),
        vec![
            ("draft", "intake"),
            ("draft", "draft_completed"),
            ("judge", "judge_completed"),
            ("file", "filed"),
            ("notify", "notify_skipped"),
        ],
        "the trail is the pipeline, in order"
    );

    // Content, not row counts: the judge's reasons are the exact strings
    // the fake judge returned.
    let judged = support::event_of_kind(&events, EventKind::JudgeCompleted);
    let detail = judged
        .detail
        .as_ref()
        .expect("judge_completed carries the judgment");
    assert_eq!(detail["verdict"], serde_json::json!("file"));
    assert_eq!(
        detail["reasons"],
        serde_json::json!(support::FILE_REASONS),
        "the judge's reasons, verbatim"
    );

    let drafted = support::event_of_kind(&events, EventKind::DraftCompleted);
    assert_eq!(
        drafted
            .detail
            .as_ref()
            .expect("draft_completed carries the draft")["title"],
        serde_json::json!(support::DRAFT_TITLE)
    );

    let filed = support::event_of_kind(&events, EventKind::Filed);
    assert_eq!(
        filed.detail.as_ref().expect("filed carries the reference")["external_id"],
        serde_json::json!(support::FILED_EXTERNAL_ID)
    );

    // The intake row opens the trail and names the conversation.
    assert_eq!(events[0].kind, EventKind::Intake);
    assert_eq!(
        events[0]
            .detail
            .as_ref()
            .expect("the intake event names its conversation")["conversation_id"],
        serde_json::json!(support::CONVERSATION)
    );

    // And the seq bands keep the trail in pipeline order.
    let seqs: Vec<i64> = events.iter().map(|event| event.seq).collect();
    let mut sorted = seqs.clone();
    sorted.sort_unstable();
    assert_eq!(seqs, sorted, "the trail orders by seq");
}

/// The v0 notify limitation, pinned by a test: with no `Mailer` wired the
/// stage records its message and says why it did not send (`no_mailer`).
#[pollster::test]
async fn notify_skips_with_an_explicit_reason_when_no_mailer_is_wired() {
    let fixture = happy();
    let pipeline = fixture.pipeline();
    support::drain_all(&pipeline);

    let skipped = {
        let events = support::events(&fixture);
        support::event_of_kind(&events, EventKind::NotifySkipped).clone()
    };
    let detail = skipped.detail.as_ref().expect("a skip carries its reason");
    assert_eq!(detail["reason"], serde_json::json!("no_mailer"));
    let message = detail["message"].as_str().expect("the message is recorded");
    assert!(
        message.contains(support::FILED_EXTERNAL_ID),
        "the recorded message points at the filed ticket: {message}"
    );
}

/// ...and with a `Mailer` wired, the reason is the honest one: intake
/// carries no customer address, so there is no recipient and nothing is
/// sent.
#[pollster::test]
async fn notify_skips_with_an_explicit_reason_when_there_is_no_recipient() {
    let mut fixture = happy();
    let mailer = Arc::new(FakeMailer::new(MailerMode::SendOk));
    fixture.mailer = Some(mailer.clone());
    let pipeline = fixture.pipeline();
    support::drain_all(&pipeline);

    let skipped = {
        let events = support::events(&fixture);
        support::event_of_kind(&events, EventKind::NotifySkipped).clone()
    };
    let detail = skipped.detail.as_ref().expect("a skip carries its reason");
    assert_eq!(detail["reason"], serde_json::json!("no_recipient"));
    assert!(
        mailer.sent().is_empty(),
        "no recipient means nothing was sent"
    );
}
