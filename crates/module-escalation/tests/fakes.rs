//! Smoke test for the test doubles these suites lean on: the published
//! `cratefield_testing::{FakeTextModel, FakeTracker}` (the ports and their
//! fakes publish together in core 0.5, so nothing is mirrored in this
//! crate) and the crate-local `testing` fixtures. This pins the shapes a
//! test can drive: per-tier completion modes with their errors, recorded
//! prompts, and a tracker asserted called exactly once (or never).

use std::time::Duration;

use cratefield_core::{
    Completion, Credential, Destination, ModelTier, Prompt, Severity, TextModel as _, TicketDraft,
    TicketState, Tracker as _, TrackerError,
};
use cratefield_testing::{FakeTextModel, FakeTracker, TextModelMode, TrackerMode};

#[test]
fn the_fake_text_model_answers_per_tier_and_records_the_prompts() {
    let draft = Completion::new("a draft", "fake-fast").json(serde_json::json!({
        "title": "Checkout failing",
        "severity": "error",
    }));
    let model = FakeTextModel::new(TextModelMode::Transient {
        retry_after: Some(Duration::from_secs(30)),
    });
    model.set_mode_for(ModelTier::Fast, TextModelMode::Complete(draft));

    let schema = serde_json::json!({ "type": "object" });
    let prompt = |tier| {
        Prompt::new(tier)
            .system("Judge.")
            .json_schema(schema.clone())
    };

    let first =
        pollster::block_on(model.complete(&prompt(ModelTier::Fast))).expect("per-tier complete");
    assert_eq!(
        first.json.as_ref().and_then(|j| j["severity"].as_str()),
        Some("error")
    );

    // The global mode is what a tier without an override answers with.
    let second = pollster::block_on(model.complete(&prompt(ModelTier::Strong))).unwrap_err();
    assert_eq!(second.retry_after(), Some(Duration::from_secs(30)));

    // Only the answered prompts are recorded, each carrying the tier and
    // schema the caller set.
    let prompts = model.prompts();
    assert_eq!(prompts.len(), 1, "failed calls are not recorded");
    assert_eq!(prompts[0].tier, ModelTier::Fast);
    assert_eq!(prompts[0].system.as_deref(), Some("Judge."));
    assert_eq!(prompts[0].json_schema, Some(schema));
}

#[test]
fn the_reply_mode_answers_text_under_a_deterministic_model_name() {
    let model = FakeTextModel::new(TextModelMode::Reply("hello".to_owned()));

    let completion = pollster::block_on(model.complete(&Prompt::new(ModelTier::Fast).user("hi")))
        .expect("reply");
    assert_eq!(completion.model, "fake-fast");
    assert_eq!(completion.text, "hello");
    assert_eq!(completion.json, None, "plain text, not parsed JSON");
}

#[test]
fn the_fake_tracker_records_accepted_files_so_a_test_can_count_them() {
    let tracker = FakeTracker::new(TrackerMode::FileOk);
    let dest = Destination::GitHub {
        owner: "acme".to_owned(),
        repo: "api".to_owned(),
    };
    let draft = TicketDraft::new("conv-1", "Checkout failing", "500s", Severity::Error);

    let filed = pollster::block_on(tracker.file(&dest, &Credential::new("t"), &draft))
        .expect("the accepted file");
    assert_eq!(filed.external_id, "fake-0");

    // Called exactly once: the retry-after-transient path must not file
    // the ticket a second time.
    assert_eq!(tracker.filed().len(), 1);
    let call = &tracker.filed()[0];
    assert_eq!(call.dest, dest);
    assert_eq!(call.draft.idempotency_key, "conv-1");
    assert_eq!(call.draft.severity, Severity::Error);
    assert!(
        !call.credential_fingerprint.is_empty(),
        "the credential arrives as a fingerprint, not the secret"
    );

    // And the status port answers, with its calls recorded too.
    tracker.set_state(TicketState::Resolved);
    let status = pollster::block_on(tracker.status(&dest, &Credential::new("t"), "fake-0"))
        .expect("status answers");
    assert_eq!(status.state, TicketState::Resolved);
    assert_eq!(tracker.statused().len(), 1);
    assert_eq!(tracker.statused()[0].external_id, "fake-0");
    assert_eq!(tracker.filed().len(), 1, "status is not a file");
}

#[test]
fn the_fake_tracker_modes_name_the_error_vocabulary() {
    let unauthorized = FakeTracker::new(TrackerMode::Unauthorized);
    let dest = Destination::GitHub {
        owner: "acme".to_owned(),
        repo: "api".to_owned(),
    };
    let draft = TicketDraft::new("conv-1", "Checkout failing", "500s", Severity::Error);
    let error = pollster::block_on(unauthorized.file(&dest, &Credential::new("t"), &draft))
        .expect_err("the mode answers");
    assert!(matches!(error, TrackerError::Unauthorized));
    assert!(
        unauthorized.filed().is_empty(),
        "a refused call is not recorded"
    );

    let custom = FakeTracker::new(TrackerMode::Error(TrackerError::Rejected(
        "draft title exceeds the tracker limit".to_owned(),
    )));
    let error = pollster::block_on(custom.file(&dest, &Credential::new("t"), &draft))
        .expect_err("the mode answers");
    assert!(
        format!("{error}").contains("exceeds the tracker limit"),
        "the exact error a test asked for: {error}"
    );
}

#[test]
fn a_fresh_fake_tracker_was_never_called() {
    let tracker = FakeTracker::new(TrackerMode::FileOk);
    assert!(tracker.filed().is_empty(), "no escalation, no file");
    assert!(tracker.statused().is_empty());
}
