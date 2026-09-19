//! Smoke test for the `testing` feature: these fakes live behind
//! `module-escalation`'s `testing` feature, enabled here by the crate's
//! self dev-dependency. They implement the mirrored ports in
//! `module_escalation::ports`, so this also pins the port shapes a test
//! can drive: scripted completions with their errors, recorded prompts,
//! and a tracker asserted called exactly once (or never).

use std::time::Duration;

use module_escalation::ports::text_model::{
    Completion, ModelTier, Prompt, TextModel, TextModelError,
};
use module_escalation::ports::tracker::{
    Credential, Destination, Filed, Severity, TicketDraft, TicketState, TicketStatus, Tracker,
};
use module_escalation::testing::{FakeTextModel, FakeTracker};

#[test]
fn the_fake_text_model_answers_in_order_and_records_the_prompts() {
    let model = FakeTextModel::scripted(vec![
        Ok(
            Completion::new("a draft", "fake-1").json(serde_json::json!({
                "title": "Checkout failing",
                "severity": "error",
            })),
        ),
        Err(TextModelError::Transient {
            retry_after: Some(Duration::from_secs(30)),
        }),
        Err(TextModelError::Rejected("bad schema".to_owned())),
    ]);

    let schema = serde_json::json!({ "type": "object" });
    let prompt = |tier| {
        Prompt::new(tier)
            .system("Judge.")
            .json_schema(schema.clone())
    };

    let first = pollster::block_on(model.complete(&prompt(ModelTier::Strong))).expect("scripted");
    assert_eq!(
        first.json.as_ref().and_then(|j| j["severity"].as_str()),
        Some("error")
    );

    let second = pollster::block_on(model.complete(&prompt(ModelTier::Strong))).unwrap_err();
    assert_eq!(second.retry_after(), Some(Duration::from_secs(30)));

    let third = pollster::block_on(model.complete(&prompt(ModelTier::Strong))).unwrap_err();
    assert!(matches!(third, TextModelError::Rejected(_)));

    // Every prompt arrived, and carried the tier and schema the caller set.
    let prompts = model.prompts();
    assert_eq!(prompts.len(), 3);
    assert!(prompts.iter().all(|p| p.tier == ModelTier::Strong));
    assert_eq!(prompts[0].system.as_deref(), Some("Judge."));
    assert_eq!(prompts[0].json_schema, Some(schema));
}

#[test]
fn the_json_convenience_answers_one_structured_completion() {
    let model = FakeTextModel::json(
        ModelTier::Fast,
        serde_json::json!({ "file": true, "title": "x" }),
    );

    let completion = pollster::block_on(model.complete(&Prompt::new(ModelTier::Fast).user("hi")))
        .expect("the one scripted response");
    assert_eq!(completion.model, "fake-fast");
    assert_eq!(
        completion.json,
        Some(serde_json::json!({ "file": true, "title": "x" }))
    );

    // Off the end of the one-response script: a loud transport error.
    let error = pollster::block_on(model.complete(&Prompt::new(ModelTier::Fast))).unwrap_err();
    assert!(
        format!("{error}").contains("exhausted"),
        "the exhausted script fails loudly: {error}"
    );
}

#[test]
fn the_fake_tracker_records_file_calls_so_a_test_can_count_them() {
    let tracker = FakeTracker::accepting(Filed {
        external_id: "acme/api#7".to_owned(),
        url: "https://github.test/acme/api/7".to_owned(),
    });
    let dest = Destination::GitHub {
        owner: "acme".to_owned(),
        repo: "api".to_owned(),
    };
    let draft = TicketDraft::new("conv-1", "Checkout failing", "500s", Severity::Error);

    let filed = pollster::block_on(tracker.file(&dest, &Credential::new("t"), &draft))
        .expect("the one acceptance");
    assert_eq!(filed.external_id, "acme/api#7");

    // Called exactly once: the retry-after-transient path must not file
    // the ticket a second time.
    assert_eq!(tracker.filed().len(), 1);
    let (recorded_dest, recorded_draft) = &tracker.filed()[0];
    assert_eq!(recorded_dest, &dest);
    assert_eq!(recorded_draft.idempotency_key, "conv-1");
    assert_eq!(recorded_draft.severity, Severity::Error);

    // And the status port answers, with its calls recorded too.
    tracker.status_scripted(vec![TicketStatus {
        external_id: "acme/api#7".to_owned(),
        state: TicketState::Resolved,
        url: None,
    }]);
    let status = pollster::block_on(tracker.status(&dest, &Credential::new("t"), "acme/api#7"))
        .expect("scripted status");
    assert_eq!(status.state, TicketState::Resolved);
    assert_eq!(tracker.status_calls(), vec!["acme/api#7".to_owned()]);
    assert_eq!(tracker.filed().len(), 1, "status is not a file");
}

#[test]
fn a_fresh_fake_tracker_was_never_called() {
    let tracker = FakeTracker::scripted(vec![]);
    assert!(tracker.filed().is_empty(), "no escalation, no file");
    assert!(tracker.status_calls().is_empty());
}
