//! Route-level acceptance for issue #3, one test per done-criterion. The
//! model is the crate's own `FakeTextModel`, so every outcome — answered,
//! clarify, the fabricated-citation downgrade, not-configured, transient —
//! is scripted, not stochastic.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{HeaderValue, Method, Request, StatusCode, header};
use module_support::{FakeTextModel, ModelTier, ScriptedReply, Support};
use serde_json::Value;
use tower::ServiceExt;

use cratefield_core::{IMPLICIT_TENANT, MapConfig, Statement};
use cratefield_testing::{Dialect, TestHarness, TestResponse, request};

/// The base URI core emits for every problem `type`. Core keeps the
/// constant private to its `problem` module, so the tests pin the literal,
/// as the reference module's route tests do.
const PROBLEM_TYPE_BASE: &str = "https://factory0.ventures/problems/";

const ADMIN: &str = "test-admin-token-0123456789abcdef";
const MESSAGES: &str = "/v1/support/messages";
const SETTINGS: &str = "/v1/support/admin/settings";

/// A kit whose model answers `answer`/`citations`/`confidence` at the
/// deployment-default threshold of 0.60.
fn kit(model: &FakeTextModel) -> TestHarness {
    TestHarness::with_database(
        vec![Box::new(Support::new().text_model(Arc::new(model.clone())))],
        Dialect::Sqlite,
    )
}

/// A kit with `ADMIN_TOKEN` configured, so the admin settings route is on.
fn admin_kit(model: &FakeTextModel) -> TestHarness {
    TestHarness::with_database_and_ports(
        vec![Box::new(Support::new().text_model(Arc::new(model.clone())))],
        Dialect::Sqlite,
        |ports| {
            ports.config = Arc::new(MapConfig::from_pairs([("ADMIN_TOKEN", ADMIN)]));
        },
    )
}

/// A kit with **no model at all** — the builder default.
fn unconfigured_kit() -> TestHarness {
    TestHarness::with_database(vec![Box::new(Support::new())], Dialect::Sqlite)
}

/// Seeds one chunk of the implicit tenant's knowledge base.
async fn seed_chunk(kit: &TestHarness, id: &str, body: &str) {
    kit.db
        .execute(&Statement::with_values(
            "INSERT INTO sg_chunks (id, tenant_id, source_id, body, created_at) \
             VALUES (?, ?, ?, ?, ?)",
            vec![
                id.into(),
                IMPLICIT_TENANT.into(),
                "source-1".into(),
                body.into(),
                "2026-01-01T00:00:00Z".into(),
            ],
        ))
        .await
        .expect("chunk insert");
}

async fn post_message(kit: &TestHarness, body: &str) -> TestResponse {
    request(&kit.router, Method::POST, MESSAGES, Some(body)).await
}

fn message_body(message: &str, conversation_id: Option<&str>) -> String {
    match conversation_id {
        Some(id) => {
            format!(r#"{{"message":"{message}","conversationId":"{id}"}}"#)
        }
        None => format!(r#"{{"message":"{message}"}}"#),
    }
}

/// A request carrying headers, for the admin route's bearer token (the
/// shared `request` helper sends none). Returns status and parsed body —
/// driven straight through `oneshot`, in the shape the waitlist tests use.
async fn request_with_token(
    kit: &TestHarness,
    method: Method,
    path: &str,
    token: Option<&str>,
    json: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(token) = token {
        builder = builder.header(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).expect("header builds"),
        );
    }
    if json.is_some() {
        builder = builder.header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
    }
    let body = Body::from(json.unwrap_or_default().to_owned());
    let response = kit
        .router
        .clone()
        .oneshot(builder.body(body).expect("request builds"))
        .await
        .expect("router answers");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("body reads");
    let parsed: Value = serde_json::from_slice(&bytes).expect("body is JSON");
    (status, parsed)
}

async fn count_rows(kit: &TestHarness, table: &str) -> i64 {
    let rows = kit
        .db
        .query(&Statement::new(format!(
            "SELECT COUNT(*) AS n FROM {table}"
        )))
        .await
        .expect("count query");
    rows.first()
        .and_then(|row| row.get::<i64>("n"))
        .expect("count column")
}

/// `(status, needs_escalation)` of the conversation row, when one exists.
async fn conversation_row(kit: &TestHarness) -> Option<(String, bool)> {
    let rows = kit
        .db
        .query(&Statement::new(
            "SELECT status, needs_escalation FROM sg_conversations",
        ))
        .await
        .expect("conversation query");
    rows.first().map(|row| {
        (
            row.get::<String>("status").expect("status"),
            row.get::<i64>("needs_escalation").expect("flag") != 0,
        )
    })
}

/// Every conversation row in full — the comparison set for the
/// "the failure consumed nothing" assertions, which compare row equality
/// (id, status, flag, both timestamps) rather than just counting.
async fn conversation_rows(kit: &TestHarness) -> Vec<(String, String, bool, String, String)> {
    kit.db
        .query(&Statement::new(
            "SELECT id, status, needs_escalation, created_at, updated_at \
             FROM sg_conversations ORDER BY id",
        ))
        .await
        .expect("conversation query")
        .rows
        .iter()
        .map(|row| {
            (
                row.get::<String>("id").expect("id"),
                row.get::<String>("status").expect("status"),
                row.get::<i64>("needs_escalation").expect("flag") != 0,
                row.get::<String>("created_at").expect("created"),
                row.get::<String>("updated_at").expect("updated"),
            )
        })
        .collect()
}

async fn answered_response(kit: &TestHarness, conversation: Option<&str>) -> Value {
    let response = post_message(
        kit,
        &message_body("How do I reset my password?", conversation),
    )
    .await;
    assert_eq!(
        response.status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(response.body())
    );
    response.json()
}

#[pollster::test]
async fn an_answered_turn_persists_the_conversation_and_both_messages() {
    let model = FakeTextModel::answering(
        "Reset it from Settings > Security.",
        &[("c1", "Reset your password from the settings page.")],
        0.9,
    );
    let kit = kit(&model);
    seed_chunk(&kit, "c1", "Reset your password from the settings page.").await;

    let body = answered_response(&kit, None).await;
    assert_eq!(body["outcome"], "answered");
    assert_eq!(body["citations"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["citations"][0]["chunkId"], "c1");
    assert_eq!(body["needsEscalation"], false, "answered does not escalate");
    assert_eq!(body["confidence"], 0.9);
    assert!(
        body["conversationId"]
            .as_str()
            .is_some_and(|id| !id.is_empty()),
        "a conversation id comes back"
    );

    // One conversation, open, not escalated — and both messages stored.
    assert_eq!(count_rows(&kit, "sg_conversations").await, 1);
    assert_eq!(count_rows(&kit, "sg_messages").await, 2);
    assert_eq!(
        conversation_row(&kit).await,
        Some(("open".to_owned(), false))
    );

    // The raw citations were persisted on the assistant row, not just the
    // published ones — in the model-facing snake_case shape the module
    // received them in.
    let rows = kit
        .db
        .query(&Statement::new(
            "SELECT outcome, citations FROM sg_messages WHERE role = 'assistant'",
        ))
        .await
        .expect("assistant query");
    let row = rows.first().expect("assistant row");
    assert_eq!(row.get::<String>("outcome").as_deref(), Some("answered"));
    assert_eq!(
        row.get::<String>("citations").as_deref(),
        Some(r#"[{"chunk_id":"c1","quote":"Reset your password from the settings page."}]"#)
    );
}

#[pollster::test]
async fn a_low_confidence_reply_clarifies_and_returns_no_citations() {
    let model = FakeTextModel::answering(
        "Have you tried turning it off and on again?",
        &[("c1", "Restart guidance.")],
        0.2,
    );
    let kit = kit(&model);
    seed_chunk(&kit, "c1", "Restart guidance.").await;

    let body = answered_response(&kit, None).await;
    assert_eq!(body["outcome"], "clarify");
    assert_eq!(
        body["citations"].as_array().map(Vec::len),
        Some(0),
        "no citations the module could not stand behind"
    );
    // The suppression half of the headline rule: the model's own words —
    // which it did not stand behind — never reach the caller. What goes
    // back is the canned clarify message.
    let answer = body["answer"].as_str().expect("answer is a string");
    assert!(
        !answer.contains("turning it off and on again"),
        "the model's below-threshold answer text must not reach the caller: {answer}"
    );
    assert!(
        answer.contains("could you rephrase"),
        "the answer is the canned clarify message: {answer}"
    );
    // The stored assistant outcome is the decision, not the model's hope.
    let rows = kit
        .db
        .query(&Statement::new(
            "SELECT outcome FROM sg_messages WHERE role = 'assistant'",
        ))
        .await
        .expect("assistant query");
    assert_eq!(
        rows.first().and_then(|row| row.get::<String>("outcome")),
        Some("clarify".to_owned())
    );
}

/// The headline rule: a citation that names a chunk outside the retrieved
/// context is decoration, so the answer is downgraded — even at very high
/// confidence — and the response carries no citations at all.
#[pollster::test]
async fn a_citation_to_an_unretrieved_chunk_downgrades_answered_to_clarify() {
    let model = FakeTextModel::answering(
        "Reset it from Settings > Security.",
        // "c1" is real; "c-fabricated" was never retrieved.
        &[
            ("c1", "Reset your password from the settings page."),
            ("c-fabricated", "invented"),
        ],
        0.99,
    );
    let kit = kit(&model);
    seed_chunk(&kit, "c1", "Reset your password from the settings page.").await;

    let response = post_message(&kit, &message_body("How do I reset my password?", None)).await;
    assert_eq!(response.status, StatusCode::OK);
    let body = response.json();
    assert_eq!(body["outcome"], "clarify", "the downgrade");
    assert_eq!(
        body["citations"].as_array().map(Vec::len),
        Some(0),
        "explicitly: no citations leave the module"
    );
    // The suppression half of the headline rule, asserted outright: the
    // fabricated answer's text never reaches the caller — the canned
    // message does.
    let answer = body["answer"].as_str().expect("answer is a string");
    assert!(
        !answer.contains("Settings > Security"),
        "the fabricated answer's text must not reach the caller: {answer}"
    );
    assert!(
        answer.contains("could you rephrase"),
        "the answer is the canned clarify message: {answer}"
    );
}

/// The audit half of the downgrade: the model's own words are kept even
/// when the module refuses to publish them. `body` (what the user was
/// shown) and `model_answer` (what the model said) deliberately differ on
/// a downgraded turn — that split is what lets a later escalation audit
/// read the raw material without any user-facing path leaking it.
#[pollster::test]
async fn a_downgraded_turn_stores_the_model_s_raw_answer_for_audit() {
    let model = FakeTextModel::answering(
        "Reset it from Settings > Security.",
        // One real citation, one fabricated: the answer is downgraded.
        &[
            ("c1", "Reset your password from the settings page."),
            ("c-fabricated", "invented"),
        ],
        0.99,
    );
    let kit = kit(&model);
    seed_chunk(&kit, "c1", "Reset your password from the settings page.").await;

    let body = answered_response(&kit, None).await;
    assert_eq!(body["outcome"], "clarify");

    let rows = kit
        .db
        .query(&Statement::new(
            "SELECT body, model_answer FROM sg_messages WHERE role = 'assistant'",
        ))
        .await
        .expect("assistant query");
    let row = rows.first().expect("assistant row");
    let published = row.get::<String>("body").expect("body");
    let raw = row.get::<String>("model_answer").expect("model_answer");
    assert_eq!(
        raw, "Reset it from Settings > Security.",
        "model_answer keeps the model's original text"
    );
    assert!(
        published.contains("could you rephrase"),
        "body is the canned message, not the model's answer: {published}"
    );
    assert!(
        !published.contains("Settings > Security"),
        "the unfounded answer never lands in the published column: {published}"
    );

    // The user row carries no model answer at all.
    let user_rows = kit
        .db
        .query(&Statement::new(
            "SELECT model_answer FROM sg_messages WHERE role = 'user'",
        ))
        .await
        .expect("user query");
    assert_eq!(
        user_rows
            .first()
            .and_then(|row| row.get::<String>("model_answer")),
        None,
        "model_answer is assistant-only"
    );
}

#[pollster::test]
async fn an_unconfigured_model_answers_503_and_writes_nothing() {
    // Both shapes of "no model": a fake that reports NotConfigured…
    let reporting = FakeTextModel::not_configured();
    for kit in [kit(&reporting), unconfigured_kit()] {
        seed_chunk(&kit, "c1", "Reset your password from the settings page.").await;
        let response = post_message(&kit, &message_body("How do I reset my password?", None)).await;
        assert_eq!(response.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response
                .headers
                .get(header::CONTENT_TYPE)
                .map(|v| v.to_str().expect("ascii")),
            Some("application/problem+json")
        );
        let body = response.json();
        assert_eq!(
            body["type"],
            format!("{PROBLEM_TYPE_BASE}text-model-not-configured")
        );
        // No conversation row was created: the failure consumed nothing.
        assert_eq!(count_rows(&kit, "sg_conversations").await, 0);
        assert_eq!(count_rows(&kit, "sg_messages").await, 0);
    }
}

#[pollster::test]
async fn a_transient_model_failure_is_retryable_and_consumes_nothing() {
    let model = FakeTextModel::transient("upstream overloaded");
    let kit = kit(&model);
    seed_chunk(&kit, "c1", "Reset your password from the settings page.").await;

    let first = post_message(&kit, &message_body("How do I reset my password?", None)).await;
    assert_eq!(first.status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        first.headers.contains_key("retry-after"),
        "retryability is explicit: a Retry-After header"
    );
    assert_eq!(
        first.json()["type"],
        format!("{PROBLEM_TYPE_BASE}text-model-unavailable")
    );
    // The conversation was not consumed: both tables still empty.
    assert_eq!(count_rows(&kit, "sg_conversations").await, 0);
    assert_eq!(count_rows(&kit, "sg_messages").await, 0);

    // The model comes back; the same request now succeeds.
    model.set_replying(
        r#"{"answer":"From settings.","citations":[{"chunk_id":"c1","quote":"settings page."}],"confidence":0.9}"#,
    );
    let second = post_message(&kit, &message_body("How do I reset my password?", None)).await;
    assert_eq!(
        second.status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(second.body())
    );
    assert_eq!(second.json()["outcome"], "answered");
    assert_eq!(count_rows(&kit, "sg_conversations").await, 1);
}

/// A 502 `text-model-invalid-answer`, and proof it consumed nothing. The
/// same assertions cover all three ways the route learns the answer is
/// unusable: not JSON, JSON of the wrong shape, and the model reporting
/// its own answer as invalid.
async fn assert_invalid_reply_is_a_502_that_writes_nothing(scripted: ScriptedReply) {
    let model = FakeTextModel::scripted(vec![scripted]);
    let kit = kit(&model);
    seed_chunk(&kit, "c1", "Reset your password from the settings page.").await;

    let response = post_message(&kit, &message_body("How do I reset my password?", None)).await;
    assert_eq!(
        response.status,
        StatusCode::BAD_GATEWAY,
        "{}",
        String::from_utf8_lossy(response.body())
    );
    assert_eq!(
        response
            .headers
            .get(header::CONTENT_TYPE)
            .map(|v| v.to_str().expect("ascii")),
        Some("application/problem+json")
    );
    assert_eq!(
        response.json()["type"],
        format!("{PROBLEM_TYPE_BASE}text-model-invalid-answer")
    );
    // Nothing was written: the bad answer consumed no conversation.
    assert_eq!(count_rows(&kit, "sg_conversations").await, 0);
    assert_eq!(count_rows(&kit, "sg_messages").await, 0);
}

#[pollster::test]
async fn an_unparseable_model_reply_is_a_502_and_writes_nothing() {
    assert_invalid_reply_is_a_502_that_writes_nothing(ScriptedReply::Reply(
        "I am sorry, I cannot help with that.".to_owned(),
    ))
    .await;
}

#[pollster::test]
async fn a_well_formed_reply_of_the_wrong_shape_is_a_502_too() {
    // Valid JSON, wrong schema: no `answer`, no `confidence`.
    assert_invalid_reply_is_a_502_that_writes_nothing(ScriptedReply::Reply(
        r#"{"verdict":"maybe","sources":[]}"#.to_owned(),
    ))
    .await;
}

#[pollster::test]
async fn a_model_reported_invalid_answer_maps_to_the_same_502() {
    // The port itself rejecting the answer (TextModelError::Invalid)
    // lands on the same problem as the module's own parse failure.
    assert_invalid_reply_is_a_502_that_writes_nothing(ScriptedReply::Invalid(
        "confidence 1.4 is outside 0.0..=1.0".to_owned(),
    ))
    .await;
}

/// The failure tests above post without a conversation id, so they only
/// prove nothing is written on the conversation-CREATING path. This one
/// fails on an EXISTING conversation and compares its row byte-for-byte:
/// every pre-model step is a read, so even a regression that persisted
/// the user's message before the model call cannot pass here.
#[pollster::test]
async fn a_transient_failure_on_an_existing_conversation_consumes_nothing() {
    let good = r#"{"answer":"From settings.","citations":[{"chunk_id":"c1","quote":"settings page."}],"confidence":0.9}"#;
    let model = FakeTextModel::scripted(vec![
        ScriptedReply::Reply(good.to_owned()),
        ScriptedReply::Transient("upstream overloaded".to_owned()),
        // The model comes back for the retry.
        ScriptedReply::Reply(good.to_owned()),
    ]);
    let kit = kit(&model);
    seed_chunk(&kit, "c1", "Reset your password from the settings page.").await;

    // Turn one creates the conversation.
    let first = answered_response(&kit, None).await;
    assert_eq!(first["outcome"], "answered");
    let conversation = first["conversationId"]
        .as_str()
        .expect("conversation id")
        .to_owned();
    let before = conversation_rows(&kit).await;
    assert_eq!(before.len(), 1);
    let messages_before = count_rows(&kit, "sg_messages").await;
    assert_eq!(messages_before, 2, "the user turn and the assistant turn");

    // A second message to that SAME conversation, while the model is down.
    let failed = post_message(
        &kit,
        &message_body("and what about billing?", Some(&conversation)),
    )
    .await;
    assert_eq!(failed.status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        failed.headers.contains_key("retry-after"),
        "the failure stays retryable on an existing conversation"
    );
    assert_eq!(
        failed.json()["type"],
        format!("{PROBLEM_TYPE_BASE}text-model-unavailable")
    );
    // The conversation row is byte-for-byte unchanged — status,
    // needs_escalation, created_at and updated_at all identical — and no
    // message row appeared.
    assert_eq!(conversation_rows(&kit).await, before);
    assert_eq!(count_rows(&kit, "sg_messages").await, messages_before);

    // The same follow-up now succeeds on that conversation.
    let retried = post_message(
        &kit,
        &message_body("and what about billing?", Some(&conversation)),
    )
    .await;
    assert_eq!(
        retried.status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(retried.body())
    );
    assert_eq!(retried.json()["outcome"], "answered");
    assert_eq!(
        retried.json()["conversationId"],
        Value::String(conversation),
        "the retry landed on the same conversation"
    );
    assert_eq!(count_rows(&kit, "sg_messages").await, messages_before + 2);
}

#[pollster::test]
async fn the_tenant_threshold_overrides_the_documented_default() {
    let model = FakeTextModel::answering("From settings.", &[("c1", "settings page.")], 0.7);
    let kit = admin_kit(&model);
    seed_chunk(&kit, "c1", "Reset your password from the settings page.").await;

    // The default (0.60) applies when no tenant row exists: 0.7 answers.
    let first = answered_response(&kit, None).await;
    assert_eq!(first["outcome"], "answered");
    let conversation = first["conversationId"]
        .as_str()
        .expect("conversation id")
        .to_owned();

    // Raise the tenant's bar past the model's confidence.
    let (put_status, put_body) = request_with_token(
        &kit,
        Method::PUT,
        SETTINGS,
        Some(ADMIN),
        Some(r#"{"answerThreshold":0.9}"#),
    )
    .await;
    assert_eq!(put_status, StatusCode::OK);
    assert_eq!(put_body["answerThresholdPct"], 90);

    // The same confidence that answered under the default now clarifies —
    // the threshold is per-tenant, not a constant.
    let second = post_message(
        &kit,
        &message_body("How do I reset my password?", Some(&conversation)),
    )
    .await;
    assert_eq!(second.status, StatusCode::OK);
    assert_eq!(second.json()["outcome"], "clarify");
    assert_eq!(second.json()["citations"].as_array().map(Vec::len), Some(0));
}

#[pollster::test]
async fn admin_settings_is_guarded_like_the_admin_routes_elsewhere() {
    let model = FakeTextModel::answering("a", &[("c1", "q")], 0.9);
    let kit = admin_kit(&model);

    // No token and a wrong token are refused before anything is written.
    for token in [None, Some("wrong")] {
        let (status, _) = request_with_token(
            &kit,
            Method::PUT,
            SETTINGS,
            token,
            Some(r#"{"answerThreshold":0.9}"#),
        )
        .await;
        assert!(
            status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN,
            "token {token:?} refused, got {status}"
        );
    }
    assert_eq!(count_rows(&kit, "sg_tenant_settings").await, 0);

    // An out-of-range threshold is a validation problem, not a row.
    let (bad_status, _) = request_with_token(
        &kit,
        Method::PUT,
        SETTINGS,
        Some(ADMIN),
        Some(r#"{"answerThreshold":1.5}"#),
    )
    .await;
    assert_eq!(bad_status, StatusCode::BAD_REQUEST);
    assert_eq!(count_rows(&kit, "sg_tenant_settings").await, 0);
}

#[pollster::test]
async fn a_handoff_with_nothing_retrieved_marks_the_conversation_escalated() {
    // No chunks seeded at all: retrieval is empty, so no answer could be
    // grounded and the module stops guessing instead of clarifying.
    let model = FakeTextModel::answering("Guessing freely.", &[("nowhere", "nothing")], 0.99);
    let kit = kit(&model);

    let body = answered_response(&kit, None).await;
    assert_eq!(body["outcome"], "handoff");
    assert_eq!(body["needsEscalation"], true);
    assert_eq!(body["citations"].as_array().map(Vec::len), Some(0));

    assert_eq!(
        conversation_row(&kit).await,
        Some(("escalated".to_owned(), true)),
        "the stored row carries needs_escalation = 1 and status = 'escalated'"
    );
}

#[pollster::test]
async fn a_conversation_past_the_max_clarify_turns_hands_off() {
    let model = FakeTextModel::scripted(vec![
        ScriptedReply::Reply(r#"{"answer":"Maybe this?","citations":[{"chunk_id":"c1","quote":"q"}],"confidence":0.3}"#.to_owned()),
        ScriptedReply::Reply(r#"{"answer":"Or this?","citations":[{"chunk_id":"c1","quote":"q"}],"confidence":0.3}"#.to_owned()),
        ScriptedReply::Reply(r#"{"answer":"Still guessing.","citations":[{"chunk_id":"c1","quote":"q"}],"confidence":0.3}"#.to_owned()),
    ]);
    let kit = kit(&model);
    seed_chunk(&kit, "c1", "Reset your password from the settings page.").await;

    let first = answered_response(&kit, None).await;
    assert_eq!(first["outcome"], "clarify");
    let conversation = first["conversationId"]
        .as_str()
        .expect("conversation id")
        .to_owned();

    let second = post_message(&kit, &message_body("still stuck", Some(&conversation))).await;
    assert_eq!(second.status, StatusCode::OK);
    assert_eq!(
        second.json()["outcome"],
        "clarify",
        "the first MAX_CLARIFY_TURNS turns clarify"
    );
    assert_eq!(second.json()["needsEscalation"], false);

    let third = post_message(
        &kit,
        &message_body("please just answer", Some(&conversation)),
    )
    .await;
    assert_eq!(third.status, StatusCode::OK);
    assert_eq!(
        third.json()["outcome"],
        "handoff",
        "a third non-answer stops guessing"
    );
    assert_eq!(third.json()["needsEscalation"], true);
    assert_eq!(
        conversation_row(&kit).await,
        Some(("escalated".to_owned(), true))
    );
}

/// Escalation is sticky: a follow-up that the module *can* answer does
/// not silently un-escalate a conversation a person is already picking
/// up. The turn's outcome is `answered`, but the response still carries
/// `needsEscalation: true` and the row stays `escalated`.
#[pollster::test]
async fn an_answered_follow_up_does_not_un_escalate_the_conversation() {
    let model = FakeTextModel::scripted(vec![
        // Two clarifies below threshold, then the module stops guessing
        // and hands off: the conversation is escalated.
        ScriptedReply::Reply(r#"{"answer":"Maybe this?","citations":[{"chunk_id":"c1","quote":"q"}],"confidence":0.3}"#.to_owned()),
        ScriptedReply::Reply(r#"{"answer":"Or this?","citations":[{"chunk_id":"c1","quote":"q"}],"confidence":0.3}"#.to_owned()),
        ScriptedReply::Reply(r#"{"answer":"Still guessing.","citations":[{"chunk_id":"c1","quote":"q"}],"confidence":0.3}"#.to_owned()),
        // The follow-up the module can stand behind.
        ScriptedReply::Reply(r#"{"answer":"From settings.","citations":[{"chunk_id":"c1","quote":"settings page."}],"confidence":0.9}"#.to_owned()),
    ]);
    let kit = kit(&model);
    seed_chunk(&kit, "c1", "Reset your password from the settings page.").await;

    let first = answered_response(&kit, None).await;
    assert_eq!(first["outcome"], "clarify", "turn one still has budget");
    let conversation = first["conversationId"]
        .as_str()
        .expect("conversation id")
        .to_owned();
    // Turn two spends the last clarify; turn three stops guessing.
    let second = post_message(&kit, &message_body("still stuck", Some(&conversation))).await;
    assert_eq!(second.json()["outcome"], "clarify");
    let handoff = post_message(
        &kit,
        &message_body("please just answer", Some(&conversation)),
    )
    .await;
    assert_eq!(handoff.json()["outcome"], "handoff");
    assert_eq!(handoff.json()["needsEscalation"], true);

    let follow_up = post_message(
        &kit,
        &message_body("that worked, thanks", Some(&conversation)),
    )
    .await;
    assert_eq!(
        follow_up.status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(follow_up.body())
    );
    assert_eq!(follow_up.json()["outcome"], "answered");
    assert_eq!(
        follow_up.json()["needsEscalation"],
        true,
        "answered, but the ticket behind it stays escalated"
    );
    assert_eq!(
        conversation_row(&kit).await,
        Some(("escalated".to_owned(), true)),
        "escalation is sticky across an answered follow-up"
    );
}

#[pollster::test]
async fn the_model_is_asked_for_the_fast_tier_and_sees_the_chunk_ids() {
    let model = FakeTextModel::answering("From settings.", &[("c1", "settings page.")], 0.9);
    let kit = kit(&model);
    seed_chunk(&kit, "c1", "Reset your password from the settings page.").await;
    seed_chunk(&kit, "c2", "Billing plans are changed in billing.").await;

    let body = answered_response(&kit, None).await;
    assert_eq!(body["outcome"], "answered");

    let last = model.last_request().expect("the model was asked");
    assert_eq!(
        last.tier,
        ModelTier::Fast,
        "the module asks for a tier, never a vendor"
    );
    for id in ["c1", "c2"] {
        assert!(
            last.prompt.contains(&format!("[{id}]")),
            "the prompt embeds each retrieved chunk with its id: missing {id}"
        );
    }
    assert!(
        last.prompt.contains("cite only these chunk ids"),
        "the prompt instructs the model to cite only what it was given"
    );
    assert!(
        last.system
            .contains("Cite only chunk ids that appear in the retrieved context"),
        "the system prompt carries the grounding rule"
    );
    // The schema sent to the model is the issue's literal snake_case shape,
    // not the HTTP surface's camelCase house style. The check inspects the
    // citation properties (not the raw text) because the schema embeds doc
    // comments, whose prose may mention either spelling.
    let citation_properties = &last.schema["properties"]["citations"]["items"]["properties"];
    assert!(
        citation_properties.get("chunk_id").is_some(),
        "the model schema names the field chunk_id: {}",
        last.schema
    );
    assert!(
        citation_properties.get("chunkId").is_none(),
        "the model schema must not use the HTTP spelling: {}",
        last.schema
    );
    assert!(
        last.system.contains("chunk_id + quote pairs"),
        "the system prompt names the field the schema actually uses"
    );
}

#[pollster::test]
async fn an_unknown_or_foreign_conversation_id_is_a_404() {
    let model = FakeTextModel::answering("From settings.", &[("c1", "settings page.")], 0.9);
    let kit = kit(&model);
    seed_chunk(&kit, "c1", "Reset your password from the settings page.").await;

    let response = post_message(&kit, &message_body("hello?", Some("no-such-conversation"))).await;
    assert_eq!(response.status, StatusCode::NOT_FOUND);
    assert_eq!(count_rows(&kit, "sg_conversations").await, 0);
    assert_eq!(
        model.calls(),
        0,
        "a foreign id is refused before the model is called"
    );
}

#[pollster::test]
async fn empty_and_oversized_messages_are_validation_problems() {
    let model = FakeTextModel::answering("From settings.", &[("c1", "settings page.")], 0.9);
    let kit = kit(&model);

    for (body, why) in [
        (r#"{"message":"   "}"#.to_owned(), "whitespace-only"),
        (
            format!(r#"{{"message":"{}"}}"#, "x".repeat(4001)),
            "one character over the cap",
        ),
    ] {
        let response = post_message(&kit, &body).await;
        assert_eq!(response.status, StatusCode::BAD_REQUEST, "{why}");
        assert_eq!(
            response.json()["type"],
            format!("{PROBLEM_TYPE_BASE}validation-failed"),
            "{why}"
        );
    }
    assert_eq!(model.calls(), 0, "validation happens before the model");
}
