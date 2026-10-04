//! Issue #35 acceptance, support side: per-staff keys, the conversation
//! state machine, the staff routes (inbox, takeover, reply, handback), the
//! bot's silence while a person holds a conversation, the staff turns the
//! model sees after a handback, and a saved correction answering the same
//! question next time.
//!
//! The helpers are a trimmed copy of `routes.rs`'s — the same harness, the
//! same `FakeTextModel`, the same real `/sources` ingest — kept small here
//! rather than sharing a module across test binaries.

use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode, header};
use cratefield_core::{Completion, MapConfig, Statement};
use cratefield_testing::{Dialect, FakeTextModel, TestHarness, TextModelMode};
use module_support::Support;
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;

const ADMIN_TOKEN: &str = "test-admin-token-0123456789abcdef";
const ADMIN: &str = "/v1/support/admin/tenants";
const SOURCES: &str = "/v1/support/sources";
const MESSAGES: &str = "/v1/support/messages";
const KEYS: &str = "/v1/support/keys";
const INBOX: &str = "/v1/support/inbox";
const CONVERSATIONS: &str = "/v1/support/conversations";
const QUESTION: &str = "How do I reset my password?";
const RESET_DOC: &str = "Reset your password from the settings page under Security.";
/// The problem `type` base `TestHarness` serves as.
const PROBLEMS: &str = "https://test.example/problems/";

/// A buffered JSON response.
struct Reply {
    status: StatusCode,
    body: Value,
}

impl Reply {
    async fn of(response: axum::response::Response) -> Self {
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("response body reads");
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).expect("response body is JSON")
        };
        Self { status, body }
    }

    fn problem_type(&self) -> &str {
        self.body["type"].as_str().unwrap_or_default()
    }
}

/// `cratefield_testing::request` sends no headers; every route here needs
/// an `Authorization` bearer, so the oneshot is driven directly.
async fn send(
    router: &axum::Router,
    method: Method,
    path: &str,
    bearer: Option<&str>,
    json_body: Option<&str>,
) -> Reply {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(key) = bearer {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {key}"));
    }
    let body = match json_body {
        Some(payload) => {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
            Body::from(payload.to_owned())
        }
        None => Body::empty(),
    };
    let response = router
        .clone()
        .oneshot(builder.body(body).expect("request builds"))
        .await
        .expect("router answers");
    Reply::of(response).await
}

fn kit_with_model(dialect: Dialect, model: &FakeTextModel) -> TestHarness {
    TestHarness::with_database_and_ports(vec![Box::new(Support::new())], dialect, |ports| {
        ports.config = Arc::new(MapConfig::from_pairs([("ADMIN_TOKEN", ADMIN_TOKEN)]));
        ports.text_model = Some(Arc::new(model.clone()));
    })
}

/// One `(kit, model)` pair per available dialect, each with its own fake.
fn model_kits() -> Vec<(TestHarness, FakeTextModel)> {
    Dialect::available()
        .into_iter()
        .map(|dialect| {
            let model = FakeTextModel::new(TextModelMode::NotConfigured);
            (kit_with_model(dialect, &model), model)
        })
        .collect()
}

async fn mint_tenant(kit: &TestHarness, name: &str) -> Value {
    let body = json!({ "name": name }).to_string();
    let reply = send(
        &kit.router,
        Method::POST,
        ADMIN,
        Some(ADMIN_TOKEN),
        Some(&body),
    )
    .await;
    assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
    reply.body
}

/// Mints a staff key for `staff_id` with the tenant's own key. Returns the
/// mint response (`api_key` is the staff credential).
async fn mint_staff_key(kit: &TestHarness, api_key: &str, staff_id: &str) -> Value {
    let body = json!({ "label": staff_id, "staff_id": staff_id }).to_string();
    let reply = send(&kit.router, Method::POST, KEYS, Some(api_key), Some(&body)).await;
    assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
    assert_eq!(reply.body["staff_id"], staff_id);
    reply.body
}

async fn ingest_one(kit: &TestHarness, api_key: &str, text: &str) -> String {
    let body = json!({ "title": "Help", "text": text }).to_string();
    let reply = send(
        &kit.router,
        Method::POST,
        SOURCES,
        Some(api_key),
        Some(&body),
    )
    .await;
    assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
    let source_id = reply.body["source_id"].as_str().expect("source_id");
    text_column(
        kit,
        &format!("SELECT id AS v FROM sg_chunks WHERE source_id = '{source_id}'"),
    )
    .into_iter()
    .next()
    .expect("one chunk")
}

/// A tenant with one ingested document and that document's chunk id.
struct Seeded {
    api_key: String,
    chunk_id: String,
}

async fn seed(kit: &TestHarness, name: &str, text: &str) -> Seeded {
    let tenant = mint_tenant(kit, name).await;
    let api_key = tenant["api_key"].as_str().expect("api_key").to_owned();
    let chunk_id = ingest_one(kit, &api_key, text).await;
    Seeded { api_key, chunk_id }
}

fn text_column(kit: &TestHarness, sql: &str) -> Vec<String> {
    let rows = pollster::block_on(kit.db.query(&Statement::new(sql))).expect("query runs");
    rows.rows
        .iter()
        .map(|row| row.get::<String>("v").expect("text value"))
        .collect()
}

fn count_of(kit: &TestHarness, table: &str) -> usize {
    let rows = pollster::block_on(kit.db.query(&Statement::new(format!(
        "SELECT COUNT(*) AS n FROM {table}"
    ))))
    .expect("count query runs");
    let n = rows
        .rows
        .first()
        .and_then(|row| row.get::<i64>("n"))
        .expect("aggregate row");
    usize::try_from(n).expect("count is non-negative")
}

/// A structured model reply, the way an adapter hands one back.
fn reply(answer: &str, citations: &[(&str, &str)], confidence: f64) -> TextModelMode {
    let citations: Vec<Value> = citations
        .iter()
        .map(|(chunk_id, quote)| json!({ "chunk_id": chunk_id, "quote": quote }))
        .collect();
    let value = json!({ "answer": answer, "citations": citations, "confidence": confidence });
    TextModelMode::Complete(Completion::new(value.to_string(), "fake-fast").json(value))
}

/// `POST /messages`, asserting a 200.
async fn turn(
    kit: &TestHarness,
    api_key: &str,
    message: &str,
    conversation_id: Option<&str>,
) -> Value {
    let mut body = json!({ "message": message });
    if let Some(id) = conversation_id {
        body["conversation_id"] = json!(id);
    }
    let reply = send(
        &kit.router,
        Method::POST,
        MESSAGES,
        Some(api_key),
        Some(&body.to_string()),
    )
    .await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    reply.body
}

fn conversation_path(conversation_id: &str, action: &str) -> String {
    format!("{CONVERSATIONS}/{conversation_id}/{action}")
}

/// The whole prompt the model was shown on its latest call, its turns
/// concatenated — enough to assert a staff reply appears in the history.
fn last_prompt_text(model: &FakeTextModel) -> String {
    model
        .last()
        .expect("the model was asked")
        .messages
        .iter()
        .map(|message| message.content.as_str())
        .collect()
}

#[pollster::test]
async fn takeover_stops_the_bot_and_stores_the_customer_message() {
    for (kit, model) in model_kits() {
        let seeded = seed(&kit, "Takeover", RESET_DOC).await;
        let staff = mint_staff_key(&kit, &seeded.api_key, "agent-1").await;
        model.set_mode(reply(
            "From settings.",
            &[(&seeded.chunk_id, "settings page")],
            0.9,
        ));

        let first = turn(&kit, &seeded.api_key, QUESTION, None).await;
        assert_eq!(first["outcome"], "answered");
        let conversation = first["conversation_id"].as_str().expect("conversation");

        let taken = send(
            &kit.router,
            Method::POST,
            &conversation_path(conversation, "takeover"),
            Some(staff["api_key"].as_str().expect("staff key")),
            None,
        )
        .await;
        assert_eq!(taken.status, StatusCode::OK, "{}", taken.body);
        assert_eq!(taken.body["state"], "human");
        assert_eq!(taken.body["assignee"], "agent-1");
        // The takeover response carries the transcript so far.
        assert_eq!(
            taken.body["messages"].as_array().expect("messages").len(),
            2
        );

        // A customer turn while the person holds it: stored, not answered.
        let before = model.prompts().len();
        let stored = turn(&kit, &seeded.api_key, "Still stuck!", Some(conversation)).await;
        assert_eq!(stored["outcome"], "human");
        assert_eq!(stored["answer"], "");
        assert!(
            stored["citations"]
                .as_array()
                .expect("citations")
                .is_empty()
        );
        assert_eq!(stored["confidence"], 0.0);
        assert_eq!(
            model.prompts().len(),
            before,
            "the bot was not asked while a person holds the conversation"
        );
        // The customer message landed; no assistant reply followed it.
        assert_eq!(
            text_column(&kit, "SELECT role AS v FROM sg_messages ORDER BY seq"),
            ["user", "assistant", "user"],
        );
        assert_eq!(count_of(&kit, "sg_conversations"), 1);
    }
}

#[pollster::test]
async fn handback_resumes_the_bot_with_the_staff_turn_in_its_history() {
    for (kit, model) in model_kits() {
        let seeded = seed(&kit, "Handback", RESET_DOC).await;
        let staff = mint_staff_key(&kit, &seeded.api_key, "agent-2").await;
        let staff_key = staff["api_key"].as_str().expect("staff key");
        model.set_mode(reply(
            "From settings.",
            &[(&seeded.chunk_id, "settings page")],
            0.9,
        ));

        let first = turn(&kit, &seeded.api_key, QUESTION, None).await;
        let conversation = first["conversation_id"]
            .as_str()
            .expect("conversation")
            .to_owned();

        let taken = send(
            &kit.router,
            Method::POST,
            &conversation_path(&conversation, "takeover"),
            Some(staff_key),
            None,
        )
        .await;
        assert_eq!(taken.status, StatusCode::OK, "{}", taken.body);

        // The agent answers.
        let staff_reply = "Open Settings, then Security, then Reset.";
        let replied = send(
            &kit.router,
            Method::POST,
            &conversation_path(&conversation, "reply"),
            Some(staff_key),
            Some(&json!({ "body": staff_reply }).to_string()),
        )
        .await;
        assert_eq!(replied.status, StatusCode::OK, "{}", replied.body);
        assert_eq!(replied.body["role"], "staff");

        // Hand it back to the bot.
        let handed = send(
            &kit.router,
            Method::POST,
            &conversation_path(&conversation, "handback"),
            Some(staff_key),
            Some(&json!({ "close": false }).to_string()),
        )
        .await;
        assert_eq!(handed.status, StatusCode::OK, "{}", handed.body);
        assert_eq!(handed.body["state"], "bot");
        assert_eq!(handed.body["assignee"], Value::Null);

        // The next customer turn is the bot's again, and the staff reply is
        // in the prompt the model sees.
        let before = model.prompts().len();
        let resumed = turn(&kit, &seeded.api_key, QUESTION, Some(&conversation)).await;
        assert_eq!(resumed["outcome"], "answered");
        assert_eq!(model.prompts().len(), before + 1, "the bot answered again");
        let history = last_prompt_text(&model);
        assert!(
            history.contains(staff_reply),
            "the staff turn must be in the model's history: {history}"
        );
    }
}

#[pollster::test]
async fn a_saved_correction_answers_the_same_question_next_time() {
    for (kit, model) in model_kits() {
        // Nothing indexed: the bot cannot ground an answer, so it hands off.
        let tenant = mint_tenant(&kit, "Corrections").await;
        let api_key = tenant["api_key"].as_str().expect("api_key").to_owned();
        let staff = mint_staff_key(&kit, &api_key, "agent-7").await;
        let staff_key = staff["api_key"].as_str().expect("staff key");

        model.set_mode(reply("I am not sure.", &[], 0.2));
        let first = turn(&kit, &api_key, QUESTION, None).await;
        assert_eq!(first["outcome"], "handoff", "{first}");
        let conversation = first["conversation_id"]
            .as_str()
            .expect("conversation")
            .to_owned();
        assert_eq!(
            text_column(&kit, "SELECT state AS v FROM sg_conversations"),
            ["waiting_for_human"],
            "a handoff queues the conversation for a person"
        );

        // A person takes it over and saves their reply as an answer.
        let taken = send(
            &kit.router,
            Method::POST,
            &conversation_path(&conversation, "takeover"),
            Some(staff_key),
            None,
        )
        .await;
        assert_eq!(taken.status, StatusCode::OK, "{}", taken.body);
        let answer = "Open Settings > Security and choose Reset password.";
        let replied = send(
            &kit.router,
            Method::POST,
            &conversation_path(&conversation, "reply"),
            Some(staff_key),
            Some(&json!({ "body": answer, "save_as_answer": true }).to_string()),
        )
        .await;
        assert_eq!(replied.status, StatusCode::OK, "{}", replied.body);
        let source_id = replied.body["source_id"]
            .as_str()
            .expect("a saved correction names its source")
            .to_owned();

        // Hand back so the bot answers again.
        let handed = send(
            &kit.router,
            Method::POST,
            &conversation_path(&conversation, "handback"),
            Some(staff_key),
            None,
        )
        .await;
        assert_eq!(handed.status, StatusCode::OK, "{}", handed.body);

        // A brand-new conversation asks the same question: the reviewed
        // source is retrieved and, once cited, the answer is answered.
        let chunk_id = text_column(
            &kit,
            &format!("SELECT id AS v FROM sg_chunks WHERE source_id = '{source_id}'"),
        )
        .into_iter()
        .next()
        .expect("the correction indexed a chunk");
        model.set_mode(reply(answer, &[(&chunk_id, "Reset password")], 0.9));
        let follow = turn(&kit, &api_key, QUESTION, None).await;
        assert_eq!(follow["outcome"], "answered", "{follow}");
        assert_eq!(follow["citations"][0]["chunk_id"], chunk_id);

        // The source shows as reviewed, with its provenance.
        let shown = send(
            &kit.router,
            Method::GET,
            &format!("{SOURCES}/{source_id}"),
            Some(&api_key),
            None,
        )
        .await;
        assert_eq!(shown.status, StatusCode::OK, "{}", shown.body);
        assert_eq!(shown.body["reviewed"], true);
        assert_eq!(shown.body["reviewed_by"], "agent-7");
        assert_eq!(shown.body["reviewed_conversation_id"], conversation);

        // And it can be deleted through the ordinary source route.
        let deleted = send(
            &kit.router,
            Method::DELETE,
            &format!("{SOURCES}/{source_id}"),
            Some(&api_key),
            None,
        )
        .await;
        assert_eq!(deleted.status, StatusCode::NO_CONTENT, "{}", deleted.body);
        assert_eq!(count_of(&kit, "sg_sources"), 0);
    }
}

#[pollster::test]
async fn staff_routes_need_a_staff_key_and_the_assignee() {
    for (kit, model) in model_kits() {
        let seeded = seed(&kit, "Authz", RESET_DOC).await;

        // The tenant's own key carries no staff id: 403 on every staff
        // route, before the conversation is even looked at.
        let forbidden = send(&kit.router, Method::GET, INBOX, Some(&seeded.api_key), None).await;
        assert_eq!(
            forbidden.status,
            StatusCode::FORBIDDEN,
            "{}",
            forbidden.body
        );
        assert_eq!(forbidden.problem_type(), format!("{PROBLEMS}not-staff"));
        let refused = send(
            &kit.router,
            Method::POST,
            &conversation_path("anything", "takeover"),
            Some(&seeded.api_key),
            None,
        )
        .await;
        assert_eq!(refused.status, StatusCode::FORBIDDEN, "{}", refused.body);

        // The inbox is empty of anything waiting, but a staff key sees it.
        let agent1 = mint_staff_key(&kit, &seeded.api_key, "agent-1").await;
        let agent1_key = agent1["api_key"].as_str().expect("key");
        let agent2_key = mint_staff_key(&kit, &seeded.api_key, "agent-2").await["api_key"]
            .as_str()
            .expect("key")
            .to_owned();

        let inbox = send(&kit.router, Method::GET, INBOX, Some(agent1_key), None).await;
        assert_eq!(inbox.status, StatusCode::OK, "{}", inbox.body);
        assert_eq!(
            inbox.body["conversations"]
                .as_array()
                .expect("conversations")
                .len(),
            0
        );
        // An unknown state is a 400, after the guards.
        let bad = send(
            &kit.router,
            Method::GET,
            &format!("{INBOX}?state=nonsense"),
            Some(agent1_key),
            None,
        )
        .await;
        assert_eq!(bad.status, StatusCode::BAD_REQUEST, "{}", bad.body);

        // agent-1 takes over a conversation; agent-2 may not reply to or
        // hand back what they do not hold.
        model.set_mode(reply(
            "From settings.",
            &[(&seeded.chunk_id, "settings page")],
            0.9,
        ));
        let first = turn(&kit, &seeded.api_key, QUESTION, None).await;
        let conversation = first["conversation_id"]
            .as_str()
            .expect("conversation")
            .to_owned();
        let taken = send(
            &kit.router,
            Method::POST,
            &conversation_path(&conversation, "takeover"),
            Some(agent1_key),
            None,
        )
        .await;
        assert_eq!(taken.status, StatusCode::OK, "{}", taken.body);

        let reply_as_other = send(
            &kit.router,
            Method::POST,
            &conversation_path(&conversation, "reply"),
            Some(&agent2_key),
            Some(&json!({ "body": "Hi." }).to_string()),
        )
        .await;
        assert_eq!(
            reply_as_other.status,
            StatusCode::FORBIDDEN,
            "{}",
            reply_as_other.body
        );
        assert_eq!(
            reply_as_other.problem_type(),
            format!("{PROBLEMS}not-assignee")
        );
        let handback_as_other = send(
            &kit.router,
            Method::POST,
            &conversation_path(&conversation, "handback"),
            Some(&agent2_key),
            None,
        )
        .await;
        assert_eq!(
            handback_as_other.status,
            StatusCode::FORBIDDEN,
            "{}",
            handback_as_other.body
        );
        // Nothing was written by the refused calls.
        assert_eq!(count_of(&kit, "sg_messages"), 2);
    }
}
