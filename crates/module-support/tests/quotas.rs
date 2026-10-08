//! Issue #20 acceptance, over every available dialect: the plan's
//! monthly conversation allowance (and the continuing turn it must not
//! block), the guarded increments under concurrency, the daily token
//! ceiling a runaway loop runs into, and that a turn the schema rejects
//! still pays for what it spent.
//!
//! The helpers are a trimmed copy of `routes.rs`'s — the same harness,
//! the same `FakeTextModel`, the same real `/sources` ingest — kept
//! small here rather than sharing a module across test binaries. What is
//! added is the one thing `routes.rs` has no answer for: plans are
//! admin-set by SQL, so `seed_plan` writes `sg_plans` and
//! `sg_tenant_plan` straight through the kit's database handle.

use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode, header};
use cratefield_core::{Completion, MapConfig, Statement};
use cratefield_testing::{Dialect, FakeTextModel, TestHarness, TextModelMode};
use module_support::Support;
use serde_json::{Value, json};
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use tower::ServiceExt;

const ADMIN_TOKEN: &str = "test-admin-token-0123456789abcdef";
const ADMIN: &str = "/v1/support/admin/tenants";
const SOURCES: &str = "/v1/support/sources";
const MESSAGES: &str = "/v1/support/messages";
const QUESTION: &str = "How do I reset my password?";
const RESET_DOC: &str = "Reset your password from the settings page under Security.";
/// The problem `type` base `TestHarness` serves as.
const PROBLEMS: &str = "https://test.example/problems/";
/// The two meters `sg_usage` counts in, as the module spells them.
const CONVERSATIONS: &str = "conversations";
const TOKENS: &str = "model_tokens";

/// A buffered JSON response, headers kept because the token ceiling
/// answers with `Retry-After` and a `Problem` cannot carry it.
struct Reply {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    body: Value,
}

impl Reply {
    async fn of(response: axum::response::Response) -> Self {
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("response body reads");
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).expect("response body is JSON")
        };
        Self {
            status,
            headers,
            body,
        }
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

/// A structured model reply, the way an adapter hands one back, with the
/// token counts the provider reported — the numbers the meter records.
fn reply(chunk_id: &str, tokens: (u64, u64)) -> TextModelMode {
    let value = json!({
        "answer": "Reset it from the settings page.",
        "citations": [{ "chunk_id": chunk_id, "quote": "Reset your password from the settings page" }],
        "confidence": 0.9,
    });
    TextModelMode::Complete(
        Completion::new(value.to_string(), "fake-fast")
            .json(value)
            .usage(tokens.0, tokens.1),
    )
}

/// One `(kit, model)` pair per available dialect, each with its own fake.
/// The fake starts unconfigured: a test sets its mode once ingest has
/// minted the chunk id the reply cites.
fn model_kits() -> Vec<(TestHarness, FakeTextModel)> {
    Dialect::available()
        .into_iter()
        .map(|dialect| {
            let model = FakeTextModel::new(TextModelMode::NotConfigured);
            let kit = TestHarness::with_database_and_ports(
                vec![Box::new(Support::new())],
                dialect,
                |ports| {
                    ports.config = Arc::new(MapConfig::from_pairs([("ADMIN_TOKEN", ADMIN_TOKEN)]));
                    ports.text_model = Some(Arc::new(model.clone()));
                },
            );
            (kit, model)
        })
        .collect()
}

/// A tenant with one ingested document and that document's chunk id.
struct Seeded {
    api_key: String,
    tenant_id: String,
    chunk_id: String,
}

async fn seed(kit: &TestHarness, name: &str) -> Seeded {
    let body = json!({ "name": name }).to_string();
    let minted = send(
        &kit.router,
        Method::POST,
        ADMIN,
        Some(ADMIN_TOKEN),
        Some(&body),
    )
    .await;
    assert_eq!(minted.status, StatusCode::CREATED, "{}", minted.body);
    let api_key = minted.body["api_key"].as_str().expect("api_key").to_owned();
    let tenant_id = minted.body["tenant_id"]
        .as_str()
        .expect("tenant_id")
        .to_owned();

    let body = json!({ "title": "Help", "text": RESET_DOC }).to_string();
    let ingested = send(
        &kit.router,
        Method::POST,
        SOURCES,
        Some(&api_key),
        Some(&body),
    )
    .await;
    assert_eq!(ingested.status, StatusCode::CREATED, "{}", ingested.body);
    let source_id = ingested.body["source_id"].as_str().expect("source_id");
    let chunk_id = first_text(
        kit,
        &format!("SELECT id AS n FROM sg_chunks WHERE source_id = '{source_id}'"),
    );
    Seeded {
        api_key,
        tenant_id,
        chunk_id,
    }
}

/// Puts a tenant on a plan and records its own token ceiling, as an
/// operator would by SQL — there is no admin route for either table yet.
/// A `None` limit is written as NULL, the only meaning of "unlimited".
fn seed_plan(
    kit: &TestHarness,
    tenant_id: &str,
    plan_id: &str,
    conversations_per_month: Option<u64>,
    daily_token_ceiling: Option<u64>,
) {
    let limit =
        |value: Option<u64>| value.map_or_else(|| "NULL".to_owned(), |value| value.to_string());
    for sql in [
        format!(
            "INSERT INTO sg_plans (plan_id, name, conversations_per_month, created_at, updated_at) \
             VALUES ('{plan_id}', '{plan_id}', {}, '2027-01-01T00:00:00Z', '2027-01-01T00:00:00Z')",
            limit(conversations_per_month),
        ),
        format!(
            "INSERT INTO sg_tenant_plan (tenant_id, plan_id, daily_token_ceiling, updated_at) \
             VALUES ('{tenant_id}', '{plan_id}', {}, '2027-01-01T00:00:00Z')",
            limit(daily_token_ceiling),
        ),
    ] {
        pollster::block_on(kit.db.execute(&Statement::new(sql))).expect("plan row inserts");
    }
}

async fn post_message(
    kit: &TestHarness,
    api_key: &str,
    message: &str,
    conversation_id: Option<&str>,
) -> Reply {
    let mut body = json!({ "message": message });
    if let Some(id) = conversation_id {
        body["conversation_id"] = json!(id);
    }
    send(
        &kit.router,
        Method::POST,
        MESSAGES,
        Some(api_key),
        Some(&body.to_string()),
    )
    .await
}

/// `post_message`, asserting a 200.
async fn turn(kit: &TestHarness, seeded: &Seeded) -> Value {
    let reply = post_message(kit, &seeded.api_key, QUESTION, None).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    reply.body
}

/// One non-negative count out of a query whose single integer cell is
/// named `n`; `0` for an aggregate over no rows.
fn one(kit: &TestHarness, sql: &str) -> u64 {
    let rows = pollster::block_on(kit.db.query(&Statement::new(sql))).expect("query runs");
    let n = rows
        .rows
        .first()
        .and_then(|row| row.get::<i64>("n"))
        .unwrap_or(0);
    u64::try_from(n).expect("counts are non-negative")
}

/// The first row's only text cell. SQL here is the test's own, never a
/// request's.
fn first_text(kit: &TestHarness, sql: &str) -> String {
    let rows = pollster::block_on(kit.db.query(&Statement::new(sql))).expect("query runs");
    rows.rows
        .first()
        .map(|row| row.get::<String>("n").expect("text value"))
        .expect("one row")
}

fn rows_in(kit: &TestHarness, table: &str) -> u64 {
    one(kit, &format!("SELECT COUNT(*) AS n FROM {table}"))
}

/// What one meter has recorded for the tenant under test, `0` when it
/// has no row: a tenant that has spent nothing has no row, which is the
/// same answer. `MAX` and not `SUM` so the cell stays an 8-byte integer
/// on both engines.
fn metered(kit: &TestHarness, tenant_id: &str, meter: &str) -> u64 {
    one(
        kit,
        &format!(
            "SELECT COALESCE(MAX(used), 0) AS n FROM sg_usage \
             WHERE subject = '{tenant_id}' AND meter = '{meter}'",
        ),
    )
}

/// `join_all` without a `futures` dependency (there is none in the
/// workspace, and `--locked` forbids adding one): polls every future to
/// completion on the one task `pollster` runs, so N turns really do
/// interleave at their own `.await` points instead of running one after
/// another. Results come back in completion order, which a test asking
/// only how many finished must not depend on.
async fn join_all<F: Future>(futures: Vec<F>) -> Vec<F::Output> {
    let mut pending: Vec<Pin<Box<F>>> = futures.into_iter().map(Box::pin).collect();
    let done = std::cell::RefCell::new(Vec::new());
    poll_fn(|cx| {
        let mut index = 0;
        while index < pending.len() {
            if let Poll::Ready(value) = pending[index].as_mut().poll(cx) {
                done.borrow_mut().push(value);
                pending.swap_remove(index);
            } else {
                index += 1;
            }
        }
        if pending.is_empty() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    done.into_inner()
}

/// K new-conversation turns fired at once, as the futures they are.
fn concurrent_turns(
    kit: &TestHarness,
    api_key: &str,
    count: usize,
) -> Vec<impl Future<Output = Reply>> {
    (0..count)
        .map(|_| async move { post_message(kit, api_key, QUESTION, None).await })
        .collect()
}

/// How many of `replies` answered with `status`.
fn status_count(replies: &[Reply], status: StatusCode) -> usize {
    replies
        .iter()
        .filter(|reply| reply.status == status)
        .count()
}

/// The plan's monthly allowance is a hard `402` on the turn that would
/// open the next conversation — and nothing is spent to find that out:
/// the model is never asked, and no conversation row is written.
#[pollster::test]
async fn a_spent_conversation_allowance_is_402_before_the_model_is_asked() {
    for (kit, model) in model_kits() {
        let seeded = seed(&kit, "Quota").await;
        seed_plan(&kit, &seeded.tenant_id, "solo", Some(1), None);
        model.set_mode(reply(&seeded.chunk_id, (100, 50)));

        // The one conversation the plan allows.
        assert!(turn(&kit, &seeded).await["conversation_id"].is_string());
        assert_eq!(metered(&kit, &seeded.tenant_id, CONVERSATIONS), 1);
        let asked_once = model.prompts().len();
        assert_eq!(asked_once, 1, "the allowed turn asked the model");

        // The second one opens a new conversation and is refused.
        let refused = post_message(&kit, &seeded.api_key, QUESTION, None).await;
        assert_eq!(
            refused.status,
            StatusCode::PAYMENT_REQUIRED,
            "{}",
            refused.body
        );
        assert_eq!(refused.problem_type(), format!("{PROBLEMS}quota-exceeded"));
        // The refusal names its meter and numbers as RFC 9457 §3.2
        // extension members, so a billing integration reads it as data.
        assert_eq!(refused.body["meter"], CONVERSATIONS);
        assert_eq!(refused.body["used"], 1);
        assert_eq!(refused.body["limit"], 1);
        assert_eq!(
            model.prompts().len(),
            asked_once,
            "the refused turn must not reach the model",
        );
        assert_eq!(rows_in(&kit, "sg_conversations"), 1, "no conversation row");
        assert_eq!(
            metered(&kit, &seeded.tenant_id, CONVERSATIONS),
            1,
            "a refused turn spends nothing",
        );
    }
}

/// The allowance is on *opening* a conversation. A customer already
/// talking to the bot is never cut off mid-conversation by it, however
/// many turns that conversation has already had.
#[pollster::test]
async fn a_spent_conversation_allowance_does_not_block_a_continuing_turn() {
    for (kit, model) in model_kits() {
        let seeded = seed(&kit, "Continuing").await;
        seed_plan(&kit, &seeded.tenant_id, "solo", Some(1), None);
        model.set_mode(reply(&seeded.chunk_id, (100, 50)));

        let conversation_id = turn(&kit, &seeded).await["conversation_id"]
            .as_str()
            .expect("conversation_id")
            .to_owned();
        // Still at the plan's limit — and the follow-up is answered.
        let follow_up = send(
            &kit.router,
            Method::POST,
            MESSAGES,
            Some(&seeded.api_key),
            Some(
                &json!({
                    "message": "And how do I change my email?",
                    "conversation_id": conversation_id,
                })
                .to_string(),
            ),
        )
        .await;
        assert_eq!(follow_up.status, StatusCode::OK, "{}", follow_up.body);
        assert_eq!(follow_up.body["conversation_id"], conversation_id.as_str());
        assert_eq!(
            metered(&kit, &seeded.tenant_id, CONVERSATIONS),
            1,
            "a continuing turn opens no conversation, so it spends nothing",
        );
        assert_eq!(rows_in(&kit, "sg_conversations"), 1);
    }
}

/// The guarded increment loses nothing under concurrency: K turns at
/// once leave the meter at exactly K, and the tokens at exactly K times
/// what one completion cost.
#[pollster::test]
async fn concurrent_new_conversations_each_count_once() {
    const K: usize = 8;
    const PER_TURN: (u64, u64) = (120, 30);
    for (kit, model) in model_kits() {
        // No plan row at all: unbounded, which is the case a lost
        // increment would be invisible against a ceiling.
        let seeded = seed(&kit, "Concurrent").await;
        model.set_mode(reply(&seeded.chunk_id, PER_TURN));

        let replies = join_all(concurrent_turns(&kit, &seeded.api_key, K)).await;
        assert_eq!(
            status_count(&replies, StatusCode::OK),
            K,
            "every turn answered"
        );
        assert_eq!(rows_in(&kit, "sg_conversations"), K as u64);
        assert_eq!(metered(&kit, &seeded.tenant_id, CONVERSATIONS), K as u64);
        assert_eq!(
            metered(&kit, &seeded.tenant_id, TOKENS),
            K as u64 * (PER_TURN.0 + PER_TURN.1),
            "every turn's tokens counted exactly once",
        );
    }
}

/// The same race at the limit: the guard lets exactly `L` turns through.
/// The losers are not overspend — they are turns that rolled back whole,
/// reported as the `402` the pre-model check would have given, with the
/// meter left at exactly `L`.
#[pollster::test]
async fn a_conversation_race_at_the_limit_admits_exactly_the_allowance() {
    const LIMIT: usize = 3;
    const OVER: usize = 3;
    for (kit, model) in model_kits() {
        let seeded = seed(&kit, "Race").await;
        seed_plan(&kit, &seeded.tenant_id, "tight", Some(LIMIT as u64), None);
        model.set_mode(reply(&seeded.chunk_id, (10, 5)));

        let replies = join_all(concurrent_turns(&kit, &seeded.api_key, LIMIT + OVER)).await;
        assert_eq!(status_count(&replies, StatusCode::OK), LIMIT);
        assert_eq!(
            status_count(&replies, StatusCode::PAYMENT_REQUIRED),
            OVER,
            "a refused turn answers 402, not 500",
        );
        assert_eq!(rows_in(&kit, "sg_conversations"), LIMIT as u64);
        assert_eq!(
            metered(&kit, &seeded.tenant_id, CONVERSATIONS),
            LIMIT as u64
        );
    }
}

/// A runaway loop is what the daily token ceiling is for. It arrives as
/// a `429` naming its meter, carrying `Retry-After` to the next UTC
/// midnight, and the model is not asked again after it.
#[pollster::test]
async fn a_runaway_loop_hits_the_daily_token_ceiling_and_stops_asking() {
    const CEILING: u64 = 1000;
    const PER_COMPLETION: u64 = 300;
    // The loop cannot overshoot the ceiling by more than one completion,
    // so the ceiling must bite by the turn after the meter passes it.
    let max_turns = CEILING.div_ceil(PER_COMPLETION) + 1;
    for (kit, model) in model_kits() {
        // No plan limit on conversations — only the ceiling, so this is
        // the meter and nothing else doing the stopping.
        let seeded = seed(&kit, "Runaway").await;
        seed_plan(&kit, &seeded.tenant_id, "meter-only", None, Some(CEILING));
        model.set_mode(reply(&seeded.chunk_id, (150, 150)));

        let mut refused = None;
        for turn_number in 1..=max_turns {
            let sent = post_message(&kit, &seeded.api_key, QUESTION, None).await;
            if sent.status == StatusCode::OK {
                continue;
            }
            assert_eq!(
                turn_number, max_turns,
                "the ceiling must bite by turn {max_turns}, not later",
            );
            refused = Some(sent);
            break;
        }
        let refused = refused.expect("the ceiling is reached");
        assert_eq!(
            refused.status,
            StatusCode::TOO_MANY_REQUESTS,
            "{}",
            refused.body
        );
        assert_eq!(
            refused.problem_type(),
            format!("{PROBLEMS}token-ceiling-reached")
        );
        assert_eq!(refused.body["meter"], TOKENS);
        assert_eq!(refused.body["limit"], CEILING);
        assert!(
            refused.body["used"]
                .as_u64()
                .is_some_and(|used| used >= CEILING),
            "the meter is at or over the ceiling: {}",
            refused.body,
        );

        // `Retry-After` is the pause to the next UTC midnight: a day, on
        // the kit's fixed clock, less the second already spent in it.
        let retry = refused
            .headers
            .get(header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or_else(|| panic!("Retry-After is present and numeric: {}", refused.body));
        assert!(
            (1..=86_400).contains(&retry),
            "Retry-After is seconds to the next UTC midnight, got {retry}",
        );

        // And the loop is genuinely stopped: no further model calls, and
        // no further tokens counted.
        let asked = model.prompts().len();
        let spent = metered(&kit, &seeded.tenant_id, TOKENS);
        for _ in 0..3 {
            let after = post_message(&kit, &seeded.api_key, QUESTION, None).await;
            assert_eq!(after.status, StatusCode::TOO_MANY_REQUESTS);
        }
        assert_eq!(
            model.prompts().len(),
            asked,
            "the ceiling is checked before the model, so the loop stops spending",
        );
        assert_eq!(metered(&kit, &seeded.tenant_id, TOKENS), spent);
    }
}

/// The tokens are owed the moment the model answers, whatever the turn
/// then does with them. A reply the schema rejects is a `502` and writes
/// no conversation — but the provider was still paid, so the meter moves.
#[pollster::test]
async fn a_reply_the_schema_rejects_still_moves_the_token_meter() {
    const COST: u64 = 500;
    for (kit, model) in model_kits() {
        let seeded = seed(&kit, "Garbage").await;
        model.set_mode(TextModelMode::Complete(
            Completion::new("{}", "fake-fast")
                .json(json!({ "verdict": "maybe", "sources": [] }))
                .usage(COST, 0),
        ));

        let bad = post_message(&kit, &seeded.api_key, QUESTION, None).await;
        assert_eq!(bad.status, StatusCode::BAD_GATEWAY, "{}", bad.body);
        assert_eq!(rows_in(&kit, "sg_conversations"), 0, "nothing was written");
        assert_eq!(
            metered(&kit, &seeded.tenant_id, TOKENS),
            COST,
            "the completion was paid for, so the meter counts it anyway",
        );
        // The conversation allowance was never spent by a turn that
        // opened no conversation.
        assert_eq!(metered(&kit, &seeded.tenant_id, CONVERSATIONS), 0);
    }
}
