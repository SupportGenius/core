//! The staff routes (issue #35): an inbox of conversations waiting for a
//! person, taking one over, replying as an agent, handing it back — and
//! saving a reply as a reviewed source so the bot answers the same
//! question next time. All four require a *staff* key: one minted with
//! `POST /keys {"staff_id": …}`, never a customer or integration key.
//!
//! The routes share the `state` machine a conversation carries
//! (`bot | waiting_for_human | human | closed`, see [`crate::store`]): a
//! customer turn answers automatically only while the state is `bot` or
//! `waiting_for_human`, a person answers while it is `human`, and a closed
//! conversation is read-only.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query, RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::{Value, json};

use cratefield_core::{Clock, Database, IdGen, Json, Problem, ProblemDef, Scope};

use crate::chunk::Chunker;
use crate::handlers::{ModuleState, Principal, authenticate, guard_rate_limit, required_port};
use crate::store::{self, SourceRow};
use crate::widget::transcript_json;

/// `GET /inbox` returns at most this many conversations. The queue is
/// worked front to back, not paged; a tenant with more than this waiting
/// has a staffing problem a longer list would not fix.
pub(crate) const MAX_INBOX: usize = 100;

/// The four states a conversation can be in, as the inbox's `?state=…`
/// accepts them. `status`/`needs_escalation` are the monotonic escalation
/// record; `state` is who answers next.
const STATES: [&str; 4] = [
    store::STATE_BOT,
    store::STATE_WAITING,
    store::STATE_HUMAN,
    store::STATE_CLOSED,
];

/// The longest title a saved correction gets, in characters, before it is
/// ellipsised — a title, not the question.
const TITLE_MAX_CHARS: usize = 80;

/// 403: the key is valid but belongs to no staff member — a customer or
/// integration key, which may not use the staff routes.
pub(crate) const NOT_STAFF: ProblemDef = ProblemDef {
    slug: "not-staff",
    status: StatusCode::FORBIDDEN,
    title: "Staff key required",
    description: "This route is only available to a support key minted with a \"staff_id\"; \
                  the key presented belongs to no staff member.",
};

/// 409: the conversation is not in the state this request needs — a
/// takeover of one a person already holds, or a reply to or handback of a
/// conversation that is not currently held.
const CONFLICT: ProblemDef = ProblemDef {
    slug: "conversation-state-conflict",
    status: StatusCode::CONFLICT,
    title: "Conversation is not in a state this request may change",
    description: "The conversation is not in the state the request needs — for example a \
                  takeover of one a person already holds, or a reply to a conversation handed \
                  back to the bot or closed.",
};

/// 403: a staff key is valid but is not the assignee of the conversation.
const NOT_ASSIGNEE: ProblemDef = ProblemDef {
    slug: "not-assignee",
    status: StatusCode::FORBIDDEN,
    title: "Not the assignee of this conversation",
    description: "Only the staff member a conversation is assigned to may reply to it or hand \
                  it back.",
};

/// The staff id a request acts as, or the 403 a key without one gets. The
/// key has already authenticated ([`authenticate`]); this only asks
/// whether it belongs to a person.
pub(crate) fn require_staff(principal: &Principal) -> Result<&str, Problem> {
    principal
        .staff_id
        .as_deref()
        .ok_or_else(|| Problem::new(&NOT_STAFF))
}

/// What a staff route runs on once its preamble has passed: the Db port,
/// the tenant, and the staff member.
struct Caller<'a> {
    db: &'a dyn Database,
    tenant_id: String,
    staff_id: String,
}

/// The staff preamble's outcome: the caller to serve, or the response to
/// send instead (a `401`, `403` or `429` from a guard). An enum, not a
/// `Result`, so the large `Response` is not carried as an `Err` — the
/// same shape as [`crate::handlers`]'s `Authed`.
enum StaffAuth<'a> {
    Ready(Caller<'a>),
    Done(Response),
}

/// The staff preamble: authenticate, require a staff id, rate-limit the
/// tenant, and hand back the Db port.
async fn authorize_staff<'a>(state: &'a ModuleState, headers: &HeaderMap) -> StaffAuth<'a> {
    let principal = match authenticate(&state.ctx, headers).await {
        Ok(principal) => principal,
        Err(problem) => return StaffAuth::Done(problem.into_response()),
    };
    let staff_id = match require_staff(&principal) {
        Ok(staff_id) => staff_id.to_owned(),
        Err(problem) => return StaffAuth::Done(problem.into_response()),
    };
    if let Some(rate_limited) = guard_rate_limit(&state.ctx, &principal.tenant_id).await {
        return StaffAuth::Done(rate_limited);
    }
    match required_port(state.ctx.ports.db.as_deref(), "Db") {
        Ok(db) => StaffAuth::Ready(Caller {
            db,
            tenant_id: principal.tenant_id,
            staff_id,
        }),
        Err(problem) => StaffAuth::Done(problem.into_response()),
    }
}

/// The conversation as the staff routes return it: its state, its
/// assignee, and the whole transcript in the widget's message shape.
async fn conversation_response(
    db: &dyn Database,
    tenant_id: &str,
    conversation: &store::ConversationRow,
) -> Result<Response, Problem> {
    let messages = store::conversation_messages(db, tenant_id, &conversation.id).await?;
    Ok(Json(json!({
        "conversation_id": conversation.id,
        "state": conversation.state,
        "assignee": conversation.assignee,
        "needs_escalation": conversation.needs_escalation,
        "messages": transcript_json(&messages),
    }))
    .into_response())
}

#[derive(Deserialize, Default)]
struct InboxQuery {
    state: Option<String>,
}

/// `GET /inbox?state=…` — the tenant's conversations in one state,
/// most recently active first, at most [`MAX_INBOX`]. The state defaults
/// to `waiting_for_human` (the queue a person works) and must be one of
/// the four.
pub(crate) async fn inbox(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let staff = match authorize_staff(&state, &headers).await {
        StaffAuth::Ready(staff) => staff,
        StaffAuth::Done(done) => return Ok(done),
    };
    // The query string is parsed here — after the guards, not by an
    // extractor before them — so a malformed `state` earns the 401/403/429
    // any other request would, and only an authorized caller sees the 400.
    let bad_query = || {
        Problem::validation_failed(format!("query: state must be one of {}", STATES.join(", ")))
            .instance(&scope.request_id)
    };
    let query = match query.as_deref() {
        None | Some("") => InboxQuery::default(),
        Some(raw) => {
            let uri: http::Uri = format!("https://support.local/?{raw}")
                .parse()
                .map_err(|_| bad_query())?;
            Query::<InboxQuery>::try_from_uri(&uri)
                .map_err(|_| bad_query())?
                .0
        }
    };
    let wanted = query
        .state
        .unwrap_or_else(|| store::STATE_WAITING.to_owned());
    if !STATES.contains(&wanted.as_str()) {
        return Err(bad_query());
    }
    let rows = store::list_inbox(staff.db, &staff.tenant_id, &wanted, MAX_INBOX).await?;
    let conversations: Vec<Value> = rows
        .iter()
        .map(|row| {
            json!({
                "conversation_id": row.id,
                "state": row.state,
                "assignee": row.assignee,
                "updated_at": row.updated_at,
                "needs_escalation": row.needs_escalation,
            })
        })
        .collect();
    Ok(Json(json!({ "conversations": conversations })).into_response())
}

/// `POST /conversations/{id}/takeover` — a staff member claims a
/// conversation that is the bot's or waiting for a person, moving it to
/// `human` with themselves as assignee, and gets the whole transcript
/// back. Idempotent: a conversation the caller already holds is a `200`
/// no-op. One another person holds, or a closed one, is `409`; a
/// conversation of another tenant is `404`.
pub(crate) async fn takeover(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    Path(conversation_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let staff = match authorize_staff(&state, &headers).await {
        StaffAuth::Ready(staff) => staff,
        StaffAuth::Done(done) => return Ok(done),
    };
    let clock: &dyn Clock = required_port(state.ctx.ports.clock.as_deref(), "Clock")?;
    let not_found = || Problem::not_found().instance(&scope.request_id);

    let mut conversation = store::find_conversation(staff.db, &staff.tenant_id, &conversation_id)
        .await?
        .ok_or_else(not_found)?;
    // `bot` and `waiting_for_human` are open to a takeover; `human` (at any
    // assignee) and `closed` are not, and the conditional UPDATE refuses
    // them anyway. Re-read only when the UPDATE ran, so the response
    // reflects what actually stands.
    if !matches!(
        conversation.state.as_str(),
        store::STATE_HUMAN | store::STATE_CLOSED
    ) {
        store::take_over(
            staff.db,
            &staff.tenant_id,
            &conversation_id,
            &staff.staff_id,
            &store::iso_now(clock),
        )
        .await?;
        conversation = store::find_conversation(staff.db, &staff.tenant_id, &conversation_id)
            .await?
            .ok_or_else(not_found)?;
    }
    if conversation.state == store::STATE_HUMAN
        && conversation.assignee.as_deref() == Some(staff.staff_id.as_str())
    {
        return conversation_response(staff.db, &staff.tenant_id, &conversation).await;
    }
    Err(Problem::new(&CONFLICT).instance(&scope.request_id))
}

/// The body of `POST /conversations/{id}/reply`:
/// `{"body": "…", "save_as_answer"?: false}`.
#[derive(Deserialize)]
struct ReplyBody {
    body: String,
    #[serde(default)]
    save_as_answer: bool,
}

/// `POST /conversations/{id}/reply` — the assignee answers a conversation
/// they hold. The message is stored with the `staff` role, and appears in
/// the transcript the model reads after a handback. With `save_as_answer`,
/// the reply is also ingested as a reviewed source — the question it
/// answers plus the answer — so the bot answers the same question next
/// time without a handoff; the response then carries the new `source_id`.
pub(crate) async fn reply(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    Path(conversation_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, Problem> {
    let staff = match authorize_staff(&state, &headers).await {
        StaffAuth::Ready(staff) => staff,
        StaffAuth::Done(done) => return Ok(done),
    };
    // The body is parsed after the guards, like every key-guarded route: a
    // malformed body must not earn a 4xx that tells an unauthenticated
    // caller the route exists.
    let parsed: ReplyBody = serde_json::from_slice(&body).map_err(|_| {
        Problem::validation_failed(
            "body: expected a JSON object with a string \"body\" and an optional boolean \
             \"save_as_answer\"",
        )
        .instance(&scope.request_id)
    })?;
    let text = parsed.body.trim();
    if text.is_empty() || text.chars().count() > crate::messages::MAX_MESSAGE_CHARS {
        return Err(Problem::validation_failed(format!(
            "body: required, 1..={} characters",
            crate::messages::MAX_MESSAGE_CHARS
        ))
        .instance(&scope.request_id));
    }

    let not_found = || Problem::not_found().instance(&scope.request_id);
    let conversation = store::find_conversation(staff.db, &staff.tenant_id, &conversation_id)
        .await?
        .ok_or_else(not_found)?;
    // Only the assignee of a held conversation may reply; the state is a
    // conflict, a different assignee a plain 403.
    if conversation.state != store::STATE_HUMAN {
        return Err(Problem::new(&CONFLICT).instance(&scope.request_id));
    }
    if conversation.assignee.as_deref() != Some(staff.staff_id.as_str()) {
        return Err(Problem::new(&NOT_ASSIGNEE).instance(&scope.request_id));
    }

    let clock: &dyn Clock = required_port(state.ctx.ports.clock.as_deref(), "Clock")?;
    let id_gen: &dyn IdGen = required_port(state.ctx.ports.id_gen.as_deref(), "IdGen")?;
    let counts = store::conversation_counts(staff.db, &staff.tenant_id, &conversation_id).await?;
    let now = store::iso_now(clock);
    let message_id = id_gen.ulid();

    // The message and, when asked, the reviewed source it produces: one
    // atomic batch, so a saved correction can never exist without the reply
    // it came from (or vice versa).
    let mut statements = Vec::new();
    let mut source_id = None;
    if parsed.save_as_answer {
        // The question this answers: the most recent customer message
        // before this staff turn — the one the reply responds to.
        let question = store::last_user_message(
            staff.db,
            &staff.tenant_id,
            &conversation_id,
            counts.messages,
        )
        .await?
        .unwrap_or_default();
        let question = question.trim();
        let source_text = if question.is_empty() {
            text.to_owned()
        } else {
            format!("Question: {question}\n\nAnswer: {text}")
        };
        let id = id_gen.ulid();
        let source = SourceRow {
            id: id.clone(),
            tenant_id: staff.tenant_id.clone(),
            title: correction_title(question, text),
            url: None,
            external_id: None,
            byte_len: i64::try_from(source_text.len()).unwrap_or(i64::MAX),
            created_at: now.clone(),
            updated_at: now.clone(),
        };
        let chunks = Chunker::default().split(&staff.tenant_id, &id, &source_text);
        statements.extend(store::reviewed_source_with_chunks_statements(
            &source,
            &chunks,
            &staff.staff_id,
            &conversation_id,
        ));
        source_id = Some(id);
    }
    statements.push(store::staff_message_statement(&store::StaffMessage {
        id: message_id.clone(),
        conversation_id: conversation_id.clone(),
        tenant_id: staff.tenant_id.clone(),
        seq: counts.messages,
        body: text.to_owned(),
        author: staff.staff_id.clone(),
        created_at: now,
    }));
    staff.db.batch_atomic(&statements).await?;

    Ok(Json(json!({
        "conversation_id": conversation_id,
        "message_id": message_id,
        "role": store::ROLE_STAFF,
        "source_id": source_id,
    }))
    .into_response())
}

/// The body of `POST /conversations/{id}/handback`: `{"close"?: false}`.
#[derive(Deserialize, Default)]
struct HandbackBody {
    #[serde(default)]
    close: bool,
}

/// `POST /conversations/{id}/handback` — the assignee gives the
/// conversation back: to the bot by default, or closed with
/// `{"close": true}`. Either way the assignee is cleared and the bot
/// answers the next customer turn (or the conversation is read-only).
pub(crate) async fn handback(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    Path(conversation_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, Problem> {
    let staff = match authorize_staff(&state, &headers).await {
        StaffAuth::Ready(staff) => staff,
        StaffAuth::Done(done) => return Ok(done),
    };
    let parsed: HandbackBody = if body.is_empty() {
        HandbackBody::default()
    } else {
        serde_json::from_slice(&body).map_err(|_| {
            Problem::validation_failed(
                "body: expected a JSON object with an optional boolean \"close\"",
            )
            .instance(&scope.request_id)
        })?
    };
    let not_found = || Problem::not_found().instance(&scope.request_id);
    let conversation = store::find_conversation(staff.db, &staff.tenant_id, &conversation_id)
        .await?
        .ok_or_else(not_found)?;
    if conversation.state != store::STATE_HUMAN {
        return Err(Problem::new(&CONFLICT).instance(&scope.request_id));
    }
    if conversation.assignee.as_deref() != Some(staff.staff_id.as_str()) {
        return Err(Problem::new(&NOT_ASSIGNEE).instance(&scope.request_id));
    }
    let clock: &dyn Clock = required_port(state.ctx.ports.clock.as_deref(), "Clock")?;
    store::hand_back(
        staff.db,
        &staff.tenant_id,
        &conversation_id,
        &staff.staff_id,
        parsed.close,
        &store::iso_now(clock),
    )
    .await?;
    let state_name = if parsed.close {
        store::STATE_CLOSED
    } else {
        store::STATE_BOT
    };
    Ok(Json(json!({
        "conversation_id": conversation_id,
        "state": state_name,
        "assignee": Value::Null,
    }))
    .into_response())
}

/// The title for a correction saved from a reply: the question it answers,
/// whitespace collapsed and truncated to [`TITLE_MAX_CHARS`], or the
/// reply's opening when the conversation has no customer turn.
fn correction_title(question: &str, answer: &str) -> String {
    let base = if question.trim().is_empty() {
        answer
    } else {
        question
    };
    let collapsed = base.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut title: String = collapsed.chars().take(TITLE_MAX_CHARS).collect();
    if collapsed.chars().count() > TITLE_MAX_CHARS {
        title.push('…');
    }
    title
}

#[cfg(test)]
mod tests {
    use super::correction_title;

    #[test]
    fn a_corrections_title_is_the_question_trimmed_and_truncated() {
        assert_eq!(
            correction_title("  How do I reset my password?  ", "From settings."),
            "How do I reset my password?"
        );
        // Whitespace (newlines included) collapses; a long question is cut
        // with an ellipsis at the ceiling.
        assert_eq!(correction_title("a\n\n  b", "unused"), "a b");
        let long = "x".repeat(100);
        let title = correction_title(&long, "unused");
        assert_eq!(title.chars().count(), 81, "80 chars plus the ellipsis");
        assert!(title.ends_with('…'));
        // No customer turn: the reply's own words stand in.
        assert_eq!(correction_title("   ", "Ask billing."), "Ask billing.");
    }
}
