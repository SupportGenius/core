//! `POST /messages` — one support turn — and the admin route that sets a
//! tenant's answer threshold.
//!
//! The turn's ordering is the point: conversation load, clarify count,
//! retrieval and the model call all happen before the first write, so
//! every failure — a model that is not configured, one that is down, one
//! that answered garbage — leaves the conversation exactly as it was. Only
//! a decided turn reaches the single `batch_atomic` at the end.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::{Value, json};
use text_model::{Completion, ModelTier, Prompt, TextModel, TextModelError};

use cratefield_core::{Clock, Database, IdGen, Json, Problem, ProblemDef, Scope, require_admin};

use crate::answer::{self, DEFAULT_ANSWER_THRESHOLD, ModelReply, Outcome};
use crate::handlers::{
    ModuleState, Retrieved, authenticate, guard_rate_limit, required_port, retrieve,
};
use crate::store::{self, ConversationRow};

/// How many chunks a turn retrieves and shows the model. Six passages of
/// the default chunk size fit comfortably in a fast-tier context window
/// and still leave the model a choice of what to cite.
pub(crate) const TOP_K: usize = 6;

/// The longest message the route accepts, in characters. A support
/// question that needs more than this is an email, not a chat turn.
pub(crate) const MAX_MESSAGE_CHARS: usize = 4000;

/// The model's output ceiling for one turn: an answer plus a few quotes.
const MAX_OUTPUT_TOKENS: u32 = 1024;

/// `Retry-After` for a transient model failure that named no pause of its
/// own. Short and honest: an unavailable model is usually back in
/// seconds, and an exact number would be a promise nobody can keep.
const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(2);

const CLARIFY_MESSAGE: &str = "I want to give you an accurate answer rather than a fast wrong \
     one — could you rephrase the question or add a little more detail?";
const HANDOFF_MESSAGE: &str = "I could not answer this confidently, so I have passed your \
     question to a person who can. You will hear back here.";

/// 503: no model is wired into this deployment at all. Permanent until
/// the operator acts — retrying without changing anything will not help,
/// which is why this one carries no `Retry-After`.
const TEXT_MODEL_NOT_CONFIGURED: ProblemDef = ProblemDef {
    slug: "text-model-not-configured",
    status: StatusCode::SERVICE_UNAVAILABLE,
    title: "Text model is not configured",
    description: "No text model is wired into this deployment; answers cannot be produced until \
                  the operator configures one. Nothing was written.",
};

/// 503: the model could not answer right now. Retryable — the response
/// carries `Retry-After`, and nothing was written, so the same request
/// may simply be sent again.
const TEXT_MODEL_UNAVAILABLE: ProblemDef = ProblemDef {
    slug: "text-model-unavailable",
    status: StatusCode::SERVICE_UNAVAILABLE,
    title: "Text model is temporarily unavailable",
    description: "The model could not answer right now; the turn was not consumed and the same \
                  request may be retried after Retry-After.",
};

/// 502: the model call failed or its answer is unusable — refused, lost in
/// transport, or not the required schema. A bad answer is the *model's*
/// failure, not this service's, hence bad gateway.
const TEXT_MODEL_BAD_ANSWER: ProblemDef = ProblemDef {
    slug: "text-model-invalid-answer",
    status: StatusCode::BAD_GATEWAY,
    title: "Text model returned an unusable answer",
    description: "The model call failed or its answer did not match the required schema; \
                  nothing was written.",
};

/// The system prompt: what the model may ground on, what it must cite,
/// and the shape it must answer in.
const SYSTEM_PROMPT: &str = "You answer customer-support questions using only the retrieved \
     context you are given. Each context passage starts with its chunk id in square brackets. \
     Cite only chunk ids that appear in the retrieved context, quoting the exact span that \
     grounds each claim — never invent a chunk id. If the context does not contain the answer, \
     say so instead of guessing. Answer with JSON matching the provided schema: `answer` (your \
     reply), `citations` (chunk_id + quote pairs), and `confidence` in 0.0..=1.0, your own \
     confidence that the answer is correct and fully grounded.";

#[derive(Deserialize)]
struct MessageBody {
    message: String,
    conversation_id: Option<String>,
}

#[derive(Deserialize)]
struct SettingsBody {
    answer_threshold: f32,
}

/// `POST /messages` — `{"message": "…", "conversation_id": "…"?}`. Answers
/// `{conversation_id, message_id, outcome, answer, citations, confidence,
/// needs_escalation}`, where `outcome` is `answered`, `clarify` or
/// `handoff` (see [`answer::decide`]).
pub(crate) async fn post_message(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, Problem> {
    let ctx = &state.ctx;
    // Auth before the body, as every other route here: an extractor 4xx
    // would tell an unauthenticated caller the route exists.
    let tenant_id = authenticate(ctx, &headers).await?;
    if let Some(rate_limited) = guard_rate_limit(ctx, &tenant_id).await {
        return Ok(rate_limited);
    }
    let body = parse_message(&body).map_err(|problem| problem.instance(&scope.request_id))?;
    let message = body.message.as_str();

    let clock: &dyn Clock = required_port(ctx.ports.clock.as_deref(), "Clock")?;
    let id_gen: &dyn IdGen = required_port(ctx.ports.id_gen.as_deref(), "IdGen")?;
    let db: &dyn Database = required_port(ctx.ports.db.as_deref(), "Db")?;

    // 1. The conversation, scoped to the tenant: unknown and foreign are
    //    the same 404, answered before the model is ever asked.
    let conversation = match body.conversation_id.as_deref() {
        Some(id) => Some(
            store::find_conversation(db, &tenant_id, id)
                .await?
                .ok_or_else(|| Problem::not_found().instance(&scope.request_id))?,
        ),
        None => None,
    };
    // 2. The clarify budget already spent, and the message count that
    //    orders this turn's two messages. Reads: nothing is written until
    //    the turn is decided, so a failed call below cannot spend either.
    let counts = match &conversation {
        Some(conversation) => store::conversation_counts(db, &tenant_id, &conversation.id).await?,
        None => store::ConversationCounts {
            messages: 0,
            clarifies: 0,
        },
    };

    // 3. Retrieve — the same BM25 path `GET /search` uses.
    let chunks = retrieve(db, &tenant_id, message, TOP_K).await?;

    // 4. The tenant's threshold, else the documented default.
    let threshold = store::tenant_threshold_pct(db, &tenant_id)
        .await?
        .map_or(DEFAULT_ANSWER_THRESHOLD, answer::pct_confidence);

    // 5. Ask the model. Nothing has been written yet, which is what makes
    //    every failure here consume nothing.
    let prompt = build_prompt(&chunks, message);
    let completion = match ask(state.text_model.as_deref(), &prompt, &scope).await {
        Ok(completion) => completion,
        Err(response) => return Ok(*response),
    };

    // 6. Parse. A reply that is not the schema is a bad gateway, and
    //    still nothing written.
    let mut reply = answer::parse_reply(&completion)
        .map_err(|_| Problem::new(&TEXT_MODEL_BAD_ANSWER).instance(&scope.request_id))?;
    // The threshold compares at the stored grain (whole percent), so the
    // decision and the row it writes can never disagree.
    let confidence_pct = answer::confidence_pct(reply.confidence);
    reply.confidence = answer::pct_confidence(confidence_pct);

    // 7. Decide.
    let retrieved_ids: Vec<&str> = chunks.iter().map(|hit| hit.chunk.id.as_str()).collect();
    let decision = answer::decide(&reply, &retrieved_ids, threshold, counts.clarifies);

    // 8. Write the turn — conversation (create or update) plus both
    //    messages — in one `batch_atomic`.
    let now = store::iso_now(clock);
    let conversation_id = conversation
        .as_ref()
        .map_or_else(|| id_gen.ulid(), |conversation| conversation.id.clone());
    // The user message's id first: it is the earlier of the two.
    let user_message_id = id_gen.ulid();
    let assistant_message_id = id_gen.ulid();
    // A handoff escalates; once escalated, a conversation stays escalated
    // — an answered follow-up does not un-escalate the ticket behind it.
    // Only an escalating turn writes the flag (`store::Turn::escalates`),
    // so this response value is what the stored flag is at least.
    let escalates = decision.outcome == Outcome::Handoff;
    let needs_escalation = escalates
        || conversation
            .as_ref()
            .is_some_and(|conversation: &ConversationRow| conversation.needs_escalation);
    let (shown, shown_citations) = match decision.outcome {
        Outcome::Answered => (reply.answer.clone(), decision.citations.clone()),
        // A canned message and no citations: never publish an answer the
        // module would not stand behind. The model's own words still go to
        // `model_answer` below — what the user saw and what the model said
        // deliberately differ on a downgraded turn.
        Outcome::Clarify => (CLARIFY_MESSAGE.to_owned(), Vec::new()),
        Outcome::Handoff => (HANDOFF_MESSAGE.to_owned(), Vec::new()),
    };
    let turn = store::Turn {
        conversation_id: conversation_id.clone(),
        tenant_id: tenant_id.clone(),
        conversation_existed: conversation.is_some(),
        escalates,
        now,
        user_seq: counts.messages,
        user_message_id,
        user_message: message.to_owned(),
        assistant_message_id: assistant_message_id.clone(),
        assistant_body: shown.clone(),
        model_answer: reply.answer.clone(),
        outcome: decision.outcome.as_str().to_owned(),
        confidence_pct,
        citations_json: citations_json(&reply),
    };
    // A handoff's `Escalation::intake().handoff(...)` statements belong in
    // this same batch once module-escalation is composed alongside.
    db.batch_atomic(&store::turn_statements(&turn)).await?;

    let citations: Vec<Value> = shown_citations
        .iter()
        .map(|citation| json!({ "chunk_id": citation.chunk_id, "quote": citation.quote }))
        .collect();
    Ok(Json(json!({
        "conversation_id": conversation_id,
        "message_id": assistant_message_id,
        "outcome": decision.outcome.as_str(),
        "answer": shown,
        "citations": citations,
        // The stored percentage, divided in f64 so 90 reads back as 0.9.
        "confidence": answer::pct_confidence_f64(confidence_pct),
        "needs_escalation": needs_escalation,
    }))
    .into_response())
}

/// The request body, with `message` trimmed and length-checked.
fn parse_message(body: &[u8]) -> Result<MessageBody, Problem> {
    let mut body: MessageBody = serde_json::from_slice(body).map_err(|_| {
        Problem::validation_failed(
            "body: expected a JSON object with a string \"message\" and an optional string \
             \"conversation_id\"",
        )
    })?;
    let trimmed = body.message.trim();
    if trimmed.is_empty() || trimmed.chars().count() > MAX_MESSAGE_CHARS {
        return Err(Problem::validation_failed(format!(
            "message: required, 1..={MAX_MESSAGE_CHARS} characters"
        )));
    }
    body.message = trimmed.to_owned();
    Ok(body)
}

/// Calls the model, mapping every failure to the finished response: no
/// model or `NotConfigured` is `503 text-model-not-configured`,
/// `Transient` the retryable `503` with `Retry-After`, and a refusal or a
/// transport failure `502`.
async fn ask(
    model: Option<&dyn TextModel>,
    prompt: &Prompt,
    scope: &Scope,
) -> Result<Completion, Box<Response>> {
    let problem = |def: &ProblemDef| {
        Box::new(
            Problem::new(def)
                .instance(&scope.request_id)
                .into_response(),
        )
    };
    let Some(model) = model else {
        return Err(problem(&TEXT_MODEL_NOT_CONFIGURED));
    };
    match model.complete(prompt).await {
        Ok(completion) => Ok(completion),
        Err(TextModelError::NotConfigured) => Err(problem(&TEXT_MODEL_NOT_CONFIGURED)),
        Err(err @ TextModelError::Transient { .. }) => {
            Err(Box::new(unavailable(scope, err.retry_after())))
        }
        Err(TextModelError::Rejected(_) | TextModelError::Transport(_)) => {
            Err(problem(&TEXT_MODEL_BAD_ANSWER))
        }
    }
}

/// The model request: every retrieved chunk as `[chunk_id] body`, so each
/// citation can be checked against exactly what the model was shown, and
/// the hand-written reply schema.
fn build_prompt(chunks: &[Retrieved], question: &str) -> Prompt {
    let mut context = String::new();
    for hit in chunks {
        // Writing into a `String` cannot fail.
        let _ = writeln!(context, "[{}] {}", hit.chunk.id, hit.chunk.body);
    }
    if context.is_empty() {
        context.push_str("(nothing was retrieved for this question)\n");
    }
    Prompt::new(ModelTier::Fast)
        .system(SYSTEM_PROMPT)
        .user(format!(
            "Question:\n{question}\n\nRetrieved context — cite only these chunk ids:\n{context}"
        ))
        .json_schema(answer::reply_schema())
        .max_tokens(MAX_OUTPUT_TOKENS)
}

/// The model's raw citations, stored whatever the outcome.
fn citations_json(reply: &ModelReply) -> String {
    serde_json::to_string(&reply.citations).unwrap_or_else(|_| "[]".to_owned())
}

/// The retryable 503. `Problem` carries no headers, so `Retry-After` is
/// added on the built response, the way `cratefield_core::rate_limited`
/// does it.
fn unavailable(scope: &Scope, retry_after: Option<Duration>) -> Response {
    let mut response = Problem::new(&TEXT_MODEL_UNAVAILABLE)
        .instance(&scope.request_id)
        .into_response();
    // Rounded up: a 1.5 s pause told as "1" would invite a retry that is
    // still too early.
    let pause = retry_after.unwrap_or(DEFAULT_RETRY_AFTER);
    let secs = (pause.as_secs() + u64::from(pause.subsec_nanos() > 0)).max(1);
    if let Ok(value) = HeaderValue::from_str(&secs.to_string()) {
        response.headers_mut().insert(header::RETRY_AFTER, value);
    }
    response
}

/// `PUT /admin/tenants/{tenant_id}/settings` — `{"answer_threshold": 0.9}`
/// sets the tenant's answer threshold (0.0..=1.0, stored as a whole
/// percentage). Guarded by the harness admin token, exactly like
/// `POST /admin/tenants`.
pub(crate) async fn put_settings(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    Path(tenant_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, Problem> {
    // The admin guard runs before the body is parsed; see `create_tenant`.
    require_admin(&*state.ctx.config, &headers)?;
    let body: SettingsBody = serde_json::from_slice(&body).map_err(|_| {
        Problem::validation_failed(
            "body: expected a JSON object with a number \"answer_threshold\"",
        )
        .instance(&scope.request_id)
    })?;
    // `contains` is false for NaN, so this also rejects it.
    if !(0.0..=1.0).contains(&body.answer_threshold) {
        return Err(
            Problem::validation_failed("answer_threshold: a number in 0.0..=1.0")
                .instance(&scope.request_id),
        );
    }

    let ctx = &state.ctx;
    let clock: &dyn Clock = required_port(ctx.ports.clock.as_deref(), "Clock")?;
    let db: &dyn Database = required_port(ctx.ports.db.as_deref(), "Db")?;
    if store::find_tenant(db, &tenant_id).await?.is_none() {
        return Err(Problem::not_found().instance(&scope.request_id));
    }
    let pct = answer::confidence_pct(body.answer_threshold);
    store::upsert_tenant_threshold(db, &tenant_id, pct, &store::iso_now(clock)).await?;

    Ok(Json(json!({
        "tenant_id": tenant_id,
        "answer_threshold": answer::pct_confidence_f64(pct),
    }))
    .into_response())
}
