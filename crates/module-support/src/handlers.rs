//! HTTP handlers for `/v1/support`. The turn's ordering is the point:
//! conversation load, retrieval and the model call all happen before the
//! first write, so every failure — a model that is not configured, one
//! that is down, one that answered garbage — leaves the conversation
//! exactly as it was. Only a decided turn reaches
//! [`crate::store::record_turn`]'s single `batch_atomic`.

use std::fmt::Write as _;
use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Method, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{post, put};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;
use time::format_description::well_known::Rfc3339;

use cratefield_core::{
    Action, Audience, Clock, Database, IdGen, ModuleConfig, ModuleContext, Outcome, Problem,
    ProblemDef, Scope, Surface, SystemClock, TenantConn, UlidIdGen, require_admin, schema_for,
};

// `TurnOutcome` is the module's decision (`answered|clarify|handoff`);
// core's `Outcome` is the surface declaration's (`Json|Accepted|..`).
use crate::answer::{self, ModelReply, Outcome as TurnOutcome};
use crate::store::{self, Chunk};
use crate::text_model::{ModelTier, TextModel, TextModelError, TextRequest};

/// The deployment-level answer threshold, applied when the tenant has not
/// stored one. A fraction, because configuration is read as the operator
/// wrote it; storage converts to percent.
pub(crate) const DEFAULT_ANSWER_THRESHOLD: f32 = 0.60;

/// How many chunks a turn retrieves by default (`SUPPORT_TOP_K`).
pub(crate) const DEFAULT_TOP_K: u32 = 6;

/// The longest message the route accepts. A support question that needs
/// more than this is an email, not a chat turn.
pub(crate) const MAX_MESSAGE_CHARS: usize = 4000;

/// The context window the model gets for a turn. Deliberately generous
/// for six chunks; the budget is a ceiling, not a target.
const MAX_OUTPUT_TOKENS: u32 = 1024;

const STATUS_OPEN: &str = "open";
const STATUS_ESCALATED: &str = "escalated";

const CLARIFY_MESSAGE: &str = "I want to give you an accurate answer rather than a fast wrong \
     one — could you rephrase the question or add a little more detail?";
const HANDOFF_MESSAGE: &str = "I could not answer this confidently, so I have passed your \
     question to a person who can. You will hear back here.";

/// 503: no model is wired into this deployment at all. Permanent until
/// the operator acts — retrying without changing anything will not help,
/// which is why this one carries no `Retry-After`.
const TEXT_MODEL_NOT_CONFIGURED: ProblemDef = ProblemDef {
    slug: "text-model-not-configured",
    status: axum::http::StatusCode::SERVICE_UNAVAILABLE,
    title: "Text model is not configured",
    description: "No text model is wired into this deployment; answers cannot be produced \
     until the operator configures one.",
};

/// 503: the model could not answer right now. Retryable — the response
/// carries `Retry-After`, and nothing was written.
const TEXT_MODEL_UNAVAILABLE: ProblemDef = ProblemDef {
    slug: "text-model-unavailable",
    status: axum::http::StatusCode::SERVICE_UNAVAILABLE,
    title: "Text model is temporarily unavailable",
    description: "The model could not answer right now; the turn was not consumed and the \
     same request may be retried.",
};

/// 502: the model answered, and the answer is unusable — it did not
/// parse as the required schema, or violated it. A bad answer is the
/// *model's* failure, not this service's, hence bad gateway.
const TEXT_MODEL_BAD_ANSWER: ProblemDef = ProblemDef {
    slug: "text-model-invalid-answer",
    status: axum::http::StatusCode::BAD_GATEWAY,
    title: "Text model returned an unusable answer",
    description: "The model's answer did not parse as the required schema; nothing was \
     written.",
};

/// The builder's settings, cloned into the router state.
#[derive(Clone)]
pub(crate) struct Settings {
    /// `None` is a deployment that has not been given a model. The route
    /// answers exactly as [`TextModelError::NotConfigured`] — the same
    /// clean 503, no panic. This mirrors the venture's "no silent no-op"
    /// rule: an unconfigured model reports itself, it does not pretend to
    /// answer.
    pub text_model: Option<Arc<dyn TextModel>>,
    /// The builder-level threshold, between the config value and the
    /// compile-time default in the precedence order (see
    /// [`resolve_threshold`]).
    pub answer_threshold: Option<f32>,
    pub top_k: Option<u32>,
}

pub(crate) struct ModuleState {
    pub ctx: Arc<ModuleContext>,
    pub settings: Settings,
}

pub(crate) fn router(ctx: Arc<ModuleContext>, settings: Settings) -> axum::Router {
    let state = Arc::new(ModuleState { ctx, settings });
    axum::Router::new()
        .route("/messages", post(post_message))
        .route("/admin/settings", put(put_settings))
        .with_state(state)
}

pub(crate) fn now_iso() -> String {
    SystemClock
        .now()
        .replace_nanosecond(0)
        .expect("truncation stays in range")
        .format(&Rfc3339)
        .unwrap_or_default()
}

/// The effective answer threshold, in 0.0..=1.0. Precedence, documented
/// in the crate README and enforced here:
///
/// 1. the tenant's stored `sg_tenant_settings.answer_threshold_pct`;
/// 2. `SUPPORT_ANSWER_THRESHOLD` from the deployment config;
/// 3. the builder's `.answer_threshold(..)`;
/// 4. the compile-time [`DEFAULT_ANSWER_THRESHOLD`].
///
/// A tenant row wins because the threshold is a per-tenant promise about
/// when the module will claim an answer; a deployment-wide default is
/// only ever what applies before that promise is made.
fn resolve_threshold(cfg: &ModuleConfig<'_>, settings: &Settings, tenant_pct: Option<i64>) -> f32 {
    if let Some(pct) = tenant_pct {
        return answer::pct_confidence(pct);
    }
    if let Some(raw) = cfg.get_opt("ANSWER_THRESHOLD")
        && let Ok(parsed) = raw.parse::<f32>()
        && (0.0..=1.0).contains(&parsed)
    {
        return parsed;
    }
    settings
        .answer_threshold
        .unwrap_or(DEFAULT_ANSWER_THRESHOLD)
}

/// The system prompt: what the model may ground on, what it must cite,
/// and the shape it must answer in.
fn system_prompt() -> String {
    "You answer customer-support questions using only the retrieved context you are given. \
     Cite only chunk ids that appear in the retrieved context, quoting the exact span that \
     grounds each claim — never invent a chunk id. If the context does not contain the \
     answer, say so instead of guessing. Answer with JSON matching the provided schema: \
     `answer` (your reply), `citations` (chunk_id + quote pairs), and `confidence` in \
     0.0..=1.0, your own confidence that the answer is correct and fully grounded."
        .to_owned()
}

/// Builds the model request: the retrieved chunks embedded with their ids
/// so every citation can be checked against what the model actually saw,
/// and the schema derived from [`ModelReply`] itself, so the prompt's
/// contract cannot drift from what the parser accepts.
fn build_request(chunks: &[Chunk], question: &str) -> Result<TextRequest, Problem> {
    let mut context = String::new();
    for chunk in chunks {
        // A `write!` into a `String` cannot fail, but the house rule is
        // `Problem::internal()` over a panic, so the (impossible) error
        // surfaces through the same channel as the schema failure.
        writeln!(context, "[{}] {}", chunk.id, chunk.body).map_err(|_| Problem::internal())?;
    }
    if context.is_empty() {
        context.push_str("(nothing was retrieved for this question)\n");
    }
    let prompt = format!(
        "Question:\n{question}\n\nRetrieved context — cite only these chunk \
        ids:\n{context}"
    );
    let schema = serde_json::to_value(schema_for::<ModelReply>()).map_err(|err| {
        tracing::error!(error = %err, "support: answer schema failed to serialize");
        Problem::internal()
    })?;
    Ok(TextRequest {
        tier: ModelTier::Fast,
        system: system_prompt(),
        prompt,
        schema,
        max_output_tokens: MAX_OUTPUT_TOKENS,
    })
}

/// Maps a model error to its problem. `Transient` is handled by the
/// caller (it needs a `Retry-After` header, which [`Problem`] does not
/// carry).
fn model_problem(err: &TextModelError, scope: &Scope) -> Problem {
    match err {
        TextModelError::NotConfigured => {
            Problem::new(&TEXT_MODEL_NOT_CONFIGURED).instance(&scope.request_id)
        }
        TextModelError::Invalid(_) | TextModelError::Transient(_) => {
            Problem::new(&TEXT_MODEL_BAD_ANSWER).instance(&scope.request_id)
        }
    }
}

fn canned_message(outcome: TurnOutcome) -> &'static str {
    match outcome {
        TurnOutcome::Answered | TurnOutcome::Clarify => CLARIFY_MESSAGE,
        TurnOutcome::Handoff => HANDOFF_MESSAGE,
    }
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct MessageBody {
    /// The end user's message, in full.
    message: String,
    /// An existing conversation to continue. An id that is not this
    /// tenant's is a `404`, not a new conversation.
    conversation_id: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct SettingsBody {
    /// The answer threshold as a fraction in 0.0..=1.0; stored as an
    /// integer percentage.
    answer_threshold: f32,
}

async fn post_message(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    conn: TenantConn,
    Json(body): Json<MessageBody>,
) -> Result<Response, Problem> {
    let message = body.message.trim();
    if message.is_empty() {
        return Err(
            Problem::validation_failed("message must not be empty").instance(&scope.request_id)
        );
    }
    if message.chars().count() > MAX_MESSAGE_CHARS {
        return Err(Problem::validation_failed(format!(
            "message must be at most {MAX_MESSAGE_CHARS} characters"
        ))
        .instance(&scope.request_id));
    }

    // 1. Tenant + tenant-scoped handle. Every statement below runs
    //    through `TenantConn`, so a foreign tenant's rows are not merely
    //    filtered out — they are not reachable.
    let tenant_id = conn.tenant().id().as_str();
    // `TenantConn` implements `Database`; `&conn` coerces to `&dyn Database`,
    // and every statement below runs on the request's tenant handle.
    let db = &conn;

    // 2. Load the conversation and spend-check its clarify budget. This
    //    is a read: nothing is written yet, so a later failure cannot
    //    consume the conversation or its budget.
    let conversation = match body.conversation_id.as_deref() {
        Some(id) => Some(
            store::find_conversation(db, tenant_id, id)
                .await?
                .ok_or_else(|| Problem::not_found().instance(&scope.request_id))?,
        ),
        None => None,
    };
    let prior_clarifies = match &conversation {
        Some(conversation) => store::count_clarifies(db, tenant_id, &conversation.id).await?,
        None => 0,
    };

    // 3. Retrieve.
    let cfg = ModuleConfig::new("support", &*state.ctx.config);
    let top_k = cfg.get_u32("TOP_K", state.settings.top_k.unwrap_or(DEFAULT_TOP_K));
    let chunks = store::top_chunks(db, tenant_id, message, top_k).await?;

    // 4. Threshold: tenant row, else config, else builder, else default.
    let threshold = resolve_threshold(
        &cfg,
        &state.settings,
        store::tenant_threshold_pct(db, tenant_id).await?,
    );

    // 5. The prompt.
    let request =
        build_request(&chunks, message).map_err(|problem| problem.instance(&scope.request_id))?;

    // 6. Call the model. Nothing has been written to the database at this
    //    point — that is what makes a failed call not consume the
    //    conversation: no row, no message, no spent clarify budget, so
    //    the same request can simply be retried.
    let Some(model) = state.settings.text_model.as_ref() else {
        tracing::warn!("support: no text model configured; refusing POST /messages with a 503");
        return Err(model_problem(&TextModelError::NotConfigured, &scope));
    };
    let completion = match model.complete(request).await {
        Ok(completion) => completion,
        Err(TextModelError::Transient(detail)) => {
            tracing::warn!(detail = %detail, "support: text model unavailable; nothing was written");
            // `Problem` carries no `Retry-After`, so the header is added
            // here, on the built response (the house pattern of
            // `cratefield_core::http::rate_limited`). The hint is short
            // and honest: an unavailable model is usually back in
            // seconds, and an exact number is a promise we cannot keep.
            let mut response = Problem::new(&TEXT_MODEL_UNAVAILABLE)
                .instance(&scope.request_id)
                .into_response();
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("2"));
            return Ok(response);
        }
        Err(err) => return Err(model_problem(&err, &scope)),
    };

    // A reply that does not parse as the schema is a bad gateway, and
    // still nothing written.
    let reply = answer::parse_reply(&completion.text).map_err(|err| {
        tracing::warn!(error = %err, "support: model answer unusable; nothing was written");
        Problem::new(&TEXT_MODEL_BAD_ANSWER).instance(&scope.request_id)
    })?;

    // The decision.
    let retrieved_ids: Vec<&str> = chunks.iter().map(|chunk| chunk.id.as_str()).collect();
    let decision = answer::decide(&reply, &retrieved_ids, threshold, prior_clarifies);

    // 10. Write the turn — conversation (create or update) plus both
    //     messages, in one `batch_atomic` — and publish the response.
    persist_and_respond(
        db,
        tenant_id,
        message,
        conversation.as_ref(),
        &reply,
        &decision,
    )
    .await
}

/// Writes one decided turn and builds the response for it. Split from
/// [`post_message`] so the handler reads as the ordered flow it is; this
/// is the only place in the turn that touches the database with a write.
async fn persist_and_respond(
    db: &dyn Database,
    tenant_id: &str,
    message: &str,
    conversation: Option<&store::Conversation>,
    reply: &ModelReply,
    decision: &answer::Decision,
) -> Result<Response, Problem> {
    let now = now_iso();
    let message_id = UlidIdGen.ulid();
    let conversation_id =
        conversation.map_or_else(|| UlidIdGen.ulid(), |conversation| conversation.id.clone());
    // A handoff escalates; once escalated, a conversation stays escalated
    // — an answered question does not un-escalate the ticket behind it.
    let needs_escalation = decision.outcome == TurnOutcome::Handoff
        || conversation.is_some_and(|conversation| conversation.needs_escalation);
    let conversation_status = if needs_escalation {
        STATUS_ESCALATED
    } else {
        STATUS_OPEN
    };
    let (published_body, published_citations) = match decision.outcome {
        TurnOutcome::Answered => (reply.answer.clone(), decision.citations.clone()),
        // For a clarify and a handoff: a canned message and an EMPTY
        // citations array. Never return citations the module could not
        // stand behind, and never return an answer whose citations were
        // fabricated. The published body (`body`) and the model's raw
        // answer (`model_answer`) are stored separately below: what the
        // user was shown and what the model said deliberately differ on a
        // downgraded turn, so nothing the model said is lost internally —
        // a later issue auditing escalations reads the raw material from
        // storage, not from what the user was shown.
        TurnOutcome::Clarify | TurnOutcome::Handoff => {
            (canned_message(decision.outcome).to_owned(), Vec::new())
        }
    };
    let confidence_pct = answer::confidence_pct(reply.confidence);
    store::record_turn(
        db,
        &store::Turn {
            conversation_id: conversation_id.clone(),
            tenant_id: tenant_id.to_owned(),
            conversation_existed: conversation.is_some(),
            conversation_status: conversation_status.to_owned(),
            conversation_needs_escalation: needs_escalation,
            created_at: now.clone(),
            updated_at: now,
            user_message_id: UlidIdGen.ulid(),
            user_message: message.to_owned(),
            assistant_message_id: message_id.clone(),
            assistant_body: published_body.clone(),
            // The model's own words, stored even when the module refused
            // to publish them (see the match above).
            model_answer: reply.answer.clone(),
            outcome: decision.outcome.as_str().to_owned(),
            confidence_pct,
            citations_json: serde_json::to_string(&reply.citations)
                .unwrap_or_else(|_| "[]".to_owned()),
        },
    )
    .await?;

    let citations: Vec<serde_json::Value> = published_citations
        .iter()
        .map(|citation| json!({ "chunkId": citation.chunk_id, "quote": citation.quote }))
        .collect();
    Ok(Json(json!({
        "conversationId": conversation_id,
        "messageId": message_id,
        "outcome": decision.outcome.as_str(),
        "answer": published_body,
        "citations": citations,
        // The model's own number, rounded to the stored precision —
        // divided in `f64`, so the stored 90 comes back as `0.9` and not
        // the f32 widening artifact.
        "confidence": answer::pct_confidence_f64(confidence_pct),
        "needsEscalation": needs_escalation,
    }))
    .into_response())
}

async fn put_settings(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    conn: TenantConn,
    headers: HeaderMap,
    Json(body): Json<SettingsBody>,
) -> Result<Response, Problem> {
    // Guarded exactly like the waitlist's `/admin/export.csv`: bearer
    // `ADMIN_TOKEN`, unset token disables the route with the same answer
    // a missing header gets.
    require_admin(&*state.ctx.config, &headers)
        .map_err(|problem| problem.instance(&scope.request_id))?;
    if !(0.0..=1.0).contains(&body.answer_threshold) {
        // Also rejects NaN, which no range contains.
        return Err(
            Problem::validation_failed("answerThreshold must be a number in 0.0..=1.0")
                .instance(&scope.request_id),
        );
    }
    let tenant_id = conn.tenant().id().as_str();
    let threshold_pct = answer::confidence_pct(body.answer_threshold);
    store::upsert_settings(&conn, tenant_id, threshold_pct, &now_iso()).await?;
    Ok(Json(json!({ "answerThresholdPct": threshold_pct })).into_response())
}

/// The module's surface: the support turn and the admin threshold
/// setting.
pub(crate) fn surface() -> Surface {
    Surface::new()
        .action(
            Action::post("messages", "/messages")
                .input::<MessageBody>()
                .outcome(Outcome::Json),
        )
        .action(
            Action::new("settings", Method::PUT, "/admin/settings")
                .audience(Audience::Admin)
                .input::<SettingsBody>()
                .outcome(Outcome::Json),
        )
}
