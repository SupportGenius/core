//! `POST /messages` — one support turn — and the admin route that sets a
//! tenant's answer threshold and web widget origin allowlist.
//!
//! The turn's ordering is the point: conversation load, clarify count,
//! retrieval and the model call all happen before the first write, so
//! every failure — a model that is not configured, one that is down, one
//! that answered garbage — leaves the conversation exactly as it was. Only
//! a decided turn reaches the single `batch_atomic` at the end.

use std::fmt::Write as _;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};

use cratefield_core::{
    Clock, Completion, Database, Defer, IdGen, Json, ModelTier, ModuleContext, Problem, ProblemDef,
    Prompt, Scope, Statement, TextModel, TextModelError, require_admin,
};
use cratefield_i18n::{Args, Catalog, FluentCatalog, localize};

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

/// The canned texts a turn can be shown instead of a model answer, in the
/// three languages the module can answer in. Built once — a catalog
/// parses its `.ftl` on first use and is then only ever read — and
/// rendered through [`localize`], which falls back to the default locale
/// for a turn in any other language.
static CANNED: LazyLock<FluentCatalog> = LazyLock::new(|| {
    FluentCatalog::builder()
        .default_locale("en")
        .locale("en", include_str!("../locales/en.ftl"))
        .locale("de", include_str!("../locales/de.ftl"))
        .locale("ja", include_str!("../locales/ja.ftl"))
        .build()
        .expect("the canned-text catalog parses")
});

/// One canned message (`clarify` or `handoff`), rendered in `lang` — a
/// BCP-47 primary tag from [`turn_language`] — or in English when the
/// turn had no language at all.
fn canned(key: &str, lang: Option<&str>) -> String {
    let requested = lang
        .and_then(cratefield_i18n::parse_locale)
        .unwrap_or_else(|| CANNED.default_locale().clone());
    localize(
        &*CANNED,
        &requested,
        key,
        cratefield_i18n::BODY,
        &Args::new(),
    )
    .text
}

/// The clarify prompt: shown when the module would not stand behind an
/// answer.
pub(crate) fn clarify_message(lang: Option<&str>) -> String {
    canned("clarify", lang)
}

/// The handoff notice: shown when the turn was escalated to a person.
pub(crate) fn handoff_message(lang: Option<&str>) -> String {
    canned("handoff", lang)
}

/// The plain-text transcript escalation files with the ticket: the turn's
/// user message and the handoff notice it answered with, labelled so an
/// agent reads who said what.
///
/// Deliberately the turn's own two messages, not the whole conversation.
/// The route has loaded no prior messages (it reads counts, not bodies), and
/// re-reading the transcript just to enrich a ticket would put a second,
/// failure-prone read on the hot path of every escalating turn. The turn
/// that escalates — the question that could not be answered and the notice
/// that says so — is the part an agent needs to start from.
pub(crate) fn handoff_transcript(message: &str, reply: &str) -> String {
    format!("Customer: {message}\n\nSupport: {reply}")
}

/// The turn's one atomic batch: its own statements, then — when this turn
/// escalates and a [`HandoffSink`](crate::HandoffSink) is composed — the
/// sink's statements for `transcript`, and, whenever the request carried a
/// `contact` and a sink is composed, the sink's statements remembering that
/// address. Appending here is what makes the answer, the ticket and the
/// stored contact all-or-nothing. `Support::new()` composes no sink, so
/// this is just the turn and a handoff only marks `needs_escalation`.
fn turn_batch(
    state: &ModuleState,
    turn: &store::Turn,
    transcript: Option<&str>,
    contact: Option<&str>,
) -> Vec<Statement> {
    let mut statements = store::turn_statements(turn);
    let Some(sink) = state.handoff.as_deref() else {
        return statements;
    };
    if let Some(transcript) = transcript {
        statements.extend(sink.enqueue(
            &state.ctx,
            &turn.tenant_id,
            &turn.conversation_id,
            transcript,
        ));
    }
    if let Some(email) = contact {
        statements.extend(sink.remember_contact(
            &state.ctx,
            &turn.tenant_id,
            &turn.conversation_id,
            email,
        ));
    }
    statements
}

/// The languages whose BCP-47 primary tag is not whatlang's ISO 639-3
/// code: everything a support desk is likely to answer in has a
/// two-letter 639-1 tag, and the catalogs are keyed by those. Unmapped
/// languages fall back to the three-letter code, which is itself a valid
/// primary tag — a worse catalog key beats no detection.
const LANG_TAGS: &[(whatlang::Lang, &str)] = &[
    (whatlang::Lang::Eng, "en"),
    (whatlang::Lang::Deu, "de"),
    (whatlang::Lang::Fra, "fr"),
    (whatlang::Lang::Spa, "es"),
    (whatlang::Lang::Ita, "it"),
    (whatlang::Lang::Por, "pt"),
    (whatlang::Lang::Nld, "nl"),
    (whatlang::Lang::Swe, "sv"),
    (whatlang::Lang::Dan, "da"),
    (whatlang::Lang::Nob, "nb"),
    (whatlang::Lang::Fin, "fi"),
    (whatlang::Lang::Pol, "pl"),
    (whatlang::Lang::Ces, "cs"),
    (whatlang::Lang::Rus, "ru"),
    (whatlang::Lang::Ukr, "uk"),
    (whatlang::Lang::Tur, "tr"),
    (whatlang::Lang::Ell, "el"),
    (whatlang::Lang::Heb, "he"),
    (whatlang::Lang::Ara, "ar"),
    (whatlang::Lang::Hin, "hi"),
    (whatlang::Lang::Tha, "th"),
    (whatlang::Lang::Cmn, "zh"),
    (whatlang::Lang::Jpn, "ja"),
    (whatlang::Lang::Kor, "ko"),
];

/// The language one support turn is conducted in, as a BCP-47 primary
/// tag, or `None` when nothing reliable says.
///
/// whatlang's script-and-trigram detection answers for the message
/// itself, but only when it calls itself reliable — a short or ambiguous
/// text ("How do I reset my password?" is genuinely undecidable from
/// trigrams alone) must not pick a language on a guess. Below that floor
/// the request's own `Accept-Language` answers, first entry wins; with
/// neither signal, the turn has no language and every language-dependent
/// choice falls back to English.
pub(crate) fn turn_language(message: &str, accept_language: Option<&str>) -> Option<String> {
    if let Some(info) = whatlang::detect(message)
        && info.is_reliable()
    {
        return Some(
            LANG_TAGS
                .iter()
                .find(|(detected, _)| *detected == info.lang())
                .map_or_else(
                    || info.lang().code().to_owned(),
                    |(_, tag)| (*tag).to_owned(),
                ),
        );
    }
    accept_language
        .map(cratefield_i18n::accept_language)
        .and_then(|locales| locales.first().cloned())
        .map(|locale| locale.language.to_string())
}

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

/// 409: the conversation was closed (`POST /conversations/{id}/handback`
/// with `{"close": true}`, issue #35). A customer turn into a closed
/// conversation is refused rather than silently reopening it — the
/// visitor starts a new one.
const CONVERSATION_CLOSED: ProblemDef = ProblemDef {
    slug: "conversation-closed",
    status: StatusCode::CONFLICT,
    title: "Conversation is closed",
    description: "This conversation was closed; no further customer turn is accepted. Start a \
                  new conversation to ask again.",
};

/// How many of a conversation's prior turns ride along in the prompt. Each
/// is a coalesced turn — see [`alternating_turns`] — so this bounds the
/// model's context, not the stored transcript.
const HISTORY_TURNS: usize = 10;

/// How many stored messages the prompt-history read fetches before
/// coalescing. The SQL bound ([`store::recent_conversation_messages`]) is
/// larger than [`HISTORY_TURNS`] because coalescing can shrink adjacent
/// messages into one turn.
const HISTORY_READ: usize = 20;

/// The system prompt: what the model may ground on, what it must cite,
/// which language it must answer in, and the shape it must answer in.
const SYSTEM_PROMPT: &str = "You answer customer-support questions using only the retrieved \
     context you are given. Each context passage starts with its chunk id in square brackets. \
     Cite only chunk ids that appear in the retrieved context, quoting the exact span that \
     grounds each claim — never invent a chunk id. If the context does not contain the answer, \
     say so instead of guessing. Answer in the language named by `respond_in`, or in the \
     question's own language when there is no `respond_in` line. Answer with JSON matching the \
     provided schema: `answer` (your reply), `citations` (chunk_id + quote pairs), and \
     `confidence` in 0.0..=1.0, your own confidence that the answer is correct and fully \
     grounded.";

/// The `POST /messages` body. Public because it is also the MCP `answer`
/// tool's argument type (issue #34) — the tool's `inputSchema` is
/// `schema_for::<MessageBody>()`, the same type this route deserializes.
#[derive(Deserialize, JsonSchema)]
pub struct MessageBody {
    pub(crate) message: String,
    pub(crate) conversation_id: Option<String>,
    /// The customer's contact address, when they offered one. Kept by the
    /// composed handoff sink (see [`HandoffSink::remember_contact`]) so the
    /// escalation notify stage has somewhere to send; a turn without one
    /// behaves exactly as before.
    pub(crate) contact: Option<ContactBody>,
}

/// The nested contact object: `{"contact": {"email": "…"}}`, so an address
/// is never confused with a second bare string field.
#[derive(Deserialize, JsonSchema)]
pub(crate) struct ContactBody {
    pub(crate) email: String,
}

#[derive(Deserialize)]
struct SettingsBody {
    answer_threshold: Option<f32>,
    widget_origins: Option<Vec<String>>,
}

/// What one decided turn answers with, whatever route ran it: the fields
/// `POST /messages` serializes verbatim, and which the widget route
/// extends with its own tokens.
pub(crate) struct TurnReply {
    pub conversation_id: String,
    pub message_id: String,
    /// `'answered' | 'clarify' | 'handoff'`.
    pub outcome: &'static str,
    /// What the visitor is shown: the model's answer, or the canned
    /// clarify/handoff message.
    pub answer: String,
    /// `[{chunk_id, quote}]` — retrieved-only citations, empty unless the
    /// outcome is `answered`.
    pub citations: Vec<Value>,
    /// The stored confidence percentage, as a fraction.
    pub confidence: f64,
    pub needs_escalation: bool,
}

impl TurnReply {
    /// The exact `POST /messages` JSON body.
    pub(crate) fn to_json(&self) -> Value {
        json!({
            "conversation_id": self.conversation_id,
            "message_id": self.message_id,
            "outcome": self.outcome,
            "answer": self.answer,
            "citations": self.citations,
            "confidence": self.confidence,
            "needs_escalation": self.needs_escalation,
        })
    }
}

/// Why a turn did not run to its write. `Problem` covers every ordinary
/// failure; [`TurnFailure::Response`] carries the one answer that must
/// keep headers — the retryable 503's `Retry-After` — which a `Problem`
/// has nowhere to put. (Boxed so the `Err` side of a turn result stays
/// small: a `Response` is header-plus-body plumbing, and the happy path
/// pays for its size on every call.)
pub(crate) enum TurnFailure {
    Problem(Problem),
    Response(Box<Response>),
}

impl TurnFailure {
    pub(crate) fn into_response(self) -> Response {
        match self {
            Self::Problem(problem) => problem.into_response(),
            Self::Response(response) => *response,
        }
    }
}

impl From<Problem> for TurnFailure {
    fn from(problem: Problem) -> Self {
        Self::Problem(problem)
    }
}

impl From<cratefield_core::DbError> for TurnFailure {
    fn from(err: cratefield_core::DbError) -> Self {
        Self::Problem(err.into())
    }
}

/// `POST /messages` — `{"message": "…", "conversation_id": "…"?,
/// "contact": {"email": "…"}?}`. Answers
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
    let tenant_id = authenticate(ctx, &headers).await?.tenant_id;
    if let Some(rate_limited) = guard_rate_limit(ctx, &tenant_id).await {
        return Ok(rate_limited);
    }
    let body = parse_message(&body).map_err(|problem| problem.instance(&scope.request_id))?;
    let accept_language = headers
        .get(header::ACCEPT_LANGUAGE)
        .and_then(|value| value.to_str().ok());
    match answer(&state, &scope, &tenant_id, body, accept_language).await {
        Ok(reply) => Ok(Json(reply.to_json()).into_response()),
        Err(failure) => Ok(failure.into_response()),
    }
}

/// One answer turn, shared by `POST /messages` (tenant API key) and the
/// MCP `answer` tool (issue #34): the language fallback, then [`run_turn`].
/// Both call it, so the tool makes exactly the decision the route makes —
/// no copy-pasted divergence. The caller has already authenticated and
/// rate-limited.
pub(crate) async fn answer(
    state: &ModuleState,
    scope: &Scope,
    tenant_id: &str,
    body: MessageBody,
    accept_language: Option<&str>,
) -> Result<TurnReply, TurnFailure> {
    let message = body.message.as_str();
    // The turn's language, from the message's own words first and the
    // caller's Accept-Language below that. Decided here, before anything
    // can fail, so the prompt, the canned texts and both stored messages
    // of the turn speak one language.
    let lang = turn_language(message, accept_language);
    run_turn(
        state,
        scope,
        tenant_id,
        message,
        body.conversation_id.as_deref(),
        body.contact.as_ref().map(|contact| contact.email.as_str()),
        lang,
    )
    .await
}

/// One support turn, shared by `POST /messages` (tenant API key) and the
/// widget route (publishable key): conversation load, clarify count,
/// retrieval, the model call, the decision and the single atomic write.
/// The ordering inside is the point: everything that can fail happens
/// before the first write, so every failure leaves the conversation
/// exactly as it was. Only a decided turn reaches the `batch_atomic`.
pub(crate) async fn run_turn(
    state: &ModuleState,
    scope: &Scope,
    tenant_id: &str,
    message: &str,
    conversation_id: Option<&str>,
    contact: Option<&str>,
    lang: Option<String>,
) -> Result<TurnReply, TurnFailure> {
    let ctx = &state.ctx;
    let clock: &dyn Clock = required_port(ctx.ports.clock.as_deref(), "Clock")?;
    let id_gen: &dyn IdGen = required_port(ctx.ports.id_gen.as_deref(), "IdGen")?;
    let db: &dyn Database = required_port(ctx.ports.db.as_deref(), "Db")?;
    // The language is only ever read; borrow it once for the calls below.
    let lang = lang.as_deref();

    // 1–2. The conversation this turn belongs to (or none) and its counts
    //      — both reads, taken before any write.
    let (conversation, counts) =
        resolve_conversation(db, tenant_id, conversation_id, &scope.request_id).await?;

    // A person holding the conversation answers the next customer turn,
    // not the bot (issue #35): store the message and return before
    // retrieval and the model. A closed conversation takes no turn at all.
    if let Some(conversation) = &conversation {
        if conversation.state == store::STATE_HUMAN {
            return store_held_message(state, tenant_id, conversation, message, lang, &counts)
                .await;
        }
        if conversation.state == store::STATE_CLOSED {
            return Err(Problem::new(&CONVERSATION_CLOSED)
                .instance(&scope.request_id)
                .into());
        }
    }

    // 3–6. Retrieve, resolve the threshold, ask the model with the
    //      conversation's prior turns, and parse — nothing written, so
    //      every failure here consumes nothing.
    let (chunks, threshold, reply, confidence_pct) = model_turn(
        state,
        scope,
        db,
        tenant_id,
        message,
        lang,
        conversation.as_ref(),
    )
    .await?;

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
        Outcome::Clarify => (clarify_message(lang), Vec::new()),
        Outcome::Handoff => (handoff_message(lang), Vec::new()),
    };
    let turn = store::Turn {
        conversation_id: conversation_id.clone(),
        tenant_id: tenant_id.to_owned(),
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
        lang: lang.map(str::to_owned),
    };
    // A takeover can land between step 1's read and this write (issue
    // #35). Re-read the state once, last before the bot answers: if a
    // person now holds the conversation — or it was closed meanwhile —
    // store the customer's message the held way and write no bot answer,
    // which would otherwise land in a conversation a human now owns.
    if let Some(conversation) = &conversation
        && let Some(current) = taken_over(db, tenant_id, conversation).await?
    {
        return store_held_message(state, tenant_id, &current, message, lang, &counts).await;
    }

    // 9–10. One atomic write for the whole turn, then the kick. See
    //       [`commit_turn`] for why the two are ordered this way.
    let transcript = escalates.then(|| handoff_transcript(message, &shown));
    commit_turn(
        state,
        ctx,
        db,
        &turn,
        transcript.as_deref(),
        contact,
        scope.defer.clone(),
    )
    .await?;

    let citations: Vec<Value> = shown_citations
        .iter()
        .map(|citation| json!({ "chunk_id": citation.chunk_id, "quote": citation.quote }))
        .collect();
    Ok(TurnReply {
        conversation_id,
        message_id: assistant_message_id,
        outcome: decision.outcome.as_str(),
        answer: shown,
        citations,
        // The stored percentage, divided in f64 so 90 reads back as 0.9.
        confidence: answer::pct_confidence_f64(confidence_pct),
        needs_escalation,
    })
}

/// Steps 3–6 of a turn: retrieve for the question on the same BM25 path
/// `GET /search` uses, resolve the tenant's threshold (else the documented
/// default), ask the model, and parse the reply, quantizing its confidence
/// to the stored grain so the decision and the row it writes can never
/// disagree. The conversation's prior turns ride along as history — an
/// agent's `staff` replies included, so a conversation handed back to the
/// bot is answered in context. Nothing is written here, which is what
/// makes every failure consume nothing.
async fn model_turn(
    state: &ModuleState,
    scope: &Scope,
    db: &dyn Database,
    tenant_id: &str,
    message: &str,
    lang: Option<&str>,
    conversation: Option<&ConversationRow>,
) -> Result<(Vec<Retrieved>, f32, ModelReply, i64), TurnFailure> {
    let chunks = retrieve(db, tenant_id, message, TOP_K).await?;
    let threshold = store::tenant_threshold_pct(db, tenant_id)
        .await?
        .map_or(DEFAULT_ANSWER_THRESHOLD, answer::pct_confidence);
    let history = match conversation {
        Some(c) => store::recent_conversation_messages(db, tenant_id, &c.id, HISTORY_READ).await?,
        None => Vec::new(),
    };
    let prompt = build_prompt(&chunks, message, lang, &history);
    let completion = ask(state.text_model.as_deref(), &prompt, scope).await?;
    // A reply that is not the schema is a bad gateway, and still nothing
    // written.
    let mut reply = answer::parse_reply(&completion)
        .map_err(|_| Problem::new(&TEXT_MODEL_BAD_ANSWER).instance(&scope.request_id))?;
    let confidence_pct = answer::confidence_pct(reply.confidence);
    reply.confidence = answer::pct_confidence(confidence_pct);
    Ok((chunks, threshold, reply, confidence_pct))
}

/// Re-reads a conversation's state just before the bot's write: a takeover
/// can land between the turn's opening read and here (issue #35), and a
/// person who now holds it must answer, not the bot. Returns the fresh row
/// when the state is now `human` — or `closed`, which takes the same held
/// path (the message is kept, no answer invented) rather than the `409` a
/// turn opening on an already-closed conversation gets.
async fn taken_over(
    db: &dyn Database,
    tenant_id: &str,
    conversation: &ConversationRow,
) -> Result<Option<ConversationRow>, TurnFailure> {
    let current = store::find_conversation(db, tenant_id, &conversation.id).await?;
    Ok(current.filter(|current| {
        current.state == store::STATE_HUMAN || current.state == store::STATE_CLOSED
    }))
}

/// A customer turn on a conversation a person holds (issue #35): store the
/// message and answer `human` — no retrieval, no model call, no answer.
/// The bot must not spend a model call, or write an answer, on a
/// conversation a support agent is working; the agent answers it.
async fn store_held_message(
    state: &ModuleState,
    tenant_id: &str,
    conversation: &ConversationRow,
    message: &str,
    lang: Option<&str>,
    counts: &store::ConversationCounts,
) -> Result<TurnReply, TurnFailure> {
    let ctx = &state.ctx;
    let clock: &dyn Clock = required_port(ctx.ports.clock.as_deref(), "Clock")?;
    let id_gen: &dyn IdGen = required_port(ctx.ports.id_gen.as_deref(), "IdGen")?;
    let db: &dyn Database = required_port(ctx.ports.db.as_deref(), "Db")?;
    let message_id = id_gen.ulid();
    db.batch_atomic(&store::customer_message_statements(
        &store::CustomerMessage {
            id: message_id.clone(),
            conversation_id: conversation.id.clone(),
            tenant_id: tenant_id.to_owned(),
            seq: counts.messages,
            body: message.to_owned(),
            lang: lang.map(str::to_owned),
            created_at: store::iso_now(clock),
        },
    ))
    .await?;
    Ok(TurnReply {
        conversation_id: conversation.id.clone(),
        message_id,
        outcome: answer::OUTCOME_HUMAN,
        answer: String::new(),
        citations: Vec::new(),
        confidence: 0.0,
        needs_escalation: conversation.needs_escalation,
    })
}

/// Steps 1–2: the conversation a turn belongs to, scoped to the tenant, and
/// its counts.
///
/// Both are reads — nothing is written until the turn is decided, so a
/// failure here consumes nothing. `None` is a new conversation; an id that
/// is unknown or belongs to another tenant is the same 404, answered before
/// the model is ever asked.
async fn resolve_conversation(
    db: &dyn Database,
    tenant_id: &str,
    conversation_id: Option<&str>,
    request_id: &str,
) -> Result<(Option<ConversationRow>, store::ConversationCounts), Problem> {
    let conversation = match conversation_id {
        Some(id) => Some(
            store::find_conversation(db, tenant_id, id)
                .await?
                .ok_or_else(|| Problem::not_found().instance(request_id))?,
        ),
        None => None,
    };
    // The clarify budget already spent, and the message count that orders
    // this turn's two messages.
    let counts = match &conversation {
        Some(conversation) => store::conversation_counts(db, tenant_id, &conversation.id).await?,
        None => store::ConversationCounts {
            messages: 0,
            clarifies: 0,
        },
    };
    Ok((conversation, counts))
}

/// Commits the turn's one atomic write, then kicks escalation.
///
/// The batch is the turn's own statements plus, when `transcript` is
/// `Some` (an escalating turn with a [`HandoffSink`](crate::HandoffSink)
/// composed), the sink's for that transcript, and, when `contact` is
/// `Some`, the sink's remembering that address — so the "escalated" answer,
/// the ticket behind it and the stored contact commit or roll back
/// together. The kick runs only *after* the commit: a failure there costs a
/// delay (the scheduled drain is the backstop), never the ticket the batch
/// just wrote.
///
/// Extracted from [`run_turn`] to keep the turn's ordering readable
/// as one numbered sequence without the handler tripping the function
/// length lint.
async fn commit_turn(
    state: &ModuleState,
    ctx: &ModuleContext,
    db: &dyn Database,
    turn: &store::Turn,
    transcript: Option<&str>,
    contact: Option<&str>,
    defer: Arc<dyn Defer>,
) -> Result<(), Problem> {
    db.batch_atomic(&turn_batch(state, turn, transcript, contact))
        .await?;
    if transcript.is_some()
        && let Some(sink) = state.handoff.as_deref()
    {
        sink.kick(ctx, defer);
    }
    Ok(())
}

/// The request body, with `message` trimmed and length-checked and any
/// `contact.email` validated.
fn parse_message(body: &[u8]) -> Result<MessageBody, Problem> {
    let body: MessageBody = serde_json::from_slice(body).map_err(|_| {
        Problem::validation_failed(
            "body: expected a JSON object with a string \"message\" and an optional string \
             \"conversation_id\"",
        )
    })?;
    validate_message(body)
}

/// The `message` rules: trimmed, non-empty, within
/// [`MAX_MESSAGE_CHARS`]. Shared by the raw HTTP body above and the MCP
/// `answer` tool, whose `arguments` are already JSON.
pub(crate) fn validate_message(mut body: MessageBody) -> Result<MessageBody, Problem> {
    let trimmed = body.message.trim();
    if trimmed.is_empty() || trimmed.chars().count() > MAX_MESSAGE_CHARS {
        return Err(Problem::validation_failed(format!(
            "message: required, 1..={MAX_MESSAGE_CHARS} characters"
        )));
    }
    body.message = trimmed.to_owned();
    if let Some(contact) = &mut body.contact {
        contact.email = normalize_contact_email(&contact.email).ok_or_else(|| {
            Problem::validation_failed("contact.email: not a valid email address")
        })?;
    }
    Ok(body)
}

/// The longest an email address may be (RFC 5321's 254-octet path limit).
const MAX_EMAIL_CHARS: usize = 254;

/// Trims and validates a contact address, handing back the trimmed form.
/// Deliberately structural rather than RFC-complete: one `@` splitting a
/// non-empty local part from a non-empty domain, no whitespace anywhere,
/// within [`MAX_EMAIL_CHARS`]. `None` when the shape is wrong — the caller
/// turns that into a 400, never a stored address the notify stage cannot
/// use.
fn normalize_contact_email(raw: &str) -> Option<String> {
    let email = raw.trim();
    if email.is_empty() || email.chars().count() > MAX_EMAIL_CHARS {
        return None;
    }
    if email.chars().any(char::is_whitespace) {
        return None;
    }
    let (local, domain) = email.split_once('@')?;
    if local.is_empty() || domain.is_empty() || domain.contains('@') {
        return None;
    }
    Some(email.to_owned())
}

/// Calls the model, mapping every failure to its problem: no model or
/// `NotConfigured` is `503 text-model-not-configured`, `Transient` the
/// retryable `503` with `Retry-After`, and a refusal or a transport
/// failure `502`. The error side is a [`TurnFailure`] because one leg —
/// the retryable 503 — must keep its `Retry-After` header, which a
/// `Problem` cannot carry.
async fn ask(
    model: Option<&dyn TextModel>,
    prompt: &Prompt,
    scope: &Scope,
) -> Result<Completion, TurnFailure> {
    let problem =
        |def: &ProblemDef| TurnFailure::Problem(Problem::new(def).instance(&scope.request_id));
    let Some(model) = model else {
        return Err(problem(&TEXT_MODEL_NOT_CONFIGURED));
    };
    match model.complete(prompt).await {
        Ok(completion) => Ok(completion),
        Err(TextModelError::NotConfigured) => Err(problem(&TEXT_MODEL_NOT_CONFIGURED)),
        Err(err @ TextModelError::Transient { .. }) => Err(TurnFailure::Response(Box::new(
            unavailable(scope, err.retry_after()),
        ))),
        // `Rejected`, `Transport` and — since core 0.8 — `Unsupported`
        // (the adapter cannot serve this prompt) are an upstream failure
        // that gave no usable answer: `502`, like the two before it.
        // `TextModelError` is `#[non_exhaustive]`, so a future variant
        // lands here too rather than failing to compile; the wildcard is
        // the deliberate default, not a swallowed case.
        Err(_) => Err(problem(&TEXT_MODEL_BAD_ANSWER)),
    }
}

/// The model request: the conversation's recent turns, then every
/// retrieved chunk as `[chunk_id] body` — so each citation can be checked
/// against exactly what the model was shown — and the hand-written reply
/// schema. A turn with a known language adds the one-line
/// `respond_in: <lang>` the system prompt names, so the model answers in
/// the language the question was asked in.
///
/// The prior turns are the strictly alternating sequence
/// [`alternating_turns`] builds, with this turn's question (and its
/// retrieved context) as the final user turn.
fn build_prompt(
    chunks: &[Retrieved],
    question: &str,
    lang: Option<&str>,
    history: &[store::WidgetMessage],
) -> Prompt {
    let mut context = String::new();
    for hit in chunks {
        // Writing into a `String` cannot fail.
        let _ = writeln!(context, "[{}] {}", hit.chunk.id, hit.chunk.body);
    }
    if context.is_empty() {
        context.push_str("(nothing was retrieved for this question)\n");
    }
    let respond_in = lang.map_or_else(String::new, |lang| format!("respond_in: {lang}\n"));
    let final_turn = format!(
        "Question:\n{question}\n{respond_in}\nRetrieved context — cite only these chunk \
         ids:\n{context}"
    );

    let mut prompt = Prompt::new(ModelTier::Fast).system(SYSTEM_PROMPT);
    for (is_user, body) in alternating_turns(history, &final_turn) {
        prompt = if is_user {
            prompt.user(body)
        } else {
            prompt.assistant(body)
        };
    }
    prompt
        .json_schema(answer::reply_schema())
        .max_tokens(MAX_OUTPUT_TOKENS)
}

/// The conversation's prior messages as strictly alternating model turns,
/// `(is_user, body)`, opening on the user side and ending with
/// `final_turn`.
///
/// Anthropic — and the production adapter, which passes these turns 1:1 —
/// rejects a history that is not strictly alternating or does not begin
/// with a `user` turn, and does so with a `400` on every retry, wedging
/// the conversation. Two shapes break that and are fixed here: a `staff`
/// message is the assistant side, so a bot reply followed by an agent's
/// reply is two assistant turns in a row; and a customer message stored
/// while a person held the conversation has no reply after it, so two user
/// turns can abut. Adjacent same-side messages therefore coalesce into one
/// turn (bodies joined by a blank line, the `Support agent:` prefix kept),
/// only the last [`HISTORY_TURNS`] survive, a leading assistant turn is
/// dropped, and `final_turn` either opens a new user turn or folds into a
/// trailing one.
fn alternating_turns(history: &[store::WidgetMessage], final_turn: &str) -> Vec<(bool, String)> {
    let mut turns: Vec<(bool, String)> = Vec::new();
    for message in history {
        match message.role.as_str() {
            store::ROLE_USER => push_turn(&mut turns, true, message.body.clone()),
            store::ROLE_STAFF => {
                push_turn(
                    &mut turns,
                    false,
                    format!("Support agent: {}", message.body),
                );
            }
            _ => push_turn(&mut turns, false, message.body.clone()),
        }
    }
    let recent = turns.len().saturating_sub(HISTORY_TURNS);
    let mut turns = turns.split_off(recent);
    while matches!(turns.first(), Some((false, _))) {
        turns.remove(0);
    }
    push_turn(&mut turns, true, final_turn.to_owned());
    turns
}

/// Appends `body` to the last turn when it is on the same side, and starts
/// a new turn otherwise, keeping [`alternating_turns`]'s sequence strict.
fn push_turn(turns: &mut Vec<(bool, String)>, is_user: bool, body: String) {
    match turns.last_mut() {
        Some((last_is_user, last_body)) if *last_is_user == is_user => {
            last_body.push_str("\n\n");
            last_body.push_str(&body);
        }
        _ => turns.push((is_user, body)),
    }
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

/// `PUT /admin/tenants/{tenant_id}/settings` — `{"answer_threshold": 0.9,
/// "widget_origins": ["https://support.example"]}` sets the tenant's
/// answer threshold (0.0..=1.0, stored as a whole percentage) and the
/// web widget's origin allowlist. At least one field must be present;
/// updating one leaves the other exactly as it was, in one statement.
/// The origins are
/// validated and normalized by [`crate::widget::normalize_widget_origins`]
/// — the same RFC 6454 serialization the widget routes compare against,
/// so a stored entry can never drift from what a request must present —
/// and an empty array is legal, closing the widget. Guarded by the
/// harness admin token, exactly like `POST /admin/tenants`.
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
            "body: expected a JSON object with an optional number \"answer_threshold\" \
             (0.0..=1.0) and an optional array of origin strings \"widget_origins\"",
        )
        .instance(&scope.request_id)
    })?;
    if body.answer_threshold.is_none() && body.widget_origins.is_none() {
        return Err(Problem::validation_failed(
            "body: set \"answer_threshold\" or \"widget_origins\" — at least one",
        )
        .instance(&scope.request_id));
    }
    // `contains` is false for NaN, so this also rejects it.
    if let Some(threshold) = body.answer_threshold
        && !(0.0..=1.0).contains(&threshold)
    {
        return Err(
            Problem::validation_failed("answer_threshold: a number in 0.0..=1.0")
                .instance(&scope.request_id),
        );
    }
    // Normalized before anything is read or written: a bad origin is a
    // 400 and touches nothing.
    let origins = body
        .widget_origins
        .as_deref()
        .map(crate::widget::normalize_widget_origins)
        .transpose()
        .map_err(|detail| Problem::validation_failed(detail).instance(&scope.request_id))?;

    let ctx = &state.ctx;
    let clock: &dyn Clock = required_port(ctx.ports.clock.as_deref(), "Clock")?;
    let db: &dyn Database = required_port(ctx.ports.db.as_deref(), "Db")?;
    if store::find_tenant(db, &tenant_id).await?.is_none() {
        return Err(Problem::not_found().instance(&scope.request_id));
    }
    // One atomic statement: the field this request omits keeps its
    // stored value (the store's SET list names only provided fields), a
    // first-ever row takes the documented default threshold (the column
    // is NOT NULL) and no allowlist. No read-merge-write, so two admins
    // setting different fields cannot lose one of the writes.
    let widget_origins = origins.as_deref().map(|list| {
        // Serializing a `Vec<String>` cannot fail, but an honest
        // fallback beats an unreachable: `"[]"` closes the widget.
        serde_json::to_string(list).unwrap_or_else(|_| "[]".to_owned())
    });
    store::upsert_tenant_settings(
        db,
        &tenant_id,
        body.answer_threshold.map(answer::confidence_pct),
        widget_origins.as_deref(),
        answer::confidence_pct(DEFAULT_ANSWER_THRESHOLD),
        &store::iso_now(clock),
    )
    .await?;

    // What now stands, read back — not what was sent: the widget
    // allowlist reads back as the array it is stored as (`[]` when it
    // is null).
    let stored = store::find_tenant_settings(db, &tenant_id)
        .await?
        .ok_or_else(Problem::internal)?;
    Ok(Json(json!({
        "tenant_id": tenant_id,
        "answer_threshold": answer::pct_confidence_f64(stored.answer_threshold_pct),
        "widget_origins": store::parse_widget_origins(stored.widget_origins.as_deref()),
    }))
    .into_response())
}

#[cfg(test)]
mod tests {
    use super::{build_prompt, clarify_message, handoff_message, turn_language};
    use cratefield_core::{Prompt, Role};

    use crate::store;

    /// The wording these messages have always had. The English catalog
    /// must render exactly this: the routes' long-standing texts, not a
    /// fresh translation of them.
    const CLARIFY_EN: &str = "I want to give you an accurate answer rather than a fast wrong \
         one — could you rephrase the question or add a little more detail?";
    const HANDOFF_EN: &str = "I could not answer this confidently, so I have passed your \
         question to a person who can. You will hear back here.";

    #[test]
    fn english_is_the_wording_the_canned_texts_always_had() {
        assert_eq!(clarify_message(None), CLARIFY_EN);
        assert_eq!(handoff_message(None), HANDOFF_EN);
        // A turn in a language the catalog does not carry falls back to
        // the default locale, never to a missing key.
        assert_eq!(clarify_message(Some("fr")), CLARIFY_EN);
    }

    #[test]
    fn every_catalog_locale_renders_both_canned_texts() {
        // Whatever a locale's clarify says, its handoff says something
        // different — a copy-paste across keys would show one of them to
        // the wrong turn.
        for lang in ["de", "ja"] {
            let clarify = clarify_message(Some(lang));
            let handoff = handoff_message(Some(lang));
            assert_ne!(clarify, CLARIFY_EN, "{lang} clarify fell back to en");
            assert_ne!(handoff, HANDOFF_EN, "{lang} handoff fell back to en");
            assert_ne!(clarify, handoff, "{lang} renders one text for both keys");
        }
    }

    #[test]
    fn detection_maps_to_primary_tags_and_the_header_covers_the_rest() {
        // Reliable detections map to the two-letter tags the catalogs are
        // keyed by; both verified empirically against whatlang.
        assert_eq!(
            turn_language("Wie kann ich mein Passwort zurücksetzen?", None),
            Some("de".to_owned())
        );
        assert_eq!(
            turn_language("パスワードをリセットするにはどうすればよいですか？", None),
            Some("ja".to_owned())
        );

        // A short English question is genuinely undecidable from
        // trigrams alone: unreliable, so the header answers, first entry
        // first — and with no header, there is no language.
        let ambiguous = "How do I reset my password?";
        assert_eq!(
            turn_language(ambiguous, Some("ja, en;q=0.8")),
            Some("ja".to_owned())
        );
        assert_eq!(turn_language(ambiguous, None), None);
        assert_eq!(turn_language(ambiguous, Some("not a language")), None);
    }

    /// A stored message, in the shape the transcript read returns.
    fn message(role: &str, body: &str) -> store::WidgetMessage {
        store::WidgetMessage {
            id: "m".to_owned(),
            role: role.to_owned(),
            body: body.to_owned(),
            outcome: None,
            citations: Vec::new(),
            created_at: "t".to_owned(),
        }
    }

    fn roles(prompt: &Prompt) -> Vec<Role> {
        prompt.messages.iter().map(|turn| turn.role).collect()
    }

    #[test]
    fn the_prompt_history_is_strictly_alternating() {
        // A window ending `user, assistant, staff`: the bot's answer and the
        // agent's reply are the same side, so they must coalesce into one
        // assistant turn, and the current question follows as a user turn.
        let answered_then_staff = [
            message(store::ROLE_USER, "how do I reset?"),
            message(store::ROLE_ASSISTANT, "From the settings page."),
            message(store::ROLE_STAFF, "Open Settings, then Security."),
        ];
        let prompt = build_prompt(&[], "still stuck", None, &answered_then_staff);
        assert_eq!(roles(&prompt), [Role::User, Role::Assistant, Role::User]);
        assert!(
            prompt.messages[1]
                .content
                .contains("Support agent: Open Settings, then Security."),
            "the staff reply stays attributed: {}",
            prompt.messages[1].content
        );

        // A window ending `user, user` — a customer message stored while a
        // person held the conversation — must not leave two user turns
        // abutting: it coalesces with the current question.
        let held_between = [
            message(store::ROLE_USER, "first question"),
            message(store::ROLE_USER, "held while a person worked"),
        ];
        let prompt = build_prompt(&[], "now answer this", None, &held_between);
        assert_eq!(roles(&prompt), [Role::User]);
        assert!(prompt.messages[0].content.contains("first question"));
        assert!(prompt.messages[0].content.contains("now answer this"));
    }

    #[test]
    fn the_prompt_history_opens_on_a_user_turn() {
        // The window opened on the bot's reply (the earlier user turn fell
        // outside it): Anthropic needs a `user` first, so the leading
        // assistant turn is dropped.
        let starts_on_assistant = [message(store::ROLE_ASSISTANT, "earlier answer")];
        let prompt = build_prompt(&[], "and now?", None, &starts_on_assistant);
        assert_eq!(roles(&prompt), [Role::User]);
    }
}
