//! `POST /messages` — one support turn — and the admin route that sets a
//! tenant's answer threshold.
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
fn handoff_transcript(message: &str, reply: &str) -> String {
    format!("Customer: {message}\n\nSupport: {reply}")
}

/// The turn's one atomic batch: its own statements, then — when this turn
/// escalates and a [`HandoffSink`](crate::HandoffSink) is composed — the
/// sink's statements for `transcript`. Appending here is what makes the
/// answer and the ticket all-or-nothing. `Support::new()` composes no
/// sink, so this is just the turn and a handoff only marks
/// `needs_escalation`.
fn turn_batch(state: &ModuleState, turn: &store::Turn, transcript: Option<&str>) -> Vec<Statement> {
    let mut statements = store::turn_statements(turn);
    if let (Some(transcript), Some(sink)) = (transcript, state.handoff.as_deref()) {
        statements.extend(sink.enqueue(
            &state.ctx,
            &turn.tenant_id,
            &turn.conversation_id,
            transcript,
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
fn turn_language(message: &str, accept_language: Option<&str>) -> Option<String> {
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
    // The turn's language, from the message's own words first and the
    // caller's Accept-Language below that. Decided here, before anything
    // can fail, so the prompt, the canned texts and both stored messages
    // of the turn speak one language.
    let lang = turn_language(
        message,
        headers
            .get(header::ACCEPT_LANGUAGE)
            .and_then(|value| value.to_str().ok()),
    );

    let clock: &dyn Clock = required_port(ctx.ports.clock.as_deref(), "Clock")?;
    let id_gen: &dyn IdGen = required_port(ctx.ports.id_gen.as_deref(), "IdGen")?;
    let db: &dyn Database = required_port(ctx.ports.db.as_deref(), "Db")?;

    // 1–2. The conversation this turn belongs to (or none) and its counts
    //      — both reads, taken before any write.
    let (conversation, counts) = resolve_conversation(
        db,
        &tenant_id,
        body.conversation_id.as_deref(),
        &scope.request_id,
    )
    .await?;

    // 3. Retrieve — the same BM25 path `GET /search` uses.
    let chunks = retrieve(db, &tenant_id, message, TOP_K).await?;

    // 4. The tenant's threshold, else the documented default.
    let threshold = store::tenant_threshold_pct(db, &tenant_id)
        .await?
        .map_or(DEFAULT_ANSWER_THRESHOLD, answer::pct_confidence);

    // 5. Ask the model. Nothing has been written yet, which is what makes
    //    every failure here consume nothing.
    let prompt = build_prompt(&chunks, message, lang.as_deref());
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
        Outcome::Clarify => (clarify_message(lang.as_deref()), Vec::new()),
        Outcome::Handoff => (handoff_message(lang.as_deref()), Vec::new()),
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
        lang: lang.clone(),
    };
    // 9–10. One atomic write for the whole turn, then the kick. See
    //       [`commit_turn`] for why the two are ordered this way.
    let transcript = escalates.then(|| handoff_transcript(message, &shown));
    commit_turn(
        &state,
        ctx,
        db,
        &turn,
        transcript.as_deref(),
        scope.defer.clone(),
    )
    .await?;

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
/// composed), the sink's for that transcript — so the "escalated" answer
/// and the ticket behind it commit or roll back together. The kick runs
/// only *after* the commit: a failure there costs a delay (the scheduled
/// drain is the backstop), never the ticket the batch just wrote.
///
/// Extracted from [`post_message`] to keep the turn's ordering readable
/// as one numbered sequence without the handler tripping the function
/// length lint.
async fn commit_turn(
    state: &ModuleState,
    ctx: &ModuleContext,
    db: &dyn Database,
    turn: &store::Turn,
    transcript: Option<&str>,
    defer: Arc<dyn Defer>,
) -> Result<(), Problem> {
    db.batch_atomic(&turn_batch(state, turn, transcript))
        .await?;
    if transcript.is_some()
        && let Some(sink) = state.handoff.as_deref()
    {
        sink.kick(ctx, defer);
    }
    Ok(())
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
/// the hand-written reply schema. A turn with a known language adds the
/// one-line `respond_in: <lang>` the system prompt names, so the model
/// answers in the language the question was asked in.
fn build_prompt(chunks: &[Retrieved], question: &str, lang: Option<&str>) -> Prompt {
    let mut context = String::new();
    for hit in chunks {
        // Writing into a `String` cannot fail.
        let _ = writeln!(context, "[{}] {}", hit.chunk.id, hit.chunk.body);
    }
    if context.is_empty() {
        context.push_str("(nothing was retrieved for this question)\n");
    }
    let respond_in = lang.map_or_else(String::new, |lang| format!("respond_in: {lang}\n"));
    Prompt::new(ModelTier::Fast)
        .system(SYSTEM_PROMPT)
        .user(format!(
            "Question:\n{question}\n{respond_in}\nRetrieved context — cite only these chunk \
             ids:\n{context}"
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

#[cfg(test)]
mod tests {
    use super::{clarify_message, handoff_message, turn_language};

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
}
