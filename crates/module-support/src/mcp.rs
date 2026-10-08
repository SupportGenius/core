//! MCP over Streamable HTTP (issue #34): `POST /v1/support/mcp` speaks
//! JSON-RPC 2.0 over one POST and answers JSON — no SSE — authenticated
//! with the tenant API key exactly like every other tenant route here.
//!
//! Four tools, each one a thin wrapper over an existing route so there is
//! one implementation of the behaviour: `search_sources` over the `/search`
//! retrieval, `answer` over the `/messages` decision engine, `escalate`
//! over the handoff port, and `get_ticket` over the sink's ticket lookup.
//! The tool input schemas are `schema_for::<T>()` of the very types the
//! handlers deserialize, so they cannot drift from the routes.

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use cratefield_core::{Database, Json, Scope, Statement, schema_for};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::handlers::{self, ModuleState};
use crate::messages::{self, MessageBody, TurnFailure};
use crate::quota;
use crate::store;

/// The MCP revision this server answers with when the client requests one
/// it does not know.
const PROTOCOL_VERSION: &str = "2025-06-18";

/// The revisions this server can speak: a client's requested version is
/// echoed back when it is one of these, so a newer client and an older
/// server still agree on a protocol they both understand.
const SUPPORTED_VERSIONS: &[&str] = &["2025-06-18", "2025-03-26", "2024-11-05"];

/// `search_sources` arguments — the tool form of `GET /search`, with the
/// query spelled `query` where the GET route's query string says `q`.
#[derive(Deserialize, JsonSchema)]
pub struct SearchArgs {
    pub(crate) query: Option<String>,
    pub(crate) limit: Option<u32>,
}

/// `escalate` arguments. Exactly one of `conversation_id` / `transcript`
/// is required; `kind` defaults to [`Kind::Support`].
#[derive(Deserialize, JsonSchema)]
pub struct EscalateArgs {
    pub(crate) conversation_id: Option<String>,
    pub(crate) transcript: Option<String>,
    #[serde(default)]
    pub(crate) kind: Kind,
}

/// What an escalation is about. The escalation ticket has no
/// kind/category column today, so this is validated and echoed back and
/// recorded nowhere new — routing a bug, a support question and a lead to
/// different destinations is the router work (issue #24).
#[derive(Deserialize, Serialize, JsonSchema, Default, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Bug,
    #[default]
    Support,
    Lead,
}

/// `get_ticket` arguments.
#[derive(Deserialize, JsonSchema)]
pub struct GetTicketArgs {
    pub(crate) id: String,
}

/// `POST /mcp` — one JSON-RPC 2.0 message. A notification (no `id`) is
/// answered `202` with no body; every request is answered `200` with a
/// JSON-RPC result or error. Authentication and the rate limit run before
/// the body is parsed, as on every other tenant route.
pub(crate) async fn post_mcp(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let ctx = &state.ctx;
    let tenant_id = match handlers::authenticate(ctx, &headers).await {
        Ok(principal) => principal.tenant_id,
        Err(problem) => return problem.into_response(),
    };
    if let Some(rate_limited) = handlers::guard_rate_limit(ctx, &tenant_id).await {
        return rate_limited;
    }
    let accept_language = headers
        .get(header::ACCEPT_LANGUAGE)
        .and_then(|value| value.to_str().ok());

    let Ok(message) = serde_json::from_slice::<Value>(&body) else {
        return rpc_error(Value::Null, -32700, "parse error");
    };
    let Some(object) = message.as_object() else {
        // A JSON-RPC batch is an array; this server answers single
        // messages only, so the array is one invalid request.
        return rpc_error(Value::Null, -32600, "batch requests are not supported");
    };
    // No `id` is a notification: the spec forbids a reply of any kind.
    if !object.contains_key("id") {
        return StatusCode::ACCEPTED.into_response();
    }
    let id = object.get("id").cloned().unwrap_or(Value::Null);
    let method = object
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let params = object.get("params").cloned().unwrap_or(Value::Null);

    match method {
        "initialize" => rpc_result(id, initialize(&params)),
        "ping" => rpc_result(id, json!({})),
        "tools/list" => rpc_result(id, json!({ "tools": tools() })),
        "tools/call" => call_tool(&state, &scope, &tenant_id, id, &params, accept_language).await,
        _ => rpc_error(id, -32601, "method not found"),
    }
}

/// The `initialize` result: the client's protocol revision when it is one
/// this server knows, else [`PROTOCOL_VERSION`], and the tool capability.
fn initialize(params: &Value) -> Value {
    let requested = params.get("protocolVersion").and_then(Value::as_str);
    let version = requested
        .filter(|version| SUPPORTED_VERSIONS.contains(version))
        .unwrap_or(PROTOCOL_VERSION);
    json!({
        "protocolVersion": version,
        "capabilities": { "tools": {} },
        "serverInfo": { "name": "supportgenius", "version": env!("CARGO_PKG_VERSION") },
    })
}

/// The four tools, in a stable order. Each `inputSchema` is derived from
/// the exact type its `tools/call` deserializes, so a field added to the
/// handler appears here without a second edit.
fn tools() -> Value {
    json!([
        tool(
            "search_sources",
            "Search the workspace's indexed documents and return the ranked passages with \
             their chunk ids.",
            schema_for::<SearchArgs>(),
        ),
        tool(
            "answer",
            "Answer a customer question from the workspace's documents, with citations, \
             making the same decision POST /messages makes.",
            schema_for::<MessageBody>(),
        ),
        tool(
            "escalate",
            "Escalate a conversation — or a bare transcript — to a human, filing exactly one \
             ticket.",
            schema_for::<EscalateArgs>(),
        ),
        tool(
            "get_ticket",
            "Look up an escalated ticket's status and link.",
            schema_for::<GetTicketArgs>(),
        ),
    ])
}

fn tool(name: &str, description: &str, input: impl Serialize) -> Value {
    json!({
        "name": name,
        "description": description,
        "inputSchema": serde_json::to_value(input).unwrap_or(Value::Null),
    })
}

/// One `tools/call`. A missing or undeserializable argument is the
/// caller's mistake and answers `-32602`; a tool that ran and failed
/// (an unknown ticket, a conversation that is not there) answers a
/// result with `isError: true`, so the model sees the failure rather than
/// a transport error.
async fn call_tool(
    state: &ModuleState,
    scope: &Scope,
    tenant_id: &str,
    id: Value,
    params: &Value,
    accept_language: Option<&str>,
) -> Response {
    let Some(name) = params.get("name").and_then(Value::as_str) else {
        return rpc_error(id, -32602, "params: a tools/call requires a \"name\"");
    };
    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));

    match name {
        "search_sources" => {
            run_tool(id, arguments, |args| search_sources(state, tenant_id, args)).await
        }
        "escalate" => {
            run_tool(id, arguments, |args| {
                escalate(state, scope, tenant_id, args)
            })
            .await
        }
        "get_ticket" => {
            run_tool(id, arguments, |args: GetTicketArgs| {
                get_ticket(state, tenant_id, args.id)
            })
            .await
        }
        "answer" => match from_arguments::<MessageBody>(arguments) {
            Ok(body) => match messages::validate_message(body) {
                Ok(body) => tool_attempt(
                    id,
                    messages::answer(state, scope, tenant_id, body, accept_language)
                        .await
                        .map(|reply| reply.to_json())
                        .map_err(turn_failure_message),
                ),
                Err(problem) => rpc_error(
                    id,
                    -32602,
                    problem.detail.as_deref().unwrap_or(problem.title),
                ),
            },
            Err(message) => rpc_error(id, -32602, &message),
        },
        _ => rpc_error(id, -32602, &format!("unknown tool `{name}`")),
    }
}

/// Runs one simple tool: its `arguments` deserialize to `T`, then `run`
/// answers the structured body or a failure message. A deserialization
/// failure is the caller's `-32602`, before `run` is reached.
async fn run_tool<T, F, Fut>(id: Value, arguments: Value, run: F) -> Response
where
    T: serde::de::DeserializeOwned,
    F: FnOnce(T) -> Fut,
    Fut: std::future::Future<Output = Result<Value, String>>,
{
    match from_arguments::<T>(arguments) {
        Ok(args) => tool_attempt(id, run(args).await),
        Err(message) => rpc_error(id, -32602, &message),
    }
}

/// A tool that ran: its body as the result, or its failure as `isError`.
fn tool_attempt(id: Value, outcome: Result<Value, String>) -> Response {
    match outcome {
        Ok(value) => rpc_result(id, tool_ok(value)),
        Err(message) => rpc_result(id, tool_error(message)),
    }
}

/// Deserializes a tool's `arguments`, naming the argument type on failure.
fn from_arguments<T: serde::de::DeserializeOwned>(arguments: Value) -> Result<T, String> {
    serde_json::from_value(arguments).map_err(|err| format!("arguments: {err}"))
}

/// `search_sources`: the same clamp, retrieval and result shape as
/// `GET /search`.
async fn search_sources(
    state: &ModuleState,
    tenant_id: &str,
    args: SearchArgs,
) -> Result<Value, String> {
    let db = db(state)?;
    let limit = args
        .limit
        .unwrap_or(handlers::DEFAULT_LIMIT)
        .clamp(1, handlers::MAX_LIMIT);
    let hits = handlers::retrieve(
        db,
        tenant_id,
        args.query.as_deref().unwrap_or(""),
        limit as usize,
    )
    .await
    .map_err(|err| err.to_string())?;
    Ok(handlers::search_body(&hits))
}

/// `escalate`: verify the conversation (or mint one for a bare
/// transcript), build the transcript, and enqueue the handoff — then kick
/// it, exactly as `POST /messages` does after its own atomic write.
async fn escalate(
    state: &ModuleState,
    scope: &Scope,
    tenant_id: &str,
    args: EscalateArgs,
) -> Result<Value, String> {
    let ctx = &state.ctx;
    let Some(sink) = state.handoff.as_deref() else {
        return Err("escalation is not configured for this deployment".to_owned());
    };
    let db = db(state)?;

    // The conversation half of the batch. The transcript form mints a row
    // already `escalated`; the conversation form marks the existing one the
    // same way a `/messages` handoff does, in this same atomic write.
    let (conversation_id, transcript, conversation) =
        match (args.conversation_id.as_deref(), args.transcript.as_deref()) {
            (Some(id), None) => {
                let clock = handlers::required_port(ctx.ports.clock.as_deref(), "Clock")
                    .map_err(|_| "no clock is configured".to_owned())?;
                let transcript = conversation_transcript(db, tenant_id, id).await?;
                let escalated =
                    store::escalate_conversation_stmt(tenant_id, id, &store::iso_now(clock));
                let waiting = store::wait_for_human_stmt(tenant_id, id);
                (id.to_owned(), transcript, vec![escalated, waiting])
            }
            (None, Some(transcript)) => {
                let id_gen = handlers::required_port(ctx.ports.id_gen.as_deref(), "IdGen")
                    .map_err(|_| "no id generator is configured".to_owned())?;
                let clock = handlers::required_port(ctx.ports.clock.as_deref(), "Clock")
                    .map_err(|_| "no clock is configured".to_owned())?;
                let id = id_gen.ulid();
                let now = clock.now();
                // A transcript with no conversation behind it still needs a
                // conversation row: the ticket carries a conversation id,
                // and `get_ticket` hands it back. Opened `escalated` — it
                // exists only to carry this handoff.
                let opened =
                    store::open_conversation_stmt(tenant_id, &id, true, &store::iso_now(clock));
                // …and it is a conversation like any other, so it spends
                // the plan's monthly allowance (issue #20). The tool spends
                // no model tokens, so it is admitted on the conversation
                // meter alone.
                let admission = quota::admit(
                    db,
                    tenant_id,
                    now,
                    state.daily_token_ceiling,
                    true,
                    false,
                    &scope.request_id,
                )
                .await
                .map_err(turn_failure_message)?;
                let mut statements = vec![opened];
                statements.extend(admission.statements(tenant_id));
                (id, transcript.to_owned(), statements)
            }
            _ => {
                return Err(
                    "exactly one of \"conversation_id\" or \"transcript\" is required".to_owned(),
                );
            }
        };

    let (ticket_id, statements) = sink.handoff(ctx, tenant_id, &conversation_id, &transcript);
    let mut batch: Vec<Statement> = conversation;
    batch.extend(statements);
    db.batch_atomic(&batch)
        .await
        .map_err(|err| err.to_string())?;
    // The kick runs only after the commit: a failure there is a delay, not
    // a lost ticket (the scheduled drain is the backstop).
    sink.kick(ctx, scope.defer.clone());

    Ok(json!({
        "ticket_id": ticket_id,
        "conversation_id": conversation_id,
        "kind": args.kind,
    }))
}

/// The transcript for an existing conversation: its last turn, built by
/// the same [`handoff_transcript`](messages::handoff_transcript) the
/// `/messages` handoff path uses.
async fn conversation_transcript(
    db: &dyn Database,
    tenant_id: &str,
    conversation_id: &str,
) -> Result<String, String> {
    if store::find_conversation(db, tenant_id, conversation_id)
        .await
        .map_err(|err| err.to_string())?
        .is_none()
    {
        return Err(format!("conversation {conversation_id} not found"));
    }
    let messages = store::conversation_messages(db, tenant_id, conversation_id)
        .await
        .map_err(|err| err.to_string())?;
    let user = messages.iter().rev().find(|m| m.role == store::ROLE_USER);
    let assistant = messages
        .iter()
        .rev()
        .find(|m| m.role == store::ROLE_ASSISTANT);
    match (user, assistant) {
        (Some(user), Some(assistant)) => {
            Ok(messages::handoff_transcript(&user.body, &assistant.body))
        }
        _ => Err(format!(
            "conversation {conversation_id} has no turn to escalate"
        )),
    }
}

/// `get_ticket`: the sink's tenant-scoped lookup, shaped for the tool. A
/// ticket from another tenant is a missing ticket — the sink answers
/// `None` for both.
async fn get_ticket(state: &ModuleState, tenant_id: &str, id: String) -> Result<Value, String> {
    let Some(sink) = state.handoff.as_deref() else {
        return Err("ticket lookup is not configured for this deployment".to_owned());
    };
    let Some(view) = sink.ticket(&state.ctx, tenant_id, &id).await else {
        return Err(format!("ticket {id} not found"));
    };
    Ok(json!({
        "id": view.id,
        "status": view.status,
        "stage": view.stage,
        "external_id": view.external_id,
        "link": view.external_url,
        "conversation_id": view.conversation_id,
    }))
}

/// The Db port, or the caller-visible message when it is missing (a
/// harness bug, answered rather than panicked).
fn db(state: &ModuleState) -> Result<&dyn Database, String> {
    handlers::required_port(state.ctx.ports.db.as_deref(), "Db")
        .map_err(|_| "no database is configured".to_owned())
}

/// Why a turn failed, as the text of an `isError` tool result.
fn turn_failure_message(failure: TurnFailure) -> String {
    match failure {
        TurnFailure::Problem(problem) => problem.detail.unwrap_or_else(|| problem.title.to_owned()),
        // Only the retryable 503 carries a `Response` (its `Retry-After`),
        // which has nowhere to go in a tool result but the status.
        TurnFailure::Response(response) => {
            format!("the turn failed ({})", response.status())
        }
    }
}

/// A successful tool result: the structured body plus its JSON as text.
//
// `json!` borrows its inputs, so these helpers take owned values only for
// their callers' convenience; `needless_pass_by_value` reads that as an
// unneeded move.
#[allow(clippy::needless_pass_by_value)]
fn tool_ok(value: Value) -> Value {
    json!({
        "content": [{ "type": "text", "text": value.to_string() }],
        "structuredContent": value,
        "isError": false,
    })
}

/// A tool that ran and failed: `isError: true`, so the model can react
/// instead of seeing a transport error.
#[allow(clippy::needless_pass_by_value)]
fn tool_error(message: String) -> Value {
    json!({
        "content": [{ "type": "text", "text": message }],
        "isError": true,
    })
}

#[allow(clippy::needless_pass_by_value)]
fn rpc_result(id: Value, result: Value) -> Response {
    Json(json!({ "jsonrpc": "2.0", "id": id, "result": result })).into_response()
}

#[allow(clippy::needless_pass_by_value)]
fn rpc_error(id: Value, code: i64, message: &str) -> Response {
    Json(json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message },
    }))
    .into_response()
}
