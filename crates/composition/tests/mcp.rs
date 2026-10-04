//! Issue #34: `POST /v1/support/mcp` speaks MCP over Streamable HTTP, and
//! `GET /v1/support/openapi.json` publishes the module's routes (plus
//! escalation's) as one `OpenAPI` document.
//!
//! These drive the **composed** router, not either module alone: the MCP
//! `escalate` tool files a ticket through the very handoff port a
//! `POST /messages` handoff uses, so an assertion about one is a statement
//! about the other — the parity the issue asks for.

mod common;

use axum::http::{Method, StatusCode};
use cratefield_core::schema_for;
use cratefield_testing::TestHarness;
use serde_json::{Value, json};

use module_support::MessageBody;
use module_support::mcp::{EscalateArgs, GetTicketArgs, SearchArgs};

use common::{
    MESSAGE, MESSAGES, Reply, conversation_status, count_of, kit, mint_tenant, seed_destination,
    send, ticket_column,
};

const MCP: &str = "/v1/support/mcp";
const OPENAPI: &str = "/v1/support/openapi.json";

/// One JSON-RPC message over `POST /mcp`.
async fn mcp(kit: &TestHarness, api_key: &str, message: Value) -> Reply {
    send(
        &kit.router,
        Method::POST,
        MCP,
        Some(api_key),
        Some(&message.to_string()),
    )
    .await
}

fn rpc(id: i64, method: &str, params: &Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })
}

/// One `tools/call`, the request shape an MCP client sends.
async fn mcp_call(kit: &TestHarness, api_key: &str, name: &str, arguments: Value) -> Reply {
    mcp(
        kit,
        api_key,
        rpc(
            1,
            "tools/call",
            &json!({ "name": name, "arguments": arguments }),
        ),
    )
    .await
}

/// Indexes one document so `search_sources` has a passage to find.
async fn seed_source(kit: &TestHarness, api_key: &str) {
    let reply = send(
        &kit.router,
        Method::POST,
        "/v1/support/sources",
        Some(api_key),
        Some(
            &json!({
                "title": "Resetting a password",
                "text": "Open the reset page and enter the account email; \
                         a reset link is sent to it.",
            })
            .to_string(),
        ),
    )
    .await;
    assert_eq!(reply.status, StatusCode::CREATED, "{:?}", reply.body);
}

/// The client handshake, `tools/list`, and the two tools the flow must
/// reach: `search_sources` (the retrieval `GET /search` shares) and
/// `answer` (the decision `POST /messages` makes).
#[pollster::test]
async fn mcp_handshake_lists_and_calls_tools() {
    let kit = kit();
    let (_tenant_id, api_key) = mint_tenant(&kit).await;
    seed_source(&kit, &api_key).await;

    // `initialize` echoes the client's revision when it is one this server
    // speaks, and advertises the tools capability.
    let reply = mcp(
        &kit,
        &api_key,
        rpc(0, "initialize", &json!({ "protocolVersion": "2025-03-26" })),
    )
    .await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    assert_eq!(reply.body["result"]["protocolVersion"], "2025-03-26");
    assert!(reply.body["result"]["capabilities"]["tools"].is_object());

    // A notification carries no `id`: the spec forbids a reply, so it is
    // `202` with an empty body.
    let reply = mcp(
        &kit,
        &api_key,
        json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
    )
    .await;
    assert_eq!(reply.status, StatusCode::ACCEPTED);
    assert_eq!(reply.body, Value::Null);

    // `tools/list` names four tools, each `inputSchema` the schema of the
    // very type its handler deserializes — so the two cannot drift.
    let reply = mcp(&kit, &api_key, rpc(1, "tools/list", &json!({}))).await;
    let tools = reply.body["result"]["tools"]
        .as_array()
        .expect("a tools array");
    assert_eq!(tools.len(), 4, "{:?}", reply.body);
    let schema_of = |name: &str| {
        tools
            .iter()
            .find(|tool| tool["name"] == name)
            .expect("the tool is listed")["inputSchema"]
            .clone()
    };
    for (name, schema) in [
        ("search_sources", schema_for::<SearchArgs>()),
        ("answer", schema_for::<MessageBody>()),
        ("escalate", schema_for::<EscalateArgs>()),
        ("get_ticket", schema_for::<GetTicketArgs>()),
    ] {
        assert_eq!(
            schema_of(name),
            serde_json::to_value(schema).expect("schema serializes"),
            "{name}'s inputSchema is its argument type's schema"
        );
    }

    // `tools/call` runs the tool and answers a JSON result with its
    // structured body alongside the text an older client reads.
    let reply = mcp_call(
        &kit,
        &api_key,
        "search_sources",
        json!({ "query": "reset password", "limit": 5 }),
    )
    .await;
    let result = &reply.body["result"];
    assert_eq!(result["isError"], false, "{:?}", reply.body);
    let hits = result["structuredContent"]["results"]
        .as_array()
        .expect("a results array");
    assert!(
        hits[0]["text"].as_str().expect("text").contains("reset"),
        "the seeded source is found: {:?}",
        reply.body
    );

    // `answer` reaches the same decision `POST /messages` does: both call
    // the module's one answer implementation, so neither can diverge.
    let via_mcp = mcp_call(&kit, &api_key, "answer", json!({ "message": MESSAGE })).await;
    let decided = &via_mcp.body["result"]["structuredContent"];
    let via_route = send(
        &kit.router,
        Method::POST,
        MESSAGES,
        Some(&api_key),
        Some(&json!({ "message": MESSAGE }).to_string()),
    )
    .await;
    assert_eq!(
        decided["outcome"], via_route.body["outcome"],
        "the tool and the route decide alike"
    );
    assert_eq!(
        decided["needs_escalation"],
        via_route.body["needs_escalation"]
    );

    // The transport-level failures are JSON-RPC errors, not tool results.
    let reply = mcp(&kit, &api_key, rpc(9, "does/not/exist", &json!({}))).await;
    assert_eq!(reply.body["error"]["code"], -32601);

    // A tool argument that will not deserialize is the caller's mistake.
    let reply = mcp_call(&kit, &api_key, "get_ticket", json!({ "missing": "id" })).await;
    assert_eq!(reply.body["error"]["code"], -32602, "{:?}", reply.body);

    // The tenant key is the only way in.
    let reply = mcp(&kit, "not-a-real-key", rpc(1, "tools/list", &json!({}))).await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{:?}", reply.body);
}

/// A transcript-only `escalate` mints a conversation, files exactly one
/// ticket through the same deferred run a handoff kicks, and `get_ticket`
/// reads it back — scoped to the tenant that owns it.
#[pollster::test]
async fn mcp_escalate_files_a_ticket_like_a_handoff() {
    let kit = kit();
    let (tenant_id, api_key) = mint_tenant(&kit).await;
    seed_destination(&kit, &tenant_id);

    let transcript = "customer: the export button does nothing\nagent: noted, filing it";
    let reply = mcp_call(
        &kit,
        &api_key,
        "escalate",
        json!({ "transcript": transcript }),
    )
    .await;
    assert_eq!(reply.body["result"]["isError"], false, "{:?}", reply.body);
    let ticket = &reply.body["result"]["structuredContent"];
    let ticket_id = ticket["ticket_id"]
        .as_str()
        .expect("a ticket id")
        .to_owned();
    let conversation_id = ticket["conversation_id"]
        .as_str()
        .expect("a conversation id")
        .to_owned();
    assert_eq!(ticket["kind"], "support", "the default kind is echoed");

    // Exactly one ticket, still at intake; the bare transcript opened a
    // conversation to carry it (the ticket names a conversation id).
    assert_eq!(count_of(&kit, "sg_tickets"), 1);
    assert_eq!(count_of(&kit, "sg_conversations"), 1);
    assert_eq!(ticket_column(&kit, "external_id"), None, "not filed yet");
    assert_eq!(
        ticket_column(&kit, "conversation_id").as_deref(),
        Some(conversation_id.as_str())
    );
    let stored = ticket_column(&kit, "transcript").expect("transcript column");
    assert!(stored.contains("export button"), "{stored}");

    // The kick is the same deferred escalation run a handoff kicks.
    kit.defer.drain().await;
    assert_eq!(
        ticket_column(&kit, "external_id").as_deref(),
        Some("fake-0"),
        "the deferred run filed the ticket"
    );

    // `get_ticket` reads back status and link.
    let reply = mcp_call(&kit, &api_key, "get_ticket", json!({ "id": ticket_id })).await;
    let found = &reply.body["result"]["structuredContent"];
    assert_eq!(reply.body["result"]["isError"], false, "{:?}", reply.body);
    assert_eq!(found["id"], ticket_id);
    assert_eq!(found["status"], "filed");
    assert_eq!(
        found["link"].as_str(),
        ticket_column(&kit, "external_url").as_deref()
    );

    // A second tenant holds its own key: the ticket is a missing ticket.
    let (_other_id, other_key) = mint_tenant(&kit).await;
    let reply = mcp_call(&kit, &other_key, "get_ticket", json!({ "id": ticket_id })).await;
    assert_eq!(reply.body["result"]["isError"], true, "{:?}", reply.body);
}

/// The `conversation_id` arm escalates a conversation already on record —
/// leaving it `escalated` exactly as a `/messages` handoff would, and
/// touching only a conversation this tenant owns.
#[pollster::test]
async fn mcp_escalate_by_conversation_is_tenant_scoped() {
    let kit = kit();
    let (tenant_id, api_key) = mint_tenant(&kit).await;
    seed_destination(&kit, &tenant_id);
    // A source answers the turn, so the conversation is created `open` —
    // the state the escalation below has to change.
    seed_source(&kit, &api_key).await;

    let turn = send(
        &kit.router,
        Method::POST,
        MESSAGES,
        Some(&api_key),
        Some(&json!({ "message": MESSAGE }).to_string()),
    )
    .await;
    assert_eq!(turn.status, StatusCode::OK, "{:?}", turn.body);
    assert_eq!(turn.body["needs_escalation"], false, "{:?}", turn.body);
    let conversation_id = turn.body["conversation_id"]
        .as_str()
        .expect("conversation id")
        .to_owned();
    assert_eq!(conversation_status(&kit).as_deref(), Some("open"));

    let reply = mcp_call(
        &kit,
        &api_key,
        "escalate",
        json!({ "conversation_id": conversation_id, "kind": "bug" }),
    )
    .await;
    assert_eq!(reply.body["result"]["isError"], false, "{:?}", reply.body);
    let ticket = &reply.body["result"]["structuredContent"];
    assert_eq!(ticket["conversation_id"], conversation_id);
    assert_eq!(ticket["kind"], "bug");
    assert_eq!(count_of(&kit, "sg_tickets"), 1, "the escalation filed one");
    // The tool leaves the conversation `escalated`, the flag a
    // `POST /messages` handoff sets, in the same batch as the ticket.
    assert_eq!(conversation_status(&kit).as_deref(), Some("escalated"));

    // A second escalation of the same conversation files a second ticket —
    // the same duplicate a repeated `/messages` handoff would file.
    mcp_call(
        &kit,
        &api_key,
        "escalate",
        json!({ "conversation_id": conversation_id }),
    )
    .await;
    assert_eq!(count_of(&kit, "sg_tickets"), 2, "one ticket per escalation");

    // Another tenant's key cannot escalate a conversation it does not own:
    // the lookup is tenant-scoped, so it is a missing conversation.
    let (_other_id, other_key) = mint_tenant(&kit).await;
    let reply = mcp_call(
        &kit,
        &other_key,
        "escalate",
        json!({ "conversation_id": conversation_id }),
    )
    .await;
    assert_eq!(reply.body["result"]["isError"], true, "{:?}", reply.body);
}

/// The document `GET /openapi.json` serves is valid, and every route the
/// modules mount — read from their surfaces and, independently, from the
/// router source — appears in it. A route added to a router but not to the
/// surface fails here.
#[pollster::test]
async fn openapi_document_covers_every_route() {
    let kit = kit();
    let reply = send(&kit.router, Method::GET, OPENAPI, None, None).await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    let document = &reply.body;

    // Structural validity: the OpenAPI version, an info block, the bearer
    // scheme, and — for every operation — an id and a 200.
    assert_eq!(document["openapi"], "3.1.0");
    assert!(
        document["info"]["title"]
            .as_str()
            .is_some_and(|t| !t.is_empty())
    );
    assert_eq!(
        document["components"]["securitySchemes"]["tenantKey"]["scheme"],
        "bearer"
    );
    let paths = document["paths"].as_object().expect("a paths object");
    assert!(!paths.is_empty());
    for (path, methods) in paths {
        assert!(path.starts_with('/'), "path {path} is rooted");
        for (method, operation) in methods.as_object().expect("a methods object") {
            assert!(
                ["get", "post", "put", "patch", "delete"].contains(&method.as_str()),
                "method {method} is a real verb"
            );
            assert!(
                operation["operationId"]
                    .as_str()
                    .is_some_and(|id| !id.is_empty()),
                "{method} {path} has an operationId"
            );
            assert!(
                operation["responses"]
                    .as_object()
                    .is_some_and(|responses| responses.keys().any(|code| code.starts_with('2'))),
                "{method} {path} answers a success status"
            );
        }
    }
    // `ingest-source` answers `201` on a create as well as `200` on a
    // repeat `external_id`; both are documented.
    assert!(
        document["paths"]["/v1/support/sources"]["post"]["responses"]["201"].is_object(),
        "ingest-source documents its 201"
    );

    // Every action the harness composed into a surface appears, under the
    // `/v1/<module>` prefix the harness mounts it at.
    let mut declared = 0;
    for module in &kit.harness.surface().modules {
        if module.name != "support" && module.name != "escalation" {
            continue;
        }
        for action in &module.surface.actions {
            let path = format!("/v1/{}{}", module.name, action.path);
            let method = action.method.as_str().to_lowercase();
            assert!(
                paths
                    .get(&path)
                    .and_then(|methods| methods.get(&method))
                    .is_some(),
                "declared {method} {path} is missing from the document"
            );
            declared += 1;
        }
    }
    assert!(declared >= 25, "the document covers the modules' routes");

    // And the other direction, over the router source itself: a `.route`
    // the document does not describe fails the build even if the surface
    // forgot it. `handlers.rs` mounts under `/v1/support`, `destinations.rs`
    // under `/v1/escalation`.
    for (source, prefix) in [
        (
            include_str!("../../module-support/src/handlers.rs"),
            "/v1/support",
        ),
        (
            include_str!("../../module-escalation/src/destinations.rs"),
            "/v1/escalation",
        ),
    ] {
        for route in routed_paths(source) {
            let path = format!("{prefix}{route}");
            assert!(
                paths.contains_key(&path),
                "routed {path} is missing from the document"
            );
        }
    }
}

/// The `"…"` literals passed to `.route(…)`, in source order — a literal
/// may sit on the line after `.route(` (rustfmt wraps long calls), so the
/// scan skips whitespace rather than parsing lines.
fn routed_paths(source: &str) -> Vec<String> {
    let mut paths = Vec::new();
    let mut rest = source;
    while let Some(at) = rest.find(".route(") {
        rest = &rest[at + ".route(".len()..];
        let trimmed = rest.trim_start();
        let Some(after_quote) = trimmed.strip_prefix('"') else {
            continue;
        };
        if let Some(end) = after_quote.find('"') {
            paths.push(after_quote[..end].to_owned());
        }
    }
    paths
}
