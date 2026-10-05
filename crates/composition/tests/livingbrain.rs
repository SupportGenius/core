//! Issue #64: the Living Brain answers a turn from the customer-safe scope,
//! and routes an escalation by ownership.
//!
//! These drive the **composed** router with a mock Living Brain on the HTTP
//! port. A covered question is answered from the public scope — no model, no
//! retrieval, no handoff — and an uncovered one hands off, the file stage
//! labeling the ticket for the owner `brain_route` names. The safety rule is
//! asserted at the wire: an answer quoting internal text, or citing nothing,
//! is rejected whole and never reaches the customer.

mod common;

use std::sync::{Arc, Mutex};

use axum::http::{Method, StatusCode, header};
use bytes::Bytes;
use cratefield_core::{Destination, HttpClient, HttpError, MapConfig, ModelTier, Statement};
use cratefield_testing::{TestHarness, TestResponse, TextModelMode, request_as};
use serde_json::{Value, json};

use adapter_livingbrain::{TOKEN_KEY, URL_KEY};
use common::{CREDENTIAL_REF, CREDENTIAL_SECRET, count_of, fast_completion, file_completion};

const ADMIN_TOKEN: &str = "test-admin-token-0123456789abcdef";
const ADMIN: &str = "/v1/support/admin/tenants";
const MESSAGES: &str = "/v1/support/messages";

/// The mock's scoped token, and the public page it answers a covered
/// question from.
const TOKEN: &str = "scoped-token";
const PUBLIC_URL: &str = "https://kb.example/public/reset";
const PUBLIC_TITLE: &str = "Resetting your password";
const PUBLIC_TEXT: &str = "Open the account page and choose Reset password.";

/// The internal-only text no customer may see, and the owner `brain_route`
/// names.
const INTERNAL_TEXT: &str = "INTERNAL-ONLY: refund override code ZX-9";
const OWNER: &str = "@billing-team";

/// `COVERED` is the question the public page answers; `UNCOVERED` is one
/// nothing covers, so an honest mock answers `null` and the turn hands off;
/// `INTERNAL_QUESTION` is answered only by the internal page, so a
/// mis-scoped service would leak its text on it.
const COVERED: &str = "how do I reset my password";
const UNCOVERED: &str = "reset password";
const INTERNAL_QUESTION: &str = "how do I override a refund";

/// What the mock's `brain_answer_public` does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// The public page when covered, `null` otherwise.
    Honest,
    /// Leak the internal page under an `"internal"` scope.
    InternalScope,
    /// Leak the internal page with no citation at all.
    NoCitations,
    /// Fail the call: a 500, as an unavailable service answers.
    ServerError,
    /// Answer 200 with a body that is not the schema.
    Garbage,
}

/// One request the mock served: its path, bearer, and body.
#[derive(Debug, Clone)]
struct Recorded {
    path: String,
    authorization: Option<String>,
    body: String,
}

/// A mock Living Brain over the `HttpClient` port: public and internal
/// pages, a [`Mode`], and every request recorded for assertions.
#[derive(Clone)]
struct BrainMock {
    mode: Mode,
    requests: Arc<Mutex<Vec<Recorded>>>,
}

impl BrainMock {
    fn new(mode: Mode) -> Self {
        Self {
            mode,
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn requests(&self) -> Vec<Recorded> {
        self.requests.lock().expect("mock lock").clone()
    }

    /// The status and wire body for one `brain_answer_public` call.
    fn answer(&self, body: &str) -> (StatusCode, String) {
        let covered = body.contains("reset my password");
        let payload = match self.mode {
            Mode::Honest if covered => json!({
                "answer": PUBLIC_TEXT,
                "citations": [{ "title": PUBLIC_TITLE, "url": PUBLIC_URL, "scope": "public" }],
            }),
            Mode::Honest => json!({ "answer": null, "citations": [] }),
            Mode::InternalScope => json!({
                "answer": INTERNAL_TEXT,
                "citations": [{ "title": "Internal runbook", "url": "https://kb.example/i", "scope": "internal" }],
            }),
            Mode::NoCitations => json!({ "answer": INTERNAL_TEXT, "citations": [] }),
            Mode::ServerError => {
                return (StatusCode::INTERNAL_SERVER_ERROR, "unavailable".to_owned());
            }
            Mode::Garbage => return (StatusCode::OK, "<html>not json</html>".to_owned()),
        };
        (StatusCode::OK, payload.to_string())
    }
}

#[async_trait::async_trait]
impl HttpClient for BrainMock {
    async fn send(
        &self,
        request: http::Request<Bytes>,
    ) -> Result<http::Response<Bytes>, HttpError> {
        let path = request.uri().path().to_owned();
        let authorization = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let body = String::from_utf8_lossy(request.body()).into_owned();
        self.requests.lock().expect("mock lock").push(Recorded {
            path: path.clone(),
            authorization,
            body: body.clone(),
        });
        let (status, payload) = match path.as_str() {
            "/v1/tools/brain_answer_public" => self.answer(&body),
            "/v1/tools/brain_route" => (StatusCode::OK, json!({ "target": OWNER }).to_string()),
            other => return Err(HttpError::Transport(format!("unexpected path {other}"))),
        };
        Ok(http::Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Bytes::from(payload))
            .expect("response builds"))
    }
}

/// One authenticated `POST` through the composed router — the kit's
/// [`request_as`], which carries the bearer the support routes require.
async fn post(router: &axum::Router, path: &str, bearer: &str, body: &Value) -> TestResponse {
    request_as(router, Method::POST, path, bearer, Some(&body.to_string())).await
}

/// The composed modules with the mock on the HTTP port and both model tiers
/// scripted. `connected` adds the Living Brain's two config keys; without
/// them the deployment is disconnected.
fn kit(mode: Mode, connected: bool) -> (TestHarness, BrainMock) {
    let brain = BrainMock::new(mode);
    let http = brain.clone();
    let kit = TestHarness::with_ports(
        vec![
            Box::new(supportgenius_composition::support()),
            Box::new(supportgenius_composition::escalation()),
        ],
        move |ports| {
            let mut pairs = vec![
                ("ADMIN_TOKEN".to_owned(), ADMIN_TOKEN.to_owned()),
                (CREDENTIAL_REF.to_owned(), CREDENTIAL_SECRET.to_owned()),
            ];
            if connected {
                pairs.push((URL_KEY.to_owned(), "https://brain.example".to_owned()));
                pairs.push((TOKEN_KEY.to_owned(), TOKEN.to_owned()));
            }
            ports.config = Arc::new(MapConfig::from_pairs(pairs));
            ports.http = Some(Arc::new(http));
        },
    );
    kit.text_model
        .set_mode_for(ModelTier::Fast, TextModelMode::Complete(fast_completion()));
    kit.text_model.set_mode_for(
        ModelTier::Strong,
        TextModelMode::Complete(file_completion()),
    );
    (kit, brain)
}

/// Mints one tenant and returns `(tenant_id, api_key)`.
async fn mint_tenant(kit: &TestHarness) -> (String, String) {
    let reply = post(&kit.router, ADMIN, ADMIN_TOKEN, &json!({ "name": "Acme" })).await;
    assert_eq!(reply.status, StatusCode::CREATED, "{:?}", reply.json());
    let body = reply.json();
    (
        body["tenant_id"].as_str().expect("tenant_id").to_owned(),
        body["api_key"].as_str().expect("api_key").to_owned(),
    )
}

/// Seeds the tenant's tracker destination — a GitHub repo with the
/// credential *reference* the file stage resolves through `Config`.
fn seed_destination(kit: &TestHarness, tenant_id: &str) {
    let stmt = module_escalation::store::put_destination_stmt(
        tenant_id,
        &Destination::GitHub {
            owner: "acme".to_owned(),
            repo: "api".to_owned(),
        },
        CREDENTIAL_REF,
        "2027-01-15T00:00:00Z",
    );
    pollster::block_on(kit.db.batch_atomic(&[stmt])).expect("destination seeds");
}

/// One `POST /messages` turn for `message`.
async fn turn(kit: &TestHarness, api_key: &str, message: &str) -> TestResponse {
    post(
        &kit.router,
        MESSAGES,
        api_key,
        &json!({ "message": message }),
    )
    .await
}

#[pollster::test]
async fn a_covered_question_is_answered_from_the_public_scope() {
    let (kit, brain) = kit(Mode::Honest, true);
    let (_tenant_id, api_key) = mint_tenant(&kit).await;

    let reply = turn(&kit, &api_key, COVERED).await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.json());
    let body = reply.json();
    assert_eq!(body["outcome"], "answered");
    assert_eq!(body["answer"], PUBLIC_TEXT);
    assert_eq!(body["citations"][0]["title"], PUBLIC_TITLE);
    assert_eq!(body["citations"][0]["url"], PUBLIC_URL);
    assert_eq!(body["needs_escalation"], false);
    assert!(!body.to_string().contains(INTERNAL_TEXT));

    // Answered from the service, token and all, and nothing was escalated
    // for it: one answer call, no route call, no ticket.
    let requests = brain.requests();
    assert_eq!(requests.len(), 1, "one answer call");
    assert_eq!(
        requests[0].authorization.as_deref(),
        Some("Bearer scoped-token")
    );
    assert!(requests[0].body.contains(COVERED));
    assert_eq!(count_of(&kit, "sg_tickets"), 0);

    // The citation stored in the second shape the column reads: `{title,
    // url}`, not the model's `{chunk_id, quote}`.
    let rows = pollster::block_on(kit.db.query(&Statement::new(
        "SELECT citations AS v FROM sg_messages WHERE role = 'assistant'",
    )))
    .expect("citations query runs");
    let stored = rows.rows[0].get::<String>("v").expect("citations column");
    assert!(stored.contains(PUBLIC_URL), "{stored}");
}

#[pollster::test]
async fn an_uncovered_question_files_for_the_routed_owner() {
    let (kit, brain) = kit(Mode::Honest, true);
    let (tenant_id, api_key) = mint_tenant(&kit).await;
    seed_destination(&kit, &tenant_id);

    let body = turn(&kit, &api_key, UNCOVERED).await.json();
    assert_eq!(body["outcome"], "handoff");
    assert_eq!(body["needs_escalation"], true);

    // The kick deferred the escalation run; draining files the ticket, and
    // the file stage's owner lookup labels it and names the owner in the body.
    kit.defer.drain().await;
    let filed = kit.tracker.last_filed().expect("the ticket filed");
    assert!(
        filed
            .draft
            .labels
            .iter()
            .any(|label| label == &format!("owner:{OWNER}")),
        "labels: {:?}",
        filed.draft.labels
    );
    assert!(filed.draft.body_markdown.contains(OWNER));

    // An answer call that found nothing, then a route call — both with the
    // scoped token.
    let requests = brain.requests();
    assert_eq!(requests.len(), 2, "an answer call then a route call");
    assert_eq!(requests[1].path, "/v1/tools/brain_route");
    assert!(
        requests
            .iter()
            .all(|request| request.authorization.as_deref() == Some("Bearer scoped-token")),
        "{requests:?}"
    );
}

#[pollster::test]
async fn an_answer_that_would_leak_internal_text_is_rejected_whole() {
    for mode in [
        Mode::InternalScope,
        Mode::NoCitations,
        Mode::ServerError,
        Mode::Garbage,
    ] {
        let (kit, _brain) = kit(mode, true);
        let (_tenant_id, api_key) = mint_tenant(&kit).await;

        // The internal page is the only one that covers this question, so a
        // service that answered from anything but the public scope would
        // hand the customer its text.
        let reply = turn(&kit, &api_key, INTERNAL_QUESTION).await.json();
        // Rejected whole, so the turn falls through to retrieval and the
        // model — which, over an empty workspace, hands off. The bad answer
        // never became the reply, and its text is nowhere in the JSON.
        assert_eq!(reply["outcome"], "handoff", "{mode:?}");
        assert!(!reply.to_string().contains(INTERNAL_TEXT), "{mode:?}");
        assert_eq!(
            count_of(&kit, "sg_messages"),
            2,
            "{mode:?}: one turn stored"
        );
    }
}

#[pollster::test]
async fn a_disconnected_deployment_files_without_an_owner() {
    let (kit, brain) = kit(Mode::Honest, false);
    let (tenant_id, api_key) = mint_tenant(&kit).await;
    seed_destination(&kit, &tenant_id);

    let reply = turn(&kit, &api_key, UNCOVERED).await.json();
    assert_eq!(reply["outcome"], "handoff");

    kit.defer.drain().await;
    let filed = kit.tracker.last_filed().expect("the ticket filed");
    assert!(
        !filed
            .draft
            .labels
            .iter()
            .any(|label| label.starts_with("owner:"))
    );
    assert!(
        !filed
            .draft
            .body_markdown
            .contains("Owner (from Living Brain)")
    );
    // Disconnected means no call at all: the config gate answered `None`
    // before any network hop.
    assert!(brain.requests().is_empty());
}
