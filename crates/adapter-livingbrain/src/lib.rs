//! The Living Brain adapter (issue #64): SupportGenius's client for the
//! external Living Brain service, and the only crate that knows its wire
//! contract — two authenticated JSON `POST`s. `{base}` is [`URL_KEY`], the
//! bearer [`TOKEN_KEY`]; either unset or empty means the deployment is
//! disconnected and every call answers `None` without a hop.
//!
//! - `{base}/v1/tools/brain_answer_public` — `{"question"}` →
//!   `{"answer": string|null, "citations": [{"title", "url", "scope"}]}`.
//!   `null` is "I don't know".
//! - `{base}/v1/tools/brain_route` — `{"topic"}` → `{"target": string|null}`,
//!   the owner of the topic or none.
//!
//! The shape is SupportGenius's **proposal** pending the upstream wiki
//! issue (Livingbrain-wiki/livingbrain#51), the only file to reconcile when
//! it lands. `brain_note_ticket` is deliberately absent: escalation has no
//! ticket-resolved event to call it on yet.
//!
//! The guard can vet the answer's shape and citations, not its prose:
//! `brain_answer_public` is trusted to draw only on the public scope, and
//! the guard refuses whole any answer whose citations are not all public.
//!
//! Customer safety lives here, on the contract, so every caller inherits
//! it: an answer is accepted only when non-empty, carrying at least one
//! citation, and **every** citation scoped `"public"` with a non-empty URL.
//! Anything else — internal text, a mixed list, an uncited claim — is
//! discarded whole, so the caller falls through to its own path. A
//! transport error, a non-2xx status or a body that is not the schema is
//! likewise `None` (with a `tracing` line), never an error to classify.

use std::time::Duration;

use bytes::Bytes;
use cratefield_core::{Config, HttpClient, HttpPolicy};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// The `Config` key naming the service's base URL.
pub const URL_KEY: &str = "LIVINGBRAIN_URL";

/// The `Config` key naming the scoped service credential, sent as the
/// bearer token.
pub const TOKEN_KEY: &str = "LIVINGBRAIN_TOKEN";

const ANSWER_PATH: &str = "/v1/tools/brain_answer_public";
const ROUTE_PATH: &str = "/v1/tools/brain_route";

/// The answer body's ceiling: an answer plus its citations is kilobytes,
/// far below the port's 4 MiB default — the posture the connectors take
/// toward a response they read whole.
const MAX_RESPONSE_BYTES: usize = 256 * 1024;

/// A customer turn waits on the answer call inline, so it gets a short
/// deadline: a hung brain must not stall every visitor for the port's 10s
/// default. The route call runs inside the file stage's own bounded drain
/// and keeps that default.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(3);
const ROUTE_TIMEOUT: Duration = cratefield_core::DEFAULT_RESPONSE_TIMEOUT;

/// A configured Living Brain client. Built from config, so a deployment
/// that names no service gets `None` rather than a client that fails on
/// every call.
#[derive(Debug, Clone)]
pub struct LivingBrain {
    /// The base URL, trailing slashes trimmed once here so every call
    /// joins cleanly.
    base: String,
    token: String,
}

impl LivingBrain {
    /// The service this deployment configured, or `None` when either key is
    /// unset, empty, or only whitespace — the disconnected state, which
    /// callers treat as "no Living Brain", not as an error.
    #[must_use]
    pub fn from_config(config: &dyn Config) -> Option<Self> {
        let base = config.get(URL_KEY)?;
        let token = config.get(TOKEN_KEY)?;
        let base = base.trim().trim_end_matches('/').to_owned();
        let token = token.trim().to_owned();
        (!base.is_empty() && !token.is_empty()).then_some(Self { base, token })
    }

    /// The customer-safe answer to `question`, or `None` when the service
    /// does not know, says nothing a customer may see, or fails — see the
    /// crate docs for what "customer-safe" admits. `question` is sent as
    /// given: a caller that wants emails, tokens and links redacted first
    /// redacts them.
    pub async fn answer_public(
        &self,
        http: &dyn HttpClient,
        question: &str,
    ) -> Option<PublicAnswer> {
        let response = self
            .post_json(
                http,
                ANSWER_PATH,
                &AnswerRequest { question },
                ANSWER_TIMEOUT,
            )
            .await?;
        accept_answer(response)
    }

    /// The owner `topic` routes to — an opaque target the caller turns into
    /// a label or a mention — or `None` when nobody owns it or the call
    /// fails. The caller's configured destination is the fallback rule, so
    /// a `None` here is never an error.
    pub async fn route(&self, http: &dyn HttpClient, topic: &str) -> Option<String> {
        let response: RouteResponse = self
            .post_json(http, ROUTE_PATH, &RouteRequest { topic }, ROUTE_TIMEOUT)
            .await?;
        sanitize_target(response.target)
    }

    /// One authenticated JSON `POST`, deserialized, or `None` for every
    /// failure the caller falls back from: a URL that will not build, a
    /// transport error, a non-2xx status, a body that is not the schema.
    /// The [`HttpPolicy`] bounds the exchange, so a hostile or hung
    /// upstream costs a bounded wait and allocation.
    async fn post_json<T: DeserializeOwned>(
        &self,
        http: &dyn HttpClient,
        path: &str,
        body: &impl Serialize,
        timeout: Duration,
    ) -> Option<T> {
        let mut request = http::Request::builder()
            .method(http::Method::POST)
            .uri(format!("{}{path}", self.base))
            .header(
                http::header::AUTHORIZATION,
                format!("Bearer {}", self.token),
            )
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(Bytes::from(serde_json::to_vec(body).ok()?))
            .ok()?;
        request.extensions_mut().insert(HttpPolicy {
            max_response_bytes: MAX_RESPONSE_BYTES,
            timeout,
        });
        let response = match http.send(request).await {
            Ok(response) if response.status().is_success() => response,
            Ok(response) => {
                tracing::warn!(path, status = %response.status(), "Living Brain answered non-2xx");
                return None;
            }
            Err(err) => {
                tracing::warn!(path, error = %err, "Living Brain call failed");
                return None;
            }
        };
        serde_json::from_slice(response.body()).ok()
    }
}

/// The self-contained answer guard: the shape a caller may publish, or
/// `None`. Split out so the rule is one readable function, unit-tested on
/// its own.
fn accept_answer(response: AnswerResponse) -> Option<PublicAnswer> {
    let answer = response.answer?;
    if answer.trim().is_empty() {
        return None;
    }
    if response.citations.is_empty() {
        tracing::warn!("Living Brain answer carried no citation; discarded");
        return None;
    }
    let mut citations = Vec::with_capacity(response.citations.len());
    for citation in response.citations {
        // Every citation, not any: a list mixing a public page with an
        // internal one is not partially safe.
        if citation.scope.as_deref() != Some("public") {
            tracing::warn!("Living Brain answer cited a non-public scope; discarded");
            return None;
        }
        let url = citation.url.filter(|url| !url.trim().is_empty())?;
        citations.push(PublicCitation {
            title: citation.title,
            url,
        });
    }
    Some(PublicAnswer { answer, citations })
}

/// The longest route target that is plausibly an owner. Longer is not one.
const MAX_TARGET_CHARS: usize = 100;

/// The owner a route call named, cleaned, or `None`. Trimmed; refused if it
/// is empty, carries a control character or newline (it becomes a label and
/// a body line), or exceeds [`MAX_TARGET_CHARS`].
fn sanitize_target(target: Option<String>) -> Option<String> {
    let target = target?;
    let target = target.trim();
    if target.is_empty()
        || target.chars().count() > MAX_TARGET_CHARS
        || target.chars().any(char::is_control)
    {
        return None;
    }
    Some(target.to_owned())
}

/// A customer-safe answer and the public pages it is grounded in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicAnswer {
    pub answer: String,
    pub citations: Vec<PublicCitation>,
}

/// One public citation. `title` may be empty when the service omits one;
/// `url` is never empty (the guard refused the answer otherwise).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicCitation {
    pub title: String,
    pub url: String,
}

#[derive(Serialize)]
struct AnswerRequest<'a> {
    question: &'a str,
}

#[derive(Serialize)]
struct RouteRequest<'a> {
    topic: &'a str,
}

#[derive(Deserialize)]
struct AnswerResponse {
    answer: Option<String>,
    #[serde(default)]
    citations: Vec<CitationResponse>,
}

#[derive(Deserialize)]
struct CitationResponse {
    #[serde(default)]
    title: String,
    url: Option<String>,
    scope: Option<String>,
}

#[derive(Deserialize)]
struct RouteResponse {
    target: Option<String>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// Whether the guard admits one wire answer.
    fn accepts(answer: serde_json::Value) -> bool {
        accept_answer(serde_json::from_value(answer).expect("fixture parses")).is_some()
    }

    #[test]
    fn the_guard_admits_only_a_fully_public_answer() {
        let public = json!({ "title": "Resetting your password", "url": "https://kb.example/p", "scope": "public" });
        let cases = [
            (
                json!({ "answer": "Reset it.", "citations": [public] }),
                true,
            ),
            (json!({ "answer": null, "citations": [] }), false),
            (json!({ "answer": "   ", "citations": [public] }), false),
            // Uncited, internal-scoped, mixed, and a public scope with no URL.
            (json!({ "answer": "trust me", "citations": [] }), false),
            (
                json!({ "answer": "x", "citations": [{ "url": "https://kb.example/i", "scope": "internal" }] }),
                false,
            ),
            (
                json!({ "answer": "x", "citations": [public, { "url": "https://kb.example/i", "scope": "internal" }] }),
                false,
            ),
            (
                json!({ "answer": "x", "citations": [{ "scope": "public" }] }),
                false,
            ),
        ];
        for (case, expected) in cases {
            assert_eq!(accepts(case.clone()), expected, "{case}");
        }

        // The route target is sanitised the same way: trimmed, refused on a
        // control character or an oversized value, empty is nobody.
        let too_long = "a".repeat(MAX_TARGET_CHARS + 1);
        let targets = [
            (Some("  @billing-team  "), Some("@billing-team")),
            (None, None),
            (Some("   "), None),
            (Some("@billing\nX-Injected: 1"), None),
            (Some(too_long.as_str()), None),
        ];
        for (target, expected) in targets {
            assert_eq!(
                sanitize_target(target.map(str::to_owned)),
                expected.map(str::to_owned),
                "{target:?}"
            );
        }
    }

    #[test]
    fn a_disconnected_config_builds_nothing() {
        assert!(LivingBrain::from_config(&cratefield_core::EmptyConfig).is_none());
        let configured = cratefield_core::MapConfig::from_pairs([
            (URL_KEY, "https://brain.example/"),
            (TOKEN_KEY, "scoped-token"),
        ]);
        // The trailing slash is trimmed once, so calls join cleanly.
        assert_eq!(
            LivingBrain::from_config(&configured)
                .expect("configured")
                .base,
            "https://brain.example"
        );
    }
}
