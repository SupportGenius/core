//! The `Tracker` port: filing a ticket into *someone else's* tracker — a
//! GitHub repo, a Jira site, a Linear team, a Zendesk subdomain, Intercom,
//! a Salesforce instance, `HubSpot`, a Slack channel, or a plain webhook —
//! and asking how that ticket is doing afterwards.
//!
//! The reason this is a port and not a module detail is the deployment
//! shape: one hosted Worker serves many customer companies, and each
//! company files into *its own* tracker under *its own* API token. The
//! destinations and the credentials are therefore tenant data, resolved per
//! request — which is why every [`Tracker`] method takes a
//! [`Destination`] and a [`Credential`] rather than the adapter holding
//! either at construction (see the trait's docs for why that differs from
//! the Resend/Stripe adapter shape).
//!
//! Mirrored verbatim from harness `main`; see [`crate::ports`] for the
//! mirroring rules.

use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zeroize::Zeroizing;

/// Where a ticket is filed. One variant per supported tracker, because the
/// nine do not share a shape: a GitHub destination is an owner and repo, a
/// webhook is a URL, and Intercom and `HubSpot` address nothing but the
/// credential — the workspace/portal the token belongs to *is* the
/// destination, so those variants carry no fields at all.
///
/// The fields name *where* to file, not *who may file*: an owner, a repo, a
/// site, a channel are routing facts a venture may reasonably hold, log and
/// persist. Most variants therefore hold no secret — `Webhook` is the
/// exception, because its URL routinely carries a bearer token in its path
/// or query — so the hand-written [`Debug`] renders a webhook URL as
/// `[redacted]`, following the harness `Recipient` precedent: core's log
/// redaction keys off the *field name* ([`cratefield_core::is_secret_field`])
/// and cannot see inside a `{:?}` of this enum.
///
/// [`Serialize`]/[`Deserialize`] stay, because a destination has to be
/// persisted and read back — it is tenant configuration (the credential
/// proper is [`Credential`], which refuses both) — and `Serialize`
/// deliberately does **not** redact, because round-tripping tenant
/// configuration has to work. The consequence: a serialised
/// `Destination::Webhook` **is** the credential — store it where secrets
/// go, never in a log, an event payload, or an error body.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Destination {
    /// A repository on GitHub (or GitHub Enterprise).
    GitHub {
        /// The repository's owner (a user or organisation).
        owner: String,
        /// The repository name.
        repo: String,
    },
    /// A project on a Jira site.
    Jira {
        /// The Jira site's hostname prefix (`acme` for `acme.atlassian.net`).
        site: String,
        /// The project key tickets are filed under (`PROJ`).
        project: String,
    },
    /// A team on Linear.
    Linear {
        /// The team tickets are filed under.
        team: String,
    },
    /// A Zendesk subdomain.
    Zendesk {
        /// The subdomain (`acme` for `acme.zendesk.com`).
        subdomain: String,
    },
    /// Intercom. The workspace is identified by the credential.
    Intercom,
    /// A Salesforce instance.
    Salesforce {
        /// The instance's hostname (`acme.my.salesforce.com`).
        instance: String,
    },
    /// `HubSpot`. The portal is identified by the credential.
    HubSpot,
    /// A Slack channel.
    Slack {
        /// The channel ID tickets are filed into (`C0123456789`).
        channel: String,
    },
    /// A generic webhook. **The URL is credential material**: it is the
    /// bearer capability that lets anyone holding it file tickets, so it is
    /// redacted in `Debug` and adapters must treat it like a token.
    Webhook {
        /// The URL to `POST` the ticket to.
        url: String,
    },
}

impl Destination {
    /// The kind of tracker this destination names, for logs, metrics and
    /// error messages: `"github"`, `"jira"`, `"linear"`, `"zendesk"`,
    /// `"intercom"`, `"salesforce"`, `"hubspot"`, `"slack"`, `"webhook"`.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Destination::GitHub { .. } => "github",
            Destination::Jira { .. } => "jira",
            Destination::Linear { .. } => "linear",
            Destination::Zendesk { .. } => "zendesk",
            Destination::Intercom => "intercom",
            Destination::Salesforce { .. } => "salesforce",
            Destination::HubSpot => "hubspot",
            Destination::Slack { .. } => "slack",
            Destination::Webhook { .. } => "webhook",
        }
    }
}

/// Prints the variant and its non-secret fields, never a webhook URL — a
/// webhook endpoint is a bearer capability, and the token it carries in its
/// path or query must not survive into a log line.
impl Debug for Destination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Destination::GitHub { owner, repo } => write!(
                f,
                "Destination::GitHub {{ owner: {owner:?}, repo: {repo:?} }}"
            ),
            Destination::Jira { site, project } => write!(
                f,
                "Destination::Jira {{ site: {site:?}, project: {project:?} }}"
            ),
            Destination::Linear { team } => {
                write!(f, "Destination::Linear {{ team: {team:?} }}")
            }
            Destination::Zendesk { subdomain } => {
                write!(f, "Destination::Zendesk {{ subdomain: {subdomain:?} }}")
            }
            Destination::Intercom => f.write_str("Destination::Intercom"),
            Destination::Salesforce { instance } => {
                write!(f, "Destination::Salesforce {{ instance: {instance:?} }}")
            }
            Destination::HubSpot => f.write_str("Destination::HubSpot"),
            Destination::Slack { channel } => {
                write!(f, "Destination::Slack {{ channel: {channel:?} }}")
            }
            // The whole URL is withheld, host included: a webhook endpoint
            // is itself the capability.
            Destination::Webhook { .. } => f.write_str("Destination::Webhook { url: [redacted] }"),
        }
    }
}

/// The API credential the destination's tracker expects — the customer
/// company's token, decrypted from `cratefield-secrets` immediately before
/// the call and never stored anywhere else.
///
/// The bytes are zeroised on drop ([`Zeroizing`]), and the type implements
/// neither `Display`, `Serialize`, `Deserialize` nor `PartialEq`: a secret
/// that can be formatted, serialised or compared is a secret that ends up
/// in a log line, a JSON body, or a database row nobody clears. `Debug` is
/// implemented, and redacts:
///
/// ```
/// # use module_escalation::ports::tracker::Credential;
/// let cred = Credential::new("ghp_AAAAnobodyshouldseethis");
/// assert_eq!(format!("{cred:?}"), "Credential([redacted])");
/// ```
///
/// [`Clone`] stays: a copy is a second *buffer* holding the same secret —
/// `Zeroizing`'s `Clone` clones the inner `String`, which allocates fresh —
/// and each buffer is zeroised when its own handle drops. So no copy
/// outlives unzeroised, but every clone is one more live copy of the
/// secret to clear; clone deliberately.
#[derive(Clone)]
pub struct Credential(Zeroizing<String>);

impl Credential {
    /// Wraps a credential.
    #[must_use]
    pub fn new(secret: impl Into<String>) -> Self {
        Self(Zeroizing::new(secret.into()))
    }

    /// The credential. Named `expose` so a reader has to notice — the same
    /// rule the harness `SecretBytes` follows. Every use is
    /// deliberate and auditable: an adapter reaches it once, to build the
    /// one `Authorization` header the call needs, and nothing else in the
    /// harness ever should.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Credential([redacted])")
    }
}

/// How urgently the ticket should be attended to. Adapters map this onto
/// whatever their tracker has — a GitHub label, a Jira priority, a Slack
/// `Notification-Alert` — and document what they drop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    /// Worth knowing. A note, not a page.
    Info,
    /// Wrong, and someone should look soon.
    Warning,
    /// Broken for users; file it and act on it today.
    Error,
    /// Data loss, security, or everything down; wake someone up.
    Critical,
}

impl Severity {
    /// The name used in errors, logs and labels.
    pub fn name(&self) -> &'static str {
        match self {
            Severity::Info => "info",
            Severity::Warning => "warning",
            Severity::Error => "error",
            Severity::Critical => "critical",
        }
    }
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// A ticket to file, in tracker-neutral terms: every adapter maps the
/// fields its protocol has and documents what it drops. `title` and
/// `body_markdown` are the only content every tracker takes; `labels` and
/// `environment` are requests an adapter may flatten into the body, a
/// label list, or nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TicketDraft {
    /// Makes the file idempotent under retries; the caller owns its shape.
    /// A tracker that has already accepted this key answers with the
    /// existing ticket instead of a duplicate (an adapter may also enforce
    /// it with an idempotency header where the API has one).
    pub idempotency_key: String,
    /// The ticket's title, one line.
    pub title: String,
    /// The ticket's body, Markdown. Adapters translate or quote it
    /// verbatim per their tracker's format and say which.
    pub body_markdown: String,
    /// How urgent the ticket is.
    pub severity: Severity,
    /// Free-form labels. A tracker without labels carries them in the body.
    pub labels: Vec<String>,
    /// The deployment the ticket is about (`"production"`, `"eu-1"`), when
    /// the caller knows one.
    pub environment: Option<String>,
}

impl TicketDraft {
    /// A minimal draft: the idempotency key, a title, a body and a
    /// severity. Labels and environment are added by the chaining setters.
    #[must_use]
    pub fn new(
        idempotency_key: impl Into<String>,
        title: impl Into<String>,
        body_markdown: impl Into<String>,
        severity: Severity,
    ) -> Self {
        Self {
            idempotency_key: idempotency_key.into(),
            title: title.into(),
            body_markdown: body_markdown.into(),
            severity,
            labels: Vec::new(),
            environment: None,
        }
    }

    /// Sets the labels.
    #[must_use]
    pub fn labels(mut self, labels: Vec<String>) -> Self {
        self.labels = labels;
        self
    }

    /// Sets the environment the ticket is about.
    #[must_use]
    pub fn environment(mut self, environment: impl Into<String>) -> Self {
        self.environment = Some(environment.into());
        self
    }
}

/// The result of a file that the tracker accepted: its id in the external
/// tracker (persist it — [`Tracker::status`] is keyed by it) and the
/// ticket's human-readable URL, where the tracker has one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filed {
    /// The ticket's id in the tracker it was filed into (`OWNER/REPO#42`,
    /// `PROJ-7`, a webhook's delivery id).
    pub external_id: String,
    /// The ticket's URL, when the tracker renders one.
    pub url: String,
}

/// A ticket's state as the tracker last reported it. Adapters map their
/// tracker's workflow states onto these five, and onto
/// [`TicketState::Unknown`] anything they cannot name — they never invent
/// a sixth state, and a caller that needs the tracker's own raw state name
/// asks the tracker's API by `external_id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TicketState {
    /// Filed, nobody has picked it up.
    Open,
    /// Someone is working on it.
    InProgress,
    /// The tracker reports it fixed or answered; the caller may want to
    /// verify and close it.
    Resolved,
    /// Done, closed, will not be reopened.
    Closed,
    /// The tracker reported a state this port cannot name.
    Unknown,
}

/// A ticket's current state and, where the tracker renders one, its URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TicketStatus {
    /// The ticket's id in the tracker it was filed into.
    pub external_id: String,
    /// The state the tracker reports.
    pub state: TicketState,
    /// The ticket's URL, when the tracker renders one.
    pub url: Option<String>,
}

/// Tracker failures.
///
/// [`NotConfigured`](Self::NotConfigured) is an **error**, not an outcome:
/// unlike the push port — where "not configured" is an outcome a caller
/// can degrade around — `Tracker` answers `Result<Filed, TrackerError>`,
/// and the venture code that files a ticket needs to know nobody is
/// listening rather than be told it succeeded.
///
/// The two variants that carry a message are sanitized in `Display` the
/// way the harness sanitizes its error text: what an adapter wraps is the
/// tracker's own words, and a webhook URL or a workspace name is exactly
/// the kind of value that rides in them. `Display` therefore runs it
/// through [`cratefield_core::scrub_text`]; `Debug` still shows the raw
/// string for tests.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TrackerError {
    /// No tracker adapter is configured for this destination, or the
    /// adapter has no credential: the caller should degrade, not fail.
    #[error("tracker is not configured")]
    NotConfigured,
    /// The tracker refused the **credential**: the per-tenant token was
    /// wrong, expired, or lacked the scope the call needed (`401`/`403`),
    /// so no retry and no redraft helps — a new credential has to be
    /// issued. Distinct from [`Rejected`](Self::Rejected), which is the
    /// tracker refusing the *ticket* (a `4xx` about the request, not
    /// about who made it); conflating the two either pages on a bad draft
    /// or files nothing while a tenant's token sits expired.
    #[error("tracker refused the credential")]
    Unauthorized,
    /// The tracker refused the call — the destination was wrong, the
    /// draft was rejected (`4xx`), or the adapter does not serve this
    /// destination; not retryable without a change.
    #[error("tracker rejected the request: {scrubbed}", scrubbed = cratefield_core::scrub_text(.0))]
    Rejected(String),
    /// A transient failure (a `5xx`, a `429`, a transport error): retry
    /// later, and not before `retry_after` when the provider named a
    /// delay.
    #[error(
        "tracker request failed, retryable: {delay}",
        delay = retry_after_description(.retry_after)
    )]
    Transient {
        /// How long the provider asked the caller to wait, where it said.
        retry_after: Option<Duration>,
    },
}

impl TrackerError {
    /// The rejection an adapter returns for a destination kind it does not
    /// serve.
    #[must_use]
    pub fn unsupported_destination(dest: &Destination) -> Self {
        TrackerError::Rejected(format!(
            "unsupported destination: this adapter does not serve {}",
            dest.kind()
        ))
    }

    /// How long the provider asked the caller to wait, where it said.
    #[must_use]
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            TrackerError::Transient { retry_after } => *retry_after,
            _ => None,
        }
    }
}

/// `TrackerError::Transient`'s `Display` tail: what the provider asked
/// for, or the honest "nothing was stated" — `None` and a zero delay mean
/// different things to a caller scheduling a retry.
// thiserror's `#[error(…, arg = expr(.field))]` passes the matched field
// by reference, so the signature is not ours to choose — the same reason
// the harness's `push.rs` `duration_secs` carries this allow.
#[allow(clippy::ref_option)]
fn retry_after_description(retry_after: &Option<Duration>) -> String {
    match retry_after {
        Some(after) => format!("retry after {}s", after.as_secs()),
        None => "no delay was stated".to_owned(),
    }
}

/// Files tickets into a tracker and reports their status.
///
/// **Why the credential is a per-call parameter, and not adapter
/// construction state.** Every other adapter in the harness is built once
/// with its vendor's own key — the Resend `Mailer` holds the Resend API
/// key, the Stripe `Payments` holds the Stripe secret, because on this
/// harness the vendor account belongs to the *platform*. A tracker is the
/// opposite: one hosted Worker serves many customer companies, each filing
/// into **its own** tracker under **its own** token. Those per-tenant
/// credentials live encrypted in `cratefield-secrets`, are decrypted for
/// exactly one call, and are dropped — so the credential arrives with the
/// call, and an adapter holds nothing between calls that a tenant change
/// could leak. This is the one deliberate deviation from the
/// Resend/Stripe adapter shape.
///
/// `dest` is a parameter for the same reason: which tracker to file into
/// is tenant configuration, not deployment configuration.
#[async_trait]
pub trait Tracker: Send + Sync {
    /// Files `draft` into `dest`, authenticating as the company `cred`
    /// speaks for. Idempotent on `draft.idempotency_key` where the tracker
    /// allows it.
    ///
    /// # Errors
    ///
    /// [`TrackerError::NotConfigured`] when no adapter is wired,
    /// [`TrackerError::Unauthorized`] when the tracker refuses the
    /// credential, [`TrackerError::Rejected`] when the tracker refuses
    /// the call, and [`TrackerError::Transient`] with the provider's
    /// delay, where it gave one, when the call can be retried.
    async fn file(
        &self,
        dest: &Destination,
        cred: &Credential,
        draft: &TicketDraft,
    ) -> Result<Filed, TrackerError>;

    /// Reads one ticket's current state from `dest`.
    ///
    /// # Errors
    ///
    /// The same four as [`Tracker::file`].
    async fn status(
        &self,
        dest: &Destination,
        cred: &Credential,
        external_id: &str,
    ) -> Result<TicketStatus, TrackerError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------
    // Destination

    #[test]
    fn a_destination_round_trips_through_json() {
        for dest in all_destinations() {
            let json = serde_json::to_string(&dest).expect("serialises");
            let back: Destination = serde_json::from_str(&json).expect("deserialises");
            assert_eq!(dest, back);
        }
    }

    /// Every variant, so a test that must cover all nine cannot forget one.
    fn all_destinations() -> Vec<Destination> {
        vec![
            Destination::GitHub {
                owner: "acme".to_owned(),
                repo: "api".to_owned(),
            },
            Destination::Jira {
                site: "acme".to_owned(),
                project: "PROJ".to_owned(),
            },
            Destination::Linear {
                team: "Platform".to_owned(),
            },
            Destination::Zendesk {
                subdomain: "acme".to_owned(),
            },
            Destination::Intercom,
            Destination::Salesforce {
                instance: "acme.my.salesforce.com".to_owned(),
            },
            Destination::HubSpot,
            Destination::Slack {
                channel: "C0123456789".to_owned(),
            },
            Destination::Webhook {
                url: "https://hooks.example.test/services/TOKEN/SECRET".to_owned(),
            },
        ]
    }

    #[test]
    fn every_destination_kind_names_itself() {
        let kinds: Vec<&'static str> = all_destinations().iter().map(Destination::kind).collect();
        assert_eq!(
            kinds,
            [
                "github",
                "jira",
                "linear",
                "zendesk",
                "intercom",
                "salesforce",
                "hubspot",
                "slack",
                "webhook",
            ]
        );
    }

    #[test]
    fn the_webhook_debug_prints_no_url() {
        // A destination inside a struct is redacted too: the derived Debug
        // of a holder delegates to ours.
        #[derive(Debug)]
        struct Row {
            dest: Destination,
        }

        // A webhook endpoint is a bearer capability: the token it carries
        // in its path (Slack's do, CI services' do) must never reach a log
        // through `{:?}` — core's redaction is by field name and cannot
        // see inside this enum.
        let row = Row {
            dest: Destination::Webhook {
                url: "https://hooks.example.test/services/TOKEN/SECRET".to_owned(),
            },
        };
        let printed = format!("{row:?}");
        assert!(printed.contains("Webhook"), "{printed}");
        assert!(printed.contains("[redacted]"), "{printed}");
        assert!(!printed.contains("hooks.example.test"), "{printed}");
        assert!(!printed.contains("TOKEN"), "{printed}");
        assert!(!printed.contains("SECRET"), "{printed}");
        assert!(!printed.contains("https"), "{printed}");
        assert!(matches!(row.dest, Destination::Webhook { .. }));
    }

    #[test]
    fn the_other_destinations_debug_print_their_routing_fields() {
        // The non-secret fields are the point of the hand-written Debug:
        // "which repo did this go to" must stay answerable.
        let printed = format!(
            "{:?}",
            Destination::GitHub {
                owner: "acme".to_owned(),
                repo: "api".to_owned(),
            }
        );
        assert!(printed.contains("acme"), "{printed}");
        assert!(printed.contains("api"), "{printed}");

        let printed = format!("{:?}", Destination::Intercom);
        assert_eq!(printed, "Destination::Intercom");
        let printed = format!("{:?}", Destination::HubSpot);
        assert_eq!(printed, "Destination::HubSpot");
    }

    // -----------------------------------------------------------------
    // Credential

    #[test]
    fn the_credential_debug_prints_nothing_but_redacted() {
        // Inside a holder, the derived Debug delegates to ours.
        #[derive(Debug)]
        struct Row {
            cred: Credential,
        }

        // The realistic token an adapter would be handed. Neither the
        // secret text nor any fragment of it may survive `{:?}`.
        let row = Row {
            cred: Credential::new("ghp_4a1b2c3d4e5fREALtokenVALUEnobodyshouldsee"),
        };
        let printed = format!("{row:?}");
        assert!(!printed.contains("ghp_"), "{printed}");
        assert!(!printed.contains("REALtokenVALUE"), "{printed}");
        assert!(printed.contains("Credential([redacted])"), "{printed}");
        assert_eq!(
            row.cred.expose(),
            "ghp_4a1b2c3d4e5fREALtokenVALUEnobodyshouldsee"
        );
    }

    #[test]
    fn the_credential_exposes_its_text_once_wrapped() {
        let cred = Credential::new("token-from-secrets");
        assert_eq!(cred.expose(), "token-from-secrets");
        // Cloning is a second buffer with the same secret; each is
        // zeroised when its own handle drops.
        let copy = cred.clone();
        assert_eq!(copy.expose(), "token-from-secrets");
    }

    // -----------------------------------------------------------------
    // TicketDraft and Severity

    #[test]
    fn a_draft_builds_through_the_chaining_setters() {
        let draft = TicketDraft::new(
            "outbox-42",
            "Checkout failing",
            "Stripe 500s on `/v1/charges`.",
            Severity::Error,
        )
        .labels(vec!["billing".to_owned(), "incident".to_owned()])
        .environment("production");

        assert_eq!(draft.idempotency_key, "outbox-42");
        assert_eq!(draft.title, "Checkout failing");
        assert_eq!(draft.severity, Severity::Error);
        assert_eq!(draft.labels, ["billing", "incident"]);
        assert_eq!(draft.environment.as_deref(), Some("production"));
    }

    #[test]
    fn a_minimal_draft_has_no_labels_or_environment() {
        let draft = TicketDraft::new("k", "t", "b", Severity::Info);
        assert!(draft.labels.is_empty());
        assert_eq!(draft.environment, None);
    }

    #[test]
    fn severity_names_are_snake_case_on_the_wire_and_in_display() {
        for (severity, name) in [
            (Severity::Info, "info"),
            (Severity::Warning, "warning"),
            (Severity::Error, "error"),
            (Severity::Critical, "critical"),
        ] {
            assert_eq!(severity.name(), name);
            assert_eq!(severity.to_string(), name);
            let json = serde_json::to_value(severity).unwrap();
            assert_eq!(json, name);
            let back: Severity = serde_json::from_value(json).unwrap();
            assert_eq!(back, severity);
        }
    }

    // -----------------------------------------------------------------
    // TrackerError

    #[test]
    fn transient_carries_an_optional_retry_after() {
        let plain = TrackerError::Transient { retry_after: None };
        assert_eq!(plain.retry_after(), None);
        let throttled = TrackerError::Transient {
            retry_after: Some(Duration::from_secs(30)),
        };
        assert_eq!(throttled.retry_after(), Some(Duration::from_secs(30)));
        // Only `Transient` can name a delay.
        assert_eq!(TrackerError::NotConfigured.retry_after(), None);
        assert_eq!(TrackerError::Unauthorized.retry_after(), None);
        assert_eq!(
            TrackerError::Rejected("jira 401".to_owned()).retry_after(),
            None
        );
    }

    #[test]
    fn the_transient_display_states_the_delay_where_one_was_given() {
        let error = TrackerError::Transient {
            retry_after: Some(Duration::from_secs(30)),
        };
        let text = error.to_string();
        assert!(text.contains("retryable"), "{text}");
        assert!(text.contains("30s"), "{text}");

        let text = TrackerError::Transient { retry_after: None }.to_string();
        assert!(text.contains("retryable"), "{text}");
        assert!(text.contains("no delay"), "{text}");
    }

    #[test]
    fn display_sanitizes_the_rejection_text() {
        // The text an adapter wraps is the tracker's own words, and a
        // webhook endpoint is a bearer capability URL: the token it
        // carries in its query must not survive into a log line or a
        // dead-letter row.
        let error = TrackerError::Rejected(
            "webhook 400 for https://hooks.example.test/services/TOKEN?token=SECRET".to_owned(),
        );
        let text = error.to_string();
        assert!(!text.contains("SECRET"), "{text}");
        assert!(text.contains("?[redacted]"), "{text}");

        // A tracker that quotes the account it bounced on quotes an
        // address, and that is scrubbed too.
        let error = TrackerError::Rejected("jira 403 for devops@acme.test".to_owned());
        let text = error.to_string();
        assert!(!text.contains('@'), "{text}");
        assert!(text.contains("[subject_hash:"), "{text}");

        // `Debug` still shows the raw string for a failing test to read.
        assert!(format!("{error:?}").contains("devops@acme.test"));

        assert_eq!(
            TrackerError::NotConfigured.to_string(),
            "tracker is not configured"
        );
    }

    #[test]
    fn an_unsupported_destination_names_the_kind() {
        let error = TrackerError::unsupported_destination(&Destination::Slack {
            channel: "C0123456789".to_owned(),
        });
        let message = error.to_string();
        assert!(message.contains("unsupported destination"), "{message}");
        assert!(message.contains("slack"), "{message}");
    }

    #[test]
    fn an_unauthorized_credential_is_distinct_from_a_rejected_ticket() {
        // The fourth variant: the per-tenant credential was refused —
        // wrong, expired, or lacking the scope the call needed — which
        // says something about *who* called, where `Rejected` is the
        // tracker refusing the *ticket* itself.
        let error = TrackerError::Unauthorized;
        assert_eq!(error.to_string(), "tracker refused the credential");
        assert_eq!(error.retry_after(), None, "an auth failure names no delay");
        assert_ne!(error, TrackerError::Rejected("401".to_owned()));
        // `Debug` names the variant, so a failing test can tell the two
        // apart.
        assert!(format!("{error:?}").contains("Unauthorized"));
    }
}
