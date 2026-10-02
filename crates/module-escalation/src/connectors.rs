//! The `Tracker` adapters this build can file into, and the check that
//! tells an operator which destinations those are.
//!
//! Filing is a port, not an adapter detail: the pipeline holds one
//! `Arc<dyn Tracker>` and hands each call a [`Destination`] and the
//! tenant's [`Credential`](cratefield_core::Credential). Which *adapters*
//! exist, though, is a build-time fact — a venture that must not link the
//! GitHub adapter should not link it — so the adapters are gated behind the
//! `tracker-github` / `tracker-webhook` cargo features (both on by default)
//! and [`routing_tracker`] registers whichever are compiled in.
//!
//! Two consequences, both deliberate: a destination whose adapter is not
//! compiled in is refused *by name* ([`check_destination`]) rather than
//! failing at file-time with a `NotConfigured` a misconfigured
//! `sg_destinations` row is indistinguishable from (the admin route #23
//! maps the refusal to `422`), and the webhook tracker's signing secret is
//! venture-wide, not per-tenant (see [`routing_tracker`]).

use std::sync::Arc;

use cratefield_core::{Clock, Destination, HttpClient, RoutingTracker};

#[cfg(feature = "tracker-github")]
use cratefield_adapter_github_issues::GitHubIssues;
#[cfg(feature = "tracker-webhook")]
use cratefield_adapter_webhook_tracker::WebhookTracker;

/// The [`Destination::kind`]s this build has an adapter for, built from the
/// cargo features at compile time: the truth about *this* binary. The admin
/// route (#23) reads it to answer "which trackers can this venture be
/// pointed at?"; the file stage's own gate is [`check_destination`], which
/// is the same list.
///
/// Chat destinations need no adapter of their own: Slack (and any other
/// incoming-webhook service) is just a [`Destination::Webhook`] — an
/// endpoint URL — so it is served by `"webhook"` above, not a `"slack"`
/// kind.
pub const SUPPORTED_DESTINATIONS: &[&str] = &[
    #[cfg(feature = "tracker-github")]
    "github",
    #[cfg(feature = "tracker-webhook")]
    "webhook",
];

/// A destination this build has no adapter for: its [`kind`](Self::kind)
/// names the tracker (`"jira"`, `"slack"`, …).
///
/// The destination admin route (#23) maps this to **`422
/// Unprocessable Entity`** — the configuration names a tracker this
/// deployment cannot file into, which is a fact about the request and not a
/// server fault — rather than the `500` an unhandled error would give.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("destination kind `{kind}` has no tracker adapter in this build")]
pub struct UnsupportedDestination {
    /// The [`Destination::kind`] that could not be served.
    pub kind: &'static str,
}

/// Whether this build can file into `destination`; `Err` names the kind it
/// cannot (the list is [`SUPPORTED_DESTINATIONS`]).
///
/// # Errors
///
/// [`UnsupportedDestination`] when `destination`'s kind has no adapter
/// compiled in.
pub fn check_destination(destination: &Destination) -> Result<(), UnsupportedDestination> {
    let kind = destination.kind();
    if SUPPORTED_DESTINATIONS.contains(&kind) {
        Ok(())
    } else {
        Err(UnsupportedDestination { kind })
    }
}

/// The router over whichever adapters are compiled in: `Arc<dyn Tracker>`
/// for the pipeline, dispatching each [`Destination`] to its adapter.
///
/// **The webhook secret is venture-wide.** `cratefield-adapter-webhook-tracker`
/// signs every delivery with one shared secret and deliberately *ignores*
/// the per-call [`Credential`](cratefield_core::Credential) ("the secret
/// signs the payload rather than authenticating to a tracker"). The
/// per-tenant value a webhook row carries is the endpoint URL in the
/// [`Destination`] itself; `webhook_secret` here is the venture's signing
/// secret, the same value the receiver verifies `Cratefield-Signature`
/// with — supply it from deployment config (this module's convention is
/// `ESCALATION_WEBHOOK_SECRET`), not the `sg_destinations` credential ref.
///
/// A build with `tracker-webhook` off ignores `webhook_secret`; a build
/// with both adapters off returns an empty router whose every destination
/// is `NotConfigured`.
//
// `needless_pass_by_value`: ports are handed over as `Arc`s, the way
// `Ports` holds them; a borrow here would force every caller to keep the
// handles alive for the router's lifetime.
#[allow(clippy::needless_pass_by_value)]
#[must_use]
pub fn routing_tracker(
    http: Arc<dyn HttpClient>,
    clock: Arc<dyn Clock>,
    webhook_secret: impl Into<String>,
) -> RoutingTracker {
    #[cfg(feature = "tracker-webhook")]
    let webhook_secret = webhook_secret.into();
    #[cfg(not(feature = "tracker-webhook"))]
    drop(webhook_secret);

    #[allow(unused_mut, unused_assignments)]
    let mut tracker = RoutingTracker::new();
    #[cfg(feature = "tracker-github")]
    {
        tracker = tracker.github(Arc::new(GitHubIssues::new(
            Arc::clone(&http),
            Arc::clone(&clock),
        )));
    }
    #[cfg(feature = "tracker-webhook")]
    {
        tracker = tracker.webhook(Arc::new(WebhookTracker::new(
            Arc::clone(&http),
            Arc::clone(&clock),
            webhook_secret,
        )));
    }
    #[cfg(not(any(feature = "tracker-github", feature = "tracker-webhook")))]
    drop((http, clock));
    tracker
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supported_destinations_are_exactly_the_compiled_in_adapters() {
        #[cfg(feature = "tracker-github")]
        assert_eq!(
            check_destination(&Destination::GitHub {
                owner: "acme".to_owned(),
                repo: "api".to_owned(),
            }),
            Ok(())
        );
        #[cfg(feature = "tracker-webhook")]
        assert_eq!(
            check_destination(&Destination::Webhook {
                url: "https://hooks.example.com/escalations".to_owned(),
            }),
            Ok(())
        );

        // Jira has no adapter in any build of this crate: the error names
        // the kind so a `422` body can say which tracker is missing.
        let unsupported = check_destination(&Destination::Jira {
            site: "acme.atlassian.net".to_owned(),
            project: "PROJ".to_owned(),
        })
        .expect_err("no jira adapter");
        assert_eq!(unsupported.kind, "jira");
        assert!(unsupported.to_string().contains("jira"));
        assert!(!SUPPORTED_DESTINATIONS.contains(&"jira"));

        // The list and the gate are one answer, feature for feature.
        #[cfg(feature = "tracker-github")]
        assert!(SUPPORTED_DESTINATIONS.contains(&"github"));
        #[cfg(feature = "tracker-webhook")]
        assert!(SUPPORTED_DESTINATIONS.contains(&"webhook"));
    }
}
