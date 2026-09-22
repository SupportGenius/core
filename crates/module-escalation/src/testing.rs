//! Fake [`TextModel`](crate::ports::TextModel) and
//! [`Tracker`](crate::ports::Tracker) doubles for module tests, in the
//! style of `cratefield_testing::fakes`: scriptable response queues that
//! answer in order and then fail loudly, recorded calls cloned out from
//! behind a fixture mutex, cheap [`Clone`] handles over shared interiors.
//!
//! These mirror the two ports in [`crate::ports`], so they are deleted in
//! the same pass when core publishes the real ports and this module's use
//! statements move to `cratefield_core`.
//!
//! Behind the `testing` feature: the default build (the one the venture
//! links into the Worker) never carries test doubles. Integration tests in
//! `tests/` turn the feature on through the crate's self dev-dependency.

// Recording fixtures, not request state: every accessor locks an
// unpoisoned fixture mutex; per-method `# Panics` sections would add
// noise without information.
#![allow(clippy::missing_panics_doc)]

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

use crate::ports::tracker::{
    Destination, Filed, TicketDraft, TicketState, TicketStatus, Tracker, TrackerError,
};

// ---------------------------------------------------------------------------
// FakeConfig

/// An in-memory [`cratefield_core::Config`]: the deployment key/value view
/// the file stage resolves a tenant's stored `credential_ref` against.
///
/// The real config is environment plus secrets, per runtime; this double is
/// a map a test seeds with [`FakeConfig::with`]. Empty by default, so an
/// unset credential is exactly as unset as it is in production — the case
/// the file stage must dead-letter on rather than guess.
#[derive(Clone, Default)]
pub struct FakeConfig {
    values: Arc<HashMap<String, String>>,
}

impl FakeConfig {
    /// A config with no keys set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets one key (replacing any previous value), builder-style.
    #[must_use]
    pub fn with(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        Arc::make_mut(&mut self.values).insert(key.into(), value.into());
        self
    }
}

impl cratefield_core::Config for FakeConfig {
    fn get(&self, key: &str) -> Option<String> {
        self.values.get(key).cloned()
    }
}

// ---------------------------------------------------------------------------
// SettableClock

/// A [`cratefield_core::Clock`] a test can move forward.
///
/// `cratefield_testing::FixedClock` pins one instant and cannot move, but
/// the pipeline's retry behaviour only becomes visible when time does: a
/// rescheduled outbox row has to be carried past its `next_attempt_at`
/// before `claim_due` will hand it back. This clock starts at a fixed
/// whole-second instant ([`SettableClock::at_unix`]) and
/// [`SettableClock::advance_seconds`] moves it. Whole-second by
/// construction: two of its instants format to RFC 3339 strings that
/// compare chronologically as plain strings — the same comparison the
/// outbox's `next_attempt_at <= now` predicate makes — so advancing a test
/// past a retry boundary is exact, not approximate.
#[derive(Debug, Clone)]
pub struct SettableClock {
    now: Arc<Mutex<time::OffsetDateTime>>,
}

impl SettableClock {
    /// A clock pinned to `unix_seconds` seconds past the Unix epoch. An
    /// out-of-range instant falls back to the epoch itself, the way the
    /// rest of this crate degrades rather than panics.
    #[must_use]
    pub fn at_unix(unix_seconds: i64) -> Self {
        let at = time::OffsetDateTime::from_unix_timestamp(unix_seconds)
            .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
        Self {
            now: Arc::new(Mutex::new(at)),
        }
    }

    /// Moves the clock forward by `seconds`. An unrepresentable instant
    /// keeps the current one.
    pub fn advance_seconds(&self, seconds: i64) {
        let mut now = self.now.lock().expect("clock lock");
        *now = now
            .checked_add(time::Duration::seconds(seconds))
            .unwrap_or(*now);
    }
}

impl cratefield_core::Clock for SettableClock {
    fn now(&self) -> time::OffsetDateTime {
        *self.now.lock().expect("clock lock")
    }
}

// ---------------------------------------------------------------------------
// FakeTextModel

/// The [`TextModel`](crate::ports::text_model::TextModel) double now lives
/// beside the port in the shared `text-model` crate (behind its own
/// `testing` feature, which this crate's `testing` feature turns on); it is
/// re-exported here so existing `module_escalation::testing::FakeTextModel`
/// paths keep working.
pub use text_model::testing::FakeTextModel;

// ---------------------------------------------------------------------------
// FakeTracker

/// An in-memory [`Tracker`] that files from a scripted queue and records
/// every call, so a test can assert the tracker was called **exactly
/// once** (one ticket per escalation, no duplicate on retry) or **never**
/// (a gate refused, no ticket filed).
///
/// [`file`](Tracker::file) answers from its queue in order and then fails
/// loudly with [`TrackerError::Rejected`] — again, an exhausted script is
/// a bug in the test, and `Rejected` (not `Transient`) stops a retry loop
/// from marching on. [`status`](Tracker::status) answers from its own
/// script, falling through to a fixed `Open` status for the id it was
/// asked about.
///
/// The [`Credential`] is deliberately **not** recorded: it is secret
/// material (`Zeroizing`-wrapped, deliberately neither `Debug`-printable
/// nor comparable), and a recorded copy would be a second live buffer of
/// the secret a test never needs.
///
/// [`Credential`]: crate::ports::tracker::Credential
#[derive(Clone)]
pub struct FakeTracker {
    inner: Arc<FakeTrackerInner>,
}

struct FakeTrackerInner {
    scripted: Mutex<VecDeque<Result<Filed, TrackerError>>>,
    files: Mutex<Vec<(Destination, TicketDraft)>>,
    statuses: Mutex<VecDeque<TicketStatus>>,
    status_calls: Mutex<Vec<String>>,
}

impl FakeTracker {
    /// Files answer with `responses` in order, then with the
    /// exhausted-script rejection. Statuses answer with a fixed
    /// [`TicketState::Open`] unless [`FakeTracker::status_scripted`] is
    /// called.
    #[must_use]
    pub fn scripted(responses: Vec<Result<Filed, TrackerError>>) -> Self {
        Self {
            inner: Arc::new(FakeTrackerInner {
                scripted: Mutex::new(responses.into_iter().collect()),
                files: Mutex::new(Vec::new()),
                statuses: Mutex::new(VecDeque::new()),
                status_calls: Mutex::new(Vec::new()),
            }),
        }
    }

    /// One tracker that accepts a single file with this result — the
    /// happy path, in one line.
    #[must_use]
    pub fn accepting(filed: Filed) -> Self {
        Self::scripted(vec![Ok(filed)])
    }

    /// Makes [`status`](Tracker::status) answer with `statuses` in order
    /// (then with the fixed `Open` default), so a follow-up test can walk
    /// a ticket from `Open` to `Resolved` to `Closed`.
    pub fn status_scripted(&self, statuses: Vec<TicketStatus>) {
        *self.inner.statuses.lock().expect("tracker lock") = statuses.into_iter().collect();
    }

    /// Every `(destination, draft)` this fake was asked to file, in call
    /// order — including the calls that answered with an error, since
    /// whether the tracker was *reached* is exactly what a retry test
    /// asserts on.
    #[must_use]
    pub fn filed(&self) -> Vec<(Destination, TicketDraft)> {
        self.inner.files.lock().expect("tracker lock").clone()
    }

    /// Every `external_id` a status was asked about, in call order.
    #[must_use]
    pub fn status_calls(&self) -> Vec<String> {
        self.inner
            .status_calls
            .lock()
            .expect("tracker lock")
            .clone()
    }
}

#[async_trait::async_trait]
impl Tracker for FakeTracker {
    async fn file(
        &self,
        dest: &Destination,
        _cred: &crate::ports::tracker::Credential,
        draft: &TicketDraft,
    ) -> Result<Filed, TrackerError> {
        self.inner
            .files
            .lock()
            .expect("tracker lock")
            .push((dest.clone(), draft.clone()));
        let next = self
            .inner
            .scripted
            .lock()
            .expect("tracker lock")
            .pop_front();
        next.unwrap_or_else(|| {
            Err(TrackerError::Rejected(
                "fake tracker script exhausted".to_owned(),
            ))
        })
    }

    async fn status(
        &self,
        _dest: &Destination,
        _cred: &crate::ports::tracker::Credential,
        external_id: &str,
    ) -> Result<TicketStatus, TrackerError> {
        self.inner
            .status_calls
            .lock()
            .expect("tracker lock")
            .push(external_id.to_owned());
        let next = self
            .inner
            .statuses
            .lock()
            .expect("tracker lock")
            .pop_front();
        Ok(next.unwrap_or(TicketStatus {
            external_id: external_id.to_owned(),
            state: TicketState::Open,
            url: None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cratefield_core::Clock as _;
    use cratefield_core::Config as _;

    #[test]
    fn the_settable_clock_starts_whole_second_and_advances() {
        let start = time::OffsetDateTime::from_unix_timestamp(1_789_000_000).expect("epoch");
        let clock = SettableClock::at_unix(1_789_000_000);
        assert_eq!(clock.now(), start);
        assert_eq!(
            clock.now().nanosecond(),
            0,
            "whole-second: its RFC 3339 form string-compares chronologically"
        );

        clock.advance_seconds(45);
        assert_eq!((clock.now() - start).whole_seconds(), 45);
        clock.advance_seconds(10_000);
        assert_eq!((clock.now() - start).whole_seconds(), 10_045);
    }

    #[test]
    fn the_fake_config_answers_seeded_keys_only() {
        let config = FakeConfig::new().with("ESCALATION_TRACKER_CREDENTIAL", "token-1");
        assert_eq!(
            config.get("ESCALATION_TRACKER_CREDENTIAL").as_deref(),
            Some("token-1")
        );
        assert_eq!(
            config.get("ESCALATION_NOTIFY_FROM"),
            None,
            "unseeded is unset"
        );
        let reseeded = config.with("ESCALATION_TRACKER_CREDENTIAL", "token-2");
        assert_eq!(
            reseeded.get("ESCALATION_TRACKER_CREDENTIAL").as_deref(),
            Some("token-2"),
            "a later `with` replaces the value"
        );
    }
}
