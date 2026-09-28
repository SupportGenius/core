//! Test doubles the published fakes do not cover: an in-memory
//! [`Config`](cratefield_core::Config) a test seeds, and a
//! [`Clock`](cratefield_core::Clock) a test moves forward.
//!
//! The `TextModel` and `Tracker` doubles are not here: core 0.5 publishes
//! the two ports beside the pipeline's other inputs, and
//! [`cratefield_testing`] publishes their fakes in the same crate
//! (`FakeTextModel`/`FakeTracker`, mode-scripted), so a test imports them
//! from there instead of this crate mirroring either.
//!
//! Behind the `testing` feature: the default build (the one the venture
//! links into the Worker) never carries test doubles. Integration tests in
//! `tests/` turn the feature on through the crate's self dev-dependency.

// Recording fixtures, not request state: every accessor locks an
// unpoisoned fixture mutex; per-method `# Panics` sections would add
// noise without information.
#![allow(clippy::missing_panics_doc)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

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
