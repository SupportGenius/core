//! The crate's error type: one enum over the database, the mirrored
//! `TextModel`/`Tracker` ports, the published `Mailer` port and local
//! decoding, with [`Error::is_retryable`] so a stage can decide between
//! `retry_later` (transient — the outbox will re-deliver) and a terminal
//! outcome (the ticket moves on without the work having succeeded).

use std::time::Duration;

use cratefield_core::DbError;
use thiserror::Error;

use crate::ports::text_model::TextModelError;
use crate::ports::tracker::TrackerError;

/// Everything that can fail between intake and notification.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum Error {
    /// A [`cratefield_core::Database`] operation failed. The database is
    /// downstream of everything, so a failed read or write is treated as
    /// transient by [`Error::is_retryable`] — the outbox redelivers.
    #[error("database failure: {0}")]
    Db(#[from] DbError),
    /// The [`crate::ports::text_model::TextModel`] refused or could not
    /// complete a prompt.
    #[error("text model failure: {0}")]
    Model(#[from] TextModelError),
    /// The [`crate::ports::tracker::Tracker`] refused or could not file a
    /// ticket.
    #[error("tracker failure: {0}")]
    Tracker(#[from] TrackerError),
    /// The [`cratefield_core::Mailer`] failed on the notify stage. (Core
    /// 0.4.3 does publish the mail port, unlike the mirrored model/tracker
    /// pair, so this variant wraps the real type.)
    #[error("mail failure: {0}")]
    Mail(#[from] cratefield_core::MailError),
    /// A stored value did not decode: a payload, event detail or
    /// destination JSON that no longer parses into its type. Never
    /// retryable — the bytes will not fix themselves.
    #[error("stored value failed to decode: {0}")]
    Decode(String),
    /// A JSON round-trip through a stage payload failed. The message is
    /// stored rather than the error itself because `serde_json::Error`
    /// is neither `Clone` nor `PartialEq` and this enum is both.
    #[error("json failure: {0}")]
    Json(String),
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Json(err.to_string())
    }
}

impl Error {
    /// Whether the outbox should re-deliver this stage later (`true`) or
    /// the failure is terminal (`false`) — a rejected draft or an
    /// unauthorized tracker will not succeed on retry, and retrying it
    /// would only burn the model or tracker budget.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            // The database itself failing is transient by definition: the
            // stage never ran, so the outbox row simply comes back.
            Error::Db(_) => true,
            // `Transient` is the one model failure that names itself
            // retryable, with or without a provider delay attached.
            Error::Model(err) => matches!(err, TextModelError::Transient { .. }),
            Error::Tracker(err) => matches!(err, TrackerError::Transient { .. }),
            // The mail port names its retryable failures the way the other
            // two ports name theirs: a rate limit, a provider outage or a
            // lost transport is worth another attempt; an unauthorized key,
            // an unverified domain or a refused message is not.
            Error::Mail(err) => matches!(
                err,
                cratefield_core::MailError::RateLimited { .. }
                    | cratefield_core::MailError::Upstream(_)
                    | cratefield_core::MailError::Transport(_)
            ),
            // Corrupt stored state and malformed JSON do not heal.
            Error::Decode(_) | Error::Json(_) => false,
        }
    }

    /// How long the provider asked the caller to wait, where the failure
    /// came from a port that names one; `None` lets the caller apply its
    /// own backoff.
    #[must_use]
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Error::Model(err) => err.retry_after(),
            Error::Tracker(err) => err.retry_after(),
            Error::Mail(cratefield_core::MailError::RateLimited { retry_after }) => *retry_after,
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ports::text_model::TextModelError;
    use crate::ports::tracker::TrackerError;

    #[test]
    fn transient_port_failures_are_retryable_and_terminal_ones_are_not() {
        let transient = Error::Model(TextModelError::Transient {
            retry_after: Some(Duration::from_secs(30)),
        });
        assert!(transient.is_retryable());
        assert_eq!(transient.retry_after(), Some(Duration::from_secs(30)));

        assert!(!Error::Model(TextModelError::Rejected("no".to_owned())).is_retryable());
        assert_eq!(
            Error::Model(TextModelError::Rejected("no".to_owned())).retry_after(),
            None
        );

        assert!(
            !Error::Tracker(TrackerError::Unauthorized).is_retryable(),
            "an unauthorized tracker does not heal on retry"
        );
        assert!(Error::Tracker(TrackerError::Transient { retry_after: None }).is_retryable());
    }

    #[test]
    fn decode_and_json_failures_are_never_retryable() {
        assert!(!Error::Decode("not a destination".to_owned()).is_retryable());
        assert!(!Error::from(serde_json::from_str::<String>("{").unwrap_err()).is_retryable());
    }

    #[test]
    fn mail_failures_classify_like_the_other_ports() {
        use cratefield_core::MailError;

        assert!(Error::Mail(MailError::RateLimited { retry_after: None }).is_retryable());
        assert!(Error::Mail(MailError::Upstream("502".to_owned())).is_retryable());
        assert!(Error::Mail(MailError::Transport("timed out".to_owned())).is_retryable());
        assert!(!Error::Mail(MailError::Unauthorized).is_retryable());
        assert!(
            !Error::Mail(MailError::Invalid {
                detail: "no".to_owned()
            })
            .is_retryable()
        );

        let throttled = Error::Mail(MailError::RateLimited {
            retry_after: Some(Duration::from_secs(45)),
        });
        assert_eq!(throttled.retry_after(), Some(Duration::from_secs(45)));
        assert_eq!(
            Error::Mail(MailError::Transport("x".to_owned())).retry_after(),
            None
        );
    }
}
