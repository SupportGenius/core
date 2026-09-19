//! The escalation pipeline's domain model: the stages a ticket moves
//! through, the state it carries between them, the JSON schemas the model
//! calls are constrained with, and the audit vocabulary the event trail
//! uses.
//!
//! Two naming notes. The ticket's own lifecycle enum is [`Status`] — not
//! `TicketStatus`, which is the port's check-response type in
//! [`crate::ports::tracker`]. And the `judge` stage's output type is
//! [`Judgment`], to keep `Verdict` free for the judge's actual decision.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::ports::tracker::Severity;

/// The width of one stage's band in the audit `seq` numbering: a stage's
/// events occupy `[ordinal * SEQ_BAND, ordinal * SEQ_BAND + SEQ_BAND)`.
/// See [`stage_seq`].
pub const SEQ_BAND: i64 = 10;

/// The audit `seq` of the intake event: band 0, before any stage runs.
pub const INTAKE_SEQ: i64 = 0;

/// A pipeline stage — one unit of outbox work. `Intake` is deliberately
/// not a stage: it runs inline in the caller's own batch (see
/// [`crate::intake`]), so there is no `intake` topic to route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    /// Draft the ticket from the transcript (fast model).
    Draft,
    /// Judge the draft independently (strong model).
    Judge,
    /// File the ticket with the tracker.
    File,
    /// Follow up / notify the customer.
    Notify,
}

impl Stage {
    /// The outbox topic this stage's work is enqueued under.
    #[must_use]
    pub fn as_topic(self) -> &'static str {
        match self {
            Stage::Draft => "draft",
            Stage::Judge => "judge",
            Stage::File => "file",
            Stage::Notify => "notify",
        }
    }

    /// The stage a topic names, for the router that dispatches claimed
    /// outbox records; `None` for an unknown topic.
    #[must_use]
    pub fn from_topic(topic: &str) -> Option<Self> {
        match topic {
            "draft" => Some(Stage::Draft),
            "judge" => Some(Stage::Judge),
            "file" => Some(Stage::File),
            "notify" => Some(Stage::Notify),
            _ => None,
        }
    }

    /// The stage's position in the pipeline: `Draft` = 1, `Judge` = 2,
    /// `File` = 3, `Notify` = 4. Band 0 belongs to the intake event, so
    /// the audit trail reads intake first ([`INTAKE_SEQ`]) without intake
    /// needing to be a stage.
    #[must_use]
    pub fn ordinal(self) -> i64 {
        match self {
            Stage::Draft => 1,
            Stage::Judge => 2,
            Stage::File => 3,
            Stage::Notify => 4,
        }
    }
}

/// The audit `seq` for the `index_within_stage`-th event of `stage`:
/// `ordinal * 10 + index`. The band makes the event trail order by true
/// pipeline position even under a `FixedClock` (where every `at` is
/// identical) and between ULIDs minted in the same millisecond (whose
/// suffixes are random). Retry events within one stage reuse the band and
/// are disambiguated by `at`; readers order by `(seq, at, id)`.
#[must_use]
pub fn stage_seq(stage: Stage, index_within_stage: i64) -> i64 {
    debug_assert!(
        (0..SEQ_BAND).contains(&index_within_stage),
        "an event index must stay inside its stage's band"
    );
    stage.ordinal() * SEQ_BAND + index_within_stage
}

/// Where a ticket is in its lifecycle. Stored on `sg_tickets.status` in
/// its serde (`snake_case`) form. Distinct from [`Stage`]: the stage names
/// the work currently queued; the status names the outcome so far (a
/// ticket can be at stage `File` while its status is `NeedsInfo` pending
/// the customer).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// Just recorded from a conversation; nothing has run yet.
    Intake,
    /// The draft stage has the ticket.
    Drafting,
    /// The judge stage has the ticket.
    Judging,
    /// The file stage has the ticket.
    Filing,
    /// Filed with the tracker; waiting on or doing follow-up.
    Filed,
    /// The judge asked for more information from the customer.
    NeedsInfo,
    /// The judge rejected it as not a defect.
    Rejected,
    /// The file stage exhausted its retries; parked for a human.
    DeadLetter,
}

impl Status {
    /// The stored/log form (`"intake"`, `"dead_letter"`, ...).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Intake => "intake",
            Status::Drafting => "drafting",
            Status::Judging => "judging",
            Status::Filing => "filing",
            Status::Filed => "filed",
            Status::NeedsInfo => "needs_info",
            Status::Rejected => "rejected",
            Status::DeadLetter => "dead_letter",
        }
    }
}

impl std::str::FromStr for Status {
    type Err = crate::error::Error;

    /// Parses the stored form. An unknown status (a row written by a
    /// newer schema) is a decode failure, not a guess.
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "intake" => Ok(Status::Intake),
            "drafting" => Ok(Status::Drafting),
            "judging" => Ok(Status::Judging),
            "filing" => Ok(Status::Filing),
            "filed" => Ok(Status::Filed),
            "needs_info" => Ok(Status::NeedsInfo),
            "rejected" => Ok(Status::Rejected),
            "dead_letter" => Ok(Status::DeadLetter),
            _ => Err(crate::error::Error::Decode(format!(
                "unknown ticket status `{raw}`"
            ))),
        }
    }
}

/// The judge's decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// A real defect; file it.
    File,
    /// Plausibly real but under-specified; ask the customer.
    NeedsInfo,
    /// Not a defect (a question, a duplicate, working as intended).
    Reject,
}

impl Verdict {
    /// The stored/log form (`"file"`, `"needs_info"`, `"reject"`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Verdict::File => "file",
            Verdict::NeedsInfo => "needs_info",
            Verdict::Reject => "reject",
        }
    }
}

impl std::str::FromStr for Verdict {
    type Err = crate::error::Error;

    /// Parses the stored form; a decode failure for anything else.
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "file" => Ok(Verdict::File),
            "needs_info" => Ok(Verdict::NeedsInfo),
            "reject" => Ok(Verdict::Reject),
            _ => Err(crate::error::Error::Decode(format!(
                "unknown ticket verdict `{raw}`"
            ))),
        }
    }
}

/// The `draft` stage's schema-constrained output: what a fast model
/// produces from a transcript. Validated against [`Drafted::json_schema`]
/// by the model adapter before it reaches this type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Drafted {
    /// One-line ticket title.
    pub title: String,
    /// Reproduction steps, in order.
    pub repro_steps: Vec<String>,
    /// What the customer expected to happen.
    pub expected: String,
    /// What actually happened.
    pub actual: String,
    /// The deployment the ticket is about, where the transcript named one.
    pub environment: Option<String>,
    /// How urgent the ticket is.
    pub severity: Severity,
}

impl Drafted {
    /// The JSON Schema (draft 2020-12) the draft prompt constrains the
    /// model with: exactly the fields above, `environment` the one
    /// optional field, `severity` pinned to the port's `Severity` wire
    /// forms.
    #[must_use]
    pub fn json_schema() -> Value {
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Drafted",
            "type": "object",
            "additionalProperties": false,
            "required": ["title", "repro_steps", "expected", "actual", "severity"],
            "properties": {
                "title": { "type": "string", "minLength": 1 },
                "repro_steps": {
                    "type": "array",
                    "items": { "type": "string" }
                },
                "expected": { "type": "string" },
                "actual": { "type": "string" },
                "environment": { "type": "string" },
                "severity": {
                    "type": "string",
                    "enum": ["info", "warning", "error", "critical"]
                }
            }
        })
    }
}

/// The `judge` stage's schema-constrained output: what a strong model —
/// deliberately a different one from the drafter — says about the draft.
// The five booleans are the issue's judge checklist, each an independent
// finding; folding them into an enum would invent structure the schema
// does not have.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Judgment {
    /// The draft describes a defect in the product.
    pub is_defect: bool,
    /// The reproduction steps actually reproduce something.
    pub reproducible: bool,
    /// An existing ticket this duplicates, when the judge recognized one.
    pub duplicate_of: Option<String>,
    /// The drafted severity is proportionate to the described impact.
    pub severity_ok: bool,
    /// The draft carries no customer PII that must not reach a tracker.
    pub pii_clean: bool,
    /// What to do with the ticket.
    pub verdict: Verdict,
    /// The judge's reasons, verbatim. Carried in the schema (and in the
    /// audit trail) on purpose: "the judge rejected my ticket" with no
    /// why is not an answer a customer or a support engineer can act on.
    pub reasons: Vec<String>,
}

impl Judgment {
    /// The JSON Schema (draft 2020-12) the judge prompt constrains the
    /// model with. `duplicate_of` is the only optional field; `reasons`
    /// is required so an audit row always carries the why.
    #[must_use]
    pub fn json_schema() -> Value {
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Judgment",
            "type": "object",
            "additionalProperties": false,
            "required": [
                "is_defect",
                "reproducible",
                "severity_ok",
                "pii_clean",
                "verdict",
                "reasons"
            ],
            "properties": {
                "is_defect": { "type": "boolean" },
                "reproducible": { "type": "boolean" },
                "duplicate_of": { "type": "string" },
                "severity_ok": { "type": "boolean" },
                "pii_clean": { "type": "boolean" },
                "verdict": {
                    "type": "string",
                    "enum": ["file", "needs_info", "reject"]
                },
                "reasons": {
                    "type": "array",
                    "items": { "type": "string" }
                }
            }
        })
    }
}

/// What rides in the outbox `payload` column from stage to stage: the
/// minimum a stage handler needs to load the rest of the ticket
/// (`sg_tickets` keyed by `ticket_id`, scoped by `tenant_id`). Kept small
/// on purpose — the ticket row is the source of truth, and a payload that
/// duplicates it can go stale.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagePayload {
    /// The `sg_tickets` row this work is for.
    pub ticket_id: String,
    /// The tenant the ticket belongs to.
    pub tenant_id: String,
}

/// The closed set of audit events the pipeline writes to
/// `sg_ticket_events`. One row per stage transition and per notable
/// outcome; `detail` carries the JSON where there is something to say.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    /// The conversation was accepted as a ticket (intake band, seq 0).
    Intake,
    /// The draft stage started (its outbox record was claimed).
    DraftStarted,
    /// The draft stage produced a [`Drafted`].
    DraftCompleted,
    /// The draft stage failed terminally (a rejected prompt, an
    /// unparseable completion) — the outbox stops retrying.
    DraftFailed,
    /// The judge stage started.
    JudgeStarted,
    /// The judge stage produced a [`Judgment`]; `detail` carries the
    /// judgment, `reasons` included.
    JudgeCompleted,
    /// The judge stage failed terminally.
    JudgeFailed,
    /// The file stage started.
    FileStarted,
    /// The tracker accepted the ticket; `detail` carries the external id
    /// and URL.
    Filed,
    /// The tracker refused the ticket terminally (rejected,
    /// unauthorized): the ticket is parked for a human.
    FileFailed,
    /// A transient tracker failure; the outbox will re-deliver. `detail`
    /// carries the attempt count and the provider's retry hint, where it
    /// gave one.
    FileRetryScheduled,
    /// The file stage exhausted its retries; the ticket is dead-lettered.
    FileDeadLettered,
    /// The notify stage started.
    NotifyStarted,
    /// The customer was notified of the filed ticket.
    Notified,
    /// The notify stage failed terminally.
    NotifyFailed,
    /// The notify stage had nothing to do (no destination configured for
    /// the tenant, or the ticket never reached a notify-worthy outcome).
    NotifySkipped,
    /// The judge said this is not a defect; the ticket is closed as
    /// rejected. `detail` carries the judgment reasons.
    Rejected,
    /// The judge asked the customer for more information. `detail`
    /// carries the questions/reasons.
    NeedsInfo,
}

impl EventKind {
    /// The stored form (`"intake"`, `"file_dead_lettered"`, ...).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            EventKind::Intake => "intake",
            EventKind::DraftStarted => "draft_started",
            EventKind::DraftCompleted => "draft_completed",
            EventKind::DraftFailed => "draft_failed",
            EventKind::JudgeStarted => "judge_started",
            EventKind::JudgeCompleted => "judge_completed",
            EventKind::JudgeFailed => "judge_failed",
            EventKind::FileStarted => "file_started",
            EventKind::Filed => "filed",
            EventKind::FileFailed => "file_failed",
            EventKind::FileRetryScheduled => "file_retry_scheduled",
            EventKind::FileDeadLettered => "file_dead_lettered",
            EventKind::NotifyStarted => "notify_started",
            EventKind::Notified => "notified",
            EventKind::NotifyFailed => "notify_failed",
            EventKind::NotifySkipped => "notify_skipped",
            EventKind::Rejected => "rejected",
            EventKind::NeedsInfo => "needs_info",
        }
    }
}

impl std::str::FromStr for EventKind {
    type Err = crate::error::Error;

    /// Parses the stored form; a decode failure for an unknown kind.
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "intake" => Ok(EventKind::Intake),
            "draft_started" => Ok(EventKind::DraftStarted),
            "draft_completed" => Ok(EventKind::DraftCompleted),
            "draft_failed" => Ok(EventKind::DraftFailed),
            "judge_started" => Ok(EventKind::JudgeStarted),
            "judge_completed" => Ok(EventKind::JudgeCompleted),
            "judge_failed" => Ok(EventKind::JudgeFailed),
            "file_started" => Ok(EventKind::FileStarted),
            "filed" => Ok(EventKind::Filed),
            "file_failed" => Ok(EventKind::FileFailed),
            "file_retry_scheduled" => Ok(EventKind::FileRetryScheduled),
            "file_dead_lettered" => Ok(EventKind::FileDeadLettered),
            "notify_started" => Ok(EventKind::NotifyStarted),
            "notified" => Ok(EventKind::Notified),
            "notify_failed" => Ok(EventKind::NotifyFailed),
            "notify_skipped" => Ok(EventKind::NotifySkipped),
            "rejected" => Ok(EventKind::Rejected),
            "needs_info" => Ok(EventKind::NeedsInfo),
            _ => Err(crate::error::Error::Decode(format!(
                "unknown event kind `{raw}`"
            ))),
        }
    }
}

/// A `sg_tickets` row, as read back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ticket {
    /// ULID primary key.
    pub id: String,
    /// Owning tenant.
    pub tenant_id: String,
    /// The conversation the ticket was escalated from; not unique — one
    /// conversation may escalate more than once.
    pub conversation_id: String,
    /// Lifecycle position (`snake_case` wire form).
    pub status: Status,
    /// The stage whose work is currently queued for the ticket.
    pub stage: Stage,
    /// The conversation transcript the ticket was escalated from.
    pub transcript: String,
    /// Written by the draft stage.
    pub title: Option<String>,
    /// Written by the draft stage (Markdown).
    pub body_markdown: Option<String>,
    /// Written by the draft stage.
    pub severity: Option<Severity>,
    /// Written by the draft stage, where the transcript named one.
    pub environment: Option<String>,
    /// Written by the judge stage.
    pub verdict: Option<Verdict>,
    /// Written by the judge stage: the judgment's `reasons`, as a JSON
    /// array.
    pub judge_reasons: Option<Vec<String>>,
    /// The customer's actual question, extracted by the draft stage.
    pub customer_question: Option<String>,
    /// The tracker's ticket id, written by the file stage.
    pub external_id: Option<String>,
    /// The tracker's ticket URL, written by the file stage.
    pub external_url: Option<String>,
    /// RFC 3339.
    pub created_at: String,
    /// RFC 3339; every write refreshes it.
    pub updated_at: String,
}

/// A `sg_ticket_events` row: one audit entry in a ticket's trail.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TicketEvent {
    /// ULID primary key.
    pub id: String,
    /// The ticket this event belongs to.
    pub ticket_id: String,
    /// Pipeline position; see [`stage_seq`]. Readers order by
    /// `(seq, at, id)`.
    pub seq: i64,
    /// RFC 3339.
    pub at: String,
    /// The pipeline stage the ticket occupied when the event happened
    /// (`draft` for the intake event: intake is not a stage, but the
    /// ticket it mints is at stage `Draft` from birth).
    pub stage: Stage,
    /// What happened.
    pub kind: EventKind,
    /// JSON detail, when the event carries some.
    pub detail: Option<Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stages_round_trip_through_their_topic_and_ordinal() {
        for (stage, topic, ordinal) in [
            (Stage::Draft, "draft", 1),
            (Stage::Judge, "judge", 2),
            (Stage::File, "file", 3),
            (Stage::Notify, "notify", 4),
        ] {
            assert_eq!(stage.as_topic(), topic);
            assert_eq!(Stage::from_topic(topic), Some(stage));
            assert_eq!(stage.ordinal(), ordinal);
            let wire = serde_json::to_string(&stage).expect("serialize");
            assert_eq!(serde_json::from_str::<Stage>(&wire).expect("valid"), stage);
        }
        assert_eq!(Stage::from_topic("intake"), None, "intake is not a stage");
    }

    #[test]
    fn stage_seq_bands_keep_the_audit_in_pipeline_order() {
        // The bands interleave like the pipeline runs, whatever the clock
        // said when each row was written.
        assert!(INTAKE_SEQ < stage_seq(Stage::Draft, 0));
        assert!(stage_seq(Stage::Draft, 9) < stage_seq(Stage::Judge, 0));
        assert!(stage_seq(Stage::Judge, 0) < stage_seq(Stage::File, 0));
        assert!(stage_seq(Stage::File, 0) < stage_seq(Stage::Notify, 0));
        assert_eq!(stage_seq(Stage::Draft, 0), 10);
        assert_eq!(stage_seq(Stage::Notify, 4), 44);
    }

    #[test]
    fn statuses_round_trip_through_their_wire_form() {
        for status in [
            Status::Intake,
            Status::Drafting,
            Status::Judging,
            Status::Filing,
            Status::Filed,
            Status::NeedsInfo,
            Status::Rejected,
            Status::DeadLetter,
        ] {
            assert_eq!(status.as_str().parse::<Status>(), Ok(status));
            let wire = serde_json::to_string(&status).expect("serialize");
            assert_eq!(wire, format!("\"{}\"", status.as_str()));
            assert_eq!(
                serde_json::from_str::<Status>(&wire).expect("valid"),
                status
            );
        }
        assert_eq!("filed".parse::<Status>(), Ok(Status::Filed));
        assert!(
            "FILED".parse::<Status>().is_err(),
            "the wire form is snake_case"
        );
    }

    #[test]
    fn event_kinds_round_trip_through_their_wire_form() {
        for kind in [
            EventKind::Intake,
            EventKind::DraftStarted,
            EventKind::DraftCompleted,
            EventKind::DraftFailed,
            EventKind::JudgeStarted,
            EventKind::JudgeCompleted,
            EventKind::JudgeFailed,
            EventKind::FileStarted,
            EventKind::Filed,
            EventKind::FileFailed,
            EventKind::FileRetryScheduled,
            EventKind::FileDeadLettered,
            EventKind::NotifyStarted,
            EventKind::Notified,
            EventKind::NotifyFailed,
            EventKind::NotifySkipped,
            EventKind::Rejected,
            EventKind::NeedsInfo,
        ] {
            assert_eq!(kind.as_str().parse::<EventKind>(), Ok(kind));
            let wire = serde_json::to_string(&kind).expect("serialize");
            assert_eq!(wire, format!("\"{}\"", kind.as_str()));
        }
        assert!("nonsense".parse::<EventKind>().is_err());
    }

    #[test]
    fn drafted_schema_covers_exactly_its_fields_and_the_severity_variants() {
        let schema = Drafted::json_schema();
        assert_eq!(
            schema["$schema"],
            json!("https://json-schema.org/draft/2020-12/schema")
        );
        assert_eq!(schema["additionalProperties"], json!(false));
        let required = schema["required"].as_array().expect("required list");
        let required: Vec<&str> = required
            .iter()
            .map(Value::as_str)
            .collect::<Option<_>>()
            .expect("strings");
        // `environment` is the one field a transcript may not name.
        assert_eq!(
            required,
            vec!["title", "repro_steps", "expected", "actual", "severity"]
        );
        let severity_variants = schema["properties"]["severity"]["enum"]
            .as_array()
            .expect("severity enum");
        for severity in [
            Severity::Info,
            Severity::Warning,
            Severity::Error,
            Severity::Critical,
        ] {
            let wire = serde_json::to_string(&severity).expect("serialize");
            let wire: Value = serde_json::from_str(&wire).expect("valid");
            assert!(
                severity_variants.contains(&wire),
                "{wire} must be in the schema's severity enum"
            );
        }
        for field in [
            "title",
            "repro_steps",
            "expected",
            "actual",
            "environment",
            "severity",
        ] {
            assert!(schema["properties"].get(field).is_some(), "{field}");
        }
        assert_eq!(
            schema["properties"].as_object().expect("properties").len(),
            6
        );
    }

    #[test]
    fn judgment_schema_requires_the_reasons_and_pins_the_verdicts() {
        let schema = Judgment::json_schema();
        assert_eq!(schema["additionalProperties"], json!(false));
        let required: Vec<&str> = schema["required"]
            .as_array()
            .expect("required list")
            .iter()
            .map(Value::as_str)
            .collect::<Option<_>>()
            .expect("strings");
        // `duplicate_of` is the only optional field; `reasons` is required
        // so the audit always carries the judge's why.
        assert_eq!(
            required,
            vec![
                "is_defect",
                "reproducible",
                "severity_ok",
                "pii_clean",
                "verdict",
                "reasons"
            ]
        );
        assert_eq!(
            schema["properties"]["verdict"]["enum"],
            json!(["file", "needs_info", "reject"])
        );
    }

    #[test]
    fn drafted_and_judgment_round_trip_through_serde() {
        let drafted = Drafted {
            title: "Checkout 500s on a used gift card".to_owned(),
            repro_steps: vec![
                "Add an item".to_owned(),
                "Pay with a part-used gift card".to_owned(),
            ],
            expected: "The order completes".to_owned(),
            actual: "HTTP 500".to_owned(),
            environment: Some("production".to_owned()),
            severity: Severity::Error,
        };
        let wire = serde_json::to_string(&drafted).expect("serialize");
        assert_eq!(
            serde_json::from_str::<Drafted>(&wire).expect("valid"),
            drafted
        );

        let judgment = Judgment {
            is_defect: true,
            reproducible: true,
            duplicate_of: None,
            severity_ok: true,
            pii_clean: true,
            verdict: Verdict::File,
            reasons: vec!["the steps hit a real 500".to_owned()],
        };
        let wire = serde_json::to_string(&judgment).expect("serialize");
        assert_eq!(
            serde_json::from_str::<Judgment>(&wire).expect("valid"),
            judgment
        );

        // The stage payload round-trips through the exact string form that
        // rides in the outbox `payload` column.
        let payload = StagePayload {
            ticket_id: "01JDEMO".to_owned(),
            tenant_id: "acme".to_owned(),
        };
        let wire = serde_json::to_string(&payload).expect("serialize");
        assert_eq!(
            serde_json::from_str::<StagePayload>(&wire).expect("valid"),
            payload
        );
        assert_eq!(
            wire, r#"{"ticket_id":"01JDEMO","tenant_id":"acme"}"#,
            "the payload column stores exactly these two fields"
        );
    }
}
