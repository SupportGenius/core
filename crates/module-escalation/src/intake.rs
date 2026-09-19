//! The sanctioned cross-module handoff: another module (the support
//! conversation module) turns one of its conversations into an escalation
//! ticket. Modules cannot depend on each other, so [`Intake`] does no I/O
//! of its own — it returns the [`Statement`]s the **caller** appends to
//! its own [`cratefield_core::Database::batch_atomic`], which makes the
//! handoff durable exactly when the message that caused it is: if the
//! caller's batch commits, the ticket, its first audit row and the draft
//! job all exist; if it rolls back, none do. There is no window where a
//! conversation is answered "escalated" but no ticket exists.
//!
//! Not wired into a `Module` here: step 3's `Escalation` exposes this as
//! `Escalation::intake()`; the support module reaches it the same way.

use std::sync::Arc;

use cratefield_core::{Clock, IdGen, Statement};
use time::format_description::well_known::Rfc3339;

use crate::model::{EventKind, INTAKE_SEQ, Stage, Ticket};
use crate::store;
use serde_json::json;

/// The outbox table the escalation module owns. [`Intake::new`] takes the
/// name so the module can pass its own declared table; this is the value
/// the migration creates.
pub const OUTBOX_TABLE: &str = "sg_escalation_outbox";

/// The intake handoff: a ticket id plus the statements that make it
/// exist. The caller runs `statements` inside its own atomic batch.
#[derive(Debug, Clone)]
pub struct Handoff {
    /// The minted `sg_tickets` id (a ULID) — the same id the draft job's
    /// payload and subject carry.
    pub ticket_id: String,
    /// In order: the `sg_tickets` insert, the `intake` audit row, the
    /// `draft` outbox enqueue.
    pub statements: Vec<Statement>,
}

/// Mints escalation tickets out of conversations. Pure until the caller
/// commits its statements; construct it with the module's outbox table
/// and the ports the harness already hands the module.
pub struct Intake {
    outbox_table: String,
    clock: Arc<dyn Clock>,
    idgen: Arc<dyn IdGen>,
}

impl Intake {
    /// Builds an intake over `outbox_table` (see [`OUTBOX_TABLE`]), a
    /// clock for the row timestamps, and an id generator for the ticket,
    /// audit-row and job ids.
    pub fn new(
        outbox_table: impl Into<String>,
        clock: Arc<dyn Clock>,
        idgen: Arc<dyn IdGen>,
    ) -> Self {
        Self {
            outbox_table: outbox_table.into(),
            clock,
            idgen,
        }
    }

    /// The signature issue #4 specifies: turns a conversation into the
    /// statements that escalate it. The caller appends them to its own
    /// `batch_atomic`. The minted ticket id is dropped here; use
    /// [`Intake::handoff`] when the caller wants to answer with it.
    pub fn enqueue(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        transcript: &str,
    ) -> Vec<Statement> {
        self.handoff(tenant_id, conversation_id, transcript)
            .statements
    }

    /// The same work as [`Intake::enqueue`], handing back the ticket id
    /// it minted alongside the statements.
    pub fn handoff(&self, tenant_id: &str, conversation_id: &str, transcript: &str) -> Handoff {
        let ticket_id = self.idgen.ulid();
        // A `Clock::now()` always formats as RFC 3339; the fallback keeps
        // this total the way the waitlist's `now_iso` is.
        let now = self.clock.now().format(&Rfc3339).unwrap_or_default();

        // Born at stage `Draft` with nothing drafted yet: the draft job
        // this handoff enqueues is what fills the ticket in.
        let ticket = Ticket {
            id: ticket_id.clone(),
            tenant_id: tenant_id.to_owned(),
            conversation_id: conversation_id.to_owned(),
            status: crate::model::Status::Intake,
            stage: Stage::Draft,
            transcript: transcript.to_owned(),
            title: None,
            body_markdown: None,
            severity: None,
            environment: None,
            verdict: None,
            judge_reasons: None,
            customer_question: None,
            external_id: None,
            external_url: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        };

        // Band 0 of the audit trail: intake, before any stage runs. The
        // row carries the stage the ticket is entering (`draft`), and its
        // detail names the conversation it came from.
        let intake_event = store::insert_event_stmt(
            &self.idgen.ulid(),
            &ticket_id,
            INTAKE_SEQ,
            &now,
            Stage::Draft,
            EventKind::Intake,
            &json!({ "conversation_id": conversation_id }),
        );

        let outbox = cratefield_core::Outbox::new(self.outbox_table.clone());
        let enqueue_draft = store::enqueue_stage_stmt(
            &outbox,
            &self.idgen.ulid(),
            &ticket_id,
            tenant_id,
            Stage::Draft,
            &now,
        );

        Handoff {
            ticket_id,
            statements: vec![
                store::insert_ticket_stmt(&ticket),
                intake_event,
                enqueue_draft,
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Status;
    use cratefield_core::{Database, Outbox};
    use std::sync::Mutex;

    /// A clock pinned to one instant — the reason the audit trail orders
    /// by `seq`, not `at`.
    struct FixedTime(time::OffsetDateTime);

    impl Clock for FixedTime {
        fn now(&self) -> time::OffsetDateTime {
            self.0
        }
    }

    /// Counts the ids minted, so a test can tell the three apart.
    struct CountingIdGen {
        ids: Mutex<usize>,
    }

    impl IdGen for CountingIdGen {
        fn ulid(&self) -> String {
            let mut ids = self.ids.lock().expect("uncontended");
            let n = *ids;
            *ids += 1;
            format!("01JTEST{n:026}")
        }
    }

    fn intake() -> Intake {
        let epoch = time::OffsetDateTime::from_unix_timestamp(1_789_000_000).expect("epoch");
        Intake::new(
            OUTBOX_TABLE,
            Arc::new(FixedTime(epoch)),
            Arc::new(CountingIdGen { ids: Mutex::new(0) }),
        )
    }

    /// Reads a text bind back out of a rendered statement: the outer
    /// `None` means no bind (or not text), the inner `None` means SQL
    /// NULL. The three cases are exactly what these assertions tell
    /// apart, so `Option<Option<_>>` is the honest shape here.
    #[allow(clippy::option_option)]
    fn bind(value: &sea_query::Value) -> Option<Option<String>> {
        cratefield_core::Row::new(vec![("v".to_owned(), value.clone())]).get::<Option<String>>("v")
    }

    /// A text bind that must be present.
    fn text(value: &sea_query::Value) -> Option<String> {
        bind(value).flatten()
    }

    #[test]
    fn a_handoff_is_insert_event_and_draft_enqueue_in_that_order() {
        let handoff = intake().handoff("acme", "conv-7", "customer: it broke");

        assert_eq!(handoff.statements.len(), 3);
        let (ticket, event, enqueue) = (
            &handoff.statements[0],
            &handoff.statements[1],
            &handoff.statements[2],
        );
        assert!(
            ticket.sql.starts_with("INSERT INTO \"sg_tickets\""),
            "{}",
            ticket.sql
        );
        assert!(
            event.sql.starts_with("INSERT INTO \"sg_ticket_events\""),
            "{}",
            event.sql
        );
        assert!(
            enqueue
                .sql
                .starts_with(&format!("INSERT INTO \"{OUTBOX_TABLE}\"")),
            "{}",
            enqueue.sql
        );

        // The three rows hang together: the event and the job reference
        // the ticket the first statement inserts.
        assert_eq!(
            text(&ticket.values.0[0]).as_deref(),
            Some(handoff.ticket_id.as_str())
        );
        assert_eq!(
            text(&event.values.0[1]).as_deref(),
            Some(handoff.ticket_id.as_str())
        );
        // The draft job is enqueued with the ticket as its subject.
        assert_eq!(text(&enqueue.values.0[1]).as_deref(), Some("draft"));
        assert_eq!(
            text(&enqueue.values.0[3]).as_deref(),
            Some(handoff.ticket_id.as_str())
        );
        let payload: crate::model::StagePayload =
            serde_json::from_str(&text(&enqueue.values.0[2]).expect("payload text"))
                .expect("payload json");
        assert_eq!(
            payload,
            crate::model::StagePayload {
                ticket_id: handoff.ticket_id.clone(),
                tenant_id: "acme".to_owned(),
            }
        );
    }

    #[test]
    fn the_minted_ticket_is_born_at_intake_stage_draft() {
        let handoff = intake().handoff("acme", "conv-7", "customer: it broke");
        let ticket = &handoff.statements[0];

        assert_eq!(ticket.values.0[1], "acme".into());
        assert_eq!(ticket.values.0[2], "conv-7".into());
        assert_eq!(ticket.values.0[3], Status::Intake.as_str().into());
        assert_eq!(ticket.values.0[4], Stage::Draft.as_topic().into());
        assert_eq!(ticket.values.0[5], "customer: it broke".into());
        // Nothing is drafted yet: every late-stage column binds NULL.
        for column in 6..15 {
            assert!(
                bind(&ticket.values.0[column])
                    .expect("a bind is present")
                    .is_none(),
                "column {column} must start NULL"
            );
        }
    }

    #[test]
    fn the_intake_event_opens_the_audit_at_band_zero() {
        let handoff = intake().handoff("acme", "conv-7", "customer: it broke");
        let event = &handoff.statements[1];

        assert_eq!(event.values.0[2], INTAKE_SEQ.into());
        assert_eq!(event.values.0[4], "draft".into());
        assert_eq!(event.values.0[5], EventKind::Intake.as_str().into());
        let detail: serde_json::Value =
            serde_json::from_str(&text(&event.values.0[6]).expect("detail text"))
                .expect("detail json");
        assert_eq!(detail, serde_json::json!({ "conversation_id": "conv-7" }));
    }

    #[test]
    fn enqueue_is_handoff_without_the_id() {
        let escalator = intake();
        let statements = escalator.enqueue("acme", "conv-7", "customer: it broke");
        let handoff = escalator.handoff("acme", "conv-7", "a second escalation");
        assert_eq!(statements.len(), handoff.statements.len());
    }

    /// The statements commit against the shipped migration and the
    /// readers read them back — proving the handoff is real, not just
    /// well-formed SQL.
    #[test]
    fn a_committed_handoff_reads_back_through_the_store() {
        let db = cratefield_adapter_sqlite::SqliteDatabase::in_memory().expect("in-memory db");
        db.apply_migrations(
            "module-escalation",
            std::slice::from_ref(&crate::MIGRATION_ESCALATION),
        )
        .expect("migration applies");

        let handoff = intake().handoff("acme", "conv-7", "customer: it broke");
        pollster::block_on(db.batch_atomic(&handoff.statements)).expect("handoff commits");

        let ticket = pollster::block_on(crate::store::load_ticket(&db, &handoff.ticket_id))
            .expect("read")
            .expect("ticket exists");
        assert_eq!(ticket.status, Status::Intake);
        assert_eq!(ticket.stage, Stage::Draft);

        let events =
            pollster::block_on(crate::store::ticket_events(&db, &handoff.ticket_id)).expect("read");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, EventKind::Intake);
        assert_eq!(events[0].seq, INTAKE_SEQ);

        let due = pollster::block_on(Outbox::new(OUTBOX_TABLE).claim_due(
            &db,
            "2280-01-01T00:00:00Z",
            "2280-01-01T01:00:00Z",
            10,
        ))
        .expect("claim");
        assert_eq!(due.len(), 1, "the draft job is due");
        assert_eq!(due[0].topic, "draft");
    }
}
