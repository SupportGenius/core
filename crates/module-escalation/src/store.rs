//! Sea-query data access for the escalation tables (in the
//! `cratefield-module-waitlist` idiom: `Alias` idents, every query
//! rendered with [`Statement::render`], decoded through
//! [`cratefield_core::Row`]).
//!
//! Two kinds of function, kept deliberately separate because the stages
//! compose them differently:
//!
//! 1. **Statement builders** — pure, no `db`, returning a
//!    [`Statement`]. A stage handler composes them into **one**
//!    [`Database::batch_atomic`], so "record my result, audit it, hand
//!    the ticket to the next stage, and complete my own outbox row"
//!    commits together or not at all.
//! 2. **Async readers** — take `&dyn Database` and return decoded rows.
//!
//! [`outbox_complete_stmt`] and [`outbox_retry_later_stmt`] exist only
//! because published core 0.4.3 has no statement form of
//! `Outbox::complete`/`Outbox::retry_later` — the only way to complete a
//! stage inside the same batch that enqueues the next one would
//! otherwise be a second round trip after the batch, which loses
//! atomicity. They were rendered from the exact query bodies core's own
//! methods build (verified against the 0.4.3 source and pinned by the
//! tests below), and should be deleted when core grows
//! `complete_statement`/`retry_later_statement`.

use cratefield_core::{Database, Row, Statement};
use sea_query::{Alias, Expr, Order, Query};

use crate::error::Error;
use crate::model::{
    Drafted, EventKind, Judgment, Stage, StagePayload, Status, Ticket, TicketEvent, Verdict,
};
use crate::ports::tracker::{Destination, Filed, Severity};

fn iden(name: &str) -> Alias {
    Alias::new(name)
}

/// The columns `sg_tickets` reads come back in, in decode order.
const TICKET_COLUMNS: [&str; 17] = [
    "id",
    "tenant_id",
    "conversation_id",
    "status",
    "stage",
    "transcript",
    "title",
    "body_markdown",
    "severity",
    "environment",
    "verdict",
    "judge_reasons",
    "customer_question",
    "external_id",
    "external_url",
    "created_at",
    "updated_at",
];

/// The columns `sg_ticket_events` reads come back in, in decode order.
const EVENT_COLUMNS: [&str; 7] = ["id", "ticket_id", "seq", "at", "stage", "kind", "detail"];

// ---------------------------------------------------------------------------
// Statement builders (pure; compose into a caller's `batch_atomic`)
// ---------------------------------------------------------------------------

/// Inserts one `sg_tickets` row. The intake handoff uses this with every
/// late-stage column `None`; a caller that already knows the customer's
/// question may set it here and the draft stage keeps it.
#[must_use]
pub fn insert_ticket_stmt(ticket: &Ticket) -> Statement {
    let mut insert = Query::insert();
    insert
        .into_table(iden("sg_tickets"))
        .columns(TICKET_COLUMNS)
        .values_panic([
            ticket.id.clone().into(),
            ticket.tenant_id.clone().into(),
            ticket.conversation_id.clone().into(),
            ticket.status.as_str().into(),
            // The column stores the serde wire form, which is the same
            // string the topic uses.
            ticket.stage.as_topic().into(),
            ticket.transcript.clone().into(),
            ticket.title.clone().into(),
            ticket.body_markdown.clone().into(),
            ticket.severity.map(severity_text).into(),
            ticket.environment.clone().into(),
            ticket.verdict.map(Verdict::as_str).into(),
            ticket
                .judge_reasons
                .as_ref()
                .map(|reasons| json_array_text(reasons))
                .into(),
            ticket.customer_question.clone().into(),
            ticket.external_id.clone().into(),
            ticket.external_url.clone().into(),
            ticket.created_at.clone().into(),
            ticket.updated_at.clone().into(),
        ]);
    Statement::render(&insert)
}

/// Appends one audit row to `sg_ticket_events`. `event_id` is a
/// caller-minted ULID (the builders are pure and cannot reach an
/// [`cratefield_core::IdGen`]); `seq` comes from
/// [`crate::model::stage_seq`] (or
/// [`crate::model::INTAKE_SEQ`]); `detail` is the event's JSON, or
/// `Value::Null` when it carries nothing beyond its kind.
#[must_use]
pub fn insert_event_stmt(
    event_id: &str,
    ticket_id: &str,
    seq: i64,
    at: &str,
    stage: Stage,
    kind: EventKind,
    detail: &serde_json::Value,
) -> Statement {
    let mut insert = Query::insert();
    insert
        .into_table(iden("sg_ticket_events"))
        .columns(EVENT_COLUMNS)
        .values_panic([
            event_id.to_owned().into(),
            ticket_id.to_owned().into(),
            seq.into(),
            at.to_owned().into(),
            stage.as_topic().into(),
            kind.as_str().into(),
            detail_text(detail).into(),
        ]);
    Statement::render(&insert)
}

/// Records the draft stage's result: the ticket fields the draft
/// produced (plus the Markdown body the handler renders from it and the
/// customer question it extracted), and `updated_at`. Does not move
/// `status`/`stage` — the handler composes
/// [`update_ticket_status_stmt`]/[`update_ticket_stage_stmt`] so the
/// transition is explicit next to the outbox enqueue it pairs with.
#[must_use]
pub fn update_ticket_draft_stmt(
    ticket_id: &str,
    drafted: &Drafted,
    body_markdown: &str,
    customer_question: Option<&str>,
    at: &str,
) -> Statement {
    let mut update = Query::update();
    update
        .table(iden("sg_tickets"))
        .values([
            (iden("title"), drafted.title.clone().into()),
            (iden("body_markdown"), body_markdown.to_owned().into()),
            (iden("severity"), severity_text(drafted.severity).into()),
            (iden("environment"), drafted.environment.clone().into()),
            (
                iden("customer_question"),
                customer_question.map(str::to_owned).into(),
            ),
            (iden("updated_at"), at.to_owned().into()),
        ])
        .and_where(Expr::col(iden("id")).eq(ticket_id));
    Statement::render(&update)
}

/// Records the judge stage's verdict and its reasons (stored as a JSON
/// array so the audit can quote the judge verbatim), and `updated_at`.
#[must_use]
pub fn update_ticket_judgment_stmt(ticket_id: &str, judgment: &Judgment, at: &str) -> Statement {
    let reasons = json_array_text(&judgment.reasons);
    let mut update = Query::update();
    update
        .table(iden("sg_tickets"))
        .values([
            (iden("verdict"), judgment.verdict.as_str().into()),
            (iden("judge_reasons"), reasons.into()),
            (iden("updated_at"), at.to_owned().into()),
        ])
        .and_where(Expr::col(iden("id")).eq(ticket_id));
    Statement::render(&update)
}

/// Records a successful filing: the tracker's ticket id and URL, and
/// `updated_at`.
#[must_use]
pub fn update_ticket_filed_stmt(ticket_id: &str, filed: &Filed, at: &str) -> Statement {
    let mut update = Query::update();
    update
        .table(iden("sg_tickets"))
        .values([
            (iden("external_id"), filed.external_id.clone().into()),
            (iden("external_url"), filed.url.clone().into()),
            (iden("updated_at"), at.to_owned().into()),
        ])
        .and_where(Expr::col(iden("id")).eq(ticket_id));
    Statement::render(&update)
}

/// Stores the customer-facing question a stage composed (the judge stage's
/// `NeedsInfo` verdict, phrased from its reasons) and refreshes
/// `updated_at`. Separate from [`update_ticket_draft_stmt`], which also
/// owns this column when the draft ran, because the judge rewrites it with
/// a question of its own and nothing else.
#[must_use]
pub fn update_ticket_question_stmt(ticket_id: &str, question: &str, at: &str) -> Statement {
    let mut update = Query::update();
    update
        .table(iden("sg_tickets"))
        .values([
            (iden("customer_question"), question.to_owned().into()),
            (iden("updated_at"), at.to_owned().into()),
        ])
        .and_where(Expr::col(iden("id")).eq(ticket_id));
    Statement::render(&update)
}

/// Moves a ticket's lifecycle status and refreshes `updated_at`.
#[must_use]
pub fn update_ticket_status_stmt(ticket_id: &str, status: Status, at: &str) -> Statement {
    let mut update = Query::update();
    update
        .table(iden("sg_tickets"))
        .values([
            (iden("status"), status.as_str().into()),
            (iden("updated_at"), at.to_owned().into()),
        ])
        .and_where(Expr::col(iden("id")).eq(ticket_id));
    Statement::render(&update)
}

/// Points a ticket at the stage whose work is queued next and refreshes
/// `updated_at`. Separate from [`update_ticket_status_stmt`] because the
/// two do not always move together: a `NeedsInfo` verdict changes the
/// status while the ticket stays at the stage that asked.
#[must_use]
pub fn update_ticket_stage_stmt(ticket_id: &str, stage: Stage, at: &str) -> Statement {
    let mut update = Query::update();
    update
        .table(iden("sg_tickets"))
        .values([
            (iden("stage"), stage.as_topic().into()),
            (iden("updated_at"), at.to_owned().into()),
        ])
        .and_where(Expr::col(iden("id")).eq(ticket_id));
    Statement::render(&update)
}

/// Upserts a tenant's tracker destination (`tenant_id` is the primary
/// key, so re-configuring a tenant replaces its row). `credential_ref`
/// names the Config key the secret lives under — **never the secret
/// itself**; it is resolved from the Config port at file-time.
#[must_use]
pub fn put_destination_stmt(
    tenant_id: &str,
    destination: &Destination,
    credential_ref: &str,
    updated_at: &str,
) -> Statement {
    let destination = serde_json::to_string(destination).unwrap_or_else(|_| {
        // Serializing a fieldless/struct variant enum cannot fail; the
        // fallback only keeps this builder pure.
        "null".to_owned()
    });
    let mut insert = Query::insert();
    insert
        .into_table(iden("sg_destinations"))
        .columns(["tenant_id", "destination", "credential_ref", "updated_at"])
        .values_panic([
            tenant_id.to_owned().into(),
            destination.into(),
            credential_ref.to_owned().into(),
            updated_at.to_owned().into(),
        ])
        .on_conflict(
            sea_query::OnConflict::column(iden("tenant_id"))
                .update_columns(["destination", "credential_ref", "updated_at"])
                .to_owned(),
        );
    Statement::render(&insert)
}

/// The rendered equivalent of `Outbox::complete(db, id)` — a
/// `DELETE FROM "<table>" WHERE "id" = ?` — so a stage handler can
/// complete its own outbox row **inside the same** `batch_atomic` that
/// records its result and enqueues the next stage. Exists only because
/// core 0.4.3 exposes no `complete_statement`; delete it (and switch the
/// stages to core) when core grows one. The SQL is pinned verbatim in the
/// tests below against what core's own method renders.
#[must_use]
pub fn outbox_complete_stmt(table: &str, id: &str) -> Statement {
    let mut delete = Query::delete();
    delete
        .from_table(iden(table))
        .and_where(Expr::col(iden("id")).eq(id));
    Statement::render(&delete)
}

/// Releases a held inbox claim: `DELETE FROM <table> WHERE event_key = ?`.
///
/// Core 0.4.3's [`cratefield_core::Inbox`] has `claim` and `seen` — both
/// immediate — and no statement form of *releasing* a key. A retrying stage
/// needs exactly that: the claim is taken **before** the port call, so a
/// retryable failure has to give the key back inside the same
/// [`Database::batch_atomic`] that reschedules the outbox row. Release the
/// claim without the retry and a crash between the two wedges the stage
/// forever (the key is held, the work never re-runs); retry without the
/// release and the re-run proceeds on a held key through the crash-window
/// branch, where the claim guards nothing. Delete this (and take core's
/// version) when core grows a `release_statement`.
#[must_use]
pub fn inbox_release_stmt(table: &str, event_key: &str) -> Statement {
    let mut delete = Query::delete();
    delete
        .from_table(iden(table))
        .and_where(Expr::col(iden("event_key")).eq(event_key));
    Statement::render(&delete)
}

/// The rendered equivalent of `Outbox::retry_later(db, id,
/// next_attempt_at)` — increments `attempts`, moves `next_attempt_at`,
/// clears the lease — so a transient stage failure reschedules inside the
/// same `batch_atomic` that audits the retry. Exists only because core
/// 0.4.3 exposes no `retry_later_statement`; delete it when core grows
/// one. The SQL is pinned verbatim in the tests below.
#[must_use]
pub fn outbox_retry_later_stmt(table: &str, id: &str, next_attempt_at: &str) -> Statement {
    let mut update = Query::update();
    update
        .table(iden(table))
        .value(iden("attempts"), Expr::col(iden("attempts")).add(1))
        .value(iden("next_attempt_at"), next_attempt_at)
        .value(iden("locked_until"), Option::<String>::None)
        .and_where(Expr::col(iden("id")).eq(id));
    Statement::render(&update)
}

/// The outbox `enqueue_statement` for one stage's work: topic
/// `stage.as_topic()`, a [`StagePayload`] JSON payload, the ticket id as
/// the subject (the ticket is the per-person key erasure reaches through,
/// issue #266), `at` for both `created_at` and the first
/// `next_attempt_at`. A thin wrapper so stages cannot mistype the topic
/// or forget the subject.
#[must_use]
pub fn enqueue_stage_stmt(
    outbox: &cratefield_core::Outbox,
    job_id: &str,
    ticket_id: &str,
    tenant_id: &str,
    stage: Stage,
    at: &str,
) -> Statement {
    let payload = StagePayload {
        ticket_id: ticket_id.to_owned(),
        tenant_id: tenant_id.to_owned(),
    };
    let payload = serde_json::to_string(&payload).unwrap_or_else(|_| {
        format!("{{\"ticket_id\":\"{ticket_id}\",\"tenant_id\":\"{tenant_id}\"}}")
    });
    outbox.enqueue_statement(job_id, stage.as_topic(), &payload, Some(ticket_id), at)
}

// ---------------------------------------------------------------------------
// Async readers (`&dyn Database`)
// ---------------------------------------------------------------------------

/// Loads one ticket by id.
///
/// # Errors
///
/// [`Error::Db`] when the read fails; [`Error::Decode`] when a row does
/// not match the schema in `migrations/0001_escalation.sql`.
pub async fn load_ticket(db: &dyn Database, id: &str) -> Result<Option<Ticket>, Error> {
    let mut query = select_tickets();
    query.and_where(Expr::col(iden("id")).eq(id)).limit(1);
    let rows = db.query(&Statement::render(&query)).await?;
    rows.first().map(ticket_from).transpose()
}

/// Loads the most recent ticket escalated from one conversation (the
/// `(tenant_id, conversation_id)` index is deliberately not unique — a
/// conversation may escalate more than once).
///
/// # Errors
///
/// As [`load_ticket`].
pub async fn find_ticket_by_conversation(
    db: &dyn Database,
    tenant_id: &str,
    conversation_id: &str,
) -> Result<Option<Ticket>, Error> {
    let mut query = select_tickets();
    query
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden("conversation_id")).eq(conversation_id))
        // Latest escalation first; `id` breaks a `created_at` tie.
        .order_by(iden("created_at"), Order::Desc)
        .order_by(iden("id"), Order::Desc)
        .limit(1);
    let rows = db.query(&Statement::render(&query)).await?;
    rows.first().map(ticket_from).transpose()
}

/// Loads a tenant's tracker destination and the Config key its credential
/// lives under. The secret itself never reaches the store; resolve
/// `credential_ref` through the Config port at file-time.
///
/// # Errors
///
/// As [`load_ticket`], plus [`Error::Decode`] when the stored
/// `Destination` JSON no longer parses.
pub async fn load_destination(
    db: &dyn Database,
    tenant_id: &str,
) -> Result<Option<(Destination, String)>, Error> {
    let mut query = Query::select();
    query
        .columns(["tenant_id", "destination", "credential_ref"])
        .from(iden("sg_destinations"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .limit(1);
    let rows = db.query(&Statement::render(&query)).await?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let destination = required_text(row, "destination")?;
    let destination: Destination = serde_json::from_str(&destination)
        .map_err(|err| Error::Decode(format!("sg_destinations.destination: {err}")))?;
    Ok(Some((destination, required_text(row, "credential_ref")?)))
}

/// A ticket's audit trail, in true pipeline order: `(seq, at, id)` —
/// `seq` bands by stage (see [`crate::model::stage_seq`]), `at`
/// disambiguates retries
/// inside one band, `id` is the last-resort tiebreak.
///
/// # Errors
///
/// As [`load_ticket`].
pub async fn ticket_events(db: &dyn Database, ticket_id: &str) -> Result<Vec<TicketEvent>, Error> {
    let mut query = Query::select();
    query
        .columns(EVENT_COLUMNS)
        .from(iden("sg_ticket_events"))
        .and_where(Expr::col(iden("ticket_id")).eq(ticket_id))
        .order_by(iden("seq"), Order::Asc)
        .order_by(iden("at"), Order::Asc)
        .order_by(iden("id"), Order::Asc);
    let rows = db.query(&Statement::render(&query)).await?;
    rows.rows.iter().map(event_from).collect()
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

fn select_tickets() -> sea_query::SelectStatement {
    let mut select = Query::select();
    select.columns(TICKET_COLUMNS).from(iden("sg_tickets"));
    select
}

fn required_text(row: &Row, column: &str) -> Result<String, Error> {
    // `Row::get::<Option<String>>` is `None` for a missing column or an
    // unrepresentable value — a schema drift this crate must not paper
    // over — while SQL NULL reads back as `Some(None)`.
    row.get::<Option<String>>(column)
        .ok_or_else(|| Error::Decode(format!("column `{column}` missing or not text")))?
        .ok_or_else(|| Error::Decode(format!("column `{column}` unexpectedly NULL")))
}

fn optional_text(row: &Row, column: &str) -> Result<Option<String>, Error> {
    row.get::<Option<String>>(column)
        .ok_or_else(|| Error::Decode(format!("column `{column}` missing or not text")))
}

fn parse_json_column<T: serde::de::DeserializeOwned>(
    raw: Option<String>,
    column: &str,
) -> Result<Option<T>, Error> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    serde_json::from_str(&raw).map_err(|err| Error::Decode(format!("column `{column}`: {err}")))
}

fn ticket_from(row: &Row) -> Result<Ticket, Error> {
    let status = required_text(row, "status")?.parse::<Status>()?;
    let stage = required_text(row, "stage")?;
    let stage = Stage::from_topic(&stage)
        .ok_or_else(|| Error::Decode(format!("unknown ticket stage `{stage}`")))?;
    let severity = severity_from(row)?;
    let verdict = optional_text(row, "verdict")?
        .map(|raw| raw.parse::<Verdict>())
        .transpose()?;

    Ok(Ticket {
        id: required_text(row, "id")?,
        tenant_id: required_text(row, "tenant_id")?,
        conversation_id: required_text(row, "conversation_id")?,
        status,
        stage,
        transcript: required_text(row, "transcript")?,
        title: optional_text(row, "title")?,
        body_markdown: optional_text(row, "body_markdown")?,
        severity,
        environment: optional_text(row, "environment")?,
        verdict,
        judge_reasons: parse_json_column(optional_text(row, "judge_reasons")?, "judge_reasons")?,
        customer_question: optional_text(row, "customer_question")?,
        external_id: optional_text(row, "external_id")?,
        external_url: optional_text(row, "external_url")?,
        created_at: required_text(row, "created_at")?,
        updated_at: required_text(row, "updated_at")?,
    })
}

fn event_from(row: &Row) -> Result<TicketEvent, Error> {
    let stage = required_text(row, "stage")?;
    let stage = Stage::from_topic(&stage)
        .ok_or_else(|| Error::Decode(format!("unknown event stage `{stage}`")))?;
    let kind = required_text(row, "kind")?.parse::<EventKind>()?;
    let detail: Option<serde_json::Value> =
        parse_json_column(optional_text(row, "detail")?, "detail")?;

    Ok(TicketEvent {
        id: required_text(row, "id")?,
        ticket_id: required_text(row, "ticket_id")?,
        seq: row
            .get::<i64>("seq")
            .ok_or_else(|| Error::Decode("column `seq` missing or not an integer".to_owned()))?,
        at: required_text(row, "at")?,
        stage,
        kind,
        detail,
    })
}

/// The `Severity` wire form for storage (`"info"`, ...). `Severity` is
/// `Copy`, so this takes it by value.
fn severity_text(severity: Severity) -> String {
    severity.name().to_owned()
}

/// Decodes the `severity` cell. The port type has no `from_str`, so this
/// rides its serde (the wire form is a bare string like `error`, not a
/// JSON document).
fn severity_from(row: &Row) -> Result<Option<Severity>, Error> {
    optional_text(row, "severity")?
        .map(|raw| {
            serde_json::from_value(serde_json::Value::String(raw.clone()))
                .map_err(|err| Error::Decode(format!("column `severity`: {err}: `{raw}`")))
        })
        .transpose()
}

/// Serializes a list of strings to its JSON text form (a `judge_reasons`
/// cell). Serializing an array of strings cannot fail; the fallback keeps
/// the builders pure and total.
fn json_array_text(items: &[String]) -> String {
    serde_json::to_string(items).unwrap_or_else(|_| {
        format!(
            "[{}]",
            items
                .iter()
                .map(|item| serde_json::to_string(item).unwrap_or_else(|_| "\"\"".to_owned()))
                .collect::<Vec<_>>()
                .join(",")
        )
    })
}

/// The event `detail` cell: the JSON text, or SQL NULL for a null value.
fn detail_text(detail: &serde_json::Value) -> Option<String> {
    if detail.is_null() {
        None
    } else {
        Some(
            serde_json::to_string(detail)
                .unwrap_or_else(|_| "{\"error\":\"unserializable detail\"}".to_owned()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cratefield_core::Outbox;
    use sea_query::Value as SeaValue;

    /// The two outbox statements must be byte-for-byte what core's own
    /// `complete`/`retry_later` issue, or a batch that pairs them with an
    /// enqueue would be writing a different dialect than the drainer
    /// expects. Rendered from core 0.4.3's own query bodies and pinned
    /// here; regenerate if core's SQL ever changes.
    #[test]
    fn outbox_complete_stmt_matches_core_verbatim() {
        let stmt = outbox_complete_stmt("sg_escalation_outbox", "01JDEMO");
        assert_eq!(
            stmt.sql,
            "DELETE FROM \"sg_escalation_outbox\" WHERE \"id\" = ?"
        );
        assert_eq!(stmt.values.0, vec![SeaValue::from("01JDEMO")]);
    }

    #[test]
    fn outbox_retry_later_stmt_matches_core_verbatim() {
        let stmt =
            outbox_retry_later_stmt("sg_escalation_outbox", "01JDEMO", "2026-09-19T00:05:00Z");
        assert_eq!(
            stmt.sql,
            "UPDATE \"sg_escalation_outbox\" SET \"attempts\" = \"attempts\" + ?, \
             \"next_attempt_at\" = ?, \"locked_until\" = ? WHERE \"id\" = ?"
        );
        // attempts + 1 (Int), next_attempt_at, locked_until = NULL, id.
        assert_eq!(
            stmt.values.0,
            vec![
                SeaValue::from(1_i32),
                SeaValue::from("2026-09-19T00:05:00Z".to_owned()),
                SeaValue::from(Option::<String>::None),
                SeaValue::from("01JDEMO"),
            ]
        );
    }

    #[test]
    fn enqueue_stage_stmt_subjects_the_ticket_and_carries_the_payload() {
        let stmt = enqueue_stage_stmt(
            &Outbox::new("sg_escalation_outbox"),
            "01JJOB",
            "01JTICKET",
            "acme",
            Stage::Judge,
            "2026-09-19T00:00:00Z",
        );
        assert!(stmt.sql.contains("INSERT INTO \"sg_escalation_outbox\""));
        // id, topic, payload, subject, attempts, next_attempt_at, created_at.
        assert_eq!(stmt.values.0.len(), 7);
        assert_eq!(stmt.values.0[1], SeaValue::from("judge"));
        assert_eq!(
            stmt.values.0[2],
            SeaValue::from(r#"{"ticket_id":"01JTICKET","tenant_id":"acme"}"#.to_owned())
        );
        assert_eq!(stmt.values.0[3], SeaValue::from("01JTICKET"));
    }

    #[test]
    fn inbox_release_stmt_deletes_only_the_key() {
        let stmt = inbox_release_stmt("sg_escalation_inbox", "01JTICKET:draft");
        assert_eq!(
            stmt.sql,
            "DELETE FROM \"sg_escalation_inbox\" WHERE \"event_key\" = ?"
        );
        assert_eq!(stmt.values.0, vec![SeaValue::from("01JTICKET:draft")]);
    }

    /// The judge's `NeedsInfo` question lands in the column intake and the
    /// draft stage may already have set, and the reader decodes it back.
    #[test]
    fn the_customer_question_round_trips() {
        let db = migrated_db();
        pollster::block_on(db.batch_atomic(&[insert_ticket_stmt(&fresh_ticket())]))
            .expect("insert");
        let at = "2026-09-19T00:01:00Z";
        pollster::block_on(db.batch_atomic(&[update_ticket_question_stmt(
            "01JTICKET",
            "which build is this?",
            at,
        )]))
        .expect("update commits");
        let ticket = pollster::block_on(load_ticket(&db, "01JTICKET"))
            .expect("read")
            .expect("row exists");
        assert_eq!(
            ticket.customer_question.as_deref(),
            Some("which build is this?")
        );
        assert_eq!(ticket.updated_at, at);
    }

    /// A fresh in-memory database with the shipped migration applied.
    fn migrated_db() -> cratefield_adapter_sqlite::SqliteDatabase {
        let db = cratefield_adapter_sqlite::SqliteDatabase::in_memory().expect("in-memory db");
        db.apply_migrations(
            "module-escalation",
            std::slice::from_ref(&crate::MIGRATION_ESCALATION),
        )
        .expect("migration applies");
        db
    }

    /// A ticket as intake mints it: every late-stage column `None`.
    fn fresh_ticket() -> Ticket {
        Ticket {
            id: "01JTICKET".to_owned(),
            tenant_id: "acme".to_owned(),
            conversation_id: "conv-1".to_owned(),
            status: Status::Intake,
            stage: Stage::Draft,
            transcript: "customer: checkout 500s".to_owned(),
            title: None,
            body_markdown: None,
            severity: None,
            environment: None,
            verdict: None,
            judge_reasons: None,
            customer_question: None,
            external_id: None,
            external_url: None,
            created_at: "2026-09-19T00:00:00Z".to_owned(),
            updated_at: "2026-09-19T00:00:00Z".to_owned(),
        }
    }

    /// A real round trip against the shipped migration: the batch
    /// (ticket + audit + outbox row) commits atomically and the readers
    /// decode exactly what was written.
    #[test]
    fn readers_read_what_the_builders_write() {
        let db = migrated_db();
        let ticket = fresh_ticket();
        let at = ticket.created_at.clone();

        // One batch, exactly as intake hands it to the caller.
        let batch = vec![
            insert_ticket_stmt(&ticket),
            insert_event_stmt(
                "01JEVENT",
                &ticket.id,
                crate::model::INTAKE_SEQ,
                &at,
                Stage::Draft,
                EventKind::Intake,
                &serde_json::Value::Null,
            ),
            enqueue_stage_stmt(
                &Outbox::new("sg_escalation_outbox"),
                "01JJOB",
                &ticket.id,
                &ticket.tenant_id,
                Stage::Draft,
                &at,
            ),
        ];
        pollster::block_on(db.batch_atomic(&batch)).expect("batch commits");

        let loaded = pollster::block_on(load_ticket(&db, "01JTICKET"))
            .expect("read")
            .expect("row exists");
        assert_eq!(loaded, ticket, "the row decodes back to what was inserted");

        assert!(
            pollster::block_on(find_ticket_by_conversation(&db, "acme", "conv-1"))
                .expect("read")
                .is_some(),
            "the conversation lookup finds the escalation"
        );
        assert!(
            pollster::block_on(find_ticket_by_conversation(&db, "other", "conv-1"))
                .expect("read")
                .is_none(),
            "the lookup is tenant-scoped"
        );

        let events = pollster::block_on(ticket_events(&db, "01JTICKET")).expect("read");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, EventKind::Intake);
        assert_eq!(events[0].seq, crate::model::INTAKE_SEQ);
        assert_eq!(events[0].stage, Stage::Draft);
        assert_eq!(events[0].detail, None);
    }

    /// The stage results record, and the destination round-trips through
    /// its JSON — the credential ref only, never a secret.
    #[test]
    fn stage_results_and_destinations_round_trip() {
        let db = migrated_db();
        let at = "2026-09-19T00:00:00Z";
        pollster::block_on(db.batch_atomic(&[insert_ticket_stmt(&fresh_ticket())]))
            .expect("insert");

        let drafted = Drafted {
            title: "Checkout 500s".to_owned(),
            repro_steps: vec!["pay".to_owned()],
            expected: "order completes".to_owned(),
            actual: "HTTP 500".to_owned(),
            environment: Some("production".to_owned()),
            severity: Severity::Error,
        };
        let judgment = Judgment {
            is_defect: true,
            reproducible: true,
            duplicate_of: None,
            severity_ok: false,
            pii_clean: true,
            verdict: Verdict::File,
            reasons: vec!["a real 500".to_owned(), "the steps are exact".to_owned()],
        };
        pollster::block_on(db.batch_atomic(&[
            update_ticket_draft_stmt(
                "01JTICKET",
                &drafted,
                "## Draft",
                Some("why does checkout 500?"),
                at,
            ),
            update_ticket_status_stmt("01JTICKET", Status::Drafting, at),
            update_ticket_judgment_stmt("01JTICKET", &judgment, at),
            update_ticket_stage_stmt("01JTICKET", Stage::Judge, at),
        ]))
        .expect("stage updates commit");

        let ticket = pollster::block_on(load_ticket(&db, "01JTICKET"))
            .expect("read")
            .expect("row exists");
        assert_eq!(ticket.status, Status::Drafting);
        assert_eq!(ticket.stage, Stage::Judge);
        assert_eq!(ticket.title.as_deref(), Some("Checkout 500s"));
        assert_eq!(ticket.severity, Some(Severity::Error));
        assert_eq!(ticket.verdict, Some(Verdict::File));
        assert_eq!(
            ticket.judge_reasons.as_deref(),
            Some(["a real 500".to_owned(), "the steps are exact".to_owned()].as_slice())
        );
        assert_eq!(
            ticket.customer_question.as_deref(),
            Some("why does checkout 500?")
        );

        pollster::block_on(db.batch_atomic(&[put_destination_stmt(
            "acme",
            &Destination::GitHub {
                owner: "acme".to_owned(),
                repo: "checkout".to_owned(),
            },
            "ESCALATION_TRACKER_CREDENTIAL",
            at,
        )]))
        .expect("upsert");
        let (destination, credential_ref) = pollster::block_on(load_destination(&db, "acme"))
            .expect("read")
            .expect("destination configured");
        assert_eq!(
            destination,
            Destination::GitHub {
                owner: "acme".to_owned(),
                repo: "checkout".to_owned()
            }
        );
        assert_eq!(credential_ref, "ESCALATION_TRACKER_CREDENTIAL");
    }

    /// The outbox statements drive core's own queue against the
    /// generated DDL: a batch that enqueues the next stage and completes
    /// the current one leaves exactly one due row, a retry re-arms it
    /// with `attempts` bumped, and a complete drains it.
    #[test]
    fn outbox_statements_drive_core_queue_against_sqlite() {
        let db = migrated_db();
        let outbox = Outbox::new("sg_escalation_outbox");
        let at = "2026-09-19T00:00:00Z";

        pollster::block_on(db.batch_atomic(&[
            insert_ticket_stmt(&fresh_ticket()),
            enqueue_stage_stmt(&outbox, "01JJOB1", "01JTICKET", "acme", Stage::Draft, at),
        ]))
        .expect("seed commits");
        // The stage handoff: record nothing, enqueue the judge, complete
        // the draft job — one batch.
        pollster::block_on(db.batch_atomic(&[
            enqueue_stage_stmt(&outbox, "01JJOB2", "01JTICKET", "acme", Stage::Judge, at),
            outbox_complete_stmt("sg_escalation_outbox", "01JJOB1"),
        ]))
        .expect("handoff batch commits");

        // Only the second (judge) job remains: the first was completed in
        // the same batch that enqueued its successor.
        let due = pollster::block_on(outbox.claim_due(&db, at, "2026-09-19T01:00:00Z", 10))
            .expect("claim");
        assert_eq!(due.len(), 1, "only the judge job is due");
        assert_eq!(due[0].topic, "judge");
        let payload: StagePayload = serde_json::from_str(&due[0].payload).expect("payload decodes");
        assert_eq!(payload.ticket_id, "01JTICKET");

        // A transient failure re-arms the row with a later attempt time,
        // bumping `attempts` and clearing the lease — the same effect
        // core's `Outbox::retry_later` has.
        pollster::block_on(db.batch_atomic(&[outbox_retry_later_stmt(
            "sg_escalation_outbox",
            &due[0].id,
            "2026-09-19T02:00:00Z",
        )]))
        .expect("retry commits");
        let due_again = pollster::block_on(outbox.claim_due(
            &db,
            "2026-09-19T02:00:00Z",
            "2026-09-19T03:00:00Z",
            10,
        ))
        .expect("claim");
        assert_eq!(due_again.len(), 1, "the retry is due again");
        assert_eq!(due_again[0].attempts, 1, "one failed attempt recorded");

        // And completing it in a batch empties the outbox.
        pollster::block_on(db.batch_atomic(&[outbox_complete_stmt(
            "sg_escalation_outbox",
            &due_again[0].id,
        )]))
        .expect("complete commits");
        let drained = pollster::block_on(outbox.claim_due(
            &db,
            "2026-09-19T03:00:00Z",
            "2026-09-19T04:00:00Z",
            10,
        ))
        .expect("claim");
        assert!(drained.is_empty(), "nothing left to deliver");
    }
}
