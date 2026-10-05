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
//! because core at the rev this workspace pins (see the root `Cargo.toml`)
//! has no statement form of `Outbox::complete`/`Outbox::retry_later` —
//! the only way to complete a stage inside the same batch that enqueues
//! the next one would otherwise be a second round trip after the batch,
//! which loses atomicity. They are rendered from the exact query bodies
//! core's own methods build (checked against the pinned source and pinned
//! by the tests below), and should be deleted when core grows
//! `complete_statement`/`retry_later_statement`.

use std::collections::BTreeMap;

use cratefield_core::{Database, Row, Statement};
use sea_query::{Alias, Expr, Func, Order, Query, SimpleExpr};

use crate::error::Error;
use crate::model::{
    Drafted, EventKind, Judgment, Kind, Stage, StagePayload, Status, Ticket, TicketEvent, Verdict,
};

use cratefield_core::{Destination, Filed, Severity, TicketState};

fn iden(name: &str) -> Alias {
    Alias::new(name)
}

/// The columns `sg_tickets` reads come back in, in decode order.
const TICKET_COLUMNS: [&str; 20] = [
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
    "match_count",
    "created_at",
    "updated_at",
    // Appended by migrations 0007 and 0008, so they stay at the end of the
    // insert and select lists (the intake tests pin the earlier positions).
    "kind",
    "tracker_state",
];

/// The columns `sg_ticket_events` reads come back in, in decode order.
const EVENT_COLUMNS: [&str; 7] = ["id", "ticket_id", "seq", "at", "stage", "kind", "detail"];

/// The `sg_routes.destination` sentinel for a kind that files into the
/// module's own built-in ticketing rather than an external tracker. A
/// serialized [`Destination`] is always a JSON object (or `null`), so the
/// bare string can never collide with one.
pub const LOCAL_ROUTE: &str = "local";

/// Where a `(tenant, kind)` route sends its tickets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteTarget {
    /// File into the module's own built-in ticketing: no tracker call, the
    /// ticket is filed with a synthetic `local:<ticket id>` reference.
    Local,
    /// File into an external tracker through the `Tracker` port.
    Tracker(Destination),
}

/// A resolved `(tenant, kind)` route: its target, the credential
/// *reference* the file stage resolves at file-time (never a secret), and
/// the severity→priority map the tracker's labels carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    /// Where the ticket goes.
    pub target: RouteTarget,
    /// A reference to the credential (`secret:` name or Config key), or the
    /// empty string for a [`RouteTarget::Local`] route.
    pub credential_ref: String,
    /// `severity` wire form → the tracker's priority value. Empty when the
    /// route names no priorities.
    pub priority: BTreeMap<String, String>,
}

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
            ticket.match_count.into(),
            ticket.created_at.clone().into(),
            ticket.updated_at.clone().into(),
            ticket.kind.as_str().into(),
            ticket.tracker_state.map(ticket_state_text).into(),
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
            (iden("kind"), drafted.kind.as_str().into()),
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
/// `updated_at`. An empty `url` is written as SQL NULL — the file stage
/// blanks the URL a webhook reports, because that URL is the destination's
/// secret (see `Pipeline::file_ticket`).
#[must_use]
pub fn update_ticket_filed_stmt(ticket_id: &str, filed: &Filed, at: &str) -> Statement {
    let mut update = Query::update();
    update
        .table(iden("sg_tickets"))
        .values([
            (iden("external_id"), filed.external_id.clone().into()),
            (
                iden("external_url"),
                (!filed.url.is_empty()).then(|| filed.url.clone()).into(),
            ),
            (iden("updated_at"), at.to_owned().into()),
        ])
        .and_where(Expr::col(iden("id")).eq(ticket_id));
    Statement::render(&update)
}

/// Records the tracker's reported state on the ticket and refreshes
/// `updated_at`. Written `open` when the file stage succeeds (the tracker
/// accepted the ticket) and refreshed by every follow-up poll that sees a
/// change (see `Pipeline::run_follow`). `(ticket, seq)` pairs in the audit
/// trail are not unique, so the follow stage reads the last state back from
/// this column rather than the event trail.
#[must_use]
pub fn update_ticket_tracker_state_stmt(
    ticket_id: &str,
    state: TicketState,
    at: &str,
) -> Statement {
    let mut update = Query::update();
    update
        .table(iden("sg_tickets"))
        .values([
            (iden("tracker_state"), ticket_state_text(state).into()),
            (iden("updated_at"), at.to_owned().into()),
        ])
        .and_where(Expr::col(iden("id")).eq(ticket_id));
    Statement::render(&update)
}

/// Upserts the customer's contact address for one conversation (`the
/// (tenant_id, conversation_id)` primary key means a later turn replaces an
/// earlier address). Called by `HandoffSink::remember_contact` through the
/// escalation `Intake`, so the address is written in the same atomic batch
/// as the turn that supplied it. **Only the address module-support's own
/// tables do not own lives here** — escalation reads it back at
/// notify-time (`contact_email`) and declares it in `personal_data`.
#[must_use]
pub fn upsert_contact_stmt(
    tenant_id: &str,
    conversation_id: &str,
    email: &str,
    updated_at: &str,
) -> Statement {
    let mut insert = Query::insert();
    insert
        .into_table(iden("sg_contacts"))
        .columns(["tenant_id", "conversation_id", "email", "updated_at"])
        .values_panic([
            tenant_id.to_owned().into(),
            conversation_id.to_owned().into(),
            email.to_owned().into(),
            updated_at.to_owned().into(),
        ])
        .on_conflict(
            sea_query::OnConflict::columns([iden("tenant_id"), iden("conversation_id")])
                .update_columns(["email", "updated_at"])
                .to_owned(),
        );
    Statement::render(&insert)
}
/// ticket it duplicates — so the notify stage can name and link it
/// without loading the other row — and refreshes `updated_at`. The
/// duplicate is never filed itself, so this is where its `external_id`
/// and `external_url` come from.
#[must_use]
pub fn update_ticket_duplicate_stmt(ticket_id: &str, existing: &Ticket, at: &str) -> Statement {
    let mut update = Query::update();
    update
        .table(iden("sg_tickets"))
        .values([
            (iden("external_id"), existing.external_id.clone().into()),
            (iden("external_url"), existing.external_url.clone().into()),
            (iden("updated_at"), at.to_owned().into()),
        ])
        .and_where(Expr::col(iden("id")).eq(ticket_id));
    Statement::render(&update)
}

/// Records one `sg_ticket_links` row: the duplicate ticket — and the
/// conversation it came from — is now linked to an existing filed ticket.
/// `source_ticket_id` is the primary key, so re-linking the same duplicate
/// ticket (a redelivered judge stage) is a no-op rather than a second row
/// — and a second ticket from the *same* conversation still gets its own
/// row.
#[must_use]
pub fn insert_ticket_link_stmt(
    existing: &Ticket,
    source_ticket_id: &str,
    conversation_id: &str,
    at: &str,
) -> Statement {
    let mut insert = Query::insert();
    insert
        .into_table(iden("sg_ticket_links"))
        .columns([
            "tenant_id",
            "ticket_id",
            "conversation_id",
            "source_ticket_id",
            "created_at",
        ])
        .values_panic([
            existing.tenant_id.clone().into(),
            existing.id.clone().into(),
            conversation_id.to_owned().into(),
            source_ticket_id.to_owned().into(),
            at.to_owned().into(),
        ])
        .on_conflict(
            sea_query::OnConflict::columns([iden("source_ticket_id")])
                .do_nothing()
                .to_owned(),
        );
    Statement::render(&insert)
}

/// Bumps an existing ticket's `match_count` by one — one more later
/// ticket has linked to it as a duplicate — and refreshes `updated_at`,
/// as every ticket write does.
#[must_use]
pub fn increment_match_count_stmt(ticket_id: &str, at: &str) -> Statement {
    let mut update = Query::update();
    update
        .table(iden("sg_tickets"))
        .value(iden("match_count"), Expr::col(iden("match_count")).add(1))
        .value(iden("updated_at"), at)
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
/// names where the secret lives — **never the secret itself**. Since
/// issue #23 it is a `secret:` reference into the tenant's encrypted
/// `cratefield-secrets` store; the older form is a Config key name
/// resolved from the Config port at file-time. Either way the database
/// holds a reference only.
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

/// Deletes a tenant's destination row (`DELETE /destinations`). The
/// credential and webhook-URL secrets live in the encrypted store and are
/// deleted separately by the handler; this is the row only.
#[must_use]
pub fn delete_destination_stmt(tenant_id: &str) -> Statement {
    let mut delete = Query::delete();
    delete
        .from_table(iden("sg_destinations"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id));
    Statement::render(&delete)
}

/// Upserts a tenant's route for one ticket `kind` — `(tenant_id, kind)` is
/// the primary key, so re-routing a kind replaces its row. `target` is
/// stored the same way `sg_destinations.destination` is: a serialized
/// [`Destination`], or the [`LOCAL_ROUTE`] sentinel for the built-in
/// ticketing. `credential_ref` names where the secret lives — **never the
/// secret itself** — and is `""` for a [`RouteTarget::Local`] route.
/// `priority` is the severity→priority map, stored as a JSON object.
#[must_use]
pub fn put_route_stmt(
    tenant_id: &str,
    kind: Kind,
    target: &RouteTarget,
    credential_ref: &str,
    priority: &BTreeMap<String, String>,
    at: &str,
) -> Statement {
    let destination = match target {
        RouteTarget::Local => LOCAL_ROUTE.to_owned(),
        RouteTarget::Tracker(destination) => {
            serde_json::to_string(destination).unwrap_or_else(|_| {
                // Serializing a fieldless/struct variant enum cannot fail; the
                // fallback only keeps this builder pure.
                "null".to_owned()
            })
        }
    };
    let priority = serde_json::to_string(priority).unwrap_or_else(|_| "{}".to_owned());
    let mut insert = Query::insert();
    insert
        .into_table(iden("sg_routes"))
        .columns([
            "tenant_id",
            "kind",
            "destination",
            "credential_ref",
            "priority_map",
            "created_at",
            "updated_at",
        ])
        .values_panic([
            tenant_id.to_owned().into(),
            kind.as_str().into(),
            destination.into(),
            credential_ref.to_owned().into(),
            priority.into(),
            at.to_owned().into(),
            at.to_owned().into(),
        ])
        .on_conflict(
            sea_query::OnConflict::columns([iden("tenant_id"), iden("kind")])
                .update_columns([
                    "destination",
                    "credential_ref",
                    "priority_map",
                    "updated_at",
                ])
                .to_owned(),
        );
    Statement::render(&insert)
}

/// The rendered equivalent of `Outbox::complete(db, id)` — a
/// `DELETE FROM "<table>" WHERE "id" = ?` — so a stage handler can
/// complete its own outbox row **inside the same** `batch_atomic` that
/// records its result and enqueues the next stage. Exists only because
/// core has no `complete_statement` at the rev this workspace pins;
/// delete it (and switch the stages to core) when core grows one. The SQL
/// is pinned verbatim in the tests below against what core's own method
/// renders.
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
/// Core's [`cratefield_core::Inbox`] — at the rev this workspace pins, as
/// in every published version so far — has `claim` and `seen`, both
/// immediate, and no statement form of *releasing* a key. A retrying stage
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
/// has no `retry_later_statement` at the rev this workspace pins; delete
/// it when core grows one. The SQL is pinned verbatim in the tests below.
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
/// issue #266), and `at` for the first `next_attempt_at`. `event_id` rides
/// in the payload when a status-update notify must name the
/// `status_changed` event it announces (see
/// [`crate::model::StagePayload::event_id`]). A thin wrapper so stages
/// cannot mistype the topic or forget the subject.
#[must_use]
pub fn enqueue_stage_stmt(
    outbox: &cratefield_core::Outbox,
    job_id: &str,
    ticket_id: &str,
    tenant_id: &str,
    stage: Stage,
    event_id: Option<&str>,
    at: &str,
) -> Statement {
    let payload = StagePayload {
        ticket_id: ticket_id.to_owned(),
        tenant_id: tenant_id.to_owned(),
        event_id: event_id.map(str::to_owned),
    };
    let payload = serde_json::to_string(&payload).unwrap_or_else(|_| {
        format!("{{\"ticket_id\":\"{ticket_id}\",\"tenant_id\":\"{tenant_id}\"}}")
    });
    outbox.enqueue_statement(job_id, stage.as_topic(), &payload, Some(ticket_id), at)
}

/// Sets one outbox row's `next_attempt_at` and clears its lease, without
/// incrementing `attempts` — a reschedule, not a failure. Core's
/// `Outbox::reschedule` reads the row and updates it in its own round trip;
/// this is the statement form, so the follow stage can queue its next poll
/// inside the same `batch_atomic` that records a state change. The SQL is
/// pinned verbatim in the tests below.
#[must_use]
pub fn outbox_reschedule_stmt(table: &str, id: &str, next_attempt_at: &str) -> Statement {
    let mut update = Query::update();
    update
        .table(iden(table))
        .value(iden("next_attempt_at"), next_attempt_at)
        .value(iden("locked_until"), Option::<String>::None)
        .and_where(Expr::col(iden("id")).eq(id));
    Statement::render(&update)
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

/// How many of a tenant's most recently active filed tickets enter the
/// candidate pool before BM25 ranking narrows them to the brief's short
/// list (see `pipeline::CANDIDATE_LIMIT`).
const CANDIDATE_POOL: u64 = 200;

/// The filed tickets a draft could duplicate: the tenant's *other* filed
/// tickets, most recently active first, capped at [`CANDIDATE_POOL`].
///
/// A successful file is the only writer of `external_id`, so requiring it
/// (alongside `status = 'filed'`) keeps the tracker-reached tickets and
/// nothing else — rejected, needs-info, duplicate and dead-lettered rows
/// never get one. The local store does not track whether the external
/// issue is still open, so "open or recently closed" is approximated by
/// the most recently *active* filed tickets: `updated_at DESC` with a
/// fixed-size pool. A `match_count` bump refreshes `updated_at` (see
/// [`increment_match_count_stmt`]), so a ticket still collecting
/// duplicates stays hot in the pool.
///
/// The tracker destination is per-tenant — `sg_destinations` is keyed by
/// `tenant_id` alone — so same-tenant **is** same-destination; there is
/// no second key to scope on. `exclude_ticket_id` drops the ticket
/// currently being judged.
///
/// # Errors
///
/// As [`load_ticket`].
pub async fn candidate_tickets(
    db: &dyn Database,
    tenant_id: &str,
    exclude_ticket_id: &str,
) -> Result<Vec<Ticket>, Error> {
    let mut query = select_tickets();
    query
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden("id")).ne(exclude_ticket_id))
        .and_where(Expr::col(iden("status")).eq(Status::Filed.as_str()))
        .and_where(Expr::col(iden("external_id")).is_not_null())
        // Newest first, so the bounded pool keeps the freshest tickets;
        // `id` breaks an `updated_at` tie deterministically.
        .order_by(iden("updated_at"), Order::Desc)
        .order_by(iden("id"), Order::Desc)
        .limit(CANDIDATE_POOL);
    let rows = db.query(&Statement::render(&query)).await?;
    rows.rows.iter().map(ticket_from).collect()
}

/// How many of a tenant's built-in tickets `GET /tickets` returns at most.
/// The same bounded-pool idiom as [`CANDIDATE_POOL`], newest first; the
/// route has no cursor, so this is the sane ceiling rather than a page
/// size.
const LOCAL_LIST_LIMIT: u64 = 200;

/// The predicate matching a built-in ticket: one whose `external_id` is the
/// `local:` reference for **its own** id — the exact string the file stage
/// wrote (`local:<ticket id>`; see `Pipeline::run_file` and [`LOCAL_ROUTE`]).
///
/// Comparing the reference to `'local:' || id`, rather than matching the
/// `local:` *prefix*, is what keeps a duplicate of a built-in ticket out of
/// the list and off the by-id routes: a duplicate row copies the existing
/// ticket's reference ([`update_ticket_duplicate_stmt`]) but keeps its own
/// id, so a prefix match would present it as a built-in ticket.
fn built_in_ticket() -> SimpleExpr {
    Expr::cust_with_values(
        "sg_tickets.external_id = ? || sg_tickets.id",
        [format!("{LOCAL_ROUTE}:")],
    )
}

/// Lists a tenant's built-in tickets — the ones the file stage filed into
/// the module's own ticketing, not an external tracker — newest first,
/// capped at [`LOCAL_LIST_LIMIT`]. `status`, when given, narrows the list
/// to one lifecycle status.
///
/// # Errors
///
/// As [`load_ticket`].
pub async fn local_tickets(
    db: &dyn Database,
    tenant_id: &str,
    status: Option<Status>,
) -> Result<Vec<Ticket>, Error> {
    let mut query = select_tickets();
    query
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(built_in_ticket());
    if let Some(status) = status {
        query.and_where(Expr::col(iden("status")).eq(status.as_str()));
    }
    query
        // Newest first; `id` (a ULID) breaks a `created_at` tie
        // deterministically.
        .order_by(iden("created_at"), Order::Desc)
        .order_by(iden("id"), Order::Desc)
        .limit(LOCAL_LIST_LIMIT);
    let rows = db.query(&Statement::render(&query)).await?;
    rows.rows.iter().map(ticket_from).collect()
}

/// Loads one tenant's built-in ticket by id — `None` when no such ticket
/// exists, when it belongs to another tenant, when it was filed to an
/// external tracker, or when it is only a *duplicate* of a built-in ticket
/// (see [`built_in_ticket`]). So a cross-tenant, tracker-filed or duplicate
/// id is indistinguishable from a missing one (the routes answer `404`
/// without leaking which).
///
/// # Errors
///
/// As [`load_ticket`].
pub async fn load_local_ticket(
    db: &dyn Database,
    tenant_id: &str,
    id: &str,
) -> Result<Option<Ticket>, Error> {
    let mut query = select_tickets();
    query
        .and_where(Expr::col(iden("id")).eq(id))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(built_in_ticket())
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

/// Loads the route a tenant has configured for one ticket `kind`, or
/// `None` when the kind has no row. `destination` is decoded like
/// `sg_destinations.destination`, except that the [`LOCAL_ROUTE`] sentinel
/// decodes to [`RouteTarget::Local`]. `credential_ref` may be SQL NULL
/// (local routes) and comes back as the empty string;
/// `priority_map` is a JSON object of severity wire form → priority value.
///
/// # Errors
///
/// As [`load_ticket`], plus [`Error::Decode`] when the stored
/// `Destination` or `priority_map` JSON no longer parses.
pub async fn load_route(
    db: &dyn Database,
    tenant_id: &str,
    kind: Kind,
) -> Result<Option<Route>, Error> {
    let mut query = Query::select();
    query
        .columns(["destination", "credential_ref", "priority_map"])
        .from(iden("sg_routes"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden("kind")).eq(kind.as_str()))
        .limit(1);
    let rows = db.query(&Statement::render(&query)).await?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let destination = required_text(row, "destination")?;
    let target = if destination == LOCAL_ROUTE {
        RouteTarget::Local
    } else {
        let destination: Destination = serde_json::from_str(&destination)
            .map_err(|err| Error::Decode(format!("sg_routes.destination: {err}")))?;
        RouteTarget::Tracker(destination)
    };
    let priority = parse_json_column::<BTreeMap<String, String>>(
        optional_text(row, "priority_map")?,
        "priority_map",
    )?
    .unwrap_or_default();
    Ok(Some(Route {
        target,
        credential_ref: optional_text(row, "credential_ref")?.unwrap_or_default(),
        priority,
    }))
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

/// Loads one audit event by id — the follow stage stores the id of the
/// `status_changed` event it wrote in the notify row's payload, and the
/// notify stage reads it back to learn the `to` state it must phrase the
/// update from.
///
/// # Errors
///
/// As [`load_ticket`].
pub async fn load_event(db: &dyn Database, id: &str) -> Result<Option<TicketEvent>, Error> {
    let mut query = Query::select();
    query
        .columns(EVENT_COLUMNS)
        .from(iden("sg_ticket_events"))
        .and_where(Expr::col(iden("id")).eq(id))
        .limit(1);
    let rows = db.query(&Statement::render(&query)).await?;
    rows.first().map(event_from).transpose()
}

/// How many `status_changed` events a ticket has — the follow stage's
/// monotonic transition counter, and the `n` in the claim key that keeps a
/// concurrent poll from reporting the same change twice.
///
/// # Errors
///
/// As [`load_ticket`].
pub async fn status_changed_count(db: &dyn Database, ticket_id: &str) -> Result<i64, Error> {
    let mut query = Query::select();
    query
        .expr_as(Func::count(Expr::col(iden("id"))), Alias::new("n"))
        .from(iden("sg_ticket_events"))
        .and_where(Expr::col(iden("ticket_id")).eq(ticket_id))
        .and_where(Expr::col(iden("kind")).eq("status_changed"));
    let rows = db.query(&Statement::render(&query)).await?;
    let count = rows
        .first()
        .and_then(|row| row.get::<i64>("n"))
        .unwrap_or(0);
    Ok(count)
}

/// The customer's contact address stored for one conversation, if any — the
/// notify stage's recipient. `None` when the support module never supplied a
/// contact (the module-support handoff without a `remember_contact`
/// statement), which the notify stage reports as `no_recipient`.
///
/// # Errors
///
/// [`Error::Db`] when the read fails.
pub async fn contact_email(
    db: &dyn Database,
    tenant_id: &str,
    conversation_id: &str,
) -> Result<Option<String>, Error> {
    let mut query = Query::select();
    query
        .columns(["email"])
        .from(iden("sg_contacts"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden("conversation_id")).eq(conversation_id))
        .limit(1);
    let rows = db.query(&Statement::render(&query)).await?;
    match rows.first() {
        Some(row) => optional_text(row, "email"),
        None => Ok(None),
    }
}

/// The `created_at` of one outbox row, so the follow stage can age a poll
/// row (its backoff widens from hourly to daily once the row is a day old).
/// `None` when the row is gone — a concurrent drainer completed it.
///
/// # Errors
///
/// [`Error::Db`] when the read fails.
pub async fn outbox_created_at(
    db: &dyn Database,
    table: &str,
    id: &str,
) -> Result<Option<String>, Error> {
    let mut query = Query::select();
    query
        .columns(["created_at"])
        .from(iden(table))
        .and_where(Expr::col(iden("id")).eq(id))
        .limit(1);
    let rows = db.query(&Statement::render(&query)).await?;
    match rows.first() {
        Some(row) => optional_text(row, "created_at"),
        None => Ok(None),
    }
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
    let kind = required_text(row, "kind")?.parse::<Kind>()?;

    Ok(Ticket {
        id: required_text(row, "id")?,
        tenant_id: required_text(row, "tenant_id")?,
        conversation_id: required_text(row, "conversation_id")?,
        kind,
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
        match_count: row.get::<i64>("match_count").ok_or_else(|| {
            Error::Decode("column `match_count` missing or not an integer".to_owned())
        })?,
        tracker_state: optional_text(row, "tracker_state")?
            .map(|raw| ticket_state_from(&raw))
            .transpose()?,
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

/// The `TicketState` wire form for storage (`"open"`, `"in_progress"`, ...).
/// The port type has no `name()`/`from_str`, so this rides its serde (the
/// wire form is a bare string like `open`, not a JSON document).
pub(crate) fn ticket_state_text(state: TicketState) -> String {
    serde_json::to_value(state)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "unknown".to_owned())
}

/// Decodes a `tracker_state` cell. As [`severity_from`], the port type has no
/// `from_str`, so this rides its serde.
pub(crate) fn ticket_state_from(raw: &str) -> Result<TicketState, Error> {
    serde_json::from_value(serde_json::Value::String(raw.to_owned()))
        .map_err(|err| Error::Decode(format!("column `tracker_state`: {err}: `{raw}`")))
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
    /// expects. Rendered from the pinned core's own query bodies and
    /// pinned here; regenerate if core's SQL ever changes.
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
    fn outbox_reschedule_stmt_matches_core_verbatim() {
        let stmt =
            outbox_reschedule_stmt("sg_escalation_outbox", "01JDEMO", "2026-09-19T00:15:00Z");
        // No `attempts` bump: a reschedule is not a failure.
        assert_eq!(
            stmt.sql,
            "UPDATE \"sg_escalation_outbox\" SET \"next_attempt_at\" = ?, \
             \"locked_until\" = ? WHERE \"id\" = ?"
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
            None,
            "2026-09-19T00:00:00Z",
        );
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
            &[
                crate::MIGRATION_ESCALATION,
                crate::MIGRATION_DUPLICATES,
                crate::MIGRATION_ROUTING,
                crate::MIGRATION_FOLLOW,
            ],
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
            kind: Kind::Defect,
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
            match_count: 0,
            tracker_state: None,
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
                None,
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
            kind: Kind::Defect,
            repro_steps: vec!["pay".to_owned()],
            expected: "order completes".to_owned(),
            actual: "HTTP 500".to_owned(),
            environment: Some("production".to_owned()),
            severity: Severity::Error,
            summary: None,
            customer_ask: None,
            company: None,
            seats: None,
            intent: None,
        };
        let judgment = Judgment {
            is_defect: true,
            kind_ok: true,
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

    /// `sg_routes` round-trips both targets — a serialized `Destination`
    /// and the `local` sentinel — with the priority map, treats a missing
    /// `(tenant, kind)` as `None`, and replaces a row on re-route (the
    /// conflict path).
    #[test]
    fn routes_round_trip_with_their_priority_map() {
        let db = migrated_db();
        let at = "2026-09-19T00:00:00Z";
        let github = Destination::GitHub {
            owner: "acme".to_owned(),
            repo: "checkout".to_owned(),
        };
        let priority = BTreeMap::from([("error".to_owned(), "high".to_owned())]);
        pollster::block_on(db.batch_atomic(&[
            put_route_stmt(
                "acme",
                Kind::Defect,
                &RouteTarget::Tracker(github.clone()),
                "secret:gh",
                &priority,
                at,
            ),
            put_route_stmt(
                "acme",
                Kind::Lead,
                &RouteTarget::Local,
                "",
                &BTreeMap::new(),
                at,
            ),
        ]))
        .expect("upserts commit");

        let defect = pollster::block_on(load_route(&db, "acme", Kind::Defect))
            .expect("read")
            .expect("route configured");
        assert_eq!(defect.target, RouteTarget::Tracker(github));
        assert_eq!(defect.credential_ref, "secret:gh");
        assert_eq!(defect.priority, priority);

        let lead = pollster::block_on(load_route(&db, "acme", Kind::Lead))
            .expect("read")
            .expect("route configured");
        assert_eq!(lead.target, RouteTarget::Local);
        assert_eq!(lead.credential_ref, "");
        assert!(lead.priority.is_empty());

        assert!(
            pollster::block_on(load_route(&db, "acme", Kind::SupportCase))
                .expect("read")
                .is_none(),
            "a kind with no row has no route"
        );

        // Re-routing a kind replaces its row rather than adding one.
        pollster::block_on(db.batch_atomic(&[put_route_stmt(
            "acme",
            Kind::Defect,
            &RouteTarget::Local,
            "",
            &BTreeMap::new(),
            at,
        )]))
        .expect("re-route commits");
        let defect = pollster::block_on(load_route(&db, "acme", Kind::Defect))
            .expect("read")
            .expect("route configured");
        assert_eq!(defect.target, RouteTarget::Local);
    }

    /// The customer contact upserts (one row per conversation, a later
    /// address replacing an earlier one) and the tracker state round-trips.
    #[test]
    fn contact_and_tracker_state_round_trip() {
        let db = migrated_db();
        let at = "2026-09-19T00:00:00Z";
        pollster::block_on(db.batch_atomic(&[insert_ticket_stmt(&fresh_ticket())]))
            .expect("insert");

        assert_eq!(
            pollster::block_on(contact_email(&db, "acme", "conv-1")).expect("read"),
            None,
            "no contact until one is written"
        );
        pollster::block_on(db.batch_atomic(&[
            upsert_contact_stmt("acme", "conv-1", "jane@example.com", at),
            update_ticket_tracker_state_stmt("01JTICKET", TicketState::Open, at),
        ]))
        .expect("write commits");
        assert_eq!(
            pollster::block_on(contact_email(&db, "acme", "conv-1")).expect("read"),
            Some("jane@example.com".to_owned())
        );
        // Scoped by tenant and conversation.
        assert_eq!(
            pollster::block_on(contact_email(&db, "other", "conv-1")).expect("read"),
            None
        );
        assert_eq!(
            pollster::block_on(load_ticket(&db, "01JTICKET"))
                .expect("read")
                .expect("row")
                .tracker_state,
            Some(TicketState::Open)
        );

        // A later address replaces the earlier one, still one row.
        let later = "2026-09-19T00:05:00Z";
        pollster::block_on(db.batch_atomic(&[upsert_contact_stmt(
            "acme",
            "conv-1",
            "jane.doe@example.com",
            later,
        )]))
        .expect("upsert commits");
        assert_eq!(
            pollster::block_on(contact_email(&db, "acme", "conv-1")).expect("read"),
            Some("jane.doe@example.com".to_owned())
        );
        let rows = pollster::block_on(db.query(&Statement::new(
            "SELECT COUNT(*) AS n FROM sg_contacts".to_owned(),
        )))
        .expect("count");
        assert_eq!(rows.first().expect("row").get::<i64>("n"), Some(1));
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
            enqueue_stage_stmt(
                &outbox,
                "01JJOB1",
                "01JTICKET",
                "acme",
                Stage::Draft,
                None,
                at,
            ),
        ]))
        .expect("seed commits");
        // The stage handoff: record nothing, enqueue the judge, complete
        // the draft job — one batch.
        pollster::block_on(db.batch_atomic(&[
            enqueue_stage_stmt(
                &outbox,
                "01JJOB2",
                "01JTICKET",
                "acme",
                Stage::Judge,
                None,
                at,
            ),
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
