//! Shared fixture for the issue #4 acceptance tests: an in-memory SQLite
//! database with the module's migration applied, a seeded
//! `sg_destinations` row, and a [`Pipeline`] assembled over the scripted
//! fakes from `module_escalation::testing` plus
//! `cratefield_testing::{FakeDefer, FakeMailer}`.
//!
//! The handoff helper commits [`Intake`]'s statements through
//! [`Database::batch_atomic`] — that is the real contract: the calling
//! support module appends them to **its own** batch, so the handoff is
//! durable exactly when the caller's write is. Nothing here bypasses it.

#![allow(dead_code)] // each test binary uses the helpers it needs

use std::sync::Arc;

use cratefield_adapter_sqlite::SqliteDatabase;
use cratefield_core::{Clock as _, Database as _, IdGen as _, Inbox, Outbox, Statement, UlidIdGen};
use cratefield_testing::FakeDefer;
use module_escalation::intake::OUTBOX_TABLE;
use module_escalation::model::{EventKind, Stage, Ticket, TicketEvent};
use module_escalation::ports::text_model::{Completion, ModelTier};
use module_escalation::ports::tracker::{Destination, Filed, TrackerError};
use module_escalation::store;
use module_escalation::testing::{FakeConfig, FakeTextModel, FakeTracker, SettableClock};
use module_escalation::{Intake, Pipeline, RetryPolicy};
use time::format_description::well_known::Rfc3339;

/// The tenant every test escalates for.
pub(crate) const TENANT: &str = "acme";
/// The conversation every test escalates.
pub(crate) const CONVERSATION: &str = "conv-7";
/// The transcript the draft stage is asked to draft from.
pub(crate) const TRANSCRIPT: &str = "customer: checkout returns HTTP 500 when I pay with a gift card \
     that still has a remaining balance";
/// The `sg_destinations.credential_ref` value: the *Config key* the secret
/// lives under, never the secret.
pub(crate) const CREDENTIAL_REF: &str = "ESCALATION_TRACKER_CREDENTIAL";
/// The secret behind [`CREDENTIAL_REF`], seeded into the `FakeConfig`.
pub(crate) const CREDENTIAL_SECRET: &str = "token-1";
/// The instant every test starts from. Whole-second, so its RFC 3339 form
/// string-compares chronologically — what the outbox's
/// `next_attempt_at <= now` predicate relies on.
pub(crate) const EPOCH: i64 = 1_789_000_000;

/// The draft stage's scripted answer.
pub(crate) const DRAFT_TITLE: &str = "Checkout returns 500 on a part-used gift card";
/// The judge stage's reasons on the `file` verdict, verbatim.
pub(crate) const FILE_REASONS: [&str; 2] = [
    "the steps hit a real 500",
    "the transcript names the exact endpoint",
];
/// The judge stage's single reason on the `reject` verdict, verbatim.
pub(crate) const REJECT_REASON: &str =
    "the customer is asking how to use a gift card, not reporting a defect";
/// The judge stage's single reason on the `needs_info` verdict, verbatim.
pub(crate) const NEEDS_INFO_REASON: &str = "which build number is the customer on?";

// ---------------------------------------------------------------------------
// Fakes' payloads

/// A structured completion like `FakeTextModel::json` builds, for
/// scripts that need more than one answer.
#[must_use]
pub(crate) fn completion(tier: ModelTier, json: serde_json::Value) -> Completion {
    Completion::new(json.to_string(), format!("fake-{}", tier.name())).json(json)
}

/// The `Drafted` the fake drafter writes — schema-shaped, `error`
/// severity, an environment named.
#[must_use]
pub(crate) fn drafted_json() -> serde_json::Value {
    serde_json::json!({
        "title": DRAFT_TITLE,
        "repro_steps": [
            "Add an item to the cart",
            "Pay with a gift card that still has a balance",
        ],
        "expected": "The order completes",
        "actual": "HTTP 500 from /checkout",
        "environment": "production",
        "severity": "error",
    })
}

/// The `Judgment` that sends the ticket on to the file stage.
#[must_use]
pub(crate) fn file_judgment() -> serde_json::Value {
    serde_json::json!({
        "is_defect": true,
        "reproducible": true,
        "severity_ok": true,
        "pii_clean": true,
        "verdict": "file",
        "reasons": FILE_REASONS,
    })
}

/// The `Judgment` that rejects the ticket as not a defect.
#[must_use]
pub(crate) fn reject_judgment() -> serde_json::Value {
    serde_json::json!({
        "is_defect": false,
        "reproducible": false,
        "severity_ok": true,
        "pii_clean": true,
        "verdict": "reject",
        "reasons": [REJECT_REASON],
    })
}

/// The `Judgment` that parks the ticket with a question for the customer.
#[must_use]
pub(crate) fn needs_info_judgment() -> serde_json::Value {
    serde_json::json!({
        "is_defect": true,
        "reproducible": false,
        "severity_ok": true,
        "pii_clean": true,
        "verdict": "needs_info",
        "reasons": [NEEDS_INFO_REASON],
    })
}

/// The reference the fake tracker files under.
#[must_use]
pub(crate) fn filed() -> Filed {
    Filed {
        external_id: "acme/api#7".to_owned(),
        url: "https://github.test/acme/api/7".to_owned(),
    }
}

/// A transient tracker failure with no provider delay, so the retry time
/// is the [`RetryPolicy`] schedule alone.
#[must_use]
pub(crate) fn transient() -> TrackerError {
    TrackerError::Transient { retry_after: None }
}

/// RFC 3339 of a whole-second instant.
#[must_use]
pub(crate) fn format_at(at: time::OffsetDateTime) -> String {
    at.format(&Rfc3339)
        .expect("a whole-second instant formats as RFC 3339")
}

// ---------------------------------------------------------------------------
// Database and handoff

/// A fresh in-memory database with the escalation migration applied.
#[must_use]
pub(crate) fn migrated_db() -> Arc<SqliteDatabase> {
    let db = SqliteDatabase::in_memory().expect("in-memory database");
    db.apply_migrations(
        "module-escalation",
        std::slice::from_ref(&module_escalation::MIGRATION_ESCALATION),
    )
    .expect("migration applies");
    Arc::new(db)
}

/// Seeds the tenant's tracker destination: a GitHub repo, and the
/// credential *reference* the file stage resolves through the Config port.
pub(crate) fn seed_destination(db: &SqliteDatabase) {
    let stmt = store::put_destination_stmt(
        TENANT,
        &Destination::GitHub {
            owner: "acme".to_owned(),
            repo: "api".to_owned(),
        },
        CREDENTIAL_REF,
        &format_at(time::OffsetDateTime::from_unix_timestamp(EPOCH).expect("epoch")),
    );
    pollster::block_on(db.batch_atomic(&[stmt])).expect("destination seeds");
}

/// Performs the intake handoff the way the calling module must: the
/// statements go into the caller's `batch_atomic`, so the ticket, its
/// intake audit row and the draft job exist exactly when this commits.
/// Returns the minted ticket id.
#[must_use]
pub(crate) fn commit_handoff(
    db: &SqliteDatabase,
    clock: &Arc<SettableClock>,
    tenant: &str,
    conversation: &str,
    transcript: &str,
) -> String {
    let intake = Intake::new(OUTBOX_TABLE, clock.clone(), Arc::new(UlidIdGen));
    let handoff = intake.handoff(tenant, conversation, transcript);
    pollster::block_on(db.batch_atomic(&handoff.statements))
        .expect("the caller's batch commits the handoff");
    handoff.ticket_id
}

// ---------------------------------------------------------------------------
// Fixture

/// One test's world: the database, the scripted fakes, the settable clock,
/// the deferred-self-drain collector, and the ticket id the handoff minted.
pub(crate) struct Fixture {
    /// The migrated in-memory database.
    pub(crate) db: Arc<SqliteDatabase>,
    /// The scripted text model.
    pub(crate) model: FakeTextModel,
    /// The scripted tracker.
    pub(crate) tracker: FakeTracker,
    /// The clock tests advance past retry boundaries.
    pub(crate) clock: Arc<SettableClock>,
    /// The `Defer` port: each finished stage queues a self-drain here.
    pub(crate) defer: FakeDefer,
    /// The `Mailer` port; `None` is the v0 default (no mailer wired).
    pub(crate) mailer: Option<Arc<dyn cratefield_core::Mailer>>,
    /// The ticket the handoff minted.
    pub(crate) ticket_id: String,
}

/// Assembles a fixture: migrated database, seeded destination, handoff
/// committed through `batch_atomic`, and the given scripted fakes.
#[must_use]
pub(crate) fn fixture(model: FakeTextModel, tracker: FakeTracker) -> Fixture {
    let db = migrated_db();
    seed_destination(&db);
    let clock = Arc::new(SettableClock::at_unix(EPOCH));
    let ticket_id = commit_handoff(&db, &clock, TENANT, CONVERSATION, TRANSCRIPT);
    Fixture {
        db,
        model,
        tracker,
        clock,
        defer: FakeDefer::new(),
        mailer: None,
        ticket_id,
    }
}

impl Fixture {
    /// The pipeline over this fixture, with the issue's default policy.
    #[must_use]
    pub(crate) fn pipeline(&self) -> Pipeline {
        self.pipeline_with_policy(RetryPolicy::new())
    }

    /// The pipeline over this fixture, with a shrunken policy.
    #[must_use]
    pub(crate) fn pipeline_with_policy(&self, policy: RetryPolicy) -> Pipeline {
        Pipeline::new(
            self.db.clone(),
            Arc::new(self.model.clone()),
            Arc::new(self.tracker.clone()),
            self.mailer.clone(),
            Arc::new(FakeConfig::new().with(CREDENTIAL_REF, CREDENTIAL_SECRET)),
            self.clock.clone(),
            Arc::new(UlidIdGen),
            Some(Arc::new(self.defer.clone())),
        )
        .with_retry_policy(policy)
    }
}

// ---------------------------------------------------------------------------
// Driving the pipeline

/// Drains until a sweep has nothing left to claim, bounded the way
/// `Escalation::scheduled` bounds its sweeps. Returns the rows processed.
pub(crate) fn drain_all(pipeline: &Pipeline) -> usize {
    let mut total = 0;
    for _ in 0..Pipeline::MAX_SWEEPS {
        let processed = pollster::block_on(pipeline.drain(Pipeline::SWEEP_LIMIT))
            .expect("sweep completes or records its failure durably");
        total += processed;
        if processed == 0 {
            break;
        }
    }
    total
}

/// Re-inserts the file stage's outbox row for the fixture's ticket — the
/// at-least-once redelivery the `Inbox` exists to absorb. Fresh job id,
/// same ticket, same tenant, exactly as `store::enqueue_stage_stmt` writes
/// it.
pub(crate) fn requeue_file_stage(fixture: &Fixture) {
    let job_id = UlidIdGen.ulid();
    let stmt = store::enqueue_stage_stmt(
        &Outbox::new(OUTBOX_TABLE),
        &job_id,
        &fixture.ticket_id,
        TENANT,
        Stage::File,
        &format_at(fixture.clock.now()),
    );
    pollster::block_on(fixture.db.batch_atomic(&[stmt])).expect("the redelivered row commits");
}

/// Takes the `<ticket>:<stage>` inbox claim by hand, as a crashed attempt
/// would have left it: the key is held, the stage's work never committed.
/// Returns whether the claim was fresh.
pub(crate) fn hold_inbox_key(fixture: &Fixture, stage: Stage) -> bool {
    pollster::block_on(Inbox::new(Pipeline::INBOX_TABLE).claim(
        &*fixture.db,
        &format!("{}:{}", fixture.ticket_id, stage.as_topic()),
        &format_at(fixture.clock.now()),
    ))
    .expect("claim read")
}

// ---------------------------------------------------------------------------
// Reading state back

/// The fixture's ticket row.
#[must_use]
pub(crate) fn ticket(fixture: &Fixture) -> Ticket {
    pollster::block_on(store::load_ticket(&*fixture.db, &fixture.ticket_id))
        .expect("ticket read")
        .expect("ticket row exists")
}

/// The fixture's audit trail, in `(seq, at, id)` order.
#[must_use]
pub(crate) fn events(fixture: &Fixture) -> Vec<TicketEvent> {
    pollster::block_on(store::ticket_events(&*fixture.db, &fixture.ticket_id)).expect("events read")
}

/// The trail as `(stage topic, event kind)` pairs — the readable shape an
/// audit assertion names.
#[must_use]
pub(crate) fn stage_and_kind(events: &[TicketEvent]) -> Vec<(&'static str, &'static str)> {
    events
        .iter()
        .map(|event| (event.stage.as_topic(), event.kind.as_str()))
        .collect()
}

/// The one event of `kind`; panics with the trail it searched if absent.
#[must_use]
pub(crate) fn event_of_kind(events: &[TicketEvent], kind: EventKind) -> &TicketEvent {
    events
        .iter()
        .find(|event| event.kind == kind)
        .unwrap_or_else(|| {
            let found: Vec<&str> = events.iter().map(|event| event.kind.as_str()).collect();
            panic!("no `{}` event; the trail has {found:?}", kind.as_str())
        })
}

/// `(attempts, next_attempt_at)` of the one outbox row with `topic`, if
/// any — read straight off the row, the way a retry assertion must.
#[must_use]
pub(crate) fn outbox_row(fixture: &Fixture, topic: &str) -> Option<(i64, String)> {
    let stmt = Statement::with_values(
        format!("SELECT attempts, next_attempt_at FROM {OUTBOX_TABLE} WHERE topic = ?"),
        vec![topic.to_owned().into()],
    );
    let rows = pollster::block_on(fixture.db.query(&stmt)).expect("outbox row read");
    rows.first().map(|row| {
        (
            row.get::<i64>("attempts").expect("attempts column decodes"),
            row.get::<String>("next_attempt_at")
                .expect("next_attempt_at column decodes"),
        )
    })
}

/// How many rows the escalation outbox currently holds.
#[must_use]
pub(crate) fn outbox_count(fixture: &Fixture) -> usize {
    let rows = pollster::block_on(
        fixture
            .db
            .query(&Statement::new(format!("SELECT topic FROM {OUTBOX_TABLE}"))),
    )
    .expect("outbox count read");
    rows.rows.len()
}
