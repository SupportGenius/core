//! Shared fixture for the issue #4 acceptance tests: an in-memory SQLite
//! database with the module's migration applied, a seeded
//! `sg_destinations` row, and a [`Pipeline`] assembled over the published
//! `cratefield_testing::{FakeTextModel, FakeTracker, FakeDefer}` fakes,
//! mode-scripted per tier.
//!
//! The handoff helper commits [`Intake`]'s statements through
//! [`Database::batch_atomic`] — that is the real contract: the calling
//! support module appends them to **its own** batch, so the handoff is
//! durable exactly when the caller's write is. Nothing here bypasses it.

#![allow(dead_code)] // each test binary uses the helpers it needs

use std::collections::BTreeMap;
use std::sync::Arc;

use cratefield_adapter_sqlite::SqliteDatabase;
use cratefield_core::{
    Clock as _, Completion, Database as _, Destination, IdGen as _, Inbox, ModelTier, Outbox,
    Statement, UlidIdGen,
};
use cratefield_testing::{FakeDefer, FakeTextModel, FakeTracker, TextModelMode};
use module_escalation::intake::OUTBOX_TABLE;
use module_escalation::model::{EventKind, Kind, Stage, Ticket, TicketEvent};
use module_escalation::store::{self, RouteTarget};
use module_escalation::testing::{FakeConfig, SettableClock};
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
/// The Config key and base URL the file stage reads to link a filed ticket
/// back to the conversation it came from (`<base>/<conversation_id>`).
/// Duplicated as a literal here the way [`CREDENTIAL_REF`] is: the pipeline
/// keeps the key crate-private.
pub(crate) const CONVERSATION_URL_KEY: &str = "ESCALATION_CONVERSATION_URL";
/// The base URL behind [`CONVERSATION_URL_KEY`]; the filed body carries
/// `<base>/<conversation>`.
pub(crate) const CONVERSATION_URL: &str = "https://support.acme.test/conversations";
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
/// The judge stage's single reason on the `duplicate` verdict, verbatim.
pub(crate) const DUPLICATE_REASON: &str = "this is the same defect as an existing filed ticket";

// ---------------------------------------------------------------------------
// Fakes' payloads

/// The external id `FakeTracker` answers its **first** accepted file with.
pub(crate) const FILED_EXTERNAL_ID: &str = "fake-0";
/// The url `FakeTracker` answers its **first** accepted file with.
pub(crate) const FILED_EXTERNAL_URL: &str = "https://tracker.fake.test/browse/fake-0";

/// A structured completion answered under a deterministic `fake-<tier>`
/// model name — the shape the pipeline's schema validation reads.
#[must_use]
pub(crate) fn completion(tier: ModelTier, json: serde_json::Value) -> Completion {
    Completion::new(json.to_string(), format!("fake-{}", tier.name())).json(json)
}

/// A model whose fast tier answers `draft` and whose strong tier answers
/// `judgment`, each as a structured completion. The global mode stays
/// `NotConfigured`, so a stage that asks some third tier fails loudly
/// instead of reading a neighbour's answer.
#[must_use]
pub(crate) fn scripted_model(
    draft: serde_json::Value,
    judgment: serde_json::Value,
) -> FakeTextModel {
    let model = FakeTextModel::new(TextModelMode::NotConfigured);
    model.set_mode_for(
        ModelTier::Fast,
        TextModelMode::Complete(completion(ModelTier::Fast, draft)),
    );
    model.set_mode_for(
        ModelTier::Strong,
        TextModelMode::Complete(completion(ModelTier::Strong, judgment)),
    );
    model
}

/// The happy path's model: the drafted JSON, then the `file` judgment.
#[must_use]
pub(crate) fn happy_model() -> FakeTextModel {
    scripted_model(drafted_json(), file_judgment())
}

/// The `Drafted` the fake drafter writes — schema-shaped, `error`
/// severity, an environment named.
#[must_use]
pub(crate) fn drafted_json() -> serde_json::Value {
    serde_json::json!({
        "title": DRAFT_TITLE,
        "kind": "defect",
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

/// A `Drafted` of the given `kind`, carrying only that kind's fields — the
/// shape the drafter is told to answer with (issue #24). `json` is the
/// kind's own field set; `title` and `severity` are added here.
#[must_use]
pub(crate) fn drafted_json_kind(
    kind: Kind,
    title: &str,
    fields: &serde_json::Value,
) -> serde_json::Value {
    let mut draft = serde_json::json!({
        "title": title,
        "kind": kind.as_str(),
        "severity": "info",
    });
    let object = draft.as_object_mut().expect("a JSON object");
    for (key, value) in fields.as_object().expect("a JSON object") {
        object.insert(key.clone(), value.clone());
    }
    draft
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

/// The `Judgment` that would file the ticket but flags the draft as
/// carrying customer PII — the verdict the pipeline must never honour.
#[must_use]
pub(crate) fn pii_file_judgment() -> serde_json::Value {
    serde_json::json!({
        "is_defect": true,
        "reproducible": true,
        "severity_ok": true,
        "pii_clean": false,
        "verdict": "file",
        "reasons": ["the draft quotes the customer's email address"],
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

/// The `Judgment` that links the ticket to `duplicate_of` — the id of an
/// existing filed ticket the judge was shown as a candidate.
#[must_use]
pub(crate) fn duplicate_judgment(duplicate_of: &str) -> serde_json::Value {
    serde_json::json!({
        "is_defect": true,
        "reproducible": true,
        "duplicate_of": duplicate_of,
        "severity_ok": true,
        "pii_clean": true,
        "verdict": "duplicate",
        "reasons": [DUPLICATE_REASON],
    })
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
        &[
            module_escalation::MIGRATION_ESCALATION,
            module_escalation::MIGRATION_DUPLICATES,
            module_escalation::MIGRATION_ROUTING,
            module_escalation::MIGRATION_FOLLOW,
        ],
    )
    .expect("migration applies");
    Arc::new(db)
}

/// Seeds one `(tenant, kind)` route — the file stage's per-kind destination
/// and the credential *reference* it resolves through the Config port at
/// file-time. `priority` is the severity→priority map the tracker's labels
/// carry.
pub(crate) fn seed_route(
    db: &SqliteDatabase,
    tenant: &str,
    kind: Kind,
    target: &RouteTarget,
    credential_ref: &str,
    priority: &BTreeMap<String, String>,
) {
    let stmt = store::put_route_stmt(
        tenant,
        kind,
        target,
        credential_ref,
        priority,
        &format_at(time::OffsetDateTime::from_unix_timestamp(EPOCH).expect("epoch")),
    );
    pollster::block_on(db.batch_atomic(&[stmt])).expect("route seeds");
}

/// Seeds the tenant's tracker destination — the file stage's destination
/// and the credential *reference* it resolves through the Config port.
pub(crate) fn seed_destination(db: &SqliteDatabase, destination: &Destination) {
    seed_destination_for(db, TENANT, destination);
}

/// Seeds a specific tenant's tracker destination. The destination is
/// per-tenant (`sg_destinations` is keyed by `tenant_id`), so a
/// cross-tenant test needs one row per tenant it escalates for.
pub(crate) fn seed_destination_for(db: &SqliteDatabase, tenant: &str, destination: &Destination) {
    let stmt = store::put_destination_stmt(
        tenant,
        destination,
        CREDENTIAL_REF,
        &format_at(time::OffsetDateTime::from_unix_timestamp(EPOCH).expect("epoch")),
    );
    pollster::block_on(db.batch_atomic(&[stmt])).expect("destination seeds");
}

/// The tenant's default destination: a GitHub repo.
#[must_use]
pub(crate) fn github_destination() -> Destination {
    Destination::GitHub {
        owner: "acme".to_owned(),
        repo: "api".to_owned(),
    }
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

/// One test's world: the database, the mode-scripted fakes, the settable
/// clock, the deferred-self-drain collector, and the ticket id the handoff
/// minted.
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
    fixture_for(&github_destination(), model, tracker)
}

/// Like [`fixture`], but escalating the given `transcript` — the seam the
/// scrub test needs to hand the pipeline text that carries an email.
#[must_use]
pub(crate) fn fixture_with_transcript(
    model: FakeTextModel,
    tracker: FakeTracker,
    transcript: &str,
) -> Fixture {
    fixture_full(&github_destination(), model, tracker, transcript)
}

/// [`fixture`] pointed at another tracker — a `Destination::Webhook { url }`
/// tenant, say.
#[must_use]
pub(crate) fn fixture_for(
    destination: &Destination,
    model: FakeTextModel,
    tracker: FakeTracker,
) -> Fixture {
    fixture_full(destination, model, tracker, TRANSCRIPT)
}

/// The one fixture builder: `seed` fills the database (a destination row,
/// or the `sg_routes` rows of [`fixture_routed`]), then the handoff is
/// committed and the fakes are boxed up.
fn fixture_seeded(
    seed: impl FnOnce(&SqliteDatabase),
    model: FakeTextModel,
    tracker: FakeTracker,
    transcript: &str,
) -> Fixture {
    let db = migrated_db();
    seed(&db);
    let clock = Arc::new(SettableClock::at_unix(EPOCH));
    let ticket_id = commit_handoff(&db, &clock, TENANT, CONVERSATION, transcript);
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

/// [`fixture_seeded`] seeding the legacy `sg_destinations` row (rather than
/// `sg_routes` rows).
fn fixture_full(
    destination: &Destination,
    model: FakeTextModel,
    tracker: FakeTracker,
    transcript: &str,
) -> Fixture {
    fixture_seeded(
        |db| seed_destination(db, destination),
        model,
        tracker,
        transcript,
    )
}

/// Like [`fixture`], but the file stage is driven by `sg_routes` rows the
/// caller seeds (`seed`), not a single `sg_destinations` row — no legacy
/// destination exists, so a kind with no route files into the built-in
/// ticketing. The seam the per-kind routing tests need (issue #24).
#[must_use]
pub(crate) fn fixture_routed(
    seed: impl FnOnce(&SqliteDatabase),
    model: FakeTextModel,
    tracker: FakeTracker,
) -> Fixture {
    fixture_seeded(seed, model, tracker, TRANSCRIPT)
}

/// Commits one more handoff (a fresh conversation) into an existing
/// fixture's database, returning its ticket id — the multi-ticket seam the
/// routing tests drive one kind at a time.
#[must_use]
pub(crate) fn add_ticket(fixture: &Fixture, conversation: &str, transcript: &str) -> String {
    commit_handoff(
        &fixture.db,
        &fixture.clock,
        TENANT,
        conversation,
        transcript,
    )
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
            Arc::new(
                FakeConfig::new()
                    .with(CREDENTIAL_REF, CREDENTIAL_SECRET)
                    .with(CONVERSATION_URL_KEY, CONVERSATION_URL),
            ),
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
        None,
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
    events_for(&fixture.db, &fixture.ticket_id)
}

/// One ticket's audit trail, by id — for tests with more than one ticket.
#[must_use]
pub(crate) fn events_for(db: &SqliteDatabase, ticket_id: &str) -> Vec<TicketEvent> {
    pollster::block_on(store::ticket_events(db, ticket_id)).expect("events read")
}

/// Loads any ticket by id — the multi-ticket counterpart of [`ticket`].
#[must_use]
pub(crate) fn ticket_by_id(db: &SqliteDatabase, ticket_id: &str) -> Ticket {
    pollster::block_on(store::load_ticket(db, ticket_id))
        .expect("ticket read")
        .expect("ticket row exists")
}

/// Every `sg_ticket_links` row as `(ticket_id, conversation_id,
/// source_ticket_id)`, in ticket-id order.
#[must_use]
pub(crate) fn ticket_links(db: &SqliteDatabase) -> Vec<(String, String, String)> {
    let stmt = Statement::new(
        "SELECT ticket_id, conversation_id, source_ticket_id FROM sg_ticket_links \
         ORDER BY ticket_id, conversation_id",
    );
    let rows = pollster::block_on(db.query(&stmt)).expect("link rows read");
    rows.rows
        .iter()
        .map(|row| {
            (
                row.get::<String>("ticket_id").expect("ticket_id decodes"),
                row.get::<String>("conversation_id")
                    .expect("conversation_id decodes"),
                row.get::<String>("source_ticket_id")
                    .expect("source_ticket_id decodes"),
            )
        })
        .collect()
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

/// How many rows the escalation outbox holds under one topic. A filing
/// leaves a `follow` row behind (it reschedules itself until the ticket
/// closes), so an assertion about "everything else is finished" wants this
/// rather than the raw [`outbox_count`].
#[must_use]
pub(crate) fn outbox_count_of(fixture: &Fixture, topic: &str) -> usize {
    let stmt = Statement::with_values(
        format!("SELECT topic FROM {OUTBOX_TABLE} WHERE topic = ?"),
        vec![topic.to_owned().into()],
    );
    pollster::block_on(fixture.db.query(&stmt))
        .expect("outbox count read")
        .rows
        .len()
}
