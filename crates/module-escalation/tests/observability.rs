//! Issue #39 acceptance: what the escalation pipeline says about itself.
//! Two surfaces, tested together because they answer one question from two
//! directions — `GET /v1/escalation/admin/health` for "is work piling up
//! and did the last drain finish", and the `model.call` / `pipeline.stage`
//! spans for "what did each unit of work do".
//!
//! Spans are captured through a [`tracing_subscriber`] layer rather than a
//! `fmt` one: the assertions are about *fields*, and a `fmt` layer would
//! have to be parsed back out of a string to check that `tok_in` survived
//! redaction — the one thing this file most needs to state.

mod support;

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use axum::http::{Method, StatusCode};
use cratefield_core::{Clock, IdGen, MapConfig, Module, Statement, UlidIdGen, subject_hash};
use cratefield_testing::{Dialect, TestHarness, TestResponse, request_as};
use serde_json::Value;
use tracing::span::{Attributes, Record};
use tracing::{Id, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;

use module_escalation::intake::OUTBOX_TABLE;
use module_escalation::model::{Kind, Stage, Status};
use module_escalation::store;
use module_escalation::testing::SettableClock;
use module_escalation::{Escalation, Pipeline};
use support::{EPOCH, TENANT};

/// The admin token every health request in this file presents.
const ADMIN_TOKEN: &str = "test-admin-token-0123456789abcdef";
const HEALTH: &str = "/v1/escalation/admin/health";
/// The tenant whose key is offered where the admin token belongs: a real,
/// well-formed tenant key that is simply not the admin credential.
const TENANT_KEY: &str = "sg_test.tenant-key-not-an-admin-token";

// ---------------------------------------------------------------------------
// The world

/// A harness over the escalation module with a movable clock, so an age is
/// exact rather than drifting between the row written and the request.
struct World {
    harness: TestHarness,
    clock: Arc<SettableClock>,
}

impl World {
    fn new() -> Self {
        let clock = Arc::new(SettableClock::at_unix(EPOCH));
        let for_ports = clock.clone();
        let config = MapConfig::from_pairs([("ADMIN_TOKEN", ADMIN_TOKEN.to_owned())]);
        let for_ports_config = config.clone();
        let modules: Vec<Box<dyn Module>> = vec![Box::new(Escalation::new())];
        let harness =
            TestHarness::with_database_and_ports(modules, Dialect::Sqlite, move |ports| {
                ports.config = Arc::new(for_ports_config);
                ports.clock = Some(for_ports);
            });
        Self { harness, clock }
    }

    /// Commits statements through the same database the router reads.
    fn commit(&self, statements: &[Statement]) {
        pollster::block_on(self.harness.db.batch_atomic(statements)).expect("the batch commits");
    }

    /// The current instant, RFC 3339, from the same clock the route reads.
    fn now(&self) -> String {
        Clock::now(self.clock.as_ref())
            .format(&time::format_description::well_known::Rfc3339)
            .expect("a whole-second instant formats")
    }

    /// Queues one outbox row for `stage`, due at `now - age_secs`.
    fn enqueue_due(&self, stage: Stage, ticket_id: &str, age_secs: i64) {
        let due = self.clock.now() - time::Duration::seconds(age_secs);
        self.commit(&[store::enqueue_stage_stmt(
            &cratefield_core::Outbox::new(OUTBOX_TABLE),
            &IdGen::ulid(&UlidIdGen),
            ticket_id,
            TENANT,
            stage,
            None,
            &format_at(due),
        )]);
    }

    /// Holds every row of `topic` on a lease the next drainer's clock would
    /// still see as live.
    fn lease_topic(&self, topic: &str, for_secs: i64) {
        let until = format_at(self.clock.now() + time::Duration::seconds(for_secs));
        self.commit(&[Statement::with_values(
            format!("UPDATE {OUTBOX_TABLE} SET locked_until = ? WHERE topic = ?"),
            vec![until.into(), topic.into()],
        )]);
    }

    async fn health(&self, bearer: &str) -> TestResponse {
        request_as(&self.harness.router, Method::GET, HEALTH, bearer, None).await
    }

    /// A health document, asserting the request was authorised first.
    async fn health_ok(&self) -> Value {
        let response = self.health(ADMIN_TOKEN).await;
        assert_eq!(
            response.status,
            StatusCode::OK,
            "admin token reads the health"
        );
        response.json()
    }
}

// ---------------------------------------------------------------------------
// The health document

#[pollster::test]
async fn the_health_route_admits_only_the_admin_token() {
    let world = World::new();
    assert_eq!(
        world.health("").await.status,
        StatusCode::UNAUTHORIZED,
        "no credential is not a credential"
    );
    // A tenant key is a real, correctly signed credential — it is simply not
    // the operator's. Health is every tenant's queue depth at once, so it is
    // `403`, never a narrower answer a tenant could act on.
    assert_eq!(world.health(TENANT_KEY).await.status, StatusCode::FORBIDDEN);
    assert_eq!(
        world.health(ADMIN_TOKEN).await.status,
        StatusCode::OK,
        "and the admin token gets the document"
    );
}

#[pollster::test]
async fn the_health_document_lists_every_stage_and_names_what_is_stuck() {
    let world = World::new();
    let empty = topics(&world.health_ok().await);
    assert_eq!(
        empty
            .iter()
            .map(|t| t["topic"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["draft", "judge", "file", "notify", "follow"],
        "all five stages, in pipeline order, whatever the queue says"
    );
    for topic in &empty {
        assert_eq!(topic["depth"], 0, "an empty queue reads as zero");
        assert_eq!(topic["due"], 0);
        assert!(
            topic["oldest_due_age_secs"].is_null(),
            "nothing due means no age to report"
        );
    }

    // Two due draft rows and a follow row not yet due: the follow row
    // counts towards depth but not `due`, or the chart says a stage is
    // busy when it is merely scheduled.
    world.enqueue_due(Stage::Draft, "01JTICKET", 900);
    world.enqueue_due(Stage::Draft, "01JTICKET", 120);
    world.commit(&[store::outbox_reschedule_stmt(
        OUTBOX_TABLE,
        "01JNOTDUE",
        &format_at(world.clock.now() + time::Duration::hours(1)),
    )]);

    let draft = &topics(&world.health_ok().await)[0];
    assert_eq!(draft["topic"], "draft");
    assert_eq!(draft["depth"], 2, "both draft rows are queued");
    assert_eq!(draft["due"], 2, "both are due now");
    assert_eq!(
        draft["oldest_due_age_secs"], 900,
        "the oldest of the two, exactly"
    );
    assert_eq!(
        topics(&world.health_ok().await)[4]["due"],
        0,
        "a follow row scheduled for later is depth, not work waiting"
    );

    // Move the clock on: an age that grew by exactly the elapsed time. This
    // is what proves the age comes from the module's clock.
    world.clock.advance_seconds(60);
    assert_eq!(
        topics(&world.health_ok().await)[0]["oldest_due_age_secs"],
        960,
        "a minute later the oldest row is a minute older"
    );

    // Another drainer's lease means the next sweep will not claim the row,
    // so counting it as due would send an operator chasing a queue that is
    // already being drained.
    world.lease_topic("draft", 300);
    let leased = topics(&world.health_ok().await)[0].clone();
    assert_eq!(leased["depth"], 2, "the rows are still queued");
    assert_eq!(leased["due"], 0, "but another drainer holds them");
    assert!(leased["oldest_due_age_secs"].is_null());
}

#[pollster::test]
async fn a_dead_lettered_ticket_shows_up_in_the_twenty_four_hour_count() {
    let world = World::new();
    seed_ticket(&world, "01JRECENT");
    seed_ticket(&world, "01JOLD");
    // One inside the window, one a day and a half before it: the count is a
    // window, not a total.
    dead_letter(&world, "01JRECENT", 60);
    dead_letter(&world, "01JOLD", 36 * 3600);

    assert_eq!(
        world.health_ok().await["dead_letters_24h"],
        1,
        "only the ticket inside the last 24 hours counts"
    );
}

#[pollster::test]
async fn the_last_drain_stamp_appears_after_a_drain_and_ages_with_the_clock() {
    let world = World::new();
    let before = world.health_ok().await;
    assert!(
        before["last_drain_ok_at"].is_null(),
        "a deployment whose cron has not fired yet says so rather than guessing"
    );
    assert!(before["last_drain_age_secs"].is_null());

    // A model that answers nothing usable: the drain still completes,
    // because a stage failure is recorded durably rather than failing the
    // sweep — exactly the case the stamp must cover.
    let pipeline = pipeline(
        &world,
        cratefield_testing::FakeTextModel::new(cratefield_testing::TextModelMode::Transient {
            retry_after: None,
        }),
    );
    world.enqueue_due(Stage::Draft, "01JTICKET", 60);
    pollster::block_on(pipeline.drain(Pipeline::SWEEP_LIMIT)).expect("the sweep completes");

    let after = world.health_ok().await;
    assert_eq!(
        after["last_drain_ok_at"].as_str(),
        Some(world.now().as_str()),
        "the stamp is the instant the drain finished"
    );
    assert_eq!(after["last_drain_age_secs"], 0);

    world.clock.advance_seconds(300);
    assert_eq!(
        world.health_ok().await["last_drain_age_secs"],
        300,
        "a stale drain is the thing an operator is looking for, so its age is reported"
    );
}

// ---------------------------------------------------------------------------
// The spans

#[test]
fn one_model_call_span_per_call_and_one_stage_span_per_record() {
    let world = World::new();
    seed_ticket(&world, "01JTICKET");
    world.enqueue_due(Stage::Draft, "01JTICKET", 0);
    let log = capture(|| {
        support::drain_all(&pipeline(&world, support::happy_model()));
    });

    // The happy path is two model calls: the fast drafter and the strong
    // judge. The count is the assertion — a second span per call would
    // double-count every cost figure in a dashboard built on these names.
    let calls: Vec<&SpanRecord> = log.iter().filter(|s| s.name == "model.call").collect();
    assert_eq!(calls.len(), 2, "one model.call span per model call");
    assert_eq!(calls[0].fields["tier"], "fast");
    assert_eq!(calls[1].fields["tier"], "strong");
    assert!(
        calls.iter().all(|call| call.fields["outcome"] == "ok"),
        "both calls answered"
    );
    assert!(
        calls
            .iter()
            .all(|call| call.fields.contains_key("latency_ms")),
        "latency_ms is recorded before the span closes"
    );
    assert!(
        calls
            .iter()
            .all(|call| call.fields.contains_key("tok_in") && call.fields.contains_key("tok_out")),
        "the token counts are recorded; they are named tok_in/tok_out because \
         `tokens_in` would be redacted by the sink's substring rule"
    );
    assert!(
        calls
            .iter()
            .all(|call| call.fields["tenant"] == subject_hash(TENANT)
                && call.fields["tenant"] != TENANT),
        "the tenant is a pseudonym, never the raw id"
    );

    let stage = log
        .iter()
        .find(|s| s.name == "pipeline.stage")
        .expect("a pipeline.stage span per processed record");
    assert_eq!(stage.fields["stage"], "draft");
    assert_eq!(
        stage.fields["attempt"], "1",
        "attempt is 1-based: the first try is attempt 1"
    );
    assert_eq!(stage.fields["tenant"], subject_hash(TENANT));
    assert!(
        [
            "done",
            "retry",
            "dead_letter",
            "skipped",
            "rescheduled",
            "aborted"
        ]
        .contains(&stage.fields["result"].as_str()),
        "result is one of the settled outcomes, got {:?}",
        stage.fields["result"]
    );
}

#[test]
fn a_failed_stage_settles_its_span_with_the_outcome_it_took() {
    let world = World::new();
    seed_ticket(&world, "01JTICKET");
    world.enqueue_due(Stage::Draft, "01JTICKET", 0);
    let log = capture(|| {
        let pipeline = pipeline(
            &world,
            cratefield_testing::FakeTextModel::new(cratefield_testing::TextModelMode::Transient {
                retry_after: None,
            }),
        );
        pollster::block_on(pipeline.drain(Pipeline::SWEEP_LIMIT)).expect("the sweep completes");
    });

    let stage = log
        .iter()
        .find(|s| s.name == "pipeline.stage")
        .expect("a pipeline.stage span");
    assert_eq!(
        stage.fields["result"], "retry",
        "a retryable failure with budget left retries, and says so"
    );
    assert_eq!(
        log.iter()
            .find(|s| s.name == "model.call")
            .expect("the model call is still reported")
            .fields["outcome"],
        "error",
        "the provider failing is the call's outcome, whatever the row then did"
    );
}

// ---------------------------------------------------------------------------
// Fixtures

fn pipeline(world: &World, model: cratefield_testing::FakeTextModel) -> Pipeline {
    Pipeline::new(
        world.harness.db.clone(),
        Arc::new(model),
        Arc::new(cratefield_testing::FakeTracker::new(
            cratefield_testing::TrackerMode::FileOk,
        )),
        None,
        Arc::new(module_escalation::testing::FakeConfig::new()),
        world.clock.clone(),
        Arc::new(UlidIdGen),
        None,
    )
}

fn topics(document: &Value) -> Vec<Value> {
    document["topics"]
        .as_array()
        .expect("topics is an array")
        .clone()
}

fn format_at(at: time::OffsetDateTime) -> String {
    at.format(&time::format_description::well_known::Rfc3339)
        .expect("a whole-second instant formats")
}

/// A ticket in its intake state, so a test has a row to move rather than a
/// table it has to fill by hand.
fn seed_ticket(world: &World, id: &str) {
    let ticket = module_escalation::model::Ticket {
        id: id.to_owned(),
        tenant_id: TENANT.to_owned(),
        conversation_id: id.to_owned(),
        status: Status::Intake,
        stage: Stage::Draft,
        transcript: support::TRANSCRIPT.to_owned(),
        kind: Kind::Defect,
        title: Some(String::new()),
        body_markdown: None,
        severity: None,
        environment: None,
        verdict: None,
        judge_reasons: Some(Vec::new()),
        tracker_state: None,
        customer_question: None,
        external_id: None,
        external_url: None,
        match_count: 0,
        created_at: format_at(world.clock.now()),
        updated_at: format_at(world.clock.now()),
    };
    world.commit(&[store::insert_ticket_stmt(&ticket)]);
}

/// Parks the ticket for a human `age_secs` ago — the same write the file
/// stage's exhausted branch makes, through the same statement.
fn dead_letter(world: &World, ticket_id: &str, age_secs: i64) {
    let at = world.clock.now() - time::Duration::seconds(age_secs);
    world.commit(&[store::update_ticket_status_stmt(
        ticket_id,
        Status::DeadLetter,
        &format_at(at),
    )]);
}

// ---------------------------------------------------------------------------
// Capturing spans and their fields

/// One span and the fields it carried by the time it closed.
#[derive(Debug, Clone)]
struct SpanRecord {
    name: String,
    fields: BTreeMap<String, String>,
}

/// Runs `body` with the capturing layer installed as the thread's default
/// subscriber and returns every span it closed. Thread-local on purpose:
/// `with_default` scopes to this thread, so parallel tests cannot see each
/// other's spans.
fn capture(body: impl FnOnce()) -> Vec<SpanRecord> {
    let layer = Capture::default();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer.clone()));
    tracing::dispatcher::with_default(&dispatch, body);
    layer
        .closed
        .lock()
        .expect("the capture lock is not poisoned")
        .clone()
}

/// Keeps each open span's fields until it closes, then files the finished
/// record. Keyed by span id, which is unique within one dispatch.
#[derive(Clone, Default)]
struct Capture {
    open: Arc<Mutex<HashMap<u64, SpanRecord>>>,
    closed: Arc<Mutex<Vec<SpanRecord>>>,
}

impl<S: Subscriber> Layer<S> for Capture {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, _ctx: Context<'_, S>) {
        let mut fields = BTreeMap::new();
        attrs.record(&mut FieldVisitor(&mut fields));
        self.open.lock().expect("unpoisoned").insert(
            id.into_u64(),
            SpanRecord {
                name: attrs.metadata().name().to_owned(),
                fields,
            },
        );
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, _ctx: Context<'_, S>) {
        if let Some(record) = self
            .open
            .lock()
            .expect("unpoisoned")
            .get_mut(&id.into_u64())
        {
            values.record(&mut FieldVisitor(&mut record.fields));
        }
    }

    fn on_close(&self, id: Id, _ctx: Context<'_, S>) {
        if let Some(record) = self.open.lock().expect("unpoisoned").remove(&id.into_u64()) {
            self.closed.lock().expect("unpoisoned").push(record);
        }
    }
}

/// Writes every field it is handed into a map, whatever its type — the
/// assertion is on the field's name and value, not its Rust type.
struct FieldVisitor<'a>(&'a mut BTreeMap<String, String>);

impl tracing::field::Visit for FieldVisitor<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.0.insert(field.name().to_owned(), value.to_string());
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0.insert(field.name().to_owned(), value.to_string());
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.0.insert(field.name().to_owned(), value.to_string());
    }
}
