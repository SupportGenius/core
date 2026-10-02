//! Issue #25: every escalation outcome that changes a ticket's life —
//! filed, dead-lettered, parked for a human — publishes the matching
//! `escalation.*` event through `cratefield-module-webhooks`, in the **same
//! atomic batch** as the outcome's audit row.
//!
//! The pipeline runs over a `TestHarness` (both modules' migrations applied,
//! so the webhook tables exist); the webhooks module is then drained with an
//! `HttpClient` that records the whole request — headers included, because
//! the signature lives in one and `FakeHttpClient` records only
//! method/uri/body (see `cratefield_testing::fakes`).

mod support;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use cratefield_core::{
    Clock as _, Config, Database, Destination, HttpClient, HttpError, MapConfig, ModuleContext,
    PersonalDataCatalog, Ports, ScheduledBudget, TemplateRegistry, UlidIdGen, Venture,
};
use cratefield_module_webhooks::{EVENT_TYPE_HEADER, SIGNATURE_HEADER, Webhooks};
use cratefield_testing::{FakeTextModel, FakeTracker, TestHarness, TrackerMode};
use hmac::{Hmac, KeyInit, Mac};
use http::{Request, Response};
use module_escalation::intake::OUTBOX_TABLE;
use module_escalation::model::{EventKind, Status, webhook_events};
use module_escalation::store;
use module_escalation::testing::SettableClock;
use module_escalation::{Escalation, Intake, Pipeline, RetryPolicy};
use sha2::Sha256;

const ENDPOINT_URL: &str = "https://hooks.acme.test/escalations";

// ---------------------------------------------------------------------------
// An HttpClient that records the whole request and always answers 200

#[derive(Clone, Default)]
struct CaptureClient {
    inner: Arc<Mutex<Vec<Captured>>>,
}

#[derive(Clone, Debug)]
struct Captured {
    uri: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl CaptureClient {
    fn captured(&self) -> Vec<Captured> {
        self.inner.lock().expect("http lock").clone()
    }
}

#[async_trait]
impl HttpClient for CaptureClient {
    async fn send(&self, request: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
        let (parts, body) = request.into_parts();
        self.inner.lock().expect("http lock").push(Captured {
            uri: parts.uri.to_string(),
            headers: parts
                .headers
                .iter()
                .map(|(name, value)| {
                    (
                        name.as_str().to_owned(),
                        value.to_str().unwrap_or_default().to_owned(),
                    )
                })
                .collect(),
            body: body.to_vec(),
        });
        Ok(Response::builder()
            .status(200)
            .body(Bytes::new())
            .expect("a bare 200 is a valid response"))
    }
}

/// The value of `name` on a captured request; panics naming it if absent.
fn header<'a>(captured: &'a Captured, name: &str) -> &'a str {
    match captured
        .headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
    {
        Some((_, value)) => value.as_str(),
        None => panic!("no {name} header on the delivery"),
    }
}

/// The captured request's body as the JSON envelope.
fn envelope(captured: &Captured) -> serde_json::Value {
    serde_json::from_slice(&captured.body).expect("the body is the JSON envelope")
}

/// Lowercase hex, the encoding the signature header carries.
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Recomputes the delivered signature the way a receiver does: HMAC-SHA256
/// over `{t}.{body}`, keyed by the endpoint's secret.
fn expected_signature(secret: &str, captured: &Captured) -> String {
    let signature = header(captured, SIGNATURE_HEADER);
    let (t, _) = signature
        .split_once(",v1=")
        .map(|(t, v1)| (t.trim_start_matches("t="), v1))
        .expect("the Cratefield-Signature header shape");
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("any key length");
    mac.update(t.as_bytes());
    mac.update(b".");
    mac.update(&captured.body);
    hex(&mac.finalize().into_bytes())
}

/// The `v1` half of a delivery's signature header, the part to verify.
fn delivered_signature(captured: &Captured) -> &str {
    header(captured, SIGNATURE_HEADER)
        .split_once(",v1=")
        .expect("the header shape")
        .1
}

// ---------------------------------------------------------------------------
// The kit

struct Kit {
    harness: TestHarness,
    webhooks: Webhooks,
    http: CaptureClient,
    clock: Arc<SettableClock>,
    config: Arc<dyn Config>,
    db: Arc<dyn Database>,
    /// The one-time signing secret `create_endpoint` returned.
    secret: String,
    ticket_id: String,
}

/// A world with both modules' migrations applied: the escalation pipeline
/// and the webhook outbox live in one database, exactly as a composed
/// venture wires them. The endpoint is registered for `filters` before
/// anything runs; the tenant's destination and the conversation handoff are
/// seeded the way the calling support module commits them.
fn kit(filters: &[&str]) -> Kit {
    kit_for(&support::github_destination(), filters)
}

/// [`kit`] filed into a different tracker — a `Destination::Webhook`, say.
fn kit_for(destination: &Destination, filters: &[&str]) -> Kit {
    let harness = TestHarness::new(vec![Box::new(Escalation::new()), Box::new(Webhooks::new())]);
    let db = Arc::clone(&harness.db);
    let clock = Arc::new(SettableClock::at_unix(support::EPOCH));
    let config: Arc<dyn Config> = Arc::new(MapConfig::from_pairs([
        (support::CREDENTIAL_REF, support::CREDENTIAL_SECRET),
        (support::CONVERSATION_URL_KEY, support::CONVERSATION_URL),
    ]));
    let at = support::format_at(clock.now());

    let seed =
        store::put_destination_stmt(support::TENANT, destination, support::CREDENTIAL_REF, &at);
    pollster::block_on(harness.db.batch_atomic(&[seed])).expect("destination seeds");

    let webhooks = Webhooks::new();
    let endpoint = pollster::block_on(webhooks.create_endpoint(
        &*db,
        support::TENANT,
        ENDPOINT_URL,
        filters,
        &at,
    ))
    .expect("the endpoint registers");

    let intake = Intake::new(OUTBOX_TABLE, clock.clone(), Arc::new(UlidIdGen));
    let handoff = intake.handoff(support::TENANT, support::CONVERSATION, support::TRANSCRIPT);
    pollster::block_on(harness.db.batch_atomic(&handoff.statements)).expect("handoff commits");

    Kit {
        harness,
        webhooks,
        http: CaptureClient::default(),
        clock,
        config,
        db,
        secret: endpoint.secret,
        ticket_id: handoff.ticket_id,
    }
}

impl Kit {
    /// The pipeline over this kit, wiring the webhook publisher exactly as
    /// `Escalation::scheduled` does.
    fn pipeline(&self, model: FakeTextModel, tracker: FakeTracker) -> Pipeline {
        Pipeline::new(
            Arc::clone(&self.db),
            Arc::new(model),
            Arc::new(tracker),
            None,
            Arc::clone(&self.config),
            self.clock.clone(),
            Arc::new(UlidIdGen),
            None,
        )
        .with_retry_policy(RetryPolicy::new())
        .with_webhooks(self.webhooks.clone())
    }

    /// Drains the webhooks outbox with the recording client, over a context
    /// carrying this kit's database, clock and client.
    fn drain_webhooks(&self) {
        let mut ports = Ports::with_config(Arc::clone(&self.config));
        ports.db = Some(Arc::clone(&self.db));
        ports.clock = Some(self.clock.clone());
        ports.http = Some(Arc::new(self.http.clone()));
        let ctx = ModuleContext {
            ports,
            config: Arc::clone(&self.config),
            events: self.harness.harness.events().clone(),
            templates: Arc::new(TemplateRegistry::default()),
            venture: Arc::new(Venture::new("test-venture", "test.example")),
            unprotected_writes_accepted: false,
            ui_mounted: false,
            personal_data: Arc::new(PersonalDataCatalog::default()),
            scheduled: Arc::new(ScheduledBudget::unbounded()),
        };
        pollster::block_on(self.webhooks.drain_with(&ctx)).expect("the webhook drain runs");
    }

    fn ticket_status(&self) -> Status {
        pollster::block_on(store::load_ticket(&*self.db, &self.ticket_id))
            .expect("ticket read")
            .expect("the ticket row exists")
            .status
    }
}

// ---------------------------------------------------------------------------

/// The migration landmine: a venture that mounts escalation *without*
/// `Webhooks` has none of its tables, so the publish read finds no
/// `webhooks_endpoints`. Wiring the publisher exactly as
/// `Escalation::scheduled` does must not break the filing — the fan-out is
/// skipped (with a warning), not fatal. The `Fixture`'s database carries
/// only escalation's migration, which is that venture's schema.
#[pollster::test]
async fn a_filing_without_the_webhooks_module_still_files() {
    let fixture = support::fixture(
        support::happy_model(),
        FakeTracker::new(TrackerMode::FileOk),
    );
    let pipeline = fixture.pipeline().with_webhooks(Webhooks::new());

    support::drain_all(&pipeline);

    assert_eq!(support::ticket(&fixture).status, Status::Filed);
}

#[pollster::test]
async fn a_filed_escalation_delivers_a_signed_escalation_filed_event() {
    let kit = kit(&[webhook_events::ESCALATION_FILED]);
    let pipeline = kit.pipeline(
        support::happy_model(),
        FakeTracker::new(TrackerMode::FileOk),
    );
    support::drain_all(&pipeline);
    assert_eq!(kit.ticket_status(), Status::Filed);

    kit.drain_webhooks();
    let captured = kit.http.captured();
    assert_eq!(captured.len(), 1, "one filing, one delivery: {captured:?}");
    let delivery = &captured[0];
    assert_eq!(delivery.uri, ENDPOINT_URL);

    // The delivery names the event twice over: header and envelope, so a
    // receiver can filter and dedupe without parsing the body.
    assert_eq!(
        header(delivery, EVENT_TYPE_HEADER),
        webhook_events::ESCALATION_FILED
    );
    let data = &envelope(delivery)["data"];
    assert_eq!(envelope(delivery)["subject"], support::TENANT);
    assert_eq!(data["ticket_id"], kit.ticket_id);
    // The destination's *kind* is reported; the destination itself (a
    // webhook URL is credential material) never is.
    assert_eq!(data["destination_kind"], "github");
    assert_eq!(data["external_id"], support::FILED_EXTERNAL_ID);
    assert_eq!(data["url"], support::FILED_EXTERNAL_URL);

    // The signature verifies independently, keyed by the secret
    // `create_endpoint` returned once; a tampered body would not.
    assert_eq!(
        expected_signature(&kit.secret, delivery),
        delivered_signature(delivery)
    );
}

/// A webhook destination's URL *is* credential material (issue #23): the
/// `escalation.filed` payload reports the destination's kind and the
/// tracker's external id, never the endpoint the ticket was filed into.
/// GitHub could afford a URL because a public issue URL leaks nothing; a
/// webhook destination cannot, so the same code path must omit it.
#[pollster::test]
async fn a_webhook_destination_never_puts_its_url_in_the_event() {
    let tracker_url = "https://hooks.acme.test/tracker-secret";
    let kit = kit_for(
        &Destination::Webhook {
            url: tracker_url.to_owned(),
        },
        &[webhook_events::ESCALATION_FILED],
    );
    let pipeline = kit.pipeline(
        support::happy_model(),
        FakeTracker::new(TrackerMode::FileOk),
    );
    support::drain_all(&pipeline);

    kit.drain_webhooks();
    let captured = kit.http.captured();
    assert_eq!(captured.len(), 1, "one filing, one delivery: {captured:?}");
    let data = &envelope(&captured[0])["data"];
    assert_eq!(data["destination_kind"], "webhook");
    assert!(
        data.get("url").is_none(),
        "a webhook destination's URL is credential material: {data}"
    );
    // Belt and braces: the URL appears nowhere in the delivered body, not
    // just absent from the field we expect it in.
    assert!(
        !String::from_utf8_lossy(&captured[0].body).contains(tracker_url),
        "the endpoint URL never reaches the receiver's payload: {data}"
    );

    // The audit row makes the same omission: `sg_ticket_events.detail` is
    // read by operators and copied into audit exports, so the endpoint
    // must not be recorded there either.
    let events =
        pollster::block_on(store::ticket_events(&*kit.db, &kit.ticket_id)).expect("events read");
    let filed = events
        .iter()
        .find(|event| event.kind == EventKind::Filed)
        .expect("the filed audit row exists");
    let detail = filed
        .detail
        .as_ref()
        .expect("the filed audit row carries detail");
    assert!(
        detail.get("url").is_none(),
        "the audit detail never records a webhook endpoint: {detail}"
    );
    assert!(
        !detail.to_string().contains(tracker_url),
        "the endpoint URL never reaches the audit trail: {detail}"
    );
}

#[pollster::test]
async fn a_dead_lettered_file_delivers_dead_lettered_and_needs_human() {
    let kit = kit(&[
        webhook_events::ESCALATION_DEAD_LETTERED,
        webhook_events::ESCALATION_NEEDS_HUMAN,
    ]);
    let pipeline = kit.pipeline(
        support::happy_model(),
        FakeTracker::new(TrackerMode::Rejected),
    );
    support::drain_all(&pipeline);
    assert_eq!(kit.ticket_status(), Status::DeadLetter);

    kit.drain_webhooks();
    let captured = kit.http.captured();
    let types: Vec<String> = captured
        .iter()
        .map(|delivery| header(delivery, EVENT_TYPE_HEADER).to_owned())
        .collect();
    assert!(
        types.contains(&webhook_events::ESCALATION_DEAD_LETTERED.to_owned()),
        "a file dead-letter is dead_lettered: {types:?}"
    );
    assert!(
        types.contains(&webhook_events::ESCALATION_NEEDS_HUMAN.to_owned()),
        "a parked ticket needs a human: {types:?}"
    );
    assert!(
        !types.contains(&webhook_events::ESCALATION_FILED.to_owned()),
        "nothing filed, no filed event: {types:?}"
    );

    // The payload names the ticket, the stage and the reason — and is
    // signed by the endpoint's secret, like every other delivery.
    let dead = captured
        .iter()
        .find(|delivery| {
            header(delivery, EVENT_TYPE_HEADER) == webhook_events::ESCALATION_DEAD_LETTERED
        })
        .expect("the dead-letter delivery exists");
    let data = &envelope(dead)["data"];
    assert_eq!(data["ticket_id"], kit.ticket_id);
    assert_eq!(data["stage"], "file");
    assert!(
        data["reason"]
            .as_str()
            .is_some_and(|reason| !reason.is_empty()),
        "the reason is recorded: {data}"
    );
    assert_eq!(
        expected_signature(&kit.secret, dead),
        delivered_signature(dead)
    );
}
