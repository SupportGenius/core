//! Issue #24, part 2: the built-in ticketing routes. A tenant lists and
//! reads the tickets the file stage filed into the module's own ticketing
//! (the ones with no tracker route), closes and reopens one, and another
//! tenant sees none of it. Tenant keys are minted with `tenancy::mint`
//! over the harness's `Signer`, the same call the destination-route tests
//! use.

mod support;

use std::sync::Arc;

use axum::http::{Method, StatusCode};
use cratefield_core::{DbError, MapConfig, Module, ModuleContext, SystemClock, UlidIdGen};
use cratefield_testing::{Dialect, FakeTextModel, TestHarness, TestResponse, request, request_as};
use module_escalation::intake::OUTBOX_TABLE;
use module_escalation::model::{EventKind, Kind, Status, Ticket, TicketEvent};
use module_escalation::store::{self, RouteTarget};
use module_escalation::{Escalation, Intake, Pipeline, TenantDirectory};
use serde_json::{Value, json};

/// The tenant everything is filed for.
const TENANT: &str = "acme";
/// The problem `type` base: every problem is named under the serving
/// venture's own `<public_url>/problems/`, and `TestHarness` serves as
/// `https://test.example`.
const PROBLEMS: &str = "https://test.example/problems/";
/// The route prefix the harness nests the module under.
const TICKETS: &str = "/v1/escalation/tickets";
/// The `sg_routes.credential_ref` value: the Config key the secret lives
/// under, never the secret.
const CREDENTIAL_REF: &str = "ESCALATION_TRACKER_CREDENTIAL";
/// The secret behind [`CREDENTIAL_REF`], seeded into the config.
const CREDENTIAL_SECRET: &str = "token-1";
/// The lead title the drafted local ticket carries.
const LEAD_TITLE: &str = "Acme wants 50 seats";

// ---------------------------------------------------------------------------
// The world

struct World {
    harness: TestHarness,
    config: MapConfig,
    tenants: Arc<FakeTenants>,
}

/// A [`TenantDirectory`] standing in for `module-support`'s tenant and key
/// tables: every tenant is active.
#[derive(Default)]
struct FakeTenants {
    suspended: std::sync::Mutex<std::collections::HashSet<String>>,
}

#[async_trait::async_trait]
impl TenantDirectory for FakeTenants {
    async fn is_active(&self, _ctx: &ModuleContext, tenant_id: &str) -> Result<bool, DbError> {
        Ok(!self
            .suspended
            .lock()
            .expect("tenants lock")
            .contains(tenant_id))
    }

    async fn key_is_live(
        &self,
        _ctx: &ModuleContext,
        _tenant_id: &str,
        _key_id: &str,
    ) -> Result<bool, DbError> {
        Ok(true)
    }
}

/// Builds the harness over the escalation module plus the config its
/// pipeline reads.
fn kit() -> World {
    let tenants = Arc::new(FakeTenants::default());
    let module = Escalation::new().with_tenant_directory(tenants.clone());
    let config = MapConfig::from_pairs([(CREDENTIAL_REF, CREDENTIAL_SECRET)]);
    let for_ports = config.clone();
    let modules: Vec<Box<dyn Module>> = vec![Box::new(module)];
    let harness = TestHarness::with_database_and_ports(modules, Dialect::Sqlite, move |ports| {
        ports.config = Arc::new(for_ports);
    });
    World {
        harness,
        config,
        tenants,
    }
}

/// Mints a tenant key over the harness's own `Signer`.
fn tenant_key(world: &World, tenant: &str) -> String {
    tenancy::mint(&*world.harness.signer, tenant)
        .expect("the harness signer mints a tenant key")
        .key
}

// ---------------------------------------------------------------------------
// Driving the pipeline

/// Seeds one `(tenant, kind)` route with the given target.
fn seed_route(world: &World, kind: Kind, target: &RouteTarget) {
    let stmt = store::put_route_stmt(
        TENANT,
        kind,
        target,
        CREDENTIAL_REF,
        &std::collections::BTreeMap::new(),
        "2026-01-01T00:00:00Z",
    );
    pollster::block_on(world.harness.db.batch_atomic(&[stmt])).expect("route seeds");
}

/// Commits the conversation → first-outbox-row handoff for the tenant.
fn handoff(world: &World, conversation: &str) -> String {
    let intake = Intake::new(OUTBOX_TABLE, Arc::new(SystemClock), Arc::new(UlidIdGen));
    let handoff = intake.handoff(
        TENANT,
        conversation,
        "customer: we are Acme and want to buy 50 seats",
    );
    pollster::block_on(world.harness.db.batch_atomic(&handoff.statements))
        .expect("the caller's batch commits the handoff");
    handoff.ticket_id
}

/// Drains the pipeline over this world until a sweep comes back empty.
fn drive(world: &World, model: FakeTextModel) {
    let pipeline = Pipeline::new(
        world.harness.db.clone(),
        Arc::new(model),
        Arc::new(world.harness.tracker.clone()),
        None,
        Arc::new(world.config.clone()),
        Arc::new(world.harness.clock.clone()),
        Arc::new(UlidIdGen),
        None,
    );
    let mut processed = 0;
    for _ in 0..Pipeline::MAX_SWEEPS {
        let swept = pollster::block_on(pipeline.drain(Pipeline::SWEEP_LIMIT)).expect("a sweep");
        processed += swept;
        if swept == 0 {
            break;
        }
    }
    assert!(processed > 0, "the pipeline processed the escalation");
}

/// Loads any ticket by id out of the harness's database.
fn ticket(world: &World, id: &str) -> Ticket {
    pollster::block_on(store::load_ticket(&*world.harness.db, id))
        .expect("ticket read")
        .expect("ticket row exists")
}

/// One ticket's audit trail, by id.
fn events(world: &World, id: &str) -> Vec<TicketEvent> {
    pollster::block_on(store::ticket_events(&*world.harness.db, id)).expect("events read")
}

/// Files one escalation of `kind` from `conversation` and returns its
/// ticket id.
fn file(world: &World, kind: Kind, conversation: &str) -> String {
    let model = match kind {
        Kind::Defect => support::scripted_model(support::drafted_json(), support::file_judgment()),
        other => support::scripted_model(
            support::drafted_json_kind(
                other,
                LEAD_TITLE,
                &json!({
                    "company": "Acme",
                    "seats": 50,
                    "intent": "wants to buy the enterprise plan",
                }),
            ),
            support::file_judgment(),
        ),
    };
    let id = handoff(world, conversation);
    drive(world, model);
    id
}

// ---------------------------------------------------------------------------
// Sending requests

async fn get(world: &World, path: &str, bearer: &str) -> TestResponse {
    request_as(&world.harness.router, Method::GET, path, bearer, None).await
}

async fn post(world: &World, path: &str, bearer: &str, body: Value) -> TestResponse {
    request_as(
        &world.harness.router,
        Method::POST,
        path,
        bearer,
        Some(&body.to_string()),
    )
    .await
}

// ---------------------------------------------------------------------------
// The boxes

/// A locally filed ticket (`local:<id>`, no route) is listed and readable
/// for its tenant, while a ticket filed to a tracker is not a built-in one
/// and so is absent from the list and a `404` by id.
#[pollster::test]
async fn a_local_ticket_is_listed_and_a_tracker_filed_one_is_not() {
    let world = kit();
    let key = tenant_key(&world, TENANT);
    // A defect routes to an external tracker; a lead has no route at all
    // and files into the module's own ticketing.
    seed_route(
        &world,
        Kind::Defect,
        &RouteTarget::Tracker(support::github_destination()),
    );
    let tracker_id = file(&world, Kind::Defect, "conv-tracker");
    let local_id = file(&world, Kind::Lead, "conv-lead");
    assert_ne!(tracker_id, local_id);

    // The defect reached the tracker — it is not a built-in ticket.
    assert_eq!(
        ticket(&world, &tracker_id).external_id.as_deref(),
        Some("fake-0")
    );

    let list = get(&world, TICKETS, &key).await;
    assert_eq!(list.status, StatusCode::OK, "{}", list.json());
    let items = list.json();
    let items = items.as_array().expect("a JSON array").clone();
    assert_eq!(
        items.len(),
        1,
        "only the built-in ticket is listed: {items:?}"
    );
    assert_eq!(items[0]["id"], json!(local_id));
    assert_eq!(items[0]["kind"], json!("lead"));
    assert_eq!(items[0]["status"], json!("filed"));
    assert_eq!(items[0]["conversation_id"], json!("conv-lead"));
    assert_eq!(items[0]["title"], json!(LEAD_TITLE));
    assert!(items[0]["created_at"].is_string());
    assert!(items[0]["updated_at"].is_string());

    let one = get(&world, &format!("{TICKETS}/{local_id}"), &key).await;
    assert_eq!(one.status, StatusCode::OK, "{}", one.json());
    assert_eq!(one.json()["id"], json!(local_id));

    // The tracker-filed ticket is not a built-in one: indistinguishable
    // from a missing row.
    let other = get(&world, &format!("{TICKETS}/{tracker_id}"), &key).await;
    assert_eq!(other.status, StatusCode::NOT_FOUND);
}

/// A duplicate of a built-in ticket shares the existing ticket's
/// `local:<id>` reference but keeps its own id, so it is *not* itself a
/// built-in ticket: it never appears in the list and its id is a `404`.
/// (Regression: the list/read predicates once matched the bare `local:`
/// prefix, which a duplicate row's copied reference satisfied.)
#[pollster::test]
async fn a_duplicate_of_a_built_in_ticket_is_not_listed_or_readable() {
    let world = kit();
    let key = tenant_key(&world, TENANT);
    // No route and no destination: the defect files into the built-in
    // ticketing.
    let first = file(&world, Kind::Defect, "conv-first");
    assert_eq!(
        ticket(&world, &first).external_id.as_deref(),
        Some(format!("local:{first}").as_str())
    );

    // A second report of the same defect is duplicated against the first,
    // copying its `local:<first>` reference onto the duplicate's row.
    let second = handoff(&world, "conv-second");
    drive(
        &world,
        support::scripted_model(support::drafted_json(), support::duplicate_judgment(&first)),
    );
    let duplicate = ticket(&world, &second);
    assert_eq!(duplicate.status, Status::Duplicate);
    assert_eq!(
        duplicate.external_id,
        ticket(&world, &first).external_id,
        "the duplicate copies the existing ticket's reference"
    );
    assert_ne!(second, first);

    // Only the real built-in ticket is listed...
    let list = get(&world, TICKETS, &key).await;
    assert_eq!(list.status, StatusCode::OK, "{}", list.json());
    let items = list.json();
    let items = items.as_array().expect("a JSON array").clone();
    assert_eq!(items.len(), 1, "the duplicate is not listed: {items:?}");
    assert_eq!(items[0]["id"], json!(first));

    // ...and the duplicate's own id is indistinguishable from a missing
    // one.
    let one = get(&world, &format!("{TICKETS}/{second}"), &key).await;
    assert_eq!(
        one.status,
        StatusCode::NOT_FOUND,
        "a duplicate is not a built-in ticket: {}",
        one.json()
    );
}

/// Every read is tenant-scoped: another tenant lists nothing, and gets a
/// `404` on both reading and closing the ticket.
#[pollster::test]
async fn another_tenant_sees_nothing_and_cannot_touch_the_ticket() {
    let world = kit();
    let acme = tenant_key(&world, TENANT);
    let globex = tenant_key(&world, "globex");
    let local_id = file(&world, Kind::Lead, "conv-lead");

    let list = get(&world, TICKETS, &globex).await;
    assert_eq!(list.status, StatusCode::OK);
    assert_eq!(list.json(), json!([]), "globex has no built-in tickets");

    let one = get(&world, &format!("{TICKETS}/{local_id}"), &globex).await;
    assert_eq!(one.status, StatusCode::NOT_FOUND, "another tenant's id");

    let closed = post(
        &world,
        &format!("{TICKETS}/{local_id}/status"),
        &globex,
        json!({ "status": "closed" }),
    )
    .await;
    assert_eq!(
        closed.status,
        StatusCode::NOT_FOUND,
        "cannot close it either"
    );

    // The ticket is untouched for its owner.
    let seen = get(&world, &format!("{TICKETS}/{local_id}"), &acme).await;
    assert_eq!(seen.json()["status"], json!("filed"));
}

/// A closed ticket can be reopened, a repeated close is a `409`, a status
/// this route does not own is a `400`, and the transition lands on the
/// audit trail.
#[pollster::test]
async fn closing_and_reopening_a_built_in_ticket() {
    let world = kit();
    let key = tenant_key(&world, TENANT);
    let id = file(&world, Kind::Lead, "conv-lead");

    let closed = post(
        &world,
        &format!("{TICKETS}/{id}/status"),
        &key,
        json!({ "status": "closed" }),
    )
    .await;
    assert_eq!(closed.status, StatusCode::OK, "{}", closed.json());
    assert_eq!(closed.json()["status"], json!("closed"));

    // The write is durable: a later read shows it.
    let seen = get(&world, &format!("{TICKETS}/{id}"), &key).await;
    assert_eq!(seen.json()["status"], json!("closed"));
    // ...and so is the audit event.
    let trail = events(&world, &id);
    assert!(
        trail.iter().any(|event| event.kind == EventKind::Closed),
        "the close is on the trail: {trail:?}"
    );

    // Closing an already-closed ticket is a conflict, not a no-op.
    let again = post(
        &world,
        &format!("{TICKETS}/{id}/status"),
        &key,
        json!({ "status": "closed" }),
    )
    .await;
    assert_eq!(again.status, StatusCode::CONFLICT, "{}", again.json());
    assert_eq!(
        again.json()["type"],
        json!(format!("{PROBLEMS}escalation-ticket-status-conflict"))
    );

    // Reopening it (`filed`) is allowed and recorded.
    let reopened = post(
        &world,
        &format!("{TICKETS}/{id}/status"),
        &key,
        json!({ "status": "filed" }),
    )
    .await;
    assert_eq!(reopened.status, StatusCode::OK, "{}", reopened.json());
    assert_eq!(reopened.json()["status"], json!("filed"));

    // A status this route does not own, and an unknown one, are both 400s.
    for bad in ["dead_letter", "nonsense"] {
        let reply = post(
            &world,
            &format!("{TICKETS}/{id}/status"),
            &key,
            json!({ "status": bad }),
        )
        .await;
        assert_eq!(
            reply.status,
            StatusCode::BAD_REQUEST,
            "{bad}: {}",
            reply.json()
        );
    }

    // The `status` filter narrows the list.
    let open = get(&world, &format!("{TICKETS}?status=filed"), &key).await;
    assert_eq!(open.json().as_array().expect("array").len(), 1);
    let closed = get(&world, &format!("{TICKETS}?status=closed"), &key).await;
    assert_eq!(closed.json(), json!([]), "nothing is closed right now");
}

/// No bearer is refused, an invalid one is refused, and a suspended tenant
/// is refused with the same indistinguishable `401` as a bad key.
#[pollster::test]
async fn the_routes_require_a_valid_tenant_key() {
    let world = kit();

    let none = request(&world.harness.router, Method::GET, TICKETS, None).await;
    assert_eq!(none.status, StatusCode::UNAUTHORIZED);

    let bogus = request_as(
        &world.harness.router,
        Method::GET,
        TICKETS,
        "sg_bogus.not.a.key",
        None,
    )
    .await;
    assert_eq!(bogus.status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        bogus.json()["type"],
        json!(format!("{PROBLEMS}escalation-unauthorized"))
    );

    let post_none = request(
        &world.harness.router,
        Method::POST,
        &format!("{TICKETS}/whatever/status"),
        Some(&json!({ "status": "closed" }).to_string()),
    )
    .await;
    assert_eq!(post_none.status, StatusCode::UNAUTHORIZED);

    // A suspended tenant's own key is refused identically.
    let key = tenant_key(&world, TENANT);
    world
        .tenants
        .suspended
        .lock()
        .expect("tenants lock")
        .insert(TENANT.to_owned());
    let suspended = get(&world, TICKETS, &key).await;
    assert_eq!(suspended.status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        suspended.json()["type"],
        json!(format!("{PROBLEMS}escalation-unauthorized"))
    );
}

/// The view never carries the transcript or the tracker reference — only
/// the fields a support engineer reads.
#[pollster::test]
async fn the_view_carries_the_readable_fields_only() {
    let world = kit();
    let key = tenant_key(&world, TENANT);
    file(&world, Kind::Lead, "conv-lead");

    let list = get(&world, TICKETS, &key).await;
    let item = list.json()[0].clone();
    let object = item.as_object().expect("a JSON object");
    let mut fields: Vec<&str> = object.keys().map(String::as_str).collect();
    fields.sort_unstable();
    assert_eq!(
        fields,
        [
            "body_markdown",
            "conversation_id",
            "created_at",
            "id",
            "kind",
            "severity",
            "status",
            "title",
            "updated_at",
        ]
    );
    assert!(
        object.get("transcript").is_none(),
        "the transcript stays off the wire"
    );
    assert!(
        object.get("external_id").is_none(),
        "the tracker reference stays off the wire"
    );

    // The status is the stored wire form.
    let id = list.json()[0]["id"].as_str().expect("an id").to_owned();
    assert_eq!(ticket(&world, &id).status, Status::Filed);
}
