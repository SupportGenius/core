//! Issue #23 acceptance: a tenant (or an operator acting for one) names
//! where its escalations are filed and the token to file there, and the
//! token is stored **encrypted** — never a Worker secret an operator edits
//! by hand, and never anywhere a response, a table or a log line can show
//! it. Tenant keys are minted with `tenancy::mint` over the harness's
//! `Signer`, the same call `module-support`'s admin route makes.

mod support;

use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use axum::http::{Method, StatusCode};
use cratefield_core::{
    Credential, DbError, Destination, Filed, MapConfig, Module, ModuleContext, Row, Statement,
    SystemClock, TicketDraft, TicketState, TicketStatus, Tracker, TrackerError, UlidIdGen,
};
use cratefield_testing::{
    Dialect, FakeTextModel, FiledCall, TempDir, TestHarness, TestResponse, TrackerMode, request,
    request_as,
};
use module_escalation::intake::OUTBOX_TABLE;
use module_escalation::{Escalation, Intake, Pipeline, TenantDirectory};
use serde_json::{Value, json};

const ADMIN_TOKEN: &str = "test-admin-token-0123456789abcdef";
const DESTINATIONS: &str = "/v1/escalation/destinations";
const ADMIN_TENANTS: &str = "/v1/escalation/admin/tenants";
/// The problem `type` base: since cratefield-core 0.7 every problem is named
/// under the serving venture's own `<public_url>/problems/`, and
/// `TestHarness` serves as `https://test.example`.
const PROBLEMS: &str = "https://test.example/problems/";

/// A distinctive credential: if it ever reaches a response, an audit row, a
/// table dump or a log line, the canary test fails loudly.
const CANARY_CREDENTIAL: &str = "ghp-CANARY-credential-9f3a1c7e-do-not-leak";
/// A distinctive webhook URL — the other thing a webhook destination must
/// never keep in the clear.
const CANARY_URL: &str = "https://hooks.canary.test/CANARY-wh-7b2f?token=zz";

// ---------------------------------------------------------------------------
// The world

/// A harness over the escalation module plus the config its ports were
/// built with (the pipeline the end-to-end tests build reads the same keys).
struct World {
    harness: TestHarness,
    config: MapConfig,
    /// The tenant directory the module was composed with: every tenant is
    /// active until a test suspends it.
    tenants: Arc<FakeTenants>,
}

/// A [`TenantDirectory`] standing in for `module-support`'s tenant and key
/// tables: every tenant is active except the ones a test suspends, and
/// every key is on record except the ones a test revokes.
#[derive(Default)]
struct FakeTenants {
    suspended: Mutex<HashSet<String>>,
    revoked_keys: Mutex<HashSet<String>>,
}

impl FakeTenants {
    fn suspend(&self, tenant: &str) {
        self.suspended
            .lock()
            .expect("tenants lock")
            .insert(tenant.to_owned());
    }

    /// Drops the key's row, the way `DELETE /v1/support/keys/{kid}` does.
    fn revoke_key(&self, key_id: &str) {
        self.revoked_keys
            .lock()
            .expect("tenants lock")
            .insert(key_id.to_owned());
    }
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
        key_id: &str,
    ) -> Result<bool, DbError> {
        Ok(!self
            .revoked_keys
            .lock()
            .expect("tenants lock")
            .contains(key_id))
    }
}

/// A throwaway directory holding a development KMS master key.
fn dev_key_dir() -> TempDir {
    TempDir::new("escalation-destinations")
}

/// Builds the world. `extra` config pairs come on top of the admin token;
/// when `key_file` is given, the development KMS is wired through it.
fn kit_with(extra: Vec<(&'static str, String)>, key_file: Option<&TempDir>) -> World {
    let mut pairs: Vec<(&'static str, String)> = vec![("ADMIN_TOKEN", ADMIN_TOKEN.to_owned())];
    if let Some(dir) = key_file {
        let path = dir.join("kek");
        // 32 raw bytes as hex, the form an operator writes.
        std::fs::write(&path, "ab".repeat(32)).expect("the development key file writes");
        pairs.push(("ESCALATION_KMS_KEY_FILE", path.display().to_string()));
        pairs.push(("ENV", "development".to_owned()));
    }
    pairs.extend(extra);

    let tenants = Arc::new(FakeTenants::default());
    let module = Escalation::new().with_tenant_directory(tenants.clone());
    let (harness, config) = harness_for(module, pairs);
    World {
        harness,
        config,
        tenants,
    }
}

/// The harness over `module` with `pairs` as its config.
fn harness_for(module: Escalation, pairs: Vec<(&'static str, String)>) -> (TestHarness, MapConfig) {
    let config = MapConfig::from_pairs(pairs);
    let for_ports = config.clone();
    let modules: Vec<Box<dyn Module>> = vec![Box::new(module)];
    let harness = TestHarness::with_database_and_ports(modules, Dialect::Sqlite, move |ports| {
        ports.config = Arc::new(for_ports);
    });
    (harness, config)
}

/// Mints a tenant key over the harness's own `Signer`.
fn tenant_key(world: &World, tenant: &str) -> String {
    tenancy::mint(&*world.harness.signer, tenant)
        .expect("the harness signer mints a tenant key")
        .key
}

// ---------------------------------------------------------------------------
// Sending requests

async fn put(world: &World, path: &str, bearer: &str, body: Value) -> TestResponse {
    request_as(
        &world.harness.router,
        Method::PUT,
        path,
        bearer,
        Some(&body.to_string()),
    )
    .await
}

async fn get(world: &World, path: &str, bearer: &str) -> TestResponse {
    request_as(&world.harness.router, Method::GET, path, bearer, None).await
}

// ---------------------------------------------------------------------------
// Reading state back

fn count_of(world: &World, sql: &str) -> usize {
    let rows = pollster::block_on(world.harness.db.query(&Statement::new(sql.to_owned())))
        .expect("the count query runs");
    let n = rows
        .first()
        .and_then(|row| row.get::<i64>("n"))
        .expect("an aggregate row");
    usize::try_from(n).expect("a count is non-negative")
}

/// How many live (not soft-deleted) `harness_secrets` rows hold `name`.
/// Deletion is a soft-delete, so the row count alone would not show it.
fn active_secrets(world: &World, name: &str) -> usize {
    count_of(
        world,
        &format!(
            "SELECT COUNT(*) AS n FROM harness_secrets WHERE name = '{name}' \
             AND deleted_at IS NULL"
        ),
    )
}

/// Every table's every cell, flattened — the honest "is this value anywhere
/// on disk" probe. Blobs are read as lossy UTF-8, so a plaintext credential
/// sealed into a ciphertext column would still be caught.
fn database_dump(world: &World) -> String {
    let tables = pollster::block_on(world.harness.db.query(&Statement::new(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            .to_owned(),
    )))
    .expect("the table list reads");
    let mut dump = String::new();
    for table in &tables.rows {
        let name: String = table.get("name").expect("a table name");
        let rows = pollster::block_on(
            world
                .harness
                .db
                .query(&Statement::new(format!("SELECT * FROM \"{name}\""))),
        )
        .expect("the table reads");
        for row in &rows.rows {
            dump.push_str(&name);
            dump.push(':');
            for column in row.column_names() {
                dump.push_str(&cell(row, column));
                dump.push('\u{1f}');
            }
            dump.push('\n');
        }
    }
    dump
}

fn cell(row: &Row, column: &str) -> String {
    if let Some(text) = row.get::<String>(column) {
        return text;
    }
    if let Some(bytes) = row.get::<Vec<u8>>(column) {
        return String::from_utf8_lossy(&bytes).into_owned();
    }
    if let Some(number) = row.get::<i64>(column) {
        return number.to_string();
    }
    String::new()
}

/// Asserts neither canary appears in a response's body or headers.
fn assert_no_canary(label: &str, response: &TestResponse) {
    let body = String::from_utf8_lossy(response.body());
    assert!(
        !body.contains(CANARY_CREDENTIAL),
        "{label}: the credential leaked into the body: {body}"
    );
    assert!(
        !body.contains(CANARY_URL),
        "{label}: the webhook URL leaked into the body: {body}"
    );
    for (name, value) in &response.headers {
        let value = value.to_str().unwrap_or_default();
        assert!(
            !value.contains(CANARY_CREDENTIAL),
            "{label}: the credential leaked into header {name}"
        );
        assert!(
            !value.contains(CANARY_URL),
            "{label}: the webhook URL leaked into header {name}"
        );
    }
}

/// The `FakeTracker`'s credential fingerprint, computed the same way (see
/// `cratefield_testing::fakes`) so a test can name the value the tracker
/// should have seen without ever holding the secret.
fn fingerprint(secret: &str) -> String {
    let mut hasher = std::hash::DefaultHasher::new();
    secret.hash(&mut hasher);
    format!("fp:{:016x}", hasher.finish())
}

// ---------------------------------------------------------------------------
// Forwarded control-event lines

/// `set_error_forwarder` is process-wide and first-wins, so the sink is
/// installed once per test binary and each test asserts only about needles
/// it owns. It carries what core forwards — internal errors mapped to a
/// 500, boot-time control events, and the `cratefield-secrets` audit chain
/// — not arbitrary adapter `tracing` events, which on native have nowhere
/// to go.
mod capture {
    use std::sync::{Mutex, Once};

    static LINES: Mutex<Vec<String>> = Mutex::new(Vec::new());
    static INSTALL: Once = Once::new();

    pub(crate) fn install() {
        INSTALL.call_once(|| {
            cratefield_core::set_error_forwarder(|line| {
                LINES.lock().expect("log lock").push(line.to_owned());
            });
        });
    }

    pub(crate) fn lines() -> Vec<String> {
        LINES.lock().expect("log lock").clone()
    }
}

/// Asserts neither canary appears in any forwarded control-event line.
fn assert_no_canary_in_logs(label: &str) {
    for line in capture::lines() {
        assert!(
            !line.contains(CANARY_CREDENTIAL),
            "{label}: the credential leaked into a log line: {line}"
        );
        assert!(
            !line.contains(CANARY_URL),
            "{label}: the webhook URL leaked into a log line: {line}"
        );
    }
}

// ---------------------------------------------------------------------------
// Driving the pipeline

/// Commits the conversation → first-outbox-row handoff for `tenant`, the
/// way the calling support module would.
fn handoff(world: &World, tenant: &str) {
    let intake = Intake::new(OUTBOX_TABLE, Arc::new(SystemClock), Arc::new(UlidIdGen));
    let handoff = intake.handoff(
        tenant,
        "conv-23",
        "customer: export job returns HTTP 500 for gift-card orders",
    );
    pollster::block_on(world.harness.db.batch_atomic(&handoff.statements))
        .expect("the caller's batch commits the handoff");
}

/// Drains the pipeline over this world until a sweep comes back empty.
fn drive_with(world: &World, model: FakeTextModel, tracker: Arc<dyn Tracker>) {
    let pipeline = Pipeline::new(
        world.harness.db.clone(),
        Arc::new(model),
        tracker,
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

fn drive(world: &World, model: FakeTextModel) {
    drive_with(world, model, Arc::new(world.harness.tracker.clone()));
}

/// A tracker that answers the way the harness's webhook adapter does: the
/// filed ticket's URL is the destination URL itself — the leak the file
/// stage must scrub — and it records what it was handed.
#[derive(Clone, Default)]
struct EchoUrlTracker {
    filed: Arc<Mutex<Vec<FiledCall>>>,
}

impl EchoUrlTracker {
    fn last_filed(&self) -> Option<FiledCall> {
        self.filed.lock().expect("tracker lock").last().cloned()
    }
}

#[async_trait::async_trait]
impl Tracker for EchoUrlTracker {
    async fn file(
        &self,
        dest: &Destination,
        cred: &Credential,
        draft: &TicketDraft,
    ) -> Result<Filed, TrackerError> {
        self.filed.lock().expect("tracker lock").push(FiledCall {
            dest: dest.clone(),
            draft: draft.clone(),
            credential_fingerprint: fingerprint(cred.expose()),
        });
        let url = match dest {
            Destination::Webhook { url } => url.clone(),
            other => format!("https://tracker.test/{}/1", other.kind()),
        };
        Ok(Filed {
            external_id: format!("echo-{}", draft.idempotency_key),
            url,
        })
    }

    async fn status(
        &self,
        _dest: &Destination,
        _cred: &Credential,
        external_id: &str,
    ) -> Result<TicketStatus, TrackerError> {
        Ok(TicketStatus {
            external_id: external_id.to_owned(),
            state: TicketState::Open,
            url: None,
        })
    }
}

// ---------------------------------------------------------------------------
// The boxes

/// A key acts for exactly the tenant it names: two tenants configure
/// different trackers, each reads back its own, and one's delete leaves the
/// other alone. The admin route names its tenant in the path, and a tenant
/// key cannot reach it.
#[pollster::test]
async fn a_key_acts_for_its_own_tenant_and_nothing_else() {
    let dir = dev_key_dir();
    let world = kit_with(vec![], Some(&dir));
    let acme = tenant_key(&world, "acme");
    let globex = tenant_key(&world, "globex");

    let reply = put(
        &world,
        DESTINATIONS,
        &acme,
        json!({
            "destination": { "git_hub": { "owner": "acme", "repo": "api" } },
            "credential": "acme-token",
        }),
    )
    .await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.json());

    let reply = put(
        &world,
        DESTINATIONS,
        &globex,
        json!({
            "destination": { "jira": { "site": "globex", "project": "SUP" } },
            "credential": "globex-token",
        }),
    )
    .await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.json());

    // Each key reads back its own row and only its own.
    let seen = get(&world, DESTINATIONS, &acme).await;
    assert_eq!(
        seen.json()["destination"],
        json!({ "git_hub": { "owner": "acme", "repo": "api" } })
    );
    assert_eq!(seen.json()["credential_set"], json!(true));
    let seen = get(&world, DESTINATIONS, &globex).await;
    assert_eq!(
        seen.json()["destination"],
        json!({ "jira": { "site": "globex", "project": "SUP" } })
    );

    // globex deletes its own row; acme's survives untouched.
    let reply = request_as(
        &world.harness.router,
        Method::DELETE,
        DESTINATIONS,
        &globex,
        None,
    )
    .await;
    assert_eq!(reply.status, StatusCode::NO_CONTENT);
    let seen = get(&world, DESTINATIONS, &globex).await;
    assert_eq!(seen.status, StatusCode::NOT_FOUND);
    let seen = get(&world, DESTINATIONS, &acme).await;
    assert_eq!(
        seen.status,
        StatusCode::OK,
        "acme's row survives globex's delete"
    );
    assert_eq!(
        seen.json()["destination"],
        json!({ "git_hub": { "owner": "acme", "repo": "api" } })
    );

    // The admin route names the tenant in the path, and the tenant's own
    // key then sees what the operator wrote.
    let admin = format!("{ADMIN_TENANTS}/initech/destinations");
    let reply = put(
        &world,
        &admin,
        ADMIN_TOKEN,
        json!({
            "destination": { "linear": { "team": "ENG" } },
            "credential": "initech-token",
        }),
    )
    .await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.json());
    let initech = tenant_key(&world, "initech");
    let seen = get(&world, DESTINATIONS, &initech).await;
    assert_eq!(
        seen.json()["destination"],
        json!({ "linear": { "team": "ENG" } })
    );

    // A tenant key is not an admin token.
    let reply = put(&world, &admin, &initech, json!({})).await;
    assert_eq!(reply.status, StatusCode::FORBIDDEN);

    // And no key at all is a 401, before any body is read.
    let reply = request(&world.harness.router, Method::GET, DESTINATIONS, None).await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
}

/// The canary, end to end: a distinctive credential and webhook URL go in
/// through the route and never come back out — not in a response, not in
/// any table, not in a forwarded log line — while the file stage resolves
/// the real URL and hands the tracker the stored credential. The tracker
/// answers like the harness's webhook adapter, so the file stage has to
/// drop the URL it reports.
#[pollster::test]
async fn the_canary_never_surfaces_and_the_file_stage_files_with_the_stored_credential() {
    capture::install();
    let dir = dev_key_dir();
    let world = kit_with(vec![], Some(&dir));
    let key = tenant_key(&world, "acme");

    let reply = put(
        &world,
        DESTINATIONS,
        &key,
        json!({
            "destination": { "webhook": { "url": CANARY_URL } },
            "credential": CANARY_CREDENTIAL,
        }),
    )
    .await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.json());
    assert_no_canary("PUT response", &reply);
    assert_eq!(
        reply.json()["destination"]["webhook"]["url"],
        json!("secret:escalation.tracker.destination"),
        "the response carries the marker, never the URL"
    );

    let reply = get(&world, DESTINATIONS, &key).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.json()["credential_set"], json!(true));
    assert_no_canary("GET response", &reply);

    // The credential and the webhook URL are two sealed secrets.
    assert_eq!(active_secrets(&world, "escalation.tracker.credential"), 1);
    assert_eq!(active_secrets(&world, "escalation.tracker.destination"), 1);

    // The file stage resolves both back out: the tracker is handed the real
    // webhook URL and the canary credential, and reports the URL back as the
    // filed ticket's URL.
    handoff(&world, "acme");
    let tracker = EchoUrlTracker::default();
    drive_with(&world, support::happy_model(), Arc::new(tracker.clone()));
    let filed = tracker
        .last_filed()
        .expect("the file stage reached the tracker");
    assert_eq!(
        filed.dest,
        Destination::Webhook {
            url: CANARY_URL.to_owned(),
        },
        "the file stage resolved the real webhook URL out of the encrypted store"
    );
    assert_eq!(
        filed.credential_fingerprint,
        fingerprint(CANARY_CREDENTIAL),
        "the credential the route stored is the one the tracker was handed"
    );
    // The PUT's probe carried the same credential, once.
    let statused = world.harness.tracker.statused();
    assert_eq!(statused.len(), 1, "one probe, and the file went elsewhere");
    assert_eq!(
        statused[0].credential_fingerprint,
        fingerprint(CANARY_CREDENTIAL)
    );

    // ...and neither canary is anywhere on disk, even though the ticket row
    // and the audit trail both record the filing. The marker is, which is
    // what proves the dump read the destination row.
    let dump = database_dump(&world);
    assert!(dump.contains("filed"), "the audit trail is in the dump");
    assert!(
        dump.contains("secret:escalation.tracker.destination"),
        "the dump read the destination row"
    );
    assert!(
        !dump.contains(CANARY_CREDENTIAL),
        "the credential is on disk in the clear"
    );
    assert!(
        !dump.contains(CANARY_URL),
        "the webhook URL is on disk — the file stage did not drop Filed::url"
    );
    assert_no_canary_in_logs("after filing");

    // A destination that is no longer a webhook retires the stored URL, so
    // it does not stay decryptable behind the new row.
    let reply = put(
        &world,
        DESTINATIONS,
        &key,
        json!({
            "destination": { "git_hub": { "owner": "acme", "repo": "api" } },
            "credential": CANARY_CREDENTIAL,
        }),
    )
    .await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.json());
    assert_eq!(
        active_secrets(&world, "escalation.tracker.destination"),
        0,
        "the stale webhook URL secret is retired"
    );
    assert_eq!(active_secrets(&world, "escalation.tracker.credential"), 2);
}

/// A credential the tracker refuses is a `422` and stores nothing: no
/// destination row, no secret, and the tenant is left unconfigured.
#[pollster::test]
async fn a_rejected_credential_stores_nothing() {
    let dir = dev_key_dir();
    let world = kit_with(vec![], Some(&dir));
    world.harness.tracker.set_mode(TrackerMode::Unauthorized);
    let key = tenant_key(&world, "acme");

    let reply = put(
        &world,
        DESTINATIONS,
        &key,
        json!({
            "destination": { "git_hub": { "owner": "acme", "repo": "api" } },
            "credential": "wrong-token",
        }),
    )
    .await;
    assert_eq!(
        reply.status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        reply.json()
    );
    assert_eq!(
        reply.json()["type"],
        json!(format!("{PROBLEMS}escalation-credential-rejected"))
    );

    assert_eq!(
        count_of(&world, "SELECT COUNT(*) AS n FROM sg_destinations"),
        0
    );
    assert_eq!(
        count_of(&world, "SELECT COUNT(*) AS n FROM harness_secrets"),
        0
    );

    let seen = get(&world, DESTINATIONS, &key).await;
    assert_eq!(seen.status, StatusCode::NOT_FOUND, "still unconfigured");
}

/// The probe is *not* a destination check: an unsupported tracker and an
/// unreachable one each refuse the `PUT`, while a `Rejected` destination —
/// a repo that does not exist — is accepted (see `PROBE_ID`).
#[pollster::test]
async fn the_probe_refuses_an_unsupported_or_unreachable_tracker() {
    let dir = dev_key_dir();
    let world = kit_with(vec![], Some(&dir));
    let key = tenant_key(&world, "acme");
    let body = json!({
        "destination": { "git_hub": { "owner": "acme", "repo": "api" } },
        "credential": "some-token",
    });

    for (mode, status, slug) in [
        (
            TrackerMode::NotConfigured,
            StatusCode::UNPROCESSABLE_ENTITY,
            "escalation-destination-unsupported",
        ),
        (
            TrackerMode::Transient,
            StatusCode::SERVICE_UNAVAILABLE,
            "escalation-tracker-unavailable",
        ),
        (TrackerMode::Rejected, StatusCode::OK, ""),
    ] {
        world.harness.tracker.set_mode(mode.clone());
        let reply = put(&world, DESTINATIONS, &key, body.clone()).await;
        assert_eq!(reply.status, status, "{mode:?}: {}", reply.json());
        if status != StatusCode::OK {
            assert_eq!(reply.json()["type"], json!(format!("{PROBLEMS}{slug}")));
            assert_eq!(
                count_of(&world, "SELECT COUNT(*) AS n FROM sg_destinations"),
                0,
                "{mode:?}: nothing was stored"
            );
        }
    }
}

/// A revoked key id is refused with a `401`. The list is read from the same
/// config key `module-support` reads, so one edit revokes a key for both
/// modules.
#[pollster::test]
async fn a_revoked_kid_is_refused() {
    let dir = dev_key_dir();
    // The harness's signer is fixed, so a key minted from one harness
    // verifies in another; minting first tells us the kid to revoke.
    let minted = tenancy::mint(&*kit_with(vec![], None).harness.signer, "acme")
        .expect("the harness signer mints a tenant key");

    // With no revoked list the key is a valid, merely unconfigured, key.
    let open = kit_with(vec![], Some(&dir));
    assert_eq!(
        get(&open, DESTINATIONS, &minted.key).await.status,
        StatusCode::NOT_FOUND
    );

    // Listing its kid revokes it.
    let world = kit_with(
        vec![("SUPPORT_REVOKED_KIDS", minted.kid.clone())],
        Some(&dir),
    );
    let reply = get(&world, DESTINATIONS, &minted.key).await;
    assert_eq!(
        reply.status,
        StatusCode::UNAUTHORIZED,
        "the kid `{}` is on the shared revoked list",
        minted.kid
    );
    assert_eq!(
        reply.json()["type"],
        json!(format!("{PROBLEMS}escalation-unauthorized"))
    );
}

/// Without a KMS the route refuses rather than storing a plaintext secret,
/// while the older Config-key form of `credential_ref` still files.
#[pollster::test]
async fn without_a_kms_the_route_refuses_and_the_config_form_still_files() {
    let world = kit_with(
        vec![("ESCALATION_TRACKER_CREDENTIAL", "config-token".to_owned())],
        None,
    );
    let key = tenant_key(&world, "acme");

    let reply = put(
        &world,
        DESTINATIONS,
        &key,
        json!({
            "destination": { "git_hub": { "owner": "acme", "repo": "api" } },
            "credential": "tenant-token",
        }),
    )
    .await;
    assert_eq!(reply.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        reply.json()["type"],
        json!(format!("{PROBLEMS}escalation-kms-not-configured"))
    );
    assert_eq!(
        count_of(&world, "SELECT COUNT(*) AS n FROM sg_destinations"),
        0
    );

    // The legacy row a deployment configured before the routes existed: the
    // ref names a Config key, and the file stage resolves it there.
    let seeded = module_escalation::store::put_destination_stmt(
        "acme",
        &Destination::GitHub {
            owner: "acme".to_owned(),
            repo: "api".to_owned(),
        },
        "ESCALATION_TRACKER_CREDENTIAL",
        "2026-01-01T00:00:00Z",
    );
    pollster::block_on(world.harness.db.batch_atomic(&[seeded])).expect("the row seeds");

    handoff(&world, "acme");
    drive(&world, support::happy_model());

    let filed = world
        .harness
        .tracker
        .last_filed()
        .expect("the Config-key form still files");
    assert_eq!(filed.credential_fingerprint, fingerprint("config-token"));
    assert_eq!(
        filed.dest,
        Destination::GitHub {
            owner: "acme".to_owned(),
            repo: "api".to_owned(),
        }
    );
}

/// A key the tenant revoked (its `sg_api_keys` row deleted) is refused on
/// the destination routes on its very next request, though its signature
/// still verifies — revocation is enforced on this module's auth path too,
/// not only on `module-support`'s. The tenant's other key keeps working.
#[pollster::test]
async fn a_revoked_key_is_refused_on_the_destination_routes() {
    let dir = dev_key_dir();
    let world = kit_with(vec![], Some(&dir));
    let old = tenancy::mint(&*world.harness.signer, "acme").expect("mint");
    let new = tenant_key(&world, "acme");
    let body = json!({
        "destination": { "git_hub": { "owner": "acme", "repo": "api" } },
        "credential": "acme-token",
    });
    let reply = put(&world, DESTINATIONS, &old.key, body).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.json());
    assert_eq!(
        get(&world, DESTINATIONS, &old.key).await.status,
        StatusCode::OK
    );

    world.tenants.revoke_key(&old.key_id);

    let reply = get(&world, DESTINATIONS, &old.key).await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "the revoked key");
    assert_eq!(
        reply.json()["type"],
        json!(format!("{PROBLEMS}escalation-unauthorized"))
    );
    assert_eq!(
        get(&world, DESTINATIONS, &new).await.status,
        StatusCode::OK,
        "the tenant's other key is unaffected"
    );
}

/// A suspended tenant is refused on every destination route, the way the
/// rest of the API refuses it: its own key gets the same indistinguishable
/// `401` a bad key gets, and the admin routes for it answer `404` (as
/// `module-support`'s admin routes for a non-active tenant do). Nothing it
/// had stored changes.
#[pollster::test]
async fn a_suspended_tenant_is_refused_on_every_destination_route() {
    let dir = dev_key_dir();
    let world = kit_with(vec![], Some(&dir));
    let key = tenant_key(&world, "acme");
    let body = json!({
        "destination": { "git_hub": { "owner": "acme", "repo": "api" } },
        "credential": "acme-token",
    });
    let reply = put(&world, DESTINATIONS, &key, body.clone()).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.json());

    world.tenants.suspend("acme");

    let unauthorized = json!(format!("{PROBLEMS}escalation-unauthorized"));
    let reply = get(&world, DESTINATIONS, &key).await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "GET");
    assert_eq!(reply.json()["type"], unauthorized);
    let reply = put(&world, DESTINATIONS, &key, body.clone()).await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "PUT");
    assert_eq!(reply.json()["type"], unauthorized);
    let reply = request_as(
        &world.harness.router,
        Method::DELETE,
        DESTINATIONS,
        &key,
        None,
    )
    .await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "DELETE");
    assert_eq!(reply.json()["type"], unauthorized);

    let admin = format!("{ADMIN_TENANTS}/acme/destinations");
    let reply = get(&world, &admin, ADMIN_TOKEN).await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND, "admin GET");
    let reply = put(&world, &admin, ADMIN_TOKEN, body).await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND, "admin PUT");
    let reply = request_as(
        &world.harness.router,
        Method::DELETE,
        &admin,
        ADMIN_TOKEN,
        None,
    )
    .await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND, "admin DELETE");

    // The row and its sealed credential are exactly as the tenant left them.
    assert_eq!(
        count_of(
            &world,
            "SELECT COUNT(*) AS n FROM sg_destinations WHERE tenant_id = 'acme'"
        ),
        1
    );
    assert_eq!(active_secrets(&world, "escalation.tracker.credential"), 1);

    // Another tenant is unaffected.
    let globex = tenant_key(&world, "globex");
    assert_eq!(
        get(&world, DESTINATIONS, &globex).await.status,
        StatusCode::NOT_FOUND
    );
}

/// Without a tenant directory the module cannot tell an active tenant from
/// a suspended one, so the destination routes refuse everyone (fail closed)
/// instead of trusting the key alone.
#[pollster::test]
async fn without_a_tenant_directory_every_tenant_is_refused() {
    let (harness, _config) = harness_for(
        Escalation::new(),
        vec![("ADMIN_TOKEN", ADMIN_TOKEN.to_owned())],
    );
    let key = tenancy::mint(&*harness.signer, "acme")
        .expect("the harness signer mints a tenant key")
        .key;
    let reply = request_as(&harness.router, Method::GET, DESTINATIONS, &key, None).await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
    let admin = format!("{ADMIN_TENANTS}/acme/destinations");
    let reply = request_as(&harness.router, Method::GET, &admin, ADMIN_TOKEN, None).await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND);
}

/// Every secret access the destination routes and the file stage make is
/// written to the `harness_secret_audit` chain: the tenant's store, the
/// secret's name, the action, the actor and the time — never the value or
/// its ciphertext — and the chain verifies.
#[pollster::test]
async fn every_secret_access_lands_in_the_audit_chain() {
    let dir = dev_key_dir();
    let world = kit_with(vec![], Some(&dir));
    let key = tenant_key(&world, "acme");

    let reply = put(
        &world,
        DESTINATIONS,
        &key,
        json!({
            "destination": { "webhook": { "url": CANARY_URL } },
            "credential": CANARY_CREDENTIAL,
        }),
    )
    .await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.json());
    let reply = get(&world, DESTINATIONS, &key).await;
    assert_eq!(reply.status, StatusCode::OK);
    let admin = format!("{ADMIN_TENANTS}/acme/destinations");
    let reply = request_as(
        &world.harness.router,
        Method::DELETE,
        &admin,
        ADMIN_TOKEN,
        None,
    )
    .await;
    assert_eq!(reply.status, StatusCode::NO_CONTENT);

    let rows = pollster::block_on(
        world.harness.db.query(&Statement::new(
            "SELECT store, actor, name, action, allowed, ts FROM harness_secret_audit ORDER BY seq"
                .to_owned(),
        )),
    )
    .expect("the audit chain reads");
    let entries: Vec<(String, String, String, String)> = rows
        .rows
        .iter()
        .map(|row| {
            let ts: String = row.get("ts").expect("a timestamp");
            assert!(!ts.is_empty(), "every row is timestamped");
            (
                row.get("store").expect("a store"),
                row.get("actor").expect("an actor"),
                row.get("name").expect("a name"),
                row.get("action").expect("an action"),
            )
        })
        .collect();
    let has = |actor: &str, name: &str, action: &str| {
        entries
            .iter()
            .any(|(store, a, n, act)| store == "acme" && a == actor && n == name && act == action)
    };
    assert!(
        has("tenant:acme", "escalation.tracker.credential", "put"),
        "{entries:?}"
    );
    assert!(
        has("tenant:acme", "escalation.tracker.destination", "put"),
        "{entries:?}"
    );
    assert!(
        has("tenant:acme", "escalation.tracker.credential", "get"),
        "{entries:?}"
    );
    assert!(
        has("admin", "escalation.tracker.credential", "delete"),
        "{entries:?}"
    );
    assert!(
        has("admin", "escalation.tracker.destination", "delete"),
        "{entries:?}"
    );

    // No value, no ciphertext: the canaries are nowhere in the chain.
    for (store, actor, name, action) in &entries {
        for field in [store, actor, name, action] {
            assert!(
                !field.contains(CANARY_CREDENTIAL) && !field.contains(CANARY_URL),
                "a secret value reached the audit chain"
            );
        }
    }

    // And the chain is intact.
    let store = cratefield_secrets::StoreId::Tenant("acme".to_owned());
    pollster::block_on(cratefield_secrets::verify(&store, &*world.harness.db))
        .expect("the tenant's audit chain verifies");
}
