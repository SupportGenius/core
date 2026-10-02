//! Issue #21, part B: the venture's crons written down once, and the gate
//! that keeps a module's own schedule its own.
//!
//! `crates/composition` owns the expressions ([`CRONS`]); the Worker
//! mirrors them into `[triggers] crons` and the native binary feeds them to
//! the runtime's scheduler. Because both runtimes hand **every** expression
//! to **every** module, a module whose work is only safe on one cron is
//! wrapped (`OnCron`). These tests pin the wrangler copy to the consts, and
//! drive the wrapper against the real waitlist purge and a real staged
//! escalation handoff — not a stub, because the hazard is exactly that the
//! *real* purge deletes rows whenever it is called.

mod common;

use std::sync::Arc;

use cratefield_core::{
    Destination, MapConfig, ModelTier, Module, Ports, Statement, TextModel, Tracker,
};
use cratefield_module_waitlist::Waitlist;
use cratefield_testing::{FakeTextModel, FakeTracker, TestHarness, TextModelMode, TrackerMode};

use common::{CREDENTIAL_REF, CREDENTIAL_SECRET, count, fast_completion, file_completion};

// ---------------------------------------------------------------------------
// The schedule is one list

/// The expressions as `[triggers] crons` spells them: the first bracketed
/// array after `crons =`. A comment above the key names the expressions
/// too, so this reads the assignment, not the first `*` in the file.
fn crons_from_wrangler(toml: &str) -> Vec<String> {
    let start = toml
        .find("crons = [")
        .expect("wrangler.toml has a crons array");
    let rest = &toml[start + "crons = [".len()..];
    let end = rest.find(']').expect("the crons array closes");
    rest[..end]
        .split(',')
        .map(|entry| entry.trim().trim_matches('"'))
        .filter(|entry| !entry.is_empty())
        .map(str::to_owned)
        .collect()
}

#[test]
fn wrangler_triggers_mirror_the_cron_consts() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../ventures/supportgenius/wrangler.toml"
    );
    let toml = std::fs::read_to_string(path).expect("wrangler.toml is readable");
    assert_eq!(
        crons_from_wrangler(&toml),
        supportgenius_composition::cron_expressions(),
        "wrangler.toml [triggers] crons must stay in lockstep with CRONS"
    );
}

/// The wrapper is transparent: the module list, the duplicate-table check,
/// `fz migrations collect` and `/__health` all read the inner module
/// through it, so a wrapper that changed `name`, `tables`, `migrations` or
/// `personal_data` would be a silent composition change. Assert the bare
/// and wrapped waitlist answer identically to the four that matter.
#[test]
fn the_wrapper_forwards_the_module_it_wraps() {
    let wrapped = supportgenius_composition::waitlist();
    let bare = Waitlist::new();

    assert_eq!(wrapped.name(), bare.name());
    assert_eq!(wrapped.requires(), bare.requires());
    assert_eq!(wrapped.tables(), bare.tables());
    assert_eq!(wrapped.version(), bare.version());

    let wrapped_tables: Vec<&str> = wrapped
        .personal_data()
        .iter()
        .map(|set| set.table)
        .collect();
    let bare_tables: Vec<&str> = bare.personal_data().iter().map(|set| set.table).collect();
    assert_eq!(wrapped_tables, bare_tables);

    let wrapped_ids: Vec<&str> = wrapped.migrations().sqlite.iter().map(|m| m.id).collect();
    let bare_ids: Vec<&str> = bare.migrations().sqlite.iter().map(|m| m.id).collect();
    assert_eq!(wrapped_ids, bare_ids);
}

// ---------------------------------------------------------------------------
// Gating, over the real modules

/// How many pending rows `waitlist_entries` holds.
fn pending_count(kit: &TestHarness) -> i64 {
    count(
        kit,
        "SELECT COUNT(*) AS n FROM waitlist_entries WHERE status = 'pending'",
    )
}

/// The daily purge must run on the daily cron and **not** on the
/// five-minute one. Without the wrapper the module deletes pending entries
/// on any cron it is handed — 288 times a day instead of once.
#[pollster::test]
async fn the_waitlist_purge_runs_daily_and_not_every_five_minutes() {
    let module = supportgenius_composition::waitlist();
    let kit = TestHarness::new(vec![Box::new(supportgenius_composition::waitlist())]);

    // A pending entry far past any retention window. Committed through the
    // wrapped module's own migrated table.
    pollster::block_on(kit.db.batch_atomic(&[Statement::new(
        "INSERT INTO waitlist_entries \
         (id, email, email_normalized, product, status, position, referral_code, referrals, \
          created_at) \
         VALUES ('w1', 'a@example.com', 'a@example.com', 'supportgenius', 'pending', 1, \
                 'CODE1', 0, '2000-01-01T00:00:00Z')",
    )]))
    .expect("the stale pending entry commits");
    assert_eq!(pending_count(&kit), 1, "the fixture seeded one pending row");

    // A context over the kit's own database — the `Db` port the purge reads.
    let mut ports = Ports::with_config(Arc::new(MapConfig::default()));
    ports.db = Some(Arc::clone(&kit.db));
    let ctx = kit.harness.module_context(&module, &ports);

    module
        .scheduled(&ctx, supportgenius_composition::CRON_ESCALATION_OUTBOX)
        .await
        .expect("the five-minute tick is a no-op for the wrapper");
    assert_eq!(
        pending_count(&kit),
        1,
        "the five-minute tick must not purge the waitlist"
    );

    module
        .scheduled(&ctx, supportgenius_composition::CRON_DAILY)
        .await
        .expect("the daily tick runs the purge");
    assert_eq!(
        pending_count(&kit),
        0,
        "the daily tick purges the stale pending entry"
    );
}

/// The five-minute tick is the recovery path for a handoff whose `Defer`
/// never ran: the intake row is durable, nothing kicked the pipeline, and
/// the scheduled drain is the only thing that files the ticket. The ports
/// carry **no** `Defer`, so the tick is genuinely the only driver.
#[pollster::test]
async fn the_five_minute_cron_files_a_staged_handoff() {
    let kit = TestHarness::new(vec![Box::new(supportgenius_composition::escalation())]);
    let db = Arc::clone(&kit.db);

    // The tenant's tracker destination: a GitHub repo and the credential
    // *reference* the file stage resolves through `Config`.
    let seed = module_escalation::store::put_destination_stmt(
        "acme",
        &Destination::GitHub {
            owner: "acme".to_owned(),
            repo: "api".to_owned(),
        },
        CREDENTIAL_REF,
        "2027-01-15T00:00:00Z",
    );
    pollster::block_on(db.batch_atomic(&[seed])).expect("destination seeds");

    // Support's half, by hand: the intake statements committed into the
    // caller's own batch. Nothing runs them — no kick, no defer.
    let escalation = supportgenius_composition::escalation();
    let handoff = escalation.intake().handoff(
        "acme",
        "conv-1",
        "Customer: I cannot reset my password\n\nSupport: I could not find that.",
    );
    pollster::block_on(db.batch_atomic(&handoff.statements)).expect("the handoff commits");

    // The ports a runtime resolves, minus `Defer`.
    let model = FakeTextModel::new(TextModelMode::NotConfigured);
    model.set_mode_for(ModelTier::Fast, TextModelMode::Complete(fast_completion()));
    model.set_mode_for(
        ModelTier::Strong,
        TextModelMode::Complete(file_completion()),
    );
    let mut ports = Ports::with_config(Arc::new(MapConfig::from_pairs([(
        CREDENTIAL_REF,
        CREDENTIAL_SECRET,
    )])));
    ports.db = Some(Arc::clone(&db));
    ports.text_model = Some(Arc::new(model) as Arc<dyn TextModel>);
    ports.tracker = Some(Arc::new(FakeTracker::new(TrackerMode::FileOk)) as Arc<dyn Tracker>);

    let ctx = kit.harness.module_context(&escalation, &ports);
    escalation
        .scheduled(&ctx, supportgenius_composition::CRON_ESCALATION_OUTBOX)
        .await
        .expect("the five-minute drain runs");

    let ticket = pollster::block_on(module_escalation::store::load_ticket(
        &*db,
        &handoff.ticket_id,
    ))
    .expect("ticket read")
    .expect("the staged ticket exists");
    assert_eq!(
        ticket.external_id.as_deref(),
        Some("fake-0"),
        "the five-minute tick filed the ticket with no defer port at all"
    );
}
