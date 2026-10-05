//! `Escalation::kick`: the entry point the composition's handoff sink
//! calls after `module-support` commits a handoff's statements in its own
//! batch, so the staged ticket is driven to a filed state *now* rather
//! than at the next cron tick.
//!
//! The whole point is that this needs no `Module::scheduled` call and no
//! cron: the kick hands the pipeline to the `Defer` port, and draining the
//! port runs the work. This test drives exactly that — a handoff is
//! committed, nothing drains on its own, and only after the kick does a
//! drain file the ticket.

mod support;

use std::sync::Arc;

use cratefield_core::{Clock, Defer, IdGen, ModuleContext, Ports, UlidIdGen};
use cratefield_testing::{FakeTracker, TestHarness, TrackerMode};
use module_escalation::Escalation;
use module_escalation::model::Status;
use module_escalation::testing::FakeConfig;

use support::Fixture;

/// The `ModuleContext` a caller would hand `Escalation::kick`: escalation's
/// own filtered view over a bundle wired to this fixture's database, model,
/// tracker, clock and defer. Built the way the harness builds one, through
/// the public `Harness::module_context`.
fn context(fixture: &Fixture, defer: Arc<dyn Defer>) -> ModuleContext {
    let kit = TestHarness::new(vec![Box::new(Escalation::new())]);
    let mut ports = Ports::with_config(Arc::new(
        FakeConfig::new().with(support::CREDENTIAL_REF, support::CREDENTIAL_SECRET),
    ));
    ports.db = Some(fixture.db.clone() as Arc<dyn cratefield_core::Database>);
    ports.text_model = Some(Arc::new(fixture.model.clone()) as Arc<dyn cratefield_core::TextModel>);
    ports.tracker = Some(Arc::new(fixture.tracker.clone()) as Arc<dyn cratefield_core::Tracker>);
    ports.clock = Some(fixture.clock.clone() as Arc<dyn Clock>);
    ports.id_gen = Some(Arc::new(UlidIdGen) as Arc<dyn IdGen>);
    ports.mailer.clone_from(&fixture.mailer);
    ports.defer = Some(defer);
    kit.harness.module_context(&Escalation::new(), &ports)
}

#[pollster::test]
async fn kick_defers_a_drain_that_files_the_staged_handoff() {
    let fixture = support::fixture(
        support::happy_model(),
        FakeTracker::new(TrackerMode::FileOk),
    );
    let defer: Arc<dyn Defer> = Arc::new(fixture.defer.clone());

    // The handoff staged the ticket, but nothing has driven it: callers
    // who never kick wait for `scheduled`, which this test never runs.
    assert_eq!(
        support::ticket(&fixture).external_id,
        None,
        "the staged ticket is not filed before the kick"
    );

    let ctx = context(&fixture, Arc::clone(&defer));
    Escalation::new().kick(&ctx, defer);

    // The kick queued its run on the defer port and did nothing else; the
    // drain is what files. Draining runs the queued stage and every stage
    // it enqueues behind it.
    assert_eq!(
        fixture.defer.deferred_count(),
        1,
        "the kick deferred exactly one run"
    );
    fixture.defer.drain().await;

    let ticket = support::ticket(&fixture);
    assert_eq!(
        ticket.status,
        Status::Filed,
        "the kicked drain filed the ticket, not `{}`",
        ticket.status.as_str()
    );
    assert_eq!(
        ticket.external_id.as_deref(),
        Some(support::FILED_EXTERNAL_ID),
        "filed through the tracker, no cron involved"
    );
}
