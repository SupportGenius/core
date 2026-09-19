//! The shared conformance suite (issue #9) plus the wasm dependency
//! boundary (ADR 0001) for `module-escalation`, in the shape
//! `module-support`'s has. `conformance` cross-checks `personal_data()`
//! against `tables()` in both directions and applies the migration-set
//! rules, so this file is the whole privacy-schema contract.
//!
//! CI runs the kit for every `crates/module-*`, so a module without this
//! file fails the `conformance (module kit)` job with "no test target
//! named `conformance`" rather than anything about the module itself.
//!
//! `Escalation` takes its two ports by argument, so the kit is handed one
//! built over the crate's own fakes: the suite reads `tables()`,
//! `personal_data()` and `migrations()`, none of which call a port, so
//! which fakes these are cannot change the verdict.

use std::sync::Arc;

use cratefield_testing::{assert_wasm_safe_deps, conformance};
use module_escalation::Escalation;
use module_escalation::ports::tracker::Filed;
use module_escalation::testing::{FakeTextModel, FakeTracker};

#[test]
fn escalation_conforms() {
    let model = FakeTextModel::scripted(vec![]);
    let tracker = FakeTracker::accepting(Filed {
        external_id: "acme/api#1".to_owned(),
        url: "https://github.test/acme/api/1".to_owned(),
    });
    conformance(Box::new(Escalation::new(
        Arc::new(model),
        Arc::new(tracker),
    )));
}

#[test]
fn escalation_deps_are_wasm_safe() {
    assert_wasm_safe_deps(env!("CARGO_PKG_NAME"));
}
