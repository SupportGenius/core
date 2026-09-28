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
//! `Escalation` carries no ports of its own — it requires `TextModel` and
//! `Tracker` and resolves them per drain — so the kit is handed the bare
//! module and wires its own full fake port set. The suite reads
//! `tables()`, `personal_data()` and `migrations()`, none of which call a
//! port, so which fakes those are cannot change the verdict.

use axum::http::{Method, StatusCode};
use cratefield_testing::{TestHarness, assert_wasm_safe_deps, conformance, request};
use module_escalation::Escalation;

#[test]
fn escalation_conforms() {
    conformance(Box::new(Escalation::new()));
}

/// What the mounted harness reports about the module: `/__health` names
/// every port `Escalation::requires` declares, so the operator reading it
/// can see the pipeline cannot run without `TextModel` and `Tracker`.
#[test]
fn health_lists_the_ports_the_pipeline_cannot_run_without() {
    let kit = TestHarness::new(vec![Box::new(Escalation::new())]);
    let health = pollster::block_on(request(&kit.router, Method::GET, "/__health", None));
    assert_eq!(health.status, StatusCode::OK);
    let body = health.json();
    let module = body["modules"]
        .as_array()
        .expect("modules array")
        .iter()
        .find(|module| module["name"] == "escalation")
        .expect("escalation is listed");
    let requires: Vec<&str> = module["requires"]
        .as_array()
        .expect("requires array")
        .iter()
        .map(|port| port.as_str().expect("a port name"))
        .collect();
    assert_eq!(requires, ["Database", "TextModel", "Tracker"]);
}

/// `/__ready` probes the `Database` port alone — `SELECT 1` under a 2 s
/// timeout, `503 problem+json` when it is absent or fails to answer — and
/// says nothing about `TextModel` or `Tracker`, which have no probe. Their
/// state surfaces only through `/__health`'s `requires` list (the test
/// above); a harness whose model is down still reports ready.
#[test]
fn ready_probes_only_the_database_and_answers_ok_when_it_responds() {
    let kit = TestHarness::new(vec![Box::new(Escalation::new())]);
    let ready = pollster::block_on(request(&kit.router, Method::GET, "/__ready", None));
    assert_eq!(ready.status, StatusCode::OK);
    assert_eq!(ready.json()["ok"], serde_json::json!(true));
}

#[test]
fn escalation_deps_are_wasm_safe() {
    assert_wasm_safe_deps(env!("CARGO_PKG_NAME"));
}
