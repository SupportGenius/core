//! The shared conformance suite (issue #9) plus the wasm dependency
//! boundary (ADR 0001) for `module-support`. `conformance` cross-checks
//! `personal_data()` against `tables()` in both directions and applies
//! the migration-set rules, so this file is the whole privacy-schema
//! contract.

use cratefield_testing::{assert_wasm_safe_deps, conformance};
use module_support::Support;

#[test]
fn support_conforms() {
    conformance(Box::new(Support::new()));
}

#[test]
fn support_deps_are_wasm_safe() {
    assert_wasm_safe_deps(env!("CARGO_PKG_NAME"));
}
