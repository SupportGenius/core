//! The shared conformance suite plus the wasm dependency boundary for
//! `module-support`.

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
