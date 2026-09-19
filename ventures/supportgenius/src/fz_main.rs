//! The venture-linked `fz`: it sees the compiled-in harness, so
//! `fz migrations collect` and `fz doctor` work here. It is only built on
//! the host, behind this crate's `cli` feature: `cratefield-cli` pulls
//! native-only dependencies that must never reach the wasm build of the
//! very same crate.
fn main() {
    cratefield_cli::main_for(supportgenius::harness);
}
