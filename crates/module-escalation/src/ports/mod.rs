//! A temporary **local mirror** of two harness ports: [`text_model`]
//! (`TextModel`) and [`tracker`] (`Tracker`).
//!
//! `text_model` now lives in the shared, non-module `text-model` crate
//! (`crates/text-model`), because the support module answers through the
//! same port and modules must not depend on each other. It is re-exported
//! here unchanged, so `crate::ports::text_model::*` and
//! `module_escalation::ports::text_model::*` keep resolving; the rules
//! below still describe it.
//!
//! Both exist on harness `main`
//! (`crates/core/src/ports/text_model.rs`, `crates/core/src/ports/tracker.rs`)
//! but are **absent from published `cratefield-core 0.4.3`** — they are the
//! two blockers issue #4 names at the top of `/workspace/README.md` ("Two
//! ports this needs do not exist in the harness yet"). Until core publishes
//! them, the escalation pipeline is written against the copies here, so
//! `crates/module-escalation` builds against the core line the whole
//! workspace pins (`0.4`, never `0.5` — see the root `Cargo.toml`).
//!
//! The shapes are copied **verbatim** from harness `main` (names, variants,
//! fields, builder methods, serde wire forms, `Display` texts), so when core
//! publishes them the swap is mechanical: delete this module and re-point
//! the `use` statements from `crate::ports::{text_model, tracker}` to
//! `cratefield_core::ports::{text_model, tracker}`.
//!
//! Deviations from harness `main`, all deliberate:
//!
//! - `RoutingTextModel` / `RoutingTracker` are **not** mirrored. They are
//!   venture wiring, not module surface: the escalation pipeline holds one
//!   `Arc<dyn TextModel>` / `Arc<dyn Tracker>` handed to it by the venture,
//!   and never routes by tier or destination itself. When the swap happens
//!   they come from core like everything else.
//! - `Display` scrubs provider text with `cratefield_core::scrub_text`
//!   (re-exported by core 0.4.3) instead of the harness-internal
//!   `crate::logging::scrub_text` — the same function, reached through the
//!   published path.
//! - Harness's own `#[cfg(test)]` router tests are dropped with the routers;
//!   the remaining unit tests travel with the types they pin.

pub use ::text_model;
pub mod tracker;
