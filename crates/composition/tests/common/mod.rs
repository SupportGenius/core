//! Fixtures shared by the composed-venture tests (`handoff.rs`, `cron.rs`):
//! the tracker credential pair, the model completions both script, and the
//! row-count helpers.
//!
//! The completion builders are shared because both modules read the **same**
//! object: support parses `answer`/`citations`/`confidence`, escalation's
//! draft stage parses the ticket fields, and each `Deserialize`s past the
//! other's — so one fast-tier completion scripts both a handoff answer and
//! the ticket it becomes.

#![allow(dead_code)] // each test binary uses the helpers it needs

use cratefield_core::{Completion, Statement};
use cratefield_testing::TestHarness;
use serde_json::json;

/// The `Config` key the escalation file stage resolves the tracker
/// credential under (never the secret itself, which lives here only
/// because this is a test).
pub(crate) const CREDENTIAL_REF: &str = "ESCALATION_TRACKER_CREDENTIAL";
/// The secret behind [`CREDENTIAL_REF`].
pub(crate) const CREDENTIAL_SECRET: &str = "token-1";

/// The fast-tier answer. Support parses `answer`/`citations`/`confidence`
/// from it; escalation's draft stage parses the same object as its draft
/// (both `Deserialize` and ignore each other's fields), so one completion
/// scripts both a handoff answer and the ticket it becomes.
pub(crate) fn fast_completion() -> Completion {
    let payload = json!({
        "answer": "I could not find that in this workspace's documents.",
        "citations": [],
        "confidence": 0.2,
        "title": "Customer cannot reset their password",
        "repro_steps": ["Open the reset page", "Submit the address"],
        "expected": "A reset link arrives",
        "actual": "Nothing arrives",
        "environment": "production",
        "severity": "error",
    });
    Completion::new(payload.to_string(), "fake-fast").json(payload)
}

/// The strong-tier judge's `file` verdict.
pub(crate) fn file_completion() -> Completion {
    let payload = json!({
        "is_defect": true,
        "reproducible": true,
        "severity_ok": true,
        "pii_clean": true,
        "verdict": "file",
        "reasons": ["the steps name a real failure"],
    });
    Completion::new(payload.to_string(), "fake-strong").json(payload)
}

/// The `n` column of a single `SELECT COUNT(*) AS n ...` query. Shared so a
/// row-count helper below is the query it asks, not the extraction.
pub(crate) fn count(kit: &TestHarness, sql: &str) -> i64 {
    let rows = pollster::block_on(kit.db.query(&Statement::new(sql))).expect("count query runs");
    rows.rows
        .first()
        .and_then(|row| row.get::<i64>("n"))
        .expect("aggregate row")
}

/// How many rows `table` holds.
pub(crate) fn count_of(kit: &TestHarness, table: &str) -> i64 {
    count(kit, &format!("SELECT COUNT(*) AS n FROM {table}"))
}
