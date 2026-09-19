-- Issue #173: the deterministic position mutex. `confirm_entry` claims
-- exactly this one row per product (INSERT ... ON CONFLICT DO NOTHING,
-- then a single-row UPDATE) before computing `1 + MAX(position)`,
-- serializing concurrent confirms of one product on every engine. It
-- replaces the multi-row `UPDATE waitlist_entries ... WHERE product = ?`
-- bulk lock, whose progressive row-lock ordering deadlocked concurrent
-- confirms on Postgres. One row per product; never pruned.
CREATE TABLE IF NOT EXISTS waitlist_position_lock (
    product TEXT PRIMARY KEY,
    updated_at TEXT NOT NULL
);
