-- Issue #707: give a counter row to every product that has confirmed
-- positions, including the ones that never had one.
--
-- 0006 added `next_position` and backfilled it — but only over the rows
-- already in `waitlist_position_lock`. Those rows are only ever created
-- by `confirm_entry`'s `ensure_lock_row` (INSERT ... ON CONFLICT DO
-- NOTHING), so they exist exactly for the products somebody confirmed
-- since 0004 shipped. A database whose confirmed positions predate the
-- lock table has no row for them: a database that ran 0001 to 0003,
-- accumulated a queue, then upgraded. 0006's `MAX(position)` subquery
-- never saw those products, and the UNIQUE(product, position) index it
-- added has nothing to backstop a counter that starts at zero.
--
-- The failure is not subtle and never self-heals. The first confirm
-- after the upgrade materialises the row at counter 0, the lock UPDATE
-- increments it to 1, and the flip assigns position 1 — which an entry
-- has held since before the upgrade. The commit fails on the index
-- added in 0006, every later confirm for that product takes the same
-- path, and the product's waitlist is stuck: confirmations return a
-- DbError until somebody deletes rows by hand.
--
-- This is a new migration rather than an edit to 0006 because an
-- applied migration is never edited (docs/MODULE-AUTHORING.md); the
-- checksums in `harness_migrations` make an edit a boot-time error
-- rather than a fix (crates/adapter-sqlite/src/lib.rs).
--
-- `WHERE position IS NOT NULL` does two jobs. A product whose entries
-- are all still pending needs no row: its first confirm legitimately
-- takes the counter 0 -> 1 and assigns position 1, which is correct for
-- a queue that has never handed out a position. And `MAX(position)` over
-- such a group is NULL, which cannot be inserted into a NOT NULL column
-- — the WHERE is what makes the statement legal, not just tidy.
--
-- `ON CONFLICT (product) DO NOTHING` protects rows that already exist.
-- 0006's backfill plus every increment since leaves those counters
-- ahead of `MAX(position)` — a counter is monotonic and entries can be
-- anonymised (0005) without a position ever being freed below it — so
-- this must never walk a live counter back. Rerunning the migration, or
-- running it on a database that is already healthy, is a no-op.
--
-- SQLite's parser needs the SELECT's WHERE to tell `ON CONFLICT` (this
-- statement's conflict clause) from a join constraint; this one has a
-- WHERE, so the statement parses.
--
-- `updated_at` is written with CURRENT_TIMESTAMP because nothing reads
-- it back — the lock column exists to be UPDATEd so the write takes a
-- row lock, and store.rs never selects it. The portable-SQL lint
-- (crates/core/src/lint.rs) bans the Postgres-only NOW() but not the
-- SQLite/Postgres-standard CURRENT_TIMESTAMP, and migrations have no
-- bind parameters to thread a timestamp through.
INSERT INTO waitlist_position_lock (product, updated_at, next_position)
    SELECT product, CURRENT_TIMESTAMP, MAX(position)
    FROM waitlist_entries
    WHERE position IS NOT NULL
    GROUP BY product
    ON CONFLICT (product) DO NOTHING;
