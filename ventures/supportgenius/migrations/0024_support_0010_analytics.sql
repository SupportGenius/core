-- module-support analytics (issue #36): the daily rollups the
-- `/v1/support/analytics` routes read, so a dashboard never has to scan
-- the message and ticket tables on every request. Portable SQL only
-- (ADR 0004; linted by `fz doctor`), the same conventions as 0001-0008:
-- TEXT ids, TEXT ISO-8601 days, INTEGER counters, `tenant_id` on every
-- table because every query filters on it.
--
-- A rollup row is **recomputed**, never incremented: the daily sweep
-- deletes the day's rows and reinstates them from the source tables in
-- one atomic batch, so re-running a day (or recomputing the trailing
-- week after a backfill) converges on the same numbers instead of
-- double-counting. See `analytics.rs`.
--
-- `sg_daily_stats` is one row per (tenant, day): the day's counters and
-- the median model confidence of its assistant turns (an integer
-- percentage, computed in Rust; an even turn count takes the floor of the
-- middle pair's mean). NULL when the day had no assistant turn.
--
-- `sg_daily_gaps` is the normalized query terms behind a day's
-- unanswered turns (outcome `handoff`/`clarify` with nothing retrieved).
-- Only terms are stored, never the question text.
--
-- `sg_daily_citations` counts, per (tenant, day, source), how many of the
-- day's answered turns cited at least one of that source's chunks — a
-- turn quoting two passages of one source counts once for it.

-- Assistant turns carry how many chunks retrieval returned for them, so
-- the gap rollup can tell "nothing was retrieved" from "retrieved, but
-- the model still would not answer". NULL on every user message and on
-- assistant rows written before this migration. Same portability as
-- 0008's `ALTER TABLE … ADD COLUMN`.
ALTER TABLE sg_messages ADD COLUMN retrieved_chunks INTEGER;

CREATE TABLE IF NOT EXISTS sg_daily_stats (
    tenant_id TEXT NOT NULL,
    day TEXT NOT NULL,
    conversations INTEGER NOT NULL DEFAULT 0,
    answered INTEGER NOT NULL DEFAULT 0,
    clarify INTEGER NOT NULL DEFAULT 0,
    handoff INTEGER NOT NULL DEFAULT 0,
    handed_off INTEGER NOT NULL DEFAULT 0,
    filed INTEGER NOT NULL DEFAULT 0,
    rejected INTEGER NOT NULL DEFAULT 0,
    needs_info INTEGER NOT NULL DEFAULT 0,
    duplicates INTEGER NOT NULL DEFAULT 0,
    dead_lettered INTEGER NOT NULL DEFAULT 0,
    median_confidence INTEGER,
    PRIMARY KEY (tenant_id, day)
);

CREATE TABLE IF NOT EXISTS sg_daily_gaps (
    tenant_id TEXT NOT NULL,
    day TEXT NOT NULL,
    term TEXT NOT NULL,
    hits INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, day, term)
);

CREATE TABLE IF NOT EXISTS sg_daily_citations (
    tenant_id TEXT NOT NULL,
    day TEXT NOT NULL,
    source_id TEXT NOT NULL,
    cites INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, day, source_id)
);

-- The rollup's own write deletes a whole day across every tenant
-- (`WHERE day = ?`), which the `(tenant_id, day)` primary key above does
-- not serve; one index per rollup table keeps that delete an index range
-- rather than a scan.
CREATE INDEX IF NOT EXISTS idx_sg_daily_stats_day ON sg_daily_stats (day);
CREATE INDEX IF NOT EXISTS idx_sg_daily_gaps_day ON sg_daily_gaps (day);
CREATE INDEX IF NOT EXISTS idx_sg_daily_citations_day ON sg_daily_citations (day);
