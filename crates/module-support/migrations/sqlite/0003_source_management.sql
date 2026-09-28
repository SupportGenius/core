-- module-support source management: the columns listing, replacement and
-- caller-side identity need. The same portable conventions as 0001/0002
-- (ADR 0004; linted by `fz doctor`): TEXT columns, ISO-8601 TEXT
-- timestamps bound from code, no dialect functions. `ALTER TABLE ADD
-- COLUMN` is portable to SQLite, Postgres and D1, and existing rows read
-- NULL until the backfill below, so the whole change lands in one
-- migration.
--
-- `external_id` is the caller's own key for a document ("handbook", or
-- the URL the `{"url"}` form fetched it from): a later ingest with the
-- same external id replaces that source in place instead of adding a
-- second copy. The unique index is per tenant, so two workspaces may both
-- say "handbook"; NULL — an anonymous source — never collides, because
-- neither SQLite nor Postgres counts NULLs as equal in a unique index.

ALTER TABLE sg_sources ADD COLUMN external_id TEXT;
ALTER TABLE sg_sources ADD COLUMN updated_at TEXT;

-- Sources indexed before this migration were current when they were
-- written, so their updated_at starts as the instant they were created.
UPDATE sg_sources SET updated_at = created_at WHERE updated_at IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_sg_sources_tenant_external
    ON sg_sources (tenant_id, external_id);
