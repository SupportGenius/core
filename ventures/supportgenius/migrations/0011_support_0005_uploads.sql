-- module-support chunked uploads (issue #30): a document larger than the
-- 64 KiB `/v1/*` body cap arrives as parts, is assembled on `complete`, and
-- is turned into text by the `extract` job. Portable SQL only (ADR 0004;
-- linted by `fz doctor`), the same conventions as 0001–0004: TEXT ULID ids,
-- ISO-8601 TEXT timestamps bound from code, INTEGER counters, and
-- `tenant_id` on every table a query filters by. The part *bytes* never
-- touch the database — they live in the `Blob` port (R2 on Workers, a
-- directory when self-hosted) — so this schema carries ids, sizes and
-- state only.
--
-- sg_support_outbox is **generated** from cratefield-core's own helper —
--     Outbox::new("sg_support_outbox").create_table_sql()
-- — and pasted verbatim, so the DDL cannot drift from what
-- `claim_due`/`complete`/`retry_later` actually query. Regenerate it if
-- core's schema changes; do not hand-edit that block.

CREATE TABLE IF NOT EXISTS sg_uploads (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    -- As the uploader named it; the extract job uses it as the source
    -- title, never as a filesystem path.
    filename TEXT NOT NULL,
    -- One of the four types the upload route accepts (lowercased at the
    -- door): text/plain, text/markdown, text/html, application/pdf.
    content_type TEXT NOT NULL,
    -- The size the uploader declared and every part must add up to.
    declared_bytes INTEGER NOT NULL,
    -- SUM of the parts actually stored; informational, the parts table
    -- is the source of truth.
    received_bytes INTEGER NOT NULL,
    -- 'open' | 'complete' | 'extracted' | 'failed'. An 'open' upload
    -- accepts parts and is garbage-collected after the TTL; 'complete'
    -- means the extract job is enqueued; 'extracted'/'failed' are
    -- terminal, both after their part blobs were deleted.
    status TEXT NOT NULL,
    -- Set when the extract job indexed the document (sg_sources.id);
    -- NULL for every other status.
    source_id TEXT,
    -- Why an extract failed (a malformed PDF, non-UTF-8 text, text past
    -- the extraction ceiling). Terminal bookkeeping, surfaced to the
    -- uploader by GET /uploads/{id}.
    error TEXT,
    created_at TEXT NOT NULL,
    completed_at TEXT
);

-- The quota read (SUM over a tenant's retained uploads) and the cron GC
-- read (open uploads past the TTL) both scan by status.
CREATE INDEX IF NOT EXISTS idx_sg_uploads_tenant_status
    ON sg_uploads (tenant_id, status);

CREATE TABLE IF NOT EXISTS sg_upload_parts (
    upload_id TEXT NOT NULL,
    -- Part ordinal, contiguous from 0. The blob key is
    -- uploads/<tenant>/<upload>/<n> under the module scope.
    n INTEGER NOT NULL,
    bytes INTEGER NOT NULL,
    PRIMARY KEY (upload_id, n)
);

-- Generated from cratefield_core::Outbox::new("sg_support_outbox")
--   .create_table_sql() — do not hand-edit; regenerate if core's schema
-- changes.
CREATE TABLE IF NOT EXISTS sg_support_outbox (
    id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    payload TEXT NOT NULL,
    subject TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    locked_until TEXT,
    created_at TEXT NOT NULL
);
