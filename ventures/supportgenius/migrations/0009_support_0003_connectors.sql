-- module-support v2 schema: connectors — cron-driven source syncing from
-- a sitemap, a URL prefix, or a GitHub repository (issue #29). The same
-- conventions as 0001/0002: portable SQL only (ADR 0004; linted by
-- `fz doctor`), TEXT ULID ids, ISO-8601 TEXT timestamps bound from code,
-- INTEGER counters, and `tenant_id` on every table because every query
-- filters on it.
--
-- `credential_ref` follows the sg_destinations rule (module-escalation):
-- it names the Config key the secret lives under (e.g. GITHUB_TOKEN) and
-- the secret is resolved from the Config port at fetch time. Storing the
-- credential itself here would put it in a table that gets exported,
-- backed up and replicated like any other row.

-- ---------------------------------------------------------------------------
-- sg_sources gains its connector upsert key. A connector re-ingests a page
-- by replacing the source it previously indexed for that URL, so the page
-- → source mapping has to be queryable; `external_id` is that key (the
-- fetched URL, or `github:{owner}/{repo}:{path}`). Nullable: the inline
-- `POST /sources` forms stay anonymous — NULLs are distinct in a unique
-- index on both SQLite and Postgres, so manual ingests never collide.
-- ---------------------------------------------------------------------------
ALTER TABLE sg_sources ADD COLUMN external_id TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_sg_sources_tenant_external
    ON sg_sources (tenant_id, external_id);

-- ---------------------------------------------------------------------------
-- sg_connectors: one crawl root per row. `config` is the connector's own
-- JSON — {"url": …} for sitemap/url_prefix, {"owner", "repo", "path_glob",
-- "ref"} for github — because the three kinds share no fields worth
-- columns. The caps (`max_pages`, `max_bytes`, `max_depth`) are clamped to
-- the module's hard maxima at insert time; see src/connectors.rs.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sg_connectors (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    -- 'sitemap' | 'url_prefix' | 'github'
    kind TEXT NOT NULL,
    config TEXT NOT NULL,
    credential_ref TEXT,
    max_pages INTEGER NOT NULL,
    max_bytes INTEGER NOT NULL,
    max_depth INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sg_connectors_tenant ON sg_connectors (tenant_id);

-- ---------------------------------------------------------------------------
-- sg_ingest_pages: per-connector fetch state, one row per URL the
-- connector has ever fetched (the primary key is the pair). It carries
-- the role the URL was discovered as (`sitemap` | `page` | `github_tree`
-- | `github_file` — it decides the fetch's Accept header and extraction,
-- and which re-sync job re-runs it), the conditional-GET validators
-- (`etag`, `last_modified`), the crawl `depth` the URL was enqueued at,
-- and the `sg_sources` id the URL currently indexes — NULL for fetches
-- that index nothing (a sitemap or a GitHub tree listing). This table is
-- the page cap, the re-sync frontier and the delete path: a URL that
-- answers 404/410 drops its row and its source together.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sg_ingest_pages (
    connector_id TEXT NOT NULL,
    url TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    role TEXT NOT NULL,
    depth INTEGER NOT NULL,
    etag TEXT,
    last_modified TEXT,
    source_id TEXT,
    PRIMARY KEY (connector_id, url)
);

CREATE INDEX IF NOT EXISTS idx_sg_ingest_pages_tenant ON sg_ingest_pages (tenant_id);

-- Generated from cratefield_core::Outbox::new("sg_ingest_outbox")
--   .create_table_sql() — do not hand-edit; regenerate if core's schema
-- changes. One `fetch` row per URL to fetch; drained by the module's
-- scheduled hook and, opportunistically, right after the connector is
-- created (see src/connectors.rs).
CREATE TABLE IF NOT EXISTS sg_ingest_outbox (
    id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    payload TEXT NOT NULL,
    subject TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    locked_until TEXT,
    created_at TEXT NOT NULL
);
