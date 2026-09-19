-- module-support v0 schema, portable SQL only (ADR 0004; linted by
-- `fz doctor`): TEXT ULID ids, ISO-8601 TEXT timestamps bound from code,
-- INTEGER counters, no dialect functions of any kind. The same file is
-- applied to sqlite and Postgres, so `Migrations::postgres` stays empty.
--
-- Tenancy is app-level: one venture, many customer companies, and every
-- table here carries `tenant_id` because every query filters on it. The
-- self-hosted binary is the same code with exactly one tenant row.

CREATE TABLE IF NOT EXISTS sg_tenants (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sg_api_keys (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    kid TEXT NOT NULL,
    label TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sg_sources (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    title TEXT NOT NULL,
    url TEXT,
    byte_len INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sg_chunks (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    body TEXT NOT NULL,
    term_count INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

-- The inverted index. One row per (tenant, term, chunk); the primary key
-- is the composite, so a re-ingest of unchanged text would overwrite the
-- identical row rather than duplicate it.
CREATE TABLE IF NOT EXISTS sg_postings (
    tenant_id TEXT NOT NULL,
    term TEXT NOT NULL,
    chunk_id TEXT NOT NULL,
    tf INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, term, chunk_id)
);

CREATE INDEX IF NOT EXISTS idx_sg_api_keys_tenant ON sg_api_keys (tenant_id);
CREATE INDEX IF NOT EXISTS idx_sg_sources_tenant ON sg_sources (tenant_id);
CREATE INDEX IF NOT EXISTS idx_sg_chunks_tenant_source ON sg_chunks (tenant_id, source_id);
CREATE INDEX IF NOT EXISTS idx_sg_postings_tenant_term ON sg_postings (tenant_id, term);
