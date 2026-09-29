-- module-support: persisted corpus statistics, so a query never has to
-- derive them from the rows it fetches. Portable SQL only (ADR 0004;
-- linted by `fz doctor`), the same conventions as 0001/0002: TEXT ids,
-- INTEGER counters, `tenant_id` on every table because every query
-- filters on it.
--
-- Why these tables exist: BM25 needs N (the tenant's chunk count) and
-- each term's df (the number of chunks carrying it) before it can score
-- anything. Deriving them per request means a COUNT/AVG over every chunk
-- and an unbounded postings fetch, so `/search` and `/messages` cost
-- grows with the corpus. Persisted here, they turn both numbers into a
-- primary-key read, and the postings fetch into one bounded per-term
-- top-k: df and N are exact whatever subset of postings is read.

-- One row per (tenant, indexed term). df counts the tenant's chunks
-- whose postings carry the term — exactly what `bm25::rank` needs, kept
-- as its own table so it never has to be counted back out of sg_postings.
CREATE TABLE IF NOT EXISTS sg_terms (
    tenant_id TEXT NOT NULL,
    term TEXT NOT NULL,
    df INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, term)
);

-- One row per tenant that has indexed at least one chunk. `total_len` is
-- the exact integer sum of sg_chunks.term_count, so the mean document
-- length BM25 normalises by is derived as total_len / n_chunks instead of
-- being stored as a float that every increment would have to re-average.
CREATE TABLE IF NOT EXISTS sg_tenant_stats (
    tenant_id TEXT PRIMARY KEY,
    n_chunks INTEGER NOT NULL,
    total_len INTEGER NOT NULL
);

-- Backfill for tenants that predate this migration. The ingest path
-- maintains both tables incrementally from the incoming batch, so rows
-- that already exist (a migration applied twice, a re-run against a
-- database that grew rows meanwhile) must not be counted twice: DO
-- NOTHING, never DO UPDATE. The WHERE clause before GROUP BY is not
-- filtering anything — it disambiguates the upsert for SQLite, whose
-- parser otherwise reads the trailing ON as a join condition.
INSERT INTO sg_terms (tenant_id, term, df)
    SELECT tenant_id, term, COUNT(*) FROM sg_postings WHERE true
    GROUP BY tenant_id, term
    ON CONFLICT (tenant_id, term) DO NOTHING;

INSERT INTO sg_tenant_stats (tenant_id, n_chunks, total_len)
    SELECT tenant_id, COUNT(*), SUM(term_count) FROM sg_chunks WHERE true
    GROUP BY tenant_id
    ON CONFLICT (tenant_id) DO NOTHING;

-- The query path reads one term's postings as `WHERE tenant_id = ? AND
-- term = ? ORDER BY tf DESC, chunk_id ASC LIMIT k`, so the index must
-- hand those rows over already in that order: an equality prefix, then
-- tf descending and chunk_id ascending, matching the ORDER BY exactly.
-- This covers the old (tenant_id, term) index, which is dropped rather
-- than left doubling every posting write.
CREATE INDEX IF NOT EXISTS idx_sg_postings_tenant_term_tf
    ON sg_postings (tenant_id, term, tf DESC, chunk_id);
DROP INDEX IF EXISTS idx_sg_postings_tenant_term;
