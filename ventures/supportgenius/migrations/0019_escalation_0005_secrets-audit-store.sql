-- Store attribution on the audit chain (issue #142). A chain lives in
-- its store's own database and the hash of every row written from here
-- on covers the store it belongs to, so a chain copied verbatim from
-- another database cannot verify as this store's history. Rows written
-- before this migration carry the empty default and keep their
-- original (v1) hash bytes, so existing chains stay valid — an
-- append-only table's past is not rewritten.
ALTER TABLE harness_secret_audit ADD COLUMN store TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS harness_secret_audit_store_seq
    ON harness_secret_audit (store, seq);
