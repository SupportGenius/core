-- The secrets store's own tables (issue #39, docs/SECRETS-DESIGN.md §3).
-- The same two tables exist in every store: once in the control database
-- as the global store, once in each tenant database as that tenant's.
-- No table mixes tiers and no database holds both.
--
-- The wrapped DEK lives here, not in the KMS: a dump of this database is
-- ciphertext plus a blob that cannot be unwrapped anywhere but the KMS,
-- and a backup, a tenant move and an offboarding shred are all local.
CREATE TABLE IF NOT EXISTS harness_secret_keys (
    key_id       TEXT PRIMARY KEY,
    kms_provider TEXT NOT NULL,
    kms_key_ref  TEXT NOT NULL,
    wrapped_dek  BLOB NOT NULL,
    cipher       TEXT NOT NULL,
    state        TEXT NOT NULL,
    created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS harness_secrets (
    name       TEXT NOT NULL,
    version    INTEGER NOT NULL,
    key_id     TEXT NOT NULL REFERENCES harness_secret_keys (key_id),
    nonce      BLOB NOT NULL,
    ciphertext BLOB NOT NULL,
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL,
    deleted_at TEXT,
    PRIMARY KEY (name, version)
);
