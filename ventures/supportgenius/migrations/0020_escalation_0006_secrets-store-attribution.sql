-- Store attribution on the secrets and key tables (the row-level twin
-- of the audit chain's #142): one physical database may host several
-- stores' rows -- the control database holds the global store and every
-- tenant store the platform runs, side by side -- and until this
-- migration those rows were indistinguishable at the SQL layer. The AAD
-- kept them separate under encryption (a row never decrypts in the
-- wrong store), but every query was store-blind: `active_key` served
-- whichever key row was newest regardless of store, `delete` soft-deleted
-- every store's rows of that name, `rotate_dek` tried to re-encrypt the
-- other stores' rows and failed closed on their AAD, and the primary
-- key (name, version) made the same name in two stores of one database
-- impossible outright.
--
-- `harness_secrets` is rebuilt rather than altered because its primary
-- key must grow the store column, and neither engine alters a primary
-- key. The copy rewrites nothing but the table's shape, which in a table
-- holding ciphertext is the entire point.
--
-- A row written before this migration would carry the empty store, and
-- an empty store is nobody's: every query here scopes to `store = ?`
-- exactly, so such a row is invisible rather than visible to all. The
-- audit chain's #142 chose the opposite (`OR store = ''`) because it is
-- append-only -- its past cannot be rewritten, so the clause can only
-- let an old row be read. These tables are deleted, re-encrypted and
-- re-keyed from, where the same clause would instead hand every store a
-- write over every other store's unstamped rows. Nothing is lost by the
-- stricter rule: no composition ever applied this schema before
-- attribution, so no such row exists to be rescued.
CREATE TABLE harness_secrets_attributed (
    name       TEXT NOT NULL,
    version    INTEGER NOT NULL,
    key_id     TEXT NOT NULL REFERENCES harness_secret_keys (key_id),
    nonce      BLOB NOT NULL,
    ciphertext BLOB NOT NULL,
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL,
    deleted_at TEXT,
    store      TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (store, name, version)
);

INSERT INTO harness_secrets_attributed
    (name, version, key_id, nonce, ciphertext, created_at, created_by, deleted_at, store)
    SELECT name, version, key_id, nonce, ciphertext, created_at, created_by, deleted_at, ''
    FROM harness_secrets;

DROP TABLE harness_secrets;

ALTER TABLE harness_secrets_attributed RENAME TO harness_secrets;

ALTER TABLE harness_secret_keys ADD COLUMN store TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS harness_secrets_store_name
    ON harness_secrets (store, name);

CREATE INDEX IF NOT EXISTS harness_secret_keys_store_state
    ON harness_secret_keys (store, state);
