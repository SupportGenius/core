-- The secret-access audit chain (issue #41). One per store: the global
-- store's chain in the control database, each tenant's in its own, so a
-- tenant's auditor sees only that tenant's chain and the log lives and
-- dies with the data it describes.
--
-- Every row's hash covers the previous row's hash, so altering or
-- removing any row breaks every link after it.
CREATE TABLE IF NOT EXISTS harness_secret_audit (
    seq        INTEGER PRIMARY KEY,
    ts         TEXT NOT NULL,
    actor      TEXT NOT NULL,
    name       TEXT NOT NULL,
    version    INTEGER,
    action     TEXT NOT NULL,
    allowed    INTEGER NOT NULL,
    request_id TEXT,
    prev_hash  BLOB NOT NULL,
    hash       BLOB NOT NULL
);

-- Append-only, enforced by the database rather than by convention. The
-- role grants that stop an application role even trying are deployment
-- configuration; these triggers stop everything that reaches the table,
-- including the migration role.
CREATE TRIGGER IF NOT EXISTS harness_secret_audit_no_update
BEFORE UPDATE ON harness_secret_audit
BEGIN
    SELECT RAISE(ABORT, 'harness_secret_audit is append-only');
END;

CREATE TRIGGER IF NOT EXISTS harness_secret_audit_no_delete
BEFORE DELETE ON harness_secret_audit
BEGIN
    SELECT RAISE(ABORT, 'harness_secret_audit is append-only');
END;
