-- Issue #265: make `waitlist_entries` anonymisable by dropping NOT NULL from
-- the two address columns.
--
-- An erasure request on a waitlist is the one case `Disposition::Anonymise`
-- was added for. `position` is a dense per-product join order that is never
-- recomputed and `referrals` is a credit already granted to somebody else, so
-- deleting a confirmed row changes a count other people can see and orphans
-- every `referred_by` that points at its code. Keeping the row and taking the
-- person out of it answers the request without rewriting anyone else's place
-- in the queue.
--
-- `cratefield-module-privacy` carries that out as `UPDATE waitlist_entries SET
-- email = NULL, email_normalized = NULL, answers = NULL WHERE id = ?`, and
-- `email`/`email_normalized` were NOT NULL, so the statement the declaration
-- promises would have failed on the first request. `answers` was already
-- nullable. Nothing else about the table changes.
--
-- Its own migration because 0001 to 0004 are applied, and an applied migration
-- is never edited (docs/MODULE-AUTHORING.md). SQLite cannot drop a NOT NULL in
-- place, so the table is rebuilt and the rows copied across, exactly as
-- auth-core's 0003 does; the DDL is the portable subset (ADR 0004) and renders
-- identically on D1 and Postgres, which is why there is no dialect override.
--
-- `UNIQUE(email_normalized, product)` is kept. Both engines treat NULLs as
-- distinct in a unique index, so any number of anonymised entries can sit on
-- the same product while two live signups still cannot.
CREATE TABLE IF NOT EXISTS waitlist_entries_rebuild (
    id TEXT PRIMARY KEY,
    email TEXT,
    email_normalized TEXT,
    product TEXT NOT NULL,
    status TEXT NOT NULL,
    position INTEGER,
    referral_code TEXT UNIQUE,
    referred_by TEXT,
    referrals INTEGER NOT NULL DEFAULT 0,
    answers TEXT,
    created_at TEXT NOT NULL,
    confirmed_at TEXT,
    generation INTEGER NOT NULL DEFAULT 1,
    UNIQUE(email_normalized, product)
);

INSERT INTO waitlist_entries_rebuild (
    id, email, email_normalized, product, status, position, referral_code,
    referred_by, referrals, answers, created_at, confirmed_at, generation
)
    SELECT id, email, email_normalized, product, status, position, referral_code,
           referred_by, referrals, answers, created_at, confirmed_at, generation
    FROM waitlist_entries;

DROP TABLE waitlist_entries;
ALTER TABLE waitlist_entries_rebuild RENAME TO waitlist_entries;
