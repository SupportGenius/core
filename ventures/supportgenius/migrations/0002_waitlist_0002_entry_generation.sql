-- Issue #127: confirm tokens bind to (id, generation), mirroring
-- module-email-signup. A waitlist entry never re-enters pending today
-- (confirmed rows are never refreshed, purged rows are recreated under
-- a new id), so the generation stays 1 in practice; the binding makes
-- the guarantee structural instead of incidental.
ALTER TABLE waitlist_entries ADD COLUMN generation INTEGER NOT NULL DEFAULT 1;
