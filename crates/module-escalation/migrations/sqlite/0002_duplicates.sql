-- Issue #27: duplicate detection. When the judge recognizes the draft as
-- an already-filed ticket, the ticket is linked to it instead of filed a
-- second time. This adds the counter a linked ticket carries and the
-- table recording each link. Portable SQL subset only (TEXT/INTEGER), so a
-- Postgres set is a mechanical addition later (ADR 0004), exactly as 0001.

-- How many later tickets have linked to this one as a duplicate. Existing
-- rows default to zero; the judge stage's duplicate branch is the only
-- writer (see src/pipeline.rs), and it bumps this and `updated_at`
-- together.
ALTER TABLE sg_tickets ADD COLUMN match_count INTEGER NOT NULL DEFAULT 0;

-- sg_ticket_links: one row per duplicate ticket linked to an existing
-- filed ticket. `ticket_id` is the existing filed ticket; `source_ticket_id`
-- is the duplicate ticket that pointed at it; `conversation_id` is the
-- conversation that duplicate came from. The primary key is the duplicate
-- ticket itself — a ticket links at most once — so re-linking it (a
-- redelivered judge stage) is a no-op rather than a second row, even when
-- one conversation escalates twice and both tickets duplicate the same one.
CREATE TABLE IF NOT EXISTS sg_ticket_links (
    tenant_id TEXT NOT NULL,
    ticket_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    source_ticket_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (source_ticket_id)
);

-- "Which duplicate tickets linked to this one?": the lookup the linked
-- ticket's side of the relation is read by, and tenant-scoped so a query
-- never crosses tenants.
CREATE INDEX IF NOT EXISTS idx_sg_ticket_links_tenant
    ON sg_ticket_links (tenant_id, ticket_id);
