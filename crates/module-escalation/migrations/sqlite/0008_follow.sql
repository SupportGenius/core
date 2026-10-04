-- Issue #26: follow-up polling and the customer's contact address.
--
-- Appended after the embedded cratefield-secrets schema (0003-0006), whose
-- files are already collected and sha256-locked in the venture; a new
-- highest id adds only, never renumbers. Portable SQL subset only, as 0001.

-- The tracker's last-reported state, written `open` when the file stage
-- succeeds and refreshed by every follow-up poll (see src/pipeline.rs).
ALTER TABLE sg_tickets ADD COLUMN tracker_state TEXT;

-- The customer's contact per conversation, read at notify-time (see
-- src/store.rs `contact_email`) and upserted by the support module's
-- `HandoffSink::remember_contact` in the caller's own atomic batch. It is
-- escalation's own table, declared in `personal_data` (subject `email`).
CREATE TABLE IF NOT EXISTS sg_contacts (
    tenant_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    email TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (tenant_id, conversation_id)
);
