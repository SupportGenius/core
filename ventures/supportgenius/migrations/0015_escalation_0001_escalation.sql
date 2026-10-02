-- Issue #4: SupportGenius escalation schema. Portable SQL subset only
-- (TEXT/INTEGER, no engine-specific syntax) so a Postgres set is a
-- mechanical addition later (ADR 0004).
--
-- sg_escalation_outbox and sg_escalation_inbox are **generated** from
-- cratefield-core's own helpers —
--     Outbox::new("sg_escalation_outbox").create_table_sql()
--     Inbox::new("sg_escalation_inbox").create_table_sql()
-- — and pasted verbatim, so the DDL cannot drift from what
-- `claim_due`/`complete`/`retry_later` actually query. Regenerate them
-- if core's schema changes; do not hand-edit those two blocks.

-- ---------------------------------------------------------------------------
-- sg_tickets: one escalated support ticket, one row per escalation.
--
-- Nullable columns are the ones a later stage fills in: the draft stage
-- writes title/body_markdown/severity/environment, the judge stage writes
-- verdict/judge_reasons, the file stage writes external_id/external_url,
-- and customer_question is whatever the draft stage extracted as the
-- customer's actual question. `status` and `stage` are the module's own
-- enum wire forms (snake_case); see src/model.rs.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sg_tickets (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    status TEXT NOT NULL,
    stage TEXT NOT NULL,
    transcript TEXT NOT NULL,
    title TEXT,
    body_markdown TEXT,
    severity TEXT,
    environment TEXT,
    verdict TEXT,
    judge_reasons TEXT,
    customer_question TEXT,
    external_id TEXT,
    external_url TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Deliberately NOT unique: one conversation may escalate more than once
-- (a second defect later in the same conversation is a new ticket).
CREATE INDEX IF NOT EXISTS idx_sg_tickets_tenant_conversation
    ON sg_tickets (tenant_id, conversation_id);

-- ---------------------------------------------------------------------------
-- sg_ticket_events: the audit trail. One row per stage transition and per
-- notable outcome; `detail` is JSON (NULL when an event carries nothing
-- beyond its kind). `seq` orders the trail deterministically by pipeline
-- position, not by clock: stage ordinal * 10 + index within the stage
-- (see src/model.rs `stage_seq`), so a FixedClock or two ULIDs minted in
-- the same millisecond still read in true order. Readers order by
-- (seq, at, id) — `at` disambiguates retry events that share a stage
-- band, `id` is the last-resort tiebreak.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sg_ticket_events (
    id TEXT PRIMARY KEY,
    ticket_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    at TEXT NOT NULL,
    stage TEXT NOT NULL,
    kind TEXT NOT NULL,
    detail TEXT
);

CREATE INDEX IF NOT EXISTS idx_sg_ticket_events_ticket_seq
    ON sg_ticket_events (ticket_id, seq);

-- ---------------------------------------------------------------------------
-- sg_destinations: per-tenant tracker destination. `destination` is the
-- JSON of the `Destination` port enum. **`credential_ref` is a reference,
-- never a secret**: it names the Config key the secret lives under (e.g.
-- `ESCALATION_TRACKER_CREDENTIAL`), and the secret is resolved from the
-- Config port at file-time. Storing the credential itself here would put
-- it in a table that gets exported, backed up and replicated like any
-- other row.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sg_destinations (
    tenant_id TEXT PRIMARY KEY,
    destination TEXT NOT NULL,
    credential_ref TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Generated from cratefield_core::Outbox::new("sg_escalation_outbox")
--   .create_table_sql() — do not hand-edit; regenerate if core's schema
-- changes (issue #128).
CREATE TABLE IF NOT EXISTS sg_escalation_outbox (
    id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    payload TEXT NOT NULL,
    subject TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    locked_until TEXT,
    created_at TEXT NOT NULL
);

-- Generated from cratefield_core::Inbox::new("sg_escalation_inbox")
--   .create_table_sql() — do not hand-edit; regenerate if core's schema
-- changes (issue #134).
CREATE TABLE IF NOT EXISTS sg_escalation_inbox (
    event_key TEXT PRIMARY KEY,
    seen_at TEXT NOT NULL
);
