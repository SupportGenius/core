-- module-support human-in-the-loop (issue #35), the support side: staff
-- keys, a conversation `state` recording who drives the next turn,
-- staff-role messages and reviewed sources. Portable SQL only (ADR 0004;
-- linted by `fz doctor`), the same conventions as 0001–0008: TEXT
-- columns, ISO-8601 TEXT timestamps bound from code, INTEGER flags, and
-- `tenant_id` on every table.
--
-- `state` is deliberately separate from `status`/`needs_escalation`.
-- Those record *that* a conversation was escalated and stay monotonic;
-- `state` records who answers the next customer turn right now —
-- 'bot' | 'waiting_for_human' | 'human' | 'closed'. A conversation handed
-- back to the bot keeps needs_escalation = 1 and status = 'escalated'.
--
-- `sg_api_keys.staff_id` names the person a key belongs to (NULL for a
-- customer or integration key): only a key carrying one may use the
-- staff routes.
--
-- `sg_messages.author` records which staff id wrote a 'staff' row.
--
-- `sg_sources.reviewed` marks a source written by a support agent as a
-- correction (the `save_as_answer` reply): retrieval boosts it, and the
-- provenance columns name the agent and the conversation it came from.

ALTER TABLE sg_api_keys ADD COLUMN staff_id TEXT;

ALTER TABLE sg_conversations ADD COLUMN state TEXT NOT NULL DEFAULT 'bot';
ALTER TABLE sg_conversations ADD COLUMN assignee TEXT;
-- Everything already escalated is waiting for a person under the new
-- state machine; a conversation that was not keeps the 'bot' default.
UPDATE sg_conversations SET state = 'waiting_for_human' WHERE status = 'escalated';

ALTER TABLE sg_messages ADD COLUMN author TEXT;

ALTER TABLE sg_sources ADD COLUMN reviewed INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sg_sources ADD COLUMN reviewed_by TEXT;
ALTER TABLE sg_sources ADD COLUMN reviewed_conversation_id TEXT;

CREATE INDEX IF NOT EXISTS idx_sg_conversations_tenant_state
    ON sg_conversations (tenant_id, state);
