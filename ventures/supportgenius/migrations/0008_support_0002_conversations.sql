-- module-support v1 schema: conversations, their messages, and the
-- per-tenant answer threshold. Portable SQL only (ADR 0004; linted by
-- `fz doctor`), the same conventions as 0001: TEXT ULID ids, ISO-8601
-- TEXT timestamps bound from code, INTEGER flags and counters, and
-- `tenant_id` on every table because every query filters on it.
--
-- Confidence and threshold are stored as integer percentages (0..100) on
-- purpose: an INTEGER renders identically on D1, SQLite and Postgres,
-- where a float would raise rounding and format questions on every engine
-- that touches the row. The threshold compares at percentage grain.

CREATE TABLE IF NOT EXISTS sg_conversations (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    -- 'open' | 'escalated'; 'escalated' iff needs_escalation = 1. Both
    -- are monotonic: only an escalating turn writes them, so no later
    -- (or concurrent) turn can clear them.
    status TEXT NOT NULL,
    needs_escalation INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sg_messages (
    id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    -- 'user' | 'assistant'
    role TEXT NOT NULL,
    -- The message's position in its conversation, from 0: a turn's user
    -- message is the conversation's prior message count and its assistant
    -- reply the next. The order of a conversation is this, never
    -- created_at (both messages of a turn share one second-grain instant)
    -- nor id (ULIDs are not monotonic within a millisecond).
    seq INTEGER NOT NULL,
    -- What the user was actually shown.
    body TEXT NOT NULL,
    -- Assistant only: what the model actually said. For a downgraded turn
    -- (clarify/handoff) `body` is the canned message and this is the
    -- model's own answer, kept out of every user-facing path.
    model_answer TEXT,
    -- Assistant only: 'answered' | 'clarify' | 'handoff'.
    outcome TEXT,
    -- Assistant only: the model's confidence, rounded to 0..100.
    confidence_pct INTEGER,
    -- Assistant only: the model's raw citations, a JSON array of
    -- {chunk_id, quote}, stored whatever the outcome.
    citations TEXT,
    created_at TEXT NOT NULL
);

-- One row per tenant that has overridden the default answer threshold
-- (0.60, `module_support::DEFAULT_ANSWER_THRESHOLD`).
CREATE TABLE IF NOT EXISTS sg_tenant_settings (
    tenant_id TEXT PRIMARY KEY,
    answer_threshold_pct INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sg_conversations_tenant ON sg_conversations (tenant_id, updated_at);
CREATE INDEX IF NOT EXISTS idx_sg_messages_conversation ON sg_messages (tenant_id, conversation_id, seq);
