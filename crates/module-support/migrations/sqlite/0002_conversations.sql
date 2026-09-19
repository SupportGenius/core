-- Support v1's own schema: conversations, their messages, and the
-- per-tenant answer threshold. Owned by issue #3.
--
-- Confidence is stored as an integer percentage on purpose (0..100):
-- an INTEGER renders identically on D1, SQLite and Postgres, where a
-- float would raise rounding and format questions on every engine that
-- touches the row. One decimal place was never enough to matter — the
-- threshold compares at percentage grain.
CREATE TABLE sg_conversations (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  status TEXT NOT NULL,              -- 'open' | 'escalated'
  needs_escalation INTEGER NOT NULL, -- 0 | 1
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX idx_sg_conversations_tenant ON sg_conversations (tenant_id, updated_at);

CREATE TABLE sg_messages (
  id TEXT PRIMARY KEY,
  conversation_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  role TEXT NOT NULL,                -- 'user' | 'assistant'
  body TEXT NOT NULL,                -- what the user was actually shown
  -- assistant only: what the model actually said. `body` is what was
  -- published; for a downgraded turn (clarify/handoff) the two
  -- deliberately differ, and a future transcript endpoint reads `body` by
  -- default so an unfounded answer cannot leak out through it.
  model_answer TEXT,
  outcome TEXT,                      -- assistant only: 'answered' | 'clarify' | 'handoff'
  confidence_pct INTEGER,            -- assistant only: 0..100, the model's confidence rounded
  citations TEXT,                    -- assistant only: JSON array of {chunk_id, quote}
  created_at TEXT NOT NULL
);
CREATE INDEX idx_sg_messages_conversation ON sg_messages (conversation_id, created_at);

CREATE TABLE sg_tenant_settings (
  tenant_id TEXT PRIMARY KEY,
  answer_threshold_pct INTEGER NOT NULL,
  updated_at TEXT NOT NULL
);
