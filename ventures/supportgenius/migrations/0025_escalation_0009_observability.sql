-- Issue #39: what the pipeline is doing, said in a way an operator can
-- query — the depth of each stage's queue and when the last drain
-- succeeded.
--
-- Appended after the follow migration (0008) like it was appended after
-- the secrets schema: the earlier ids are already collected into
-- `ventures/supportgenius/migrations` and locked by sha256, so a new
-- highest id adds a file where a renumber would rewrite history.
-- Portable SQL subset only, as 0001 and 0008 (ADR 0004), which is why
-- the postgres set carries these same bytes.

-- One row, id 1: when `Pipeline::drain` last completed without error.
-- A single row rather than a table of drains because the only question
-- asked of it is "is it still running?", and an accumulating history
-- would name nobody and answer nothing the last row does not.
CREATE TABLE IF NOT EXISTS sg_escalation_drain (
    id INTEGER PRIMARY KEY,
    last_ok_at TEXT NOT NULL
);
