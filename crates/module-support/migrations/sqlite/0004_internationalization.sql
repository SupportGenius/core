-- module-support internationalization: the columns retrieval and the
-- message turn need to work across languages. The same portable
-- conventions as 0001-0003 (ADR 0004; linted by `fz doctor`): TEXT
-- columns, INTEGER counters, `ALTER TABLE ADD COLUMN` with a constant
-- default, so the change applies identically to SQLite, Postgres and D1
-- in one migration.
--
-- `tokenizer_version` is the watermark the scheduled re-index stands on.
-- Version 1 is the original tokenizer, which indexed a run of Chinese,
-- Japanese, Korean or Thai as one long unsegmented token; version 2
-- indexes such runs as overlapping character bigrams. Rows written
-- before this migration were current when they were written, so they
-- start stamped 1 and the scheduled job re-tokenizes them from their
-- stored text a bounded batch at a time; new ingests write the current
-- version from code, never this default.
--
-- `lang` is the language one message turn was conducted in, as a BCP-47
-- primary tag (`de`, `ja`) — detected from the user's words, or from
-- their Accept-Language header when detection is unsure, and written on
-- both messages of the turn. Nullable: a turn with neither signal has no
-- language, and rows from before this migration read NULL.

ALTER TABLE sg_chunks ADD COLUMN tokenizer_version INTEGER NOT NULL DEFAULT 1;

ALTER TABLE sg_messages ADD COLUMN lang TEXT;
