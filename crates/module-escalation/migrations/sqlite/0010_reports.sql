-- Issue #65: error and bug intake.
--
-- Appended after the embedded cratefield-secrets schema (0003-0006), the
-- follow-up migration (0008) and the observability migration (0009), whose
-- files are already collected and sha256-locked in the venture; a new
-- highest id adds only, never renumbers. Portable SQL subset only, as 0001.

-- sg_report_groups: one row per (tenant, server-computed error fingerprint)
-- — the deduplication key. A thousand reports of the same failure are one
-- issue, and this row is what says so: `count` is the running occurrence
-- total, `issue_external_id`/`issue_url` the tracker issue it filed into,
-- `status` how this module last believed the issue stood (`open`, `closed`
-- once the tracker reported a finished state, `regressed` when a newer
-- release produced the same failure again). No free text is stored beyond
-- the redacted `title`; `max_release` is the newest release the group has
-- been seen in, and is what a regression is measured against.
CREATE TABLE IF NOT EXISTS sg_report_groups (
    tenant_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    title TEXT NOT NULL,
    issue_external_id TEXT,
    issue_url TEXT,
    count INTEGER NOT NULL DEFAULT 0,
    max_release TEXT,
    status TEXT NOT NULL DEFAULT 'open',
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    PRIMARY KEY (tenant_id, fingerprint)
);

-- sg_reports: the intake audit — one row per accepted report, holding what
-- routing decisions need and nothing else. Deliberately **no** free text:
-- the message, the stack frames and the bug description are redacted on the
-- way in (see src/redact.rs) and live only in the tracker issue, so an
-- erasure or a read of this table cannot surface them.
CREATE TABLE IF NOT EXISTS sg_reports (
    id TEXT NOT NULL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    fingerprint TEXT,
    release TEXT,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL
);

-- sg_report_budget: the per-tenant, per-hour ceiling on tracker writes (the
-- config key is ESCALATION_REPORTS_MAX_ISSUES_PER_HOUR). `window_start` is
-- the hour the window opens, so a stale row simply stops being matched.
-- `spike_notified_at` is NULL until the cap is first hit in this window, and
-- is what makes the spike notification fire exactly once per window.
CREATE TABLE IF NOT EXISTS sg_report_budget (
    tenant_id TEXT NOT NULL,
    window_start TEXT NOT NULL,
    filed_count INTEGER NOT NULL DEFAULT 0,
    spike_notified_at TEXT,
    PRIMARY KEY (tenant_id, window_start)
);

CREATE INDEX IF NOT EXISTS sg_reports_by_tenant
    ON sg_reports (tenant_id, created_at);
