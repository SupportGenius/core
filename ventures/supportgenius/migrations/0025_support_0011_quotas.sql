-- module-support quotas (issue #20). Same conventions as 0001-0010:
-- portable SQL only (ADR 0004; linted by `fz doctor`), TEXT ids, TEXT
-- RFC 3339 UTC timestamps bound from code, and NULL meaning "unlimited"
-- on every limit column.
--
-- Plans are **admin-set**: no route writes these tables, an operator
-- sets them by SQL, and the module only ever reads them.

CREATE TABLE IF NOT EXISTS sg_plans (
    plan_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    conversations_per_month INTEGER NULL
        CHECK (conversations_per_month IS NULL OR conversations_per_month >= 0),
    -- Recorded, not yet enforced: billing reads them, a turn does not.
    source_bytes INTEGER NULL CHECK (source_bytes IS NULL OR source_bytes >= 0),
    connectors INTEGER NULL CHECK (connectors IS NULL OR connectors >= 0),
    escalations_per_month INTEGER NULL
        CHECK (escalations_per_month IS NULL OR escalations_per_month >= 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Which plan a tenant is on, and its **own** ceiling on daily model
-- tokens. The ceiling lives here rather than on the plan because it is
-- the one limit an engineer tunes per tenant against a runaway
-- customer; it overrides the module default and applies whether or not
-- the tenant has a plan row, so a tenant with no plan is still bounded.
CREATE TABLE IF NOT EXISTS sg_tenant_plan (
    tenant_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL,
    daily_token_ceiling INTEGER NULL
        CHECK (daily_token_ceiling IS NULL OR daily_token_ceiling >= 0),
    updated_at TEXT NOT NULL
);

-- The usage meter, generated verbatim from
-- `cratefield_core::Usage::new("sg_usage").create_table_sql()`.
-- `subject` is the tenant, `meter` is 'conversations' or 'model_tokens',
-- `period_start` the window's first instant in RFC 3339 UTC and `used`
-- the BIGINT total spent in it. The primary key is what lets the
-- guarded upsert increment atomically under concurrency.
CREATE TABLE IF NOT EXISTS sg_usage (
    subject TEXT NOT NULL,
    meter TEXT NOT NULL,
    period_start TEXT NOT NULL,
    used BIGINT NOT NULL,
    PRIMARY KEY (subject, meter, period_start)
);
