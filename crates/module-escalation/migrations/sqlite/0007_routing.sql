-- Issue #24: a ticket's *kind* and per-kind routing. The drafter now
-- classifies each escalation as a defect, a support case or a lead, and a
-- tenant may route each kind to its own tracker destination. Portable SQL
-- subset only (TEXT/INTEGER, no engine-specific syntax), so the Postgres
-- set carries these same bytes (ADR 0004), exactly as 0001 and 0002.

-- Which kind of escalation this ticket is, in the module's own enum wire
-- form (`defect`/`support_case`/`lead`; see src/model.rs). Existing rows
-- predate classification and are defects — the only kind that could exist
-- before this migration — so the default is honest rather than a guess.
ALTER TABLE sg_tickets ADD COLUMN kind TEXT NOT NULL DEFAULT 'defect';

-- sg_routes: per-(tenant, kind) tracker routing. `destination` is stored
-- the way `sg_destinations.destination` is — the JSON of the `Destination`
-- port enum — except that the bare sentinel `local` means the module's own
-- built-in ticketing (see `store::LOCAL_ROUTE`); a serialized `Destination`
-- is always a JSON object or `null`, so it can never collide with it.
-- `credential_ref` is a reference, **never a secret**: it names a `secret:`
-- entry in the tenant's encrypted store (issue #23), or a Config key, and
-- is resolved at file-time; it is the empty string for a `local` route.
-- `priority_map`
-- is a JSON object mapping a severity's wire form to the tracker's
-- priority value; `{}` means the route names none.
CREATE TABLE IF NOT EXISTS sg_routes (
    tenant_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    destination TEXT NOT NULL,
    credential_ref TEXT,
    priority_map TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (tenant_id, kind)
);
