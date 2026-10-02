-- module-support widget settings: the per-tenant origin allowlist the web
-- widget's CORS-simple routes answer against. Portable SQL only (ADR 0004;
-- linted by `fz doctor`), the same conventions as 0001-0003.
--
-- `widget_origins` is a JSON array of normalized origin strings
-- (`["https://support.example"]`) as TEXT: a column per allowed origin
-- would need a child table and a second read per request for a list that
-- is at most a handful of entries, and a JSON TEXT column renders and
-- round-trips identically on D1, SQLite and Postgres. NULL — the default
-- for every tenant that has not run the admin route — means the widget is
-- refused outright: a tenant opts in deliberately or not at all. The
-- array is normalized at write time (lowercase host, default ports
-- stripped, no path), so the request-time comparison is exact string
-- membership.

ALTER TABLE sg_tenant_settings ADD COLUMN widget_origins TEXT;
