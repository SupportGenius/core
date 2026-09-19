-- The minimum retrieval table support v1 needs to be testable: chunks of
-- the tenant's knowledge sources, one row per chunk. This is the piece
-- support v0 (issue #2) really owns — when v0 lands with sources and BM25
-- retrieval, this migration is expected to be dropped or folded into v0's
-- own schema. Until then it exists so the retrieval seam
-- (`store::top_chunks`) has something to rank and the route has something
-- to cite.
CREATE TABLE sg_chunks (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  source_id TEXT NOT NULL,
  body TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX idx_sg_chunks_tenant ON sg_chunks (tenant_id);
