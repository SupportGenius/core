# module-support

The SupportGenius support module: tenants, sources, retrieval,
conversations, answers with citations — eventually a Cratefield `Module`
named `support`.

## Built so far

Only the retrieval core, as pure Rust with no I/O, so it runs unchanged
in a Cloudflare Worker isolate and in `cargo test`:

- `chunk` — `tokenize` (lowercased alphanumeric terms, the one tokenizer
  shared by ingest and query) and `Chunker` (overlapping word windows
  with stable, content-addressed chunk ids, deduplicated by id so
  repeated boilerplate is indexed once).
- `bm25` — Okapi BM25 ranking over the `sg_postings` rows the caller
  fetched for the query's terms, tenant-scoped.

## v1 constraints, on purpose

- **BM25, not vectors.** Portable SQL forbids FTS5 and pgvector
  (ADR 0004, linted by `fz doctor`), and Cloudflare D1 has no vector
  type. Retrieval is lexical only: no embeddings, no semantic matching,
  no reranking.
- **≤ 48 KiB of text per ingest.** The ingest path (not built yet)
  rejects larger documents rather than truncating them; the chunker
  assumes the cap holds.
- **No CJK segmentation.** `tokenize` splits on non-alphanumeric
  characters, so scripts written without spaces (Chinese, Japanese,
  Thai) are indexed as long unsegmented runs and search poorly. Known
  limitation, not an oversight.
- **Ranking trusts the caller.** `bm25::rank` computes `df` from the
  postings it is handed; a `LIMIT` on the SQL that fetches them silently
  skews every idf. The contract is documented on `rank`.

## Not built yet

The `Module` impl, tenants and sources, fetching, conversations,
answers with citations, the D1 migrations for `sg_chunks` and
`sg_postings`. A later issue adds them.
