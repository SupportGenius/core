# module-support

The SupportGenius support module: a Cratefield `Module` named `support`,
mounted at `/v1/support`. Tenants, sources, BM25 retrieval, and
conversations answered with citations. Documents larger than the inline
ceiling arrive through the chunked-upload routes (issue #30), which turn
a manual or PDF into a searchable source.

## Routes

Paths are relative to `/v1/support`. Two credentials guard them and never
mix: the harness admin token (`Authorization: Bearer $ADMIN_TOKEN`) for
the `/admin/*` routes, and a tenant API key (the `sg_…` bearer minted by
`POST /admin/tenants`) for everything a tenant does. Every guard runs
before the body is parsed. The key routes share one per-tenant budget on
the optional `RateLimiter` port.

| Route | Credential | What it does |
| --- | --- | --- |
| `POST /admin/tenants` | admin token | `{"name"}` → a tenant and its first API key (shown once) |
| `PUT /admin/tenants/{tenant_id}/settings` | admin token | `{"answer_threshold": 0.9}` → the tenant's answer threshold |
| `POST /sources` | API key | `{"title"?, "text"}` or `{"title"?, "url"}` → chunked and indexed |
| `POST /uploads` | API key | `{"filename", "content_type", "bytes"}` → `201 {id, part_bytes, …}` |
| `PUT /uploads/{id}/parts/{n}` | API key | raw part bytes (1..=48 KiB), contiguous from 0 |
| `POST /uploads/{id}/complete` | API key | checks the parts add up, enqueues the `extract` job → `202` |
| `GET /uploads/{id}` | API key | the upload's status, `received_bytes`, and once extracted its `source_id` |
| `GET /search?q=…&limit=…` | API key | BM25 over the tenant's own index |
| `POST /messages` | API key | `{"message", "conversation_id"?}` → one support turn |

### Chunked uploads

A manual or policy PDF is bigger than any `/v1/*` request body, so it
arrives in parts. Open an upload with the filename, content type and
declared size; `PUT` the bytes one part at a time into the `Blob` port
(R2 on Workers, a directory when self-hosted); then complete. The parts
are checked for contiguity against the declaration, the upload flips to
`complete`, and the `extract` job is enqueued in the same batch — the
job reads the parts back, turns the document into text (`text/plain`,
`text/markdown`, `text/html`, `application/pdf`), and indexes it through
the same path as `POST /sources`. `GET /uploads/{id}` reports
`extracted` with the `source_id` once that has run, or `failed` with the
reason a document could not be read.

Limits, and why:

| Limit | Value | Why |
| --- | --- | --- |
| Part size | ≤ 48 KiB (`part_bytes` in the open response) | the same per-request ceiling the inline form has; a part must fit the 64 KiB body cap |
| Document size | ≤ 4 MiB | far past any manual that belongs in a support index; 86 parts maximum |
| Per-tenant upload storage | 50 MiB default, `SUPPORT_UPLOAD_QUOTA_BYTES` | counts `open` and `complete` uploads only — extraction returns the bytes |
| Extracted text | ≤ 2 MiB | the index is sized for manuals; a document whose text expands past this fails the upload |
| Abandoned-upload lifetime | 24 h, then cron deletes it | an `open` upload nobody completed is storage nobody is coming back for |

The `extract` job runs inline as soon as the `202` is on its way (the
`Defer` port) and, durably, from cron either way — a deployment that
never drains it inline still gets the document indexed at the next tick.
A run that fails on infrastructure (not on the document) is retried with
the outbox's own attempt counter, five tries, and then the upload is
failed with the reason. An upload that was never completed is
garbage-collected by the same cron sweep. All of this needs the `Blob`
port; without one the upload routes answer `503 not-ready`, and every
other route is unaffected.

### `POST /messages`

The module retrieves the top 6 chunks for the message (the same BM25 path
`/search` uses). It then asks the `TextModel` at the **fast** tier for
JSON matching `{answer, citations: [{chunk_id, quote}], confidence}`. The
prompt embeds each chunk as `[chunk_id] body`. The response is:

```json
{"conversation_id": "…", "message_id": "…", "outcome": "answered",
 "answer": "…", "citations": [{"chunk_id": "…", "quote": "…"}],
 "confidence": 0.9, "needs_escalation": false}
```

`outcome` is one of:

- **`answered`**: every citation names a chunk retrieved for *this*
  request, there is at least one citation, and the confidence is at or
  above the tenant's threshold.
- **`clarify`**: anything else. The answer is a canned request to
  rephrase and `citations` is empty.
- **`handoff`**: nothing was retrieved, or the conversation has already
  had two clarify turns. The conversation is stored with
  `needs_escalation = 1` (status `escalated`). The answer is a canned
  message and `citations` is empty.

**The citation rule.** A citation naming a chunk id that was not retrieved
for this request downgrades the turn to `clarify` (or `handoff`),
whatever the confidence. That includes a chunk that exists but belongs to
another tenant. The model's raw answer and citations are still stored
(`sg_messages.model_answer`, `citations`), but only an `answered` turn
ever returns them.

**Escalation is sticky.** Once a conversation needs escalation, an
answered follow-up does not clear it. Only an escalating turn writes the
flag, so this holds even for a turn running in parallel with the handoff.
The clarify budget is weaker: two parallel turns on one conversation read
the same count, so each may spend the last clarify. Messages are ordered by
`seq` (0, 1, 2, … per conversation). Parallel turns can also produce
duplicate `seq` values, because nothing makes them unique.

**The answer threshold** defaults to **0.60**
(`module_support::DEFAULT_ANSWER_THRESHOLD`). A tenant overrides it with
`PUT /admin/tenants/{tenant_id}/settings` (0.0..=1.0, stored as a whole
percentage in `sg_tenant_settings`). There is no deployment-wide knob.

**Failures write nothing.** The conversation, both messages and the
clarify budget are written in one `batch_atomic`, and only after the model
has answered and the turn has been decided.

| Failure | Response |
| --- | --- |
| No `TextModel` port in the runtime `Ports`, or the model reports `NotConfigured` | `503 text-model-not-configured` |
| `Transient` | `503 text-model-unavailable`, with `Retry-After` (the provider's pause, else 2 s) |
| Refused, transport failure, unparseable or wrong-shape reply | `502 text-model-invalid-answer` |
| Unknown or another tenant's `conversation_id` | `404`, before the model is called |
| Empty or over 4000-character message | `400 validation-failed` |

The model arrives through the runtime's `Ports`
(`Port::TextModel`, declared optional): the harness hands whatever it
resolved to the module's router, and nothing about the model is passed
through a builder. It is the `TextModel` port of `cratefield-core`,
pinned by the workspace's single `cratefield-*` git rev (see the root
`Cargo.toml`).

## Retrieval, and its constraints on purpose

- `chunk`: `tokenize` (lowercased alphanumeric terms, the one tokenizer
  that ingest and query share) and `Chunker` (overlapping word windows with
  stable, content-addressed chunk ids).
- `bm25`: Okapi BM25 ranking over the `sg_postings` rows fetched for the
  query's terms, scoped to the tenant.

- **BM25, not vectors.** Portable SQL forbids FTS5 and pgvector
  (ADR 0004, linted by `fz doctor`), and Cloudflare D1 has no vector
  type. Retrieval is lexical only: no embeddings, no semantic matching,
  no reranking.
- **≤ 48 KiB of text per ingest; 4 MiB per uploaded document.** The
  `/v1/*` body cap leaves no room for more inline, and URL ingest
  truncates to the same ceiling. A larger manual or PDF arrives through
  the upload routes above, in parts.
- **No CJK segmentation.** `tokenize` splits on non-alphanumeric
  characters, so scripts written without spaces (Chinese, Japanese,
  Thai) are indexed as long unsegmented runs and search poorly. This is
  a known limitation, not an oversight.
- **Ranking trusts the caller.** `bm25::rank` computes `df` from the
  postings it is given, so a `LIMIT` on the SQL that fetches them silently
  skews every idf. The contract is documented on `rank`.

## Not built yet

Source deletion (an uploaded document that was replaced can be superseded
by uploading the new version, but the old source stays indexed), and
composing module-escalation so that a handoff also files an escalation in
the same batch.
