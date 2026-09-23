# module-support

The SupportGenius support module: a Cratefield `Module` named `support`,
mounted at `/v1/support`. Tenants, sources, BM25 retrieval, and
conversations answered with citations.

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
| `GET /search?q=…&limit=…` | API key | BM25 over the tenant's own index |
| `POST /messages` | API key | `{"message", "conversation_id"?}` → one support turn |

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
| No model given to `Support::new().text_model(..)`, or the model reports `NotConfigured` | `503 text-model-not-configured` |
| `Transient` | `503 text-model-unavailable`, with `Retry-After` (the provider's pause, else 2 s) |
| Refused, transport failure, unparseable or wrong-shape reply | `502 text-model-invalid-answer` |
| Unknown or another tenant's `conversation_id` | `404`, before the model is called |
| Empty or over 4000-character message | `400 validation-failed` |

The model is wired with `Support::new().text_model(Arc<dyn TextModel>)`.
The port is the shared `text-model` crate, a local mirror of the harness
port until core publishes one.

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
- **≤ 48 KiB of text per ingest.** The `/v1/*` body cap leaves no room
  for more, and URL ingest truncates to the same ceiling.
- **No CJK segmentation.** `tokenize` splits on non-alphanumeric
  characters, so scripts written without spaces (Chinese, Japanese,
  Thai) are indexed as long unsegmented runs and search poorly. This is
  a known limitation, not an oversight.
- **Ranking trusts the caller.** `bm25::rank` computes `df` from the
  postings it is given, so a `LIMIT` on the SQL that fetches them silently
  skews every idf. The contract is documented on `rank`.

## Not built yet

Source deletion, and composing module-escalation so that a handoff also
files an escalation in the same batch.
