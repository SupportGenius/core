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
| `POST /sources` | API key | `{"title"?, "text"}` or `{"title"?, "url"}`, each with an optional `external_id` → chunked and indexed; a repeat `external_id` replaces that source in place (`200`, same id), a fresh one is created (`201`) |
| `GET /sources?limit=…&after=…` | API key | the tenant's sources, keyset-paginated by id (default 50, 1..=100 per page) |
| `GET /sources/{source_id}` | API key | one source, in the list's item shape |
| `PUT /sources/{source_id}` | API key | replace a source's content and metadata, body shaped like `POST /sources` |
| `DELETE /sources/{source_id}` | API key | remove a source and its whole index; `204`, no body |
| `POST /uploads` | API key | `{"filename", "content_type", "bytes"}` → `201 {id, part_bytes, …}` |
| `PUT /uploads/{id}/parts/{n}` | API key | raw part bytes (1..=48 KiB), contiguous from 0 |
| `POST /uploads/{id}/complete` | API key | checks the parts add up, enqueues the `extract` job → `202` |
| `GET /uploads/{id}` | API key | the upload's status, `received_bytes`, and once extracted its `source_id` |
| `POST /connectors` | API key | `{"kind": "sitemap" \| "url_prefix", "url"}` or `{"kind": "github", "owner", "repo", "path_glob"?, "ref"?, "credential_ref"?}`, with optional `max_pages`/`max_bytes`/`max_depth` → `201`; the crawl runs asynchronously and re-syncs on cron |
| `GET /search?q=…&limit=…` | API key | BM25 over the tenant's own index |
| `POST /messages` | API key | `{"message", "conversation_id"?, "contact"?: {"email"}}` → one support turn |
| `GET /keys` | API key | the tenant's own API keys — `{kid, label, created_at}` each, oldest first |
| `POST /keys` | API key | `{"label"?}` → mint another API key for the tenant (shown once, like provisioning) |
| `DELETE /keys/{kid}` | API key | delete one of the tenant's own keys; `204`, or `404` for a foreign/unknown kid and `409` for the last remaining key |

### Sources

Every source answers the same item shape — `{id, title, origin, external_id,
bytes, chunk_count, updated_at}` — from the list, the single read and a
replacement. `origin` is `text` or `url`. `updated_at` starts at the
instant the source was created and moves with every replacement.

`external_id` is the caller's own key for a document (`"handbook"`, or the
URL the `{"url"}` form fetched it from — that form defaults it to the
URL). Re-`POST`ing an `external_id` the tenant already has replaces that
source **in place**: same id, same code path as `PUT`, answered `200`
instead of `201`. The id is unique per tenant, so two tenants may both say
`"handbook"`, and omitting it keeps the original behavior — every request
creates a new source.

`PUT` replaces everything: content, title, `external_id` (a body without
one clears it) and the window set. Windows are re-derived under the same
source id, and because chunk ids are content addresses of
`(tenant, source, text)`, unchanged text keeps its chunk rows —
`created_at` and postings included; only vanished and added windows are
written. A body `external_id` another source of the same tenant already
holds is answered `409` rather than written. Listing, reading, replacing
and deleting all answer `404` — the same `404` — for an id that is
missing or another tenant's.

### API keys

A tenant manages its own keys with one of them; `kid` on these routes is
each key's own id. The `sg_api_keys` row is the source of truth: a key
authenticates only while its row exists, so `DELETE /keys/{kid}` revokes
it on the very next request, no config change. `POST /keys` shows the new
key exactly once; `GET /keys` never shows key material, because none is
stored. A foreign or unknown kid is `404`; the tenant's last remaining
key is `409` — mint its replacement first. `SUPPORT_REVOKED_KIDS` stays
the emergency override, revoking a signing generation everywhere at once.

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

The source an upload produces is an ordinary text source: it lists under
`GET /sources` with `origin` `text`, no `external_id`, the upload's
filename as its title and the extracted text's size as `bytes`, and it is
replaced or deleted through `PUT`/`DELETE /sources/{id}` like any other.
`GET /uploads/{id}` keeps reporting the `source_id` it produced even after
that source is deleted — the upload row is bookkeeping, not a live link.

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

### Connectors

A connector keeps part of the index synced from a sitemap (or sitemap
index), a URL prefix, or a GitHub repository's files. Creating one stores
the crawl root and its seed fetch in one batch; the fetches are outbox
jobs, one URL each, run inline where the `Defer` port allows and on every
cron tick regardless. A re-sync re-fetches every URL the connector knows
with its stored `ETag`/`Last-Modified`: a `304` writes nothing, a changed
body replaces the source in place, and a `404`/`410` deletes it.

Every page a connector indexes is an ordinary source: it lists under
`GET /sources` with `origin` `url`, its `external_id` is the fetched URL
(`github:{owner}/{repo}:{path}` for a repository file), `bytes` is the
indexed text's size, and `updated_at` moves on every re-index while the
first index date is kept. A source the tenant indexed by hand under the
same `external_id` is the same document, so the connector adopts and
replaces it. A connector source deleted through `DELETE /sources/{id}`
stays deleted until the page next changes.

The fetch policy is an allowlist: a sitemap connector fetches only its own
scheme and host, a URL-prefix connector only URLs under its prefix, and a
GitHub connector only `api.github.com` under its owner and repo. GitHub
fetches authenticate with the Config key named by `credential_ref` — the
key's *name* is stored, never the token. Caps (clamped, not refused):

| Cap | Default | Maximum |
| --- | --- | --- |
| `max_pages` (page rows, navigation rows included) | 200 | 2000 |
| `max_bytes` (per response) | 1 MiB | 4 MiB |
| `max_depth` (link hops from the seed) | 3 | 5 |

Without the `HttpClient` port `POST /connectors` answers `503 not-ready`
and the cron re-sync does nothing.

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

**The optional contact.** A request may carry `"contact": {"email": "…"}`.
The address is validated (one `@` splitting a non-empty local part from a
non-empty domain, no whitespace, at most 254 characters) and a malformed
one is a `400` before anything is written. A valid address is handed to
the composed `HandoffSink`'s `remember_contact` and stored in the same
atomic batch as the turn, so the escalation notify stage has a recipient
for a filed ticket. Without a handoff sink (a bare `Support::new()`) the
address is simply not kept.

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

**Handing off files a ticket, in the same batch.** `Support` carries an
optional `HandoffSink` (`Support::new().with_handoff(..)`). On an
escalating turn the sink's statements — the escalation outbox row, the
ticket row, the intake audit event — are appended to the turn's own
`batch_atomic`, and only after it commits is the sink kicked to run the
escalation pipeline immediately instead of waiting for cron. The seam is a
trait, not a dependency: support never names `module-escalation`, and the
*composition*, which depends on both, adapts escalation to the port. With
`Support::new()` and no sink, a handoff marks `needs_escalation` exactly as
before and nothing files a ticket. The port declares `Tracker` and
`Mailer` optional too, because a kicked escalation run reads them and
support's `ModuleContext` is a filtered view; a deployment with no sink
never touches them.

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
- **Unspaced scripts search by bigram.** `tokenize` still splits
  space-delimited text on non-alphanumeric characters, but runs of
  Chinese, Japanese (kanji, hiragana, katakana), Korean and Thai are
  emitted as overlapping character bigrams instead of one long
  unsegmented token, so a Japanese or Thai question matches the chunks
  that contain its words. That is a retrieval fallback, not
  segmentation: a bigram matches any text sharing two adjacent
  characters, so ranking leans on BM25's idf to keep the common ones
  quiet. The tokenizer's behaviour version is stamped on every chunk
  (`sg_chunks.tokenizer_version`), and the module's `scheduled` hook —
  the venture's daily Worker cron — re-tokenizes a bounded batch of
  chunks whose stamp is older, rewriting their postings and
  `term_count`, so a tokenizer change re-claims an existing index over
  the following ticks instead of waiting for re-ingest. The sweep's body
  is `module_support::reindex_stale_chunks`, callable directly.
- **Answers know their language.** Each turn's language is detected from
  the message itself (`whatlang`, trusted only when it calls itself
  reliable), falling back to the request's first `Accept-Language`
  entry, else to none; it is stored as `sg_messages.lang` on both
  messages of the turn, named to the model as a `respond_in: <lang>`
  line, and used to render the canned clarify/handoff texts from the
  Fluent catalogs in `locales/{en,de,ja}.ftl` — English is the fallback
  for any other language. Cross-language *retrieval* (a German question
  over English documents) is out of scope until hybrid retrieval exists:
  it needs embeddings, i.e. an upstream `VectorIndex` port, and BM25
  over translated terms is not a substitute.
- **Ranking is exact on a truncated fetch.** Corpus statistics (N, average
  length, each term's df) live in the `sg_tenant_stats` and `sg_terms`
  tables, written by the same batch as the rows they describe on every
  write path — inline ingest, upload extraction, connector sync (first
  index, diff-based replace, delete on 404/410), manual replace and
  delete, and the tokenizer re-index sweep — so `bm25::rank` never
  derives them from the rows it is handed. That is what makes the
  per-term fetch safe to bound: `df` and `idf` stay exact even though
  only each term's top 128 rows by tf are read. The query path costs a
  fixed budget of rows — one stats row, at most 32 df rows, at most 32
  bounded postings fetches, the result's chunk rows — regardless of how
  large the tenant's corpus grows, and a term in more than half of a
  tenant's chunks (once it has at least 8) is dropped as a stopword
  before its postings are read. The contract is documented on `rank`
  and `postings_for`.

## Not built yet

A real `TextModel`/`Tracker` adapter in either link target: the
composition ships `UnconfiguredTextModel`/`UnconfiguredTracker` (they
answer `NotConfigured`, so `POST /messages` degrades to `503
text-model-not-configured`), and an operator wires real ones. The
escalation module's own migrations also still need collecting by the
venture's `fz migrations collect` before the ticket tables exist on a
deployment.
