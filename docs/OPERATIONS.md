# Operating SupportGenius

Two things are observable in this system without any extra plumbing: the
**escalation pipeline's** own events, and one admin JSON document. Everything
else — dashboards, alerts — is something you point at those.

Issue #39 covers the escalation module. [docs/SELF-HOSTING.md](SELF-HOSTING.md)
covers running the binary; [docs/RELEASING.md](RELEASING.md) covers deploying
it.

## Events

`crates/module-escalation/src/observe.rs` opens one span per unit of work, and
closes each with **exactly one** `info!` event carrying the same fields. That
doubling is deliberate: both shipped runtimes — the native binary's
`RedactingJson` and the Workers console subscriber — print what an *event*
carries and nothing at span close, so the event is the thing you actually
query in logs.

Two events, both at `INFO`, both safe to build a dashboard on (one per span, so
nothing double-counts):

| Message | Emitted when | Fields |
| --- | --- | --- |
| `escalation: model call` | each model call | `tier`, `tenant`, `latency_ms`, `tok_in`, `tok_out`, `outcome` |
| `escalation: pipeline stage` | each outbox record processed | `stage`, `attempt`, `tenant`, `result` |

- `tier` is `fast` (draft stage) or `strong` (judge stage); `outcome` is `ok`
  or `error`. A call whose response fails to decode is still `ok` — it cost
  tokens either way.
- `result` is `done`, `retry`, `dead_letter`, `skipped`, `rescheduled` or
  `aborted`. `retry` means a stage failed retryably with budget left, so the row
  was rescheduled by the backoff policy; `dead_letter` means it failed
  non-retryably, or exhausted `max_attempts` (default 5), or hit an unknown
  topic; `skipped` means the row was not work to do (undecodable payload, a
  duplicate drain of already-committed work, a follow row whose ticket is gone);
  `rescheduled` is follow-only and means the stage deferred its own next poll
  without consuming the row; `aborted` means a database error escaped the stage
  before its outcome was recorded, so the row was not consumed and its lease
  will bring it back. `aborted` is a database problem, not a pipeline one —
  it correlates with `last_drain_age_secs` climbing or `/__ready` failing.
- `stage` is the outbox topic the record carried — `draft`, `judge`, `file`,
  `notify` or `follow` for anything this version enqueued, and something else
  only for a row left behind by a different version of the pipeline (which
  settles `dead_letter`). `attempt` is 1-based: the first try is attempt 1.

**`tenant` is a pseudonym, never an id.** It is `subject_hash` — an HMAC keyed
from `HARNESS_SECRET`, domain-separated, first 6 bytes hex. With no
`HARNESS_SECRET` installed it collapses to `000000000000` for every subject,
which is the fail-closed answer, not a bug; install the secret.

**`tok_in`/`tok_out`, not `tokens_*`,** because field names are load-bearing:
the log redactor blanks any field whose name contains `token`, `key`,
`secret`, `password` or `authorization` (`cratefield-core/src/logging.rs`).
`tokens_in`, `input_tokens` and `output_tokens` would all arrive `[redacted]`.
Query the names as written.

## Health

```sh
curl -fsS https://api.supportgeni.us/v1/escalation/admin/health \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

No token is `401`; a token that is not the admin token is `403`. `ADMIN_TOKEN`
is unset on many deployments — the route is closed, not open, when it is
missing. A 200 body is a fixed shape, so a dashboard never has to handle a
missing key:

```json
{"topics":[{"topic":"draft","depth":3,"due":1,"oldest_due_age_secs":94}],
 "dead_letters_24h":0,"last_drain_ok_at":"2026-10-08T09:12:03Z",
 "last_drain_age_secs":121}
```

- **All five topics are always listed**, in order, even when empty. A chart that
  drops a stage when its queue empties lies about the queue having drained.
- `depth` is rows queued, leased or not. `due` is rows due now —
  `next_attempt_at <= now` and not held by another drainer's lease — the same
  predicate the claim uses, so a leased row is queued but not due.
- `oldest_due_age_secs` is the number that says a stage is stuck: a deep queue
  that is draining is fine; a due row nobody has claimed is not.
- `dead_letters_24h` counts tickets whose status is `dead_letter`, updated in
  the last 24 hours, across every stage.
- `last_drain_ok_at` is stamped after a drain finishes **without error**, and
  a sweep that dies half way leaves the previous stamp alone. That is the
  answer an operator wants: `last_drain_age_secs` is how long since the last
  drain that actually completed, or `null` if none ever has. Either drain
  stamps it — the five-minute tick, or the one-row drain an intake kicks off —
  so the stamp answers "is work still moving", not "did the cron fire". A dead
  cron with quiet intake is what `oldest_due_age_secs` catches; a dead cron
  with busy intake is not, because the kicks are doing the draining.

The drain is the five-minute tick, `*/5 * * * *` (`crates/composition`), which
the Worker runs as `[triggers] crons` and the binary as its compiled schedule.
It catches whatever a missed kick left staged.

## Alerts

| Condition | Threshold | Read from |
| --- | --- | --- |
| Stage stuck | any `oldest_due_age_secs > 1800` | `/admin/health` |
| Dead letters | `dead_letters_24h > 0` | `/admin/health` |
| Drain stale | `last_drain_age_secs > 900` | `/admin/health` (derived — three missed ticks) |
| Model errors | `outcome=error` / total `> 5%` over 15 min | `escalation: model call` events |
| Not ready | `/__ready` non-200 or not `.ok == true` | harness probe |

`/__ready` is unauthenticated and outside the tenant guards, so a probe can hit
it directly. It is a bare `SELECT 1` natively — an unmigrated database still
reports ready — and the Worker decorates it with `text_model`, `captcha` and
`rate_limiter`, each `"configured" | "missing"`.

## Where to watch

| | Cloudflare Worker | Native binary |
| --- | --- | --- |
| Events | Workers Logs (`[observability] enabled = true`) | JSON lines on stdout, via `RUST_LOG` |
| Scrape / health | external poller on `/admin/health` | the same poller |

In Workers Logs, filter on the event fields — the messages are constants, so
field filters are the useful ones:

- errors: message `escalation: model call` with `outcome = "error"`
- throughput and error rate: same message, grouped by `tier`
- a specific stage backing up: message `escalation: pipeline stage` with
  `result = "retry"` or `result = "dead_letter"`, grouped by `stage`
- one tenant misbehaving: group either by the `tenant` pseudonym — never by a raw
  tenant id, which is not in the logs

On the binary, `RUST_LOG` defaults to `info` and the lines are one JSON object
each, so they pipe straight into whatever you already run.

Either way, point an external poller at `/admin/health`. That one document is
what the first three alert rows come from, and it is the same on both targets.

## No Prometheus endpoint

There is no `/metrics` and no metrics exporter, on either target.
`cratefield-runtime-native` 0.4.0 and `cratefield-runtime-cloudflare` 0.4.0
register no metrics surface — no `prometheus`, no `opentelemetry`, not a single
counter. That is a gap in the Cratefield harness, not something this repo
papers over with a half-metrics library.

Rate `model.call` from the event fields and poll `/admin/health`. If a metrics
scrape is a hard requirement for your stack, that is an upstream issue.

## Not yet covered

Honest list of what #39 does **not** emit:

- **Rate-limit rejections.** The escalation module declares no rate-limiter port
  at all (`Mailer, Clock, IdGen, Defer, Signer, HttpClient`), so it never
  rejects on a limit and has nothing to count. A *mail* rate limit is classed
  retryable, so it surfaces as `pipeline.stage` `result = "retry"` — visible,
  but not counted as a rejection. The only 429 in the tree is the support
  widget's (`crates/module-support`), and it emits neither of these events.
- **Retry counts as a first-class number.** Retries are visible as
  `result = "retry"` events and must be counted from them; there is no counter.
- **Per-tenant aggregates.** `tenant` is on every event as a pseudonym, but
  nothing rolls it up for you.
- **Latency percentiles across stages.** `latency_ms` is on model calls only,
  not per pipeline stage.