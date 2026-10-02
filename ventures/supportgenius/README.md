# supportgenius (the Worker)

The SupportGenius waitlist API: a stateless Cloudflare Worker standing on
the [Factory Zero] harness with the `waitlist` module composed in. Entries
live in this Worker's own D1 database; confirmation mail goes out through
Resend; a Cloudflare Turnstile widget gates the site's form.

[Factory Zero]: https://github.com/Cratefield/harness

## Routes

| Route | What it does |
| --- | --- |
| `POST /v1/waitlist` | Join the waitlist. The site form posts `product: "supportgenius"` (plus `email`, the Turnstile `token`, and optionally a referral code). |
| `GET /v1/waitlist/confirm` | Double opt-in: the link in the confirmation mail turns a pending entry into a confirmed one, then redirects the browser to the site (`status_redirect`, here `https://supportgeni.us/?token=…`). |
| `GET /v1/waitlist/status` | The entry's status as JSON (position, referrals, referral code), addressed by the status token the confirm redirect and the confirmation mail carry. |
| `GET /v1/waitlist/admin/export.csv` | The list as CSV, gated by `ADMIN_TOKEN` (Bearer auth). |
| `GET /__health` | Liveness: lists the composed modules. |
| `GET /__ready` | Readiness: reports ports that are not effectively configured. |
| `GET /__surface` | UI surface metadata for the mounted modules. |

The Worker also answers the daily cron (`23 4 * * *`, `[triggers]` in
`wrangler.toml`): the waitlist module purges pending entries past its
retention window and prunes expired mail-cooldown claims. The `#[event(scheduled)]`
handler in `src/lib.rs` is the other half.

## Deploying (a human runs these)

1. `npx wrangler d1 create supportgenius`
2. Paste the returned database UUID into `database_id` in `wrangler.toml`
   (it currently holds an obvious `REPLACE_ME...` placeholder; deploys
   fail until it is replaced).
3. Set the secrets — each with `npx wrangler secret put <NAME>`:
   - `HARNESS_SECRET` (required, at least 32 bytes; backs the harness
     `Signer` port),
   - `RESEND_API_KEY` (required; the join endpoint fails loudly without
     it — there is deliberately no no-op mailer here),
   - `TURNSTILE_SECRET` (required for production traffic; without it no
     captcha is mounted),
   - `ADMIN_TOKEN` (optional; gates the admin CSV export).
4. `npx worker-build --release` (or let `wrangler deploy` run it via
   `[build]`) and confirm `build/worker/shim.mjs` appears.
5. `npx wrangler d1 migrations apply supportgenius --remote`
6. `npx wrangler deploy`

## The site's half

The waitlist form on the site posts to `POST /v1/waitlist` with
`product: "supportgenius"` — that exact slug; the module rejects unknown
products. Flip `data-open="true"` on the form only after the secrets are
set (step 3): without the Resend key the API intentionally fails joins
rather than silently dropping the confirmation mail.

## Model tiers and judging (not wired yet)

The planned escalation flow drafts with one model and judges with a
second, selected by tier — `fast` and `strong`, the two tiers the
harness's `RoutingTextModel` defines. `fast` drafts; `strong` judges.
Neither tier is wired today, to any vendor: both ports come from the
pinned harness rev `b50c1d9` — the `Tracker` port and its GitHub and
webhook adapters ship from that same rev (#25) — and no published
`cratefield-core` release carries either (0.5.0 included). No `TextModel`
adapter for any vendor — Anthropic's included — is published at all.

The plan is to point both tiers at Anthropic once the wiring exists.
One vendor on both tiers is not an independent judge: the judge would be
reviewing the work of the same vendor's model that drafted it. Until a
second `TextModel` adapter exists to take one of the tiers, the
independent-judge property is unmet, and this caveat stays in this
README.

## Live smoke test: filing into GitHub

`module-escalation` files into GitHub Issues through
`cratefield-adapter-github-issues`. Running this end to end needs a token, a
tenant pointed at a scratch repository, and — **not yet present** — the
venture mounting the module: nothing composes `Escalation` (or the
`Webhooks` tables its `escalation.*` events need) into `SupportGenius` yet
(#21), and nothing exposes the per-tenant destination admin route (#23).
So the steps below are the contract the module already implements and tests
crate-side, not commands that run against the deployed Worker today.

1. **Token.** A GitHub PAT or fine-grained token that can create issues:
   classic needs `repo` (private repos) or `public_repo` (public); a
   fine-grained token needs *Issues: read and write* on the target
   repository. The adapter holds no token — it is sent per call as
   `Authorization: Bearer <token>` — so one adapter serves every tenant.
2. **Credential.** The tenant's `sg_destinations.credential_ref` is the
   **name of the `Config` key** the secret lives under, never the secret;
   the module's convention is `ESCALATION_TRACKER_CREDENTIAL`, resolved
   through the `Config` port at file-time. A missing key dead-letters
   (terminal), it does not retry.
3. **Destination.** Point the tenant at a scratch repo with an
   `sg_destinations` row: `destination` =
   `{"github":{"owner":"you","repo":"scratch"}}`, `credential_ref` =
   `ESCALATION_TRACKER_CREDENTIAL`. (#23 adds the admin route that sets
   this through the API; until then it is a direct row.)
4. **Observe.** The filed issue carries labels `bug` and
   `severity:<info|warning|error|critical>`, and — when
   `ESCALATION_CONVERSATION_URL` is set — a link back to
   `<base>/<conversation-id>` in the body, alongside an invisible
   `<!-- cratefield-idem: escalation:<ticket-id> -->` marker.
5. **Idempotent rerun.** The adapter searches the repo for that marker
   before creating, so a redelivered outbox row (or a re-run) finds the
   existing issue and files nothing new. A *failed* lookup fails the call
   rather than risking a duplicate.

GitHub Enterprise works by building the adapter with
`GitHubIssues::with_base(<GHE API root>)`; the default base is
`https://api.github.com`.

## Adding a module later

When a later issue composes another module into `src/lib.rs`, refresh this
directory's migrations with the venture-linked `fz`, which sees the
compiled-in harness:

```sh
cargo run --features cli --bin fz -- migrations collect
```

Commit the new `migrations/NNNN_*.sql` files it writes (they are source,
not build artifacts), then `wrangler d1 migrations apply supportgenius
--remote` on the next deploy.
