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
| `GET /__ready` | Readiness (the harness's DB probe), with this venture's `text_model`, `captcha` and `rate_limiter` fields added by the Worker itself, each `"configured"` or `"missing"`: `text_model` when `ANTHROPIC_API_KEY` is set, `captcha` when `TURNSTILE_SECRET` is set, `rate_limiter` when the `RATE_LIMITER` binding resolves. A missing port is a degradation, not unreadiness — the status code stays the harness's. |
| `GET /__surface` | UI surface metadata for the mounted modules. |

The Worker also answers the daily cron (`23 4 * * *`, `[triggers]` in
`wrangler.toml`): the waitlist module purges pending entries past its
retention window and prunes expired mail-cooldown claims; the support
module re-indexes chunks written by an older tokenizer, drains any
leftover upload `extract` jobs, collects uploads abandoned before
completion, and re-syncs its connectors' sources (issue #29; there is no
per-connector schedule to configure — the module's one `scheduled` hook
runs every sweep on the same trigger). The `#[event(scheduled)]` handler in
`src/lib.rs` is the other half.

## Deploying (a human runs these)

1. `npx wrangler d1 create supportgenius`
2. Paste the returned database UUID into `database_id` in `wrangler.toml`
   (it currently holds an obvious `REPLACE_ME...` placeholder; deploys
   fail until it is replaced).
3. `npx wrangler r2 bucket create supportgenius-uploads` — the bucket the
   chunked-upload routes (`POST /v1/support/uploads`) store document
   parts in, bound as `BLOB` in `wrangler.toml`.
4. Set the secrets — each with `npx wrangler secret put <NAME>`:
   - `HARNESS_SECRET` (required, at least 32 bytes; backs the harness
     `Signer` port),
   - `RESEND_API_KEY` (one of this or `OWLPOST_API_KEY` is required; with
     neither the join answers `mail-not-configured` — there is
     deliberately no no-op mailer here),
   - `OWLPOST_API_KEY` (optional; when set, mail goes through Owlpost and
     this key takes precedence over `RESEND_API_KEY`. Set
     `OWLPOST_BASE_URL` to point at a self-hosted instance; verify
     `send.supportgeni.us` in Owlpost before dropping the Resend key),
   - `TURNSTILE_SECRET` (required for production traffic; without it no
     captcha is mounted),
   - `ANTHROPIC_API_KEY` (optional; mounts the `TextModel` port — see
     [Model tiers](#model-tiers-and-judging) below. Without it
     `POST /v1/support/messages` answers
     `503 text-model-not-configured` and everything else works),
   - `ADMIN_TOKEN` (optional; gates the admin CSV export).
5. `npx worker-build --release` (or let `wrangler deploy` run it via
   `[build]`) and confirm `build/worker/shim.mjs` appears.
6. `npx wrangler d1 migrations apply supportgenius --remote`
7. `npx wrangler deploy`

## The site's half

The waitlist form on the site posts to `POST /v1/waitlist` with
`product: "supportgenius"` — that exact slug; the module rejects unknown
products. Flip `data-open="true"` on the form only after the secrets are
set (step 3): without the Resend key the API intentionally fails joins
rather than silently dropping the confirmation mail.

## Model tiers and judging

Both link targets mount the `TextModel` port (issue #22) the same way:
the `ANTHROPIC_API_KEY` secret (a blank value — empty or whitespace-only —
counts as unset) buys one [Anthropic] adapter per tier behind the
harness's `RoutingTextModel`. No key, no port — the port is not mounted
at all (there is no keyless adapter pretending), and
`POST /v1/support/messages` answers `503 text-model-not-configured` while
every other route works; the dev-fakes binary below mounts its stub
instead. `/__ready` reports the port as
`"text_model": "configured"` or `"missing"` (the Worker decorates the
harness's own body with `text_model`, `captcha` and `rate_limiter`; the
binary's `/__ready` is the harness's unchanged `{"ok":true}` — the native
runtime offers no hook for the venture to annotate it).

[Anthropic]: https://docs.anthropic.com/en/api/messages

| Target | Tier | Vendor | Model id | Configured with | Default |
| --- | --- | --- | --- | --- | --- |
| Cloudflare Worker | fast | Anthropic | `claude-haiku-4-5` | `SUPPORTGENIUS_MODEL_FAST` var (`wrangler.toml` `[vars]`, overridable at deploy) | `claude-haiku-4-5` |
| Cloudflare Worker | strong | Anthropic | `claude-sonnet-5` | `SUPPORTGENIUS_MODEL_STRONG` var (`wrangler.toml` `[vars]`, overridable at deploy) | `claude-sonnet-5` |
| Self-hosted binary | fast | Anthropic | `claude-haiku-4-5` | `SUPPORTGENIUS_MODEL_FAST` env var | `claude-haiku-4-5` |
| Self-hosted binary | strong | Anthropic | `claude-sonnet-5` | `SUPPORTGENIUS_MODEL_STRONG` env var | `claude-sonnet-5` |
| Dev-fakes binary | both | none (stub) | — | `SUPPORTGENIUS_DEV_FAKES=1` on a `--features dev-fakes` build, **and** no `ANTHROPIC_API_KEY` | — |

The single secret `ANTHROPIC_API_KEY` — a Worker secret or the binary's
environment — turns the port on for both tiers at once; there is no
per-tier key. On a dev-fakes build a real key beats the stub: the stub
answers only when no key is set. The stub (`bin/supportgenius`
`src/dev_fakes.rs`) cites whatever the turn actually retrieved, so it
exercises the module's real citation check and threshold decision — and
it is labelled `stub` in every completion.

**Which stage uses which tier today.** The support answer is one
fast-tier prompt: retrieve, ask, decide (`crates/module-support`
`messages.rs`). The escalation module drafts with the fast tier and
judges with the strong tier (`crates/module-escalation` `pipeline.rs`),
but it is **not composed** into either link target yet — the module list
(`crates/composition`) carries the waitlist and support modules only — so
today the strong tier is wired but idle.

**The judge is not independent, and that is a gap, not a choice.** Both
tiers are the same vendor (Anthropic), so the strong-tier judge would be
reviewing the work of the same vendor's model that drafted it. A second
vendor needs a second `TextModel` adapter, which does not exist in the
harness's pin set — an upstream adapter gap, not a task this repo can
close — so the caveat stands until one is published and the tiers are
split across vendors.

### Cost ceiling

Each stage carries an explicit output ceiling in its own prompt:

| Stage | Tier | `Prompt::max_tokens` | Where |
| --- | --- | --- | --- |
| Support answer | fast | `1024` (`MAX_OUTPUT_TOKENS`) | `crates/module-support/src/messages.rs` |
| Escalation draft | fast | `1024` (the harness `Prompt` default) | `crates/module-escalation/src/pipeline.rs` |
| Escalation judge | strong | `1024` (the harness `Prompt` default) | `crates/module-escalation/src/pipeline.rs` |

The input side is bounded by construction: a support turn retrieves at
most 6 chunks (`TOP_K` in `messages.rs`), each from a source capped at
48 KiB, into a message capped at 4000 characters. There is **no**
per-tenant spend budget yet: the per-tenant cost/budget ceiling is issue
#20, which has not landed on `main` — the only levers today are the
per-route rate limit and the token ceilings above.

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
