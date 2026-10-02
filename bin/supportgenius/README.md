# supportgenius (the static binary)

The site-promise binary: the same modules the Cloudflare Worker serves
(`ventures/supportgenius`), composed through one shared module list in
[`crates/composition`](../../crates/composition) so the two link targets
cannot drift. It links the modules against `cratefield-runtime-native`
(tokio) with SQLite behind the `Database` port by default — Postgres behind
the `postgres` feature — and ships as one fully static musl executable.

## Build (musl, fully static)

```sh
rustup target add x86_64-unknown-linux-musl   # plus musl-tools on Debian/Ubuntu
cargo build --release --target x86_64-unknown-linux-musl -p supportgenius-bin
# -> target/x86_64-unknown-linux-musl/release/supportgenius
```

No special `RUSTFLAGS` or linker env is needed: the musl build is fully
static out of the box. Postgres support is opt-in:

```sh
cargo build --release --target x86_64-unknown-linux-musl -p supportgenius-bin --features postgres
```

Smoke-test the result end to end (boots it on a scratch SQLite file, probes
health, joins the waitlist, and exercises the support module's
tenant/source/message chain — `outcome: "answered"` on a dev-fakes build,
`503 text-model-not-configured` on the plain one):

```sh
scripts/smoke.sh target/x86_64-unknown-linux-musl/release/supportgenius
```

## Environment

| Variable | Default | Meaning |
| --- | --- | --- |
| `HARNESS_SECRET` | **required** | Secret for the `Signer` port. Without it the harness cannot wire the port and the process exits at boot. |
| `LISTEN_ADDR` | `127.0.0.1:8080` | Socket to serve on; containers want `0.0.0.0:8080`. `--check-ready` probes this address. |
| `DATABASE_URL` | unset → SQLite at `./supportgenius.db` | `sqlite://<path>` or `sqlite::memory:` opens SQLite; `postgres://`/`postgresql://` opens Postgres **only in a `postgres`-feature build** (a clear error names the rebuild otherwise). The chosen path is logged at boot. |
| `REDIS_URL` | unset | When set, Redis backs the `RateLimiter` and `KeyValue` ports. **Unset in dev/staging, rate limiting fails open by design**: the ports stay unmounted, core resolves no limiter to "allowed", and the degradation is logged exactly once at boot — never per request. **Unset with `ENV=production` the binary refuses to boot** (issues #16/#17): the public mail path and support search/sources routes must not run without a volume ceiling. |
| `BLOB_DIR` | unset | When set, a directory store under this path backs the `Blob` port for the chunked-upload routes (issue #30): documents up to 4 MiB arrive in 48 KiB parts, extraction runs right after `complete`, and — **only when `CRONS` is set** — the daily sweep collects uploads abandoned `open` past their 24-hour window. Unset, those routes answer `503 not-ready` — reported, not faked; every other route is unaffected. |
| `CRONS` | unset → no scheduled work | Comma-separated cron expressions (UTC), the native counterpart of wrangler's `[triggers]`; each fires every module's scheduled hook. With none set, no scheduled work runs: the upload extraction backstop and abandoned-upload collection (issue #30), the waitlist retention purge, and the escalation retry sweeps are all lost — the inline drains after each request still run, so only the backstops go away. Example: `CRONS="23 4 * * *"`. |
| `RESEND_API_KEY` | unset | Production mailer (Resend). Unset, Resend answers `NotConfigured` and sends nothing — reported, never faked. |
| `ANTHROPIC_API_KEY` | unset | Production text model (issue #22): one Anthropic adapter per tier behind the harness `RoutingTextModel`. A blank value — empty or whitespace-only — counts as unset: the `TextModel` port is not mounted and `POST /v1/support/messages` answers `503 text-model-not-configured` while every other route works. On a dev-fakes build no key mounts `StubTextModel` instead, and a real key there wins over the stub. |
| `SUPPORTGENIUS_MODEL_FAST` | `claude-haiku-4-5` | Model id the fast tier calls (the support answer). Same variable name the Worker uses. |
| `SUPPORTGENIUS_MODEL_STRONG` | `claude-sonnet-5` | Model id the strong tier calls (the escalation judge, once that module is composed). Same variable name the Worker uses. |
| `MAIL_FROM` | compiled venture default | From address for outgoing mail. |
| `MAIL_REPLY_TO` | unset | Reply-To for outgoing mail. |
| `SUPPORTGENIUS_DOMAIN` | compiled venture default | Overrides the venture's domain. |
| `SUPPORTGENIUS_PUBLIC_URL` | compiled venture default | Overrides the venture's public URL. |
| `SUPPORTGENIUS_CORS_ORIGINS` | compiled venture default | Comma-separated CORS allowlist. |
| `SUPPORTGENIUS_DEV_FAKES` | unset | Set at boot, selects the dev fakes (stub mailer/captcha/text model). Requires the `dev-fakes` feature; a non-`dev-fakes` build warns and wires the real adapters. A real `ANTHROPIC_API_KEY` beats `StubTextModel` — the stub answers only when no key is set. Never serve production traffic from a dev-fakes process. |
| `FZ_APPLY_MIGRATIONS` | migrations run on boot | Set to `0`/`false`/`no`/`off` to skip the idempotent boot-time migration run (e.g. when your deploy pipeline applies them). |
| `CRONS` | the composition's schedule | Comma-separated five-field cron expressions (UTC), the native counterpart of the Worker's `[triggers] crons`. **Unset runs the compiled default** — `*/5 * * * *` (escalation outbox drain) and `23 4 * * *` (waitlist retention purge) — which the binary spawns itself from `crates/composition`. Set it to override the whole schedule (an operator's list replaces the default, it does not add to it); each expression is fanned out to every module's `scheduled`, so a module whose work must not run on a given tick is gated inside the composition, not here. An override that omits a gated expression (today `23 4 * * *`, the daily waitlist purge) refuses to boot rather than silently never running that work. |

The Turnstile captcha port mounts only when a Turnstile secret is present in
the environment (read by the adapter itself); without one, no port and the
module decides per request. The `TextModel` port follows the same rule with
`ANTHROPIC_API_KEY` (see the table above): one adapter per tier mounts only
when a non-blank key is present; with no key a dev-fakes build mounts
`StubTextModel` instead and a plain build mounts nothing. The model ids —
and the caveat that both tiers are one vendor — are documented in the
[Worker README](../../ventures/supportgenius/README.md#model-tiers-and-judging).

## Docker

```sh
# Build context is the repository root, not bin/supportgenius:
docker build -t supportgenius -f bin/supportgenius/Dockerfile .

docker run -d -p 8080:8080 \
  -e HARNESS_SECRET="$(openssl rand -hex 32)" \
  -v supportgenius-data:/data \
  supportgenius
```

The image is `gcr.io/distroless/static-debian12:nonroot` (the binary is fully
static), listens on `0.0.0.0:8080`, writes its default SQLite file to
`/data/supportgenius.db` (a nonroot-owned volume mount), and self-checks with
`/supportgenius --check-ready` — distroless has no curl, so the binary health
checks itself. `HARNESS_SECRET` must be supplied at run time; without it the
container exits at boot.

## Redis, restated for operators

Without `REDIS_URL` there are no `RateLimiter`/`KeyValue` ports, and in
dev/staging rate limiting fails open — the service stays up and simply
does not throttle. Every module call site picks `RateLimitFailure::FailOpen`
(enforced by a composition-crate test over `requires()`), so this is the
documented self-hosted contract, not an accident; the boot log says so
exactly once. **In production it is not a contract but a gap**, so
`ENV=production` with no `REDIS_URL` refuses to boot (issues #16/#17)
rather than serving the public mail and support endpoints uncapped.
Set `REDIS_URL` to get real rate limiting (and key-value storage) back.
