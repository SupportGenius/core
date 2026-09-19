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
health, joins the waitlist):

```sh
scripts/smoke.sh target/x86_64-unknown-linux-musl/release/supportgenius
```

## Environment

| Variable | Default | Meaning |
| --- | --- | --- |
| `HARNESS_SECRET` | **required** | Secret for the `Signer` port. Without it the harness cannot wire the port and the process exits at boot. |
| `LISTEN_ADDR` | `127.0.0.1:8080` | Socket to serve on; containers want `0.0.0.0:8080`. `--check-ready` probes this address. |
| `DATABASE_URL` | unset → SQLite at `./supportgenius.db` | `sqlite://<path>` or `sqlite::memory:` opens SQLite; `postgres://`/`postgresql://` opens Postgres **only in a `postgres`-feature build** (a clear error names the rebuild otherwise). The chosen path is logged at boot. |
| `REDIS_URL` | unset | When set, Redis backs the `RateLimiter` and `KeyValue` ports. **Unset, rate limiting fails open by design**: the ports stay unmounted, core resolves no limiter to "allowed", and the degradation is logged exactly once at boot (`REDIS_URL unset: RateLimiter and KeyValue ports not configured`) — never per request. |
| `RESEND_API_KEY` | unset | Production mailer (Resend). Unset, Resend answers `NotConfigured` and sends nothing — reported, never faked. |
| `MAIL_FROM` | compiled venture default | From address for outgoing mail. |
| `MAIL_REPLY_TO` | unset | Reply-To for outgoing mail. |
| `SUPPORTGENIUS_DOMAIN` | compiled venture default | Overrides the venture's domain. |
| `SUPPORTGENIUS_PUBLIC_URL` | compiled venture default | Overrides the venture's public URL. |
| `SUPPORTGENIUS_CORS_ORIGINS` | compiled venture default | Comma-separated CORS allowlist. |
| `SUPPORTGENIUS_DEV_FAKES` | unset | Set at boot, selects the dev fakes (stub mailer/captcha). Requires the `dev-fakes` feature; a non-`dev-fakes` build warns and wires the real adapters. Never serve production traffic from a dev-fakes process. |
| `FZ_APPLY_MIGRATIONS` | migrations run on boot | Set to `0`/`false`/`no`/`off` to skip the idempotent boot-time migration run (e.g. when your deploy pipeline applies them). |

The Turnstile captcha port mounts only when a Turnstile secret is present in
the environment (read by the adapter itself); without one, no port and the
module decides per request.

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

Without `REDIS_URL` there are no `RateLimiter`/`KeyValue` ports, and rate
limiting fails open — the service stays up and simply does not throttle.
Every module call site picks `RateLimitFailure::FailOpen` (enforced by a
composition-crate test over `requires()`), so this is the documented
self-hosted contract, not an accident; the boot log says so exactly once.
Set `REDIS_URL` to get real rate limiting (and key-value storage) back.
