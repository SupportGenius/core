# Self-hosting SupportGenius

`supportgenius` is the whole SupportGenius API as **one static Linux binary**:
the same modules, routes and secrets the Cloudflare Worker serves, composed
through one shared module list (`crates/composition`) so the two cannot drift.
State is SQLite (default) or Postgres; Redis backs rate limiting.

## Quick start (fresh Linux VM, x86_64 or arm64)
```sh
# 1. Download and verify the release (aarch64-unknown-linux-musl on arm64).
VERSION=0.1.0
TARGET=x86_64-unknown-linux-musl
BASE=https://github.com/SupportGenius/core/releases/download/v$VERSION
curl -fLO $BASE/supportgenius-$TARGET.tar.gz
curl -fLO $BASE/supportgenius-$TARGET.tar.gz.sha256
sha256sum -c supportgenius-$TARGET.tar.gz.sha256   # -> supportgenius-...: OK
tar xzf supportgenius-$TARGET.tar.gz
sudo install -m 0755 supportgenius /usr/local/bin/supportgenius
sudo useradd --system --home /var/lib/supportgenius --shell /usr/sbin/nologin supportgenius
sudo install -d -o supportgenius -g supportgenius /var/lib/supportgenius /var/lib/supportgenius/blobs
sudo apt-get update && sudo apt-get install -y redis-server
sudo systemctl enable --now redis-server
# Config (HARNESS_SECRET >= 32 bytes; it signs confirm/status links):
sudo tee /etc/supportgenius.env >/dev/null <<EOF
ENV=production
HARNESS_SECRET=$(openssl rand -hex 32)
DATABASE_URL=sqlite:///var/lib/supportgenius/supportgenius.db
REDIS_URL=redis://127.0.0.1:6379
BLOB_DIR=/var/lib/supportgenius/blobs
TURNSTILE_SECRET=REPLACE_WITH_YOUR_WIDGET_SECRET
TURNSTILE_HOSTNAME=supportgeni.us
RESEND_API_KEY=REPLACE_WITH_YOUR_RESEND_KEY
EOF
sudo chmod 600 /etc/supportgenius.env
sudo tee /etc/systemd/system/supportgenius.service >/dev/null <<'EOF'
[Unit]
After=network-online.target redis-server.service
[Service]
User=supportgenius
WorkingDirectory=/var/lib/supportgenius
EnvironmentFile=/etc/supportgenius.env
ExecStart=/usr/local/bin/supportgenius
Restart=on-failure
ProtectSystem=strict
ReadWritePaths=/var/lib/supportgenius
[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload && sudo systemctl enable --now supportgenius
curl -fsS http://127.0.0.1:8080/__ready      # -> 200 {"ok":true}
curl -fsS http://127.0.0.1:8080/__health     # -> env, ports, module list
```

Migrations apply at boot, so first start schemes the SQLite file;
`--check-ready` (the Docker `HEALTHCHECK`) GETs `/__ready` and exits 0/1.

## Environment
Production-required (all modes unless noted). The Worker reads the same
variables; only the injection differs. Every knob is in
[`bin/supportgenius/README.md`](../bin/supportgenius/README.md).

| Variable | Required when | Meaning |
| --- | --- | --- |
| `HARNESS_SECRET` | always | Signs confirm/status links. >= 32 bytes. |
| `REDIS_URL` | `ENV=production` | Backs `RateLimiter` + `KeyValue`; production **refuses to boot** without it. |
| `TURNSTILE_SECRET` | `ENV=production`, for `/v1/*` | Without it the production gate answers `503 not-production-ready` to every `/v1/*`. |
| `TURNSTILE_HOSTNAME` | with the above | Expected widget hostname; without it the captcha is present but not *effectively configured*, so the gate still refuses `/v1/*`. |
| `RESEND_API_KEY` | to actually send mail | Unset, no confirmation mail is sent. |
| `ENV` | — | Unset/blank = development. `production` turns on the gates above. |

Everything else is optional with a working default: `LISTEN_ADDR`,
`DATABASE_URL` (SQLite `./supportgenius.db`), `BLOB_DIR` (unset = uploads
answer `503 not-ready`), `ANTHROPIC_API_KEY`, `MAIL_FROM`/`MAIL_REPLY_TO`,
`SUPPORTGENIUS_MODEL_FAST`/`STRONG`, `ADMIN_TOKEN`, `CRONS` (the compiled
schedule), `FZ_APPLY_MIGRATIONS`, `SUPPORTGENIUS_DOMAIN`/`PUBLIC_URL`/`CORS_ORIGINS`,
`TRUSTED_PROXY_HEADERS`, `RATE_LIMIT_MAX`/`_PERIOD_SECS`.

## SQLite vs Postgres
The release tarball is **SQLite-only** (`release.yml` builds without the
`postgres` feature); SQLite needs no service and is the default. Postgres
means building from source, then pointing `DATABASE_URL` at it:

```sh
cargo build --locked --release --target x86_64-unknown-linux-musl \
  -p supportgenius-bin --features postgres
```

A `postgres://` URL against a non-`postgres` build fails at boot.

## Redis
`REDIS_URL` mounts the `RateLimiter` and `KeyValue` ports. Unset in
development/staging they stay unmounted and rate limiting **fails open** by
design (logged once at boot). In production that leaves the public join and
support routes uncapped, so `ENV=production` without it refuses to boot.

## Docker
Build from the repository root (not `bin/supportgenius`):

```sh
docker build -t supportgenius -f bin/supportgenius/Dockerfile .
docker run -d --name supportgenius -p 8080:8080 \
  --env-file /etc/supportgenius.env \
  -e DATABASE_URL=sqlite:///data/supportgenius.db \
  -v supportgenius-data:/data supportgenius
```

Distroless static/nonroot on `0.0.0.0:8080`, SQLite at
`/data/supportgenius.db`, health-checked by `--check-ready`. Redis is
separate; point `REDIS_URL` at it.

## TLS
Terminate TLS at a reverse proxy (Caddy, nginx) pointing at `127.0.0.1:8080`; list the client-IP header you forward in `TRUSTED_PROXY_HEADERS`.

## Backups
- **SQLite.** The adapter uses the default rollback journal (no WAL), so copy
  with `sqlite3 supportgenius.db ".backup '/backup/sg.db'"` (safe while
  running) or stop the service and copy the file. Never copy a live `.db`.
- **Postgres** `pg_dump`; **`BLOB_DIR`** holds uploaded document parts the
  database does not — copy it too.
- **Secrets.** Keep `HARNESS_SECRET`: it signs confirm/status links, so losing
  it invalidates every outstanding one (`HARNESS_SECRET_PREVIOUS` keeps old
  links verifying across a rotation; `HARNESS_SECRET_REVOKED` burns ids).
  Losing the escalation KEK (`HARNESS_KEK_V<n>`) makes stored tracker
  credentials unreadable.

## Upgrades
Replace the binary and restart; migrations are idempotent, applied at boot:

```sh
sudo install -m 0755 supportgenius /usr/local/bin/supportgenius
sudo systemctl restart supportgenius
```

Back up first. To apply migrations deliberately, set `FZ_APPLY_MIGRATIONS=0`,
boot once with it on to migrate, then run with it off. Skipping does not fail
`/__ready` — that probe is a bare `SELECT 1`, so an unmigrated database still
reports ready while its tables do not exist.

## Worker-only vs binary-only
The module list and routes are identical; the backing infrastructure differs:

| Concern | Cloudflare Worker | This binary |
| --- | --- | --- |
| Database | D1 binding `DB`; `wrangler d1 migrations apply` | SQLite (default) or Postgres; migrations at boot |
| Blob (uploads) | R2 bucket `BLOB` | `BLOB_DIR` directory store |
| Rate limiting | bindings `RATE_LIMITER`, `VISITOR_RATE_LIMITER` | Redis (`REDIS_URL`) |
| Scheduled work | `[triggers] crons` + `scheduled` handler | `CRONS` env, or the compiled schedule |
| Secrets | `wrangler secret put`, `[vars]` | process environment / `EnvironmentFile` |
| KMS (escalation) | `HARNESS_KEK_CURRENT` Worker-secret ring | that ring from env, or `ESCALATION_KMS_KEY_FILE` (dev) |
| Self-check | curl | `--check-ready` (no curl in distroless; the Worker's `fetch` decorates its `/__ready` with `text_model`, `captcha` and `rate_limiter`, whereas the harness probe here is not — this binary's `/__ready` stays the bare `{"ok":true}`) |
