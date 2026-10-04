# Releasing SupportGenius

One tag ships two things: the static binary (a GitHub release) and the
Cloudflare Worker (`deploy.yml`). The wrangler steps live in
[`ventures/supportgenius/README.md`](../ventures/supportgenius/README.md#deploying-a-human-runs-these);
this file is the CI/release split around them.

## 1. Cutting a release

```sh
# `version` lives in the workspace Cargo.toml; it is 0.1.0, so v0.1.0 needs no bump.
git switch main && git pull            # main must be green in CI first
git tag -a v0.1.0 -m "SupportGenius v0.1.0"
git push origin v0.1.0
```

`.github/workflows/release.yml` then builds `x86_64-unknown-linux-musl` on
`ubuntu-latest` and `aarch64-unknown-linux-musl` on `ubuntu-24.04-arm`, asserts
each binary is statically linked, runs `scripts/smoke.sh` on each, packages
`supportgenius-<target>.tar.gz` plus a `.sha256` (produced by `sha256sum` in the
workflow dir), and a single publish job attaches all four files to the GitHub
release.

Verify the release:

```sh
gh release view v0.1.0                 # lists four assets
BASE=https://github.com/SupportGenius/core/releases/download/v0.1.0
TARGET=x86_64-unknown-linux-musl
curl -fLO $BASE/supportgenius-$TARGET.tar.gz{,.sha256}
sha256sum -c supportgenius-$TARGET.tar.gz.sha256
tar xzf supportgenius-$TARGET.tar.gz && scripts/smoke.sh ./supportgenius
```

## 2. The same tag deploys the Worker

`.github/workflows/deploy.yml` (both jobs pin `cloudflare/wrangler-action@v3`
to `wranglerVersion: "4"`, so the action's stale default wrangler is never
used):

- push to `main` → **staging**: the `staging` GitHub environment deploys
  Worker `supportgenius-staging` at `https://api-staging.supportgeni.us`
  (`wrangler.toml` `[env.staging]`, `wrangler deploy --env staging`);
- `v*` tag → **production**: the `production` GitHub environment (required
  reviewers, so a human gates it) deploys the top-level config at
  `https://api.supportgeni.us`;
- before each deploy it applies that environment's D1 migrations
  (`wrangler d1 migrations apply DB --remote [--env staging]`);
- then it smokes the deployed Worker, curling `/__health` with
  `--retry 20 --retry-delay 6 --retry-all-errors` (a custom domain takes a
  moment to attach). Staging requires only that `/__health` answers and
  `/__ready` is `.ok == true`. Production additionally requires
  `/__ready` `.captcha == "configured"` and `.rate_limiter == "configured"`,
  and `/__health` `.mailer == "configured"`. Those three `/__ready` fields
  are added by the Worker itself (`ventures/supportgenius/src/lib.rs`
  decorates the harness's DB-only `{"ok":true}` probe with `text_model`,
  `captcha` and `rate_limiter`, each `"configured" | "missing"`);
  `/__health`'s `mailer`/`captcha` are the harness's own port fields. Note
  the venture always mounts a mailer (a keyless Resend adapter still reports
  its port configured), so the `mailer` check proves the port is mounted,
  not that `RESEND_API_KEY` is set — the one real join below is what proves
  that;
- it skips with a `::notice::` when the `CLOUDFLARE_API_TOKEN` secret is
  unset (`preflight` emits `configured=false` and both deploy jobs skip, so
  `main` stays green), and fails the `preflight` step — naming the
  environment — if that environment's `database_id` in `wrangler.toml` is
  still a `REPLACE_ME…` placeholder (production's is top-level, staging's is
  under `[env.staging]`).

Repo secrets: `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID`. The token
needs, at minimum: Account → **Workers Scripts: Edit**, Account → **D1: Edit**,
Account → **Account Settings: Read**, Zone → **Workers Routes: Edit** (custom
domains), and — for a user-owned token, which wrangler requires — User →
**User Details: Read** and **Memberships: Read**. An account-owned token cannot
carry Memberships and wrangler will fail; the stock "Edit Cloudflare Workers"
template lacks D1:Edit, so add it.

## 3. First production deploy (one time, a human runs it)

```sh
npx wrangler d1 create supportgenius
npx wrangler d1 create supportgenius-staging
# paste each returned database_id into wrangler.toml ([d1_databases] and [env.staging])
npx wrangler r2 bucket create supportgenius-uploads
npx wrangler secret put HARNESS_SECRET      # required, >= 32 bytes
npx wrangler secret put RESEND_API_KEY      # required
npx wrangler secret put TURNSTILE_SECRET    # required for production traffic
npx wrangler secret put ADMIN_TOKEN         # optional (admin CSV export)
npx wrangler secret put ANTHROPIC_API_KEY   # optional (grounded answers)
# repeat each with `--env staging` for the staging Worker
npx wrangler d1 migrations apply DB --remote
npx wrangler deploy                          # or push the tag and let deploy.yml deploy
```

Confirm the `RATE_LIMITER`/`VISITOR_RATE_LIMITER` `namespace_id`s are unique in the account (1001/1002 production, 1003/1004 staging), then smoke and do one real join before opening the form:

```sh
curl -fsS https://api.supportgeni.us/__health && curl -fsS https://api.supportgeni.us/__ready
curl -fsS -X POST https://api.supportgeni.us/v1/waitlist \
  -H 'Content-Type: application/json' \
  -d '{"email":"you@example.com","product":"supportgenius","captchaToken":"<turnstile token>"}'
```

When the confirmation mail arrives, flip `data-open="true"` on the site's form (the venture README's "The site's half").

## 4. After the release

For each capability the release ships, update [`docs/CLAIMS.md`](CLAIMS.md) and
the website's `COPY.md` **Facts**, and flip that capability's chip from
`planned`. `CLAIMS.md` is the source of truth that keeps the two in step.
