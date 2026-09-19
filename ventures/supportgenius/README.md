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
