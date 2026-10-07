# SupportGenius web widget

`w.js` is a single, dependency-free, hand-written JavaScript file (classic script,
ES2017, no build step) that drops a support chat panel onto any page. It renders
into a Shadow DOM, talks to the SupportGenius API with CORS "simple" requests
only, and surfaces the server-side abuse controls (rate limits, Turnstile
captcha, origin allowlist) in its responses.

## Embed

```html
<script src="https://api.supportgeni.us/v1/support/w.js" data-key="sg_pub_…" async></script>
```

With subresource integrity (recommended):

```html
<script src="https://api.supportgeni.us/v1/support/w.js"
        integrity="sha384-…" crossorigin="anonymous" data-key="sg_pub_…" async></script>
```

The current `sha384-…` hash is published in [`w.js.sri`](./w.js.sri). Copy the
value from there; do not invent one. `crossorigin="anonymous"` is required for
integrity to apply.

## Attributes

| Attribute   | Required | Description                                                    |
| ----------- | -------- | -------------------------------------------------------------- |
| `data-key`  | yes      | A **publishable** key starting with `sg_pub_`. Missing or wrong prefix → the widget refuses to start (`console.error`). A secret `sg_…` key triggers a loud warning and is never accepted — secret keys must never be put in a page. |
| `data-title`| no       | Panel header text. Default: `Support`.                         |

The API base URL is derived from the script's own `src` origin, so the same file
works in production and against `wrangler dev`.

## Behavior notes

- Panel state (visitor string, conversation id + token) is kept in `localStorage`
  under `sg:<key-prefix>:visitor` and `sg:<key-prefix>:conv`, with an in-memory
  fallback when storage is unavailable.
- The transcript is restored on page load and polled every 10 s while the panel
  is open (paused when the tab is hidden, backed off to at most 60 s on errors).
- Captchas use Cloudflare Turnstile, loaded on demand only when the server asks.
- The panel's small print is the venture's "built with" list, fetched once when
  the panel is first opened from `GET /v1/support/built-with` on the API origin
  (no key, no `Origin` allowlist — it is the same venture-wide document for every
  embedding page). Each entry is a link with its status spelled out — `(live)` or
  `(planned)` — so nothing planned reads as live. If that request fails for any
  reason the footer simply does not appear; the chat is unaffected.

## Versioning and SRI

The version lives in the banner comment on line 1 of `w.js` (`v1.0.0`). Any
change to the file is a version bump, and each published version gets a fresh
integrity hash in `widget/w.js.sri`. Update your `integrity` attribute whenever
you adopt a new version; a stale hash makes the browser refuse the script.

Regenerate the hash after **every** edit to `w.js` — bump the version in the
banner comment first, then recompute over the final bytes:

```sh
printf 'sha384-%s' \
  "$(openssl dgst -sha384 -binary widget/w.js | openssl base64 -A)" \
  > widget/w.js.sri
```

`w.js.sri` holds the hash alone, with no trailing newline. The drift test
`the_script_is_served_with_its_integrity_pin_intact`
(`crates/module-support/tests/widget.rs`) recomputes the digest the way a
browser does and fails if the two disagree, so run it before pushing.

## Trying it locally

1. Start the API: `wrangler dev` (serves `http://localhost:8787`).
2. Mint a publishable key via `POST /v1/support/admin/tenants/{id}/publishable-keys`.
3. Serve the example page and put its origin on the tenant's allowlist
   (`widget_origins`, set via `PUT /v1/support/admin/tenants/{id}/settings`):

   ```sh
   cd widget/example
   python3 -m http.server 8000
   ```

4. Open `http://localhost:8000` and click the chat button in the bottom-right
   corner. The example page embeds the widget from `http://localhost:8787`.
