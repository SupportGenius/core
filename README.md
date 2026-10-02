# SupportGenius core

The ticketing and routing core for [SupportGenius](https://supportgeni.us),
built as [Cratefield harness](https://github.com/Cratefield/harness) modules.
MIT.

**Built so far:** the Cargo workspace, the placeholder `crates/tenancy`,
the `ventures/supportgenius` Worker — the SupportGenius waitlist API — and
`bin/supportgenius`, the same modules as one static native binary (issue #6),
both sharing their module list through `crates/composition`.
The rest of the list below is still scaffolding and an ordered backlog.

`evals/` (issue #38) is the golden set that gates retrieval: 100 questions
over a frozen MIT corpus, run in CI with the deterministic `fake` model. On
this revision it scores recall@6 0.914 against a committed baseline; the
wrong-answer rate of a real model at the default answer threshold (0.60) is
not measured yet.

The plan, in order, is the issue list. Two modules, not five:

- `crates/module-support` — tenants, sources, chunked uploads (text and
  PDF), retrieval, conversations, answers with citations
- `crates/module-escalation` — a conversation becomes a ticket: drafted by one
  model, checked by an independent one, filed by a router, followed up until
  it closes. A tenant (or an operator acting for one) sets where its
  escalations go with `PUT /v1/escalation/destinations` — or, for a tenant
  that cannot, `PUT /v1/escalation/admin/tenants/{tenant_id}/destinations`
  with the admin token. The credential is validated against the tracker once
  and then stored **encrypted** (through `cratefield-secrets`), never as a
  Worker secret an operator edits by hand. That storage is enabled by
  configuration: `HARNESS_KEK_CURRENT` (the Worker's numbered KEK ring) or
  `ESCALATION_KMS_KEY_FILE` (a development master-key file, used only with
  `ENV=development` — with `ENV` unset or anything else the key is not used
  and the native binary refuses to boot). With neither set the routes answer
  `503 escalation-kms-not-configured`; a destination whose `credential_ref`
  names a plain Config key still files as before. A suspended or closed
  tenant is refused on these routes exactly as on support's (`401` for its
  key, `404` on the admin route for it), and every read, write and delete
  of a stored credential is appended to the `harness_secret_audit` chain.
- `ventures/supportgenius` — the Cloudflare Worker (built)
- `bin/supportgenius` — the same modules as one static binary, which is what
  the site promises

The Worker lives in this repository, per the harness's one-repository
decision (ADR 0013). The site README still says it is "to live in
`SupportGenius/waitlist-backend`" — that line predates the decision, is
superseded by it, and needs correcting in the site repository.

The two ports this needs — `TextModel` and `Tracker` — exist in
`cratefield-core` 0.6: `module-escalation` requires both, `module-support`
takes the model as optional and degrades `POST /v1/support/messages` to
`503 text-model-not-configured` without one. The whole `cratefield-*` set
comes from crates.io on that one core line — see the comment in
`Cargo.toml`.

## Destinations

An escalation is filed into one tracker per tenant (`sg_destinations`).
Which trackers a build *can* file into is a compile-time fact, not config:
the adapters are cargo features (`tracker-github`, `tracker-webhook`, both
on by default), and `module-escalation`'s `check_destination` refuses any
other kind **by name** — the per-tenant destination admin route (#23) maps
that to `422`, so a tenant pointed at a tracker this build lacks is told
which kind, rather than failing at file-time.

| Destination | `kind` | Status |
| --- | --- | --- |
| GitHub Issues | `github` | works today |
| Webhook (signed HTTPS `POST`) | `webhook` | works today |
| Jira | `jira` | planned — upstream adapter not published |
| Linear | `linear` | planned — upstream adapter not published |
| Zendesk | `zendesk` | planned — upstream adapter not published |
| Intercom | `intercom` | planned — upstream adapter not published |
| Salesforce | `salesforce` | planned — upstream adapter not published |
| `HubSpot` | `hubspot` | planned — upstream adapter not published |
| Slack, as a tracker | `slack` | planned — upstream adapter not published |
| Freshdesk | — | planned — no `Destination` variant in core yet |

Slack and on-call notifications need no tracker of their own: the pipeline
publishes `escalation.filed`, `escalation.dead_lettered` and
`escalation.needs_human` through `cratefield-module-webhooks` (per-endpoint
secret, retries, dead letters, replay), and a Slack incoming webhook is just
an endpoint URL — the `webhook` row above. Each delivery is signed
`Cratefield-Signature: t=<unix>,v1=<hex HMAC-SHA256 of "{t}.{body}">`, with
`Cratefield-Event-Id` and `Cratefield-Event-Type` headers. The webhook
*tracker* signs with one venture-wide secret (`ESCALATION_WEBHOOK_SECRET`),
since it signs the payload rather than authenticating to a tracker.

A [Factory Zero](https://factory0.ventures) venture.