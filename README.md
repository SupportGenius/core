# SupportGenius core

The ticketing and routing core for [SupportGenius](https://supportgeni.us),
built as [Cratefield harness](https://github.com/Cratefield/harness) modules.
MIT.

**Built so far:** the Cargo workspace, the placeholder `crates/tenancy`,
the `ventures/supportgenius` Worker — the SupportGenius waitlist API — and
`bin/supportgenius`, the same modules as one static native binary (issue #6),
both sharing their module list through `crates/composition`.
The rest of the list below is still scaffolding and an ordered backlog.

The plan, in order, is the issue list. Two modules, not five:

- `crates/module-support` — tenants, sources, retrieval, conversations,
  answers with citations
- `crates/module-escalation` — a conversation becomes a ticket: drafted by one
  model, checked by an independent one, filed by a router, followed up until
  it closes. A tenant (or an operator acting for one) sets where its
  escalations go with `PUT /v1/escalation/destinations` — or, for a tenant
  that cannot, `PUT /v1/escalation/admin/tenants/{tenant_id}/destinations`
  with the admin token. The credential is validated against the tracker once
  and then stored **encrypted** (through `cratefield-secrets`), never as a
  Worker secret an operator edits by hand. That storage is enabled by
  configuration: `HARNESS_KEK_CURRENT` (the Worker's numbered KEK ring) or
  `ESCALATION_KMS_KEY_FILE` (a development master-key file, refused in
  `ENV=production`). With neither set the routes answer
  `503 escalation-kms-not-configured`; a destination whose `credential_ref`
  names a plain Config key still files as before.
- `ventures/supportgenius` — the Cloudflare Worker (built)
- `bin/supportgenius` — the same modules as one static binary, which is what
  the site promises

The Worker lives in this repository, per the harness's one-repository
decision (ADR 0013). The site README still says it is "to live in
`SupportGenius/waitlist-backend`" — that line predates the decision, is
superseded by it, and needs correcting in the site repository.

The two ports this needs — `TextModel` and `Tracker` — exist in
`cratefield-core` 0.5 (harness rev `b50c1d9`): `module-escalation`
requires both, `module-support` takes the model as optional and degrades
`POST /v1/support/messages` to `503 text-model-not-configured` without
one. Until core "0.6" publishes to crates.io (Cratefield/harness#558),
the whole `cratefield-*` set is pinned to that one git rev — see the
comment in `Cargo.toml`.

A [Factory Zero](https://factory0.ventures) venture.