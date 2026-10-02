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
  it closes
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

A [Factory Zero](https://factory0.ventures) venture.