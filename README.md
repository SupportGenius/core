# SupportGenius core

The ticketing and routing core for [SupportGenius](https://supportgeni.us),
built as [Cratefield harness](https://github.com/Cratefield/harness) modules.
MIT.

**Built so far:** the Cargo workspace, the placeholder `crates/tenancy`,
and the `ventures/supportgenius` Worker — the SupportGenius waitlist API.
The rest of the list below is still scaffolding and an ordered backlog.

The plan, in order, is the issue list. Two modules, not five:

- `crates/module-support` — tenants, sources, retrieval, conversations,
  answers with citations
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

Two ports this needs do not exist in the harness yet and are being added
there: `TextModel` and `Tracker`. Those are the first blockers.

A [Factory Zero](https://factory0.ventures) venture.