# SupportGenius core

The ticketing and routing core for [SupportGenius](https://supportgeni.us),
built as [Cratefield harness](https://github.com/Cratefield/harness) modules.
MIT.

**Nothing here is built yet.** This repository is scaffolding and an ordered
backlog. The site says the same thing on every page, and so does this README
until it stops being true.

The plan, in order, is the issue list. Two modules, not five:

- `crates/module-support` — tenants, sources, retrieval, conversations,
  answers with citations
- `crates/module-escalation` — a conversation becomes a ticket: drafted by one
  model, checked by an independent one, filed by a router, followed up until
  it closes
- `ventures/supportgenius` — the Cloudflare Worker
- `bin/supportgenius` — the same modules as one static binary, which is what
  the site promises

Two ports this needs do not exist in the harness yet and are being added
there: `TextModel` and `Tracker`. Those are the first blockers.

A [Factory Zero](https://factory0.ventures) venture.