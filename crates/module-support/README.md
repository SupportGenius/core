# module-support

The SupportGenius support module (issue #3): a customer asks a question at
`POST /v1/support/messages`, the module retrieves chunks of the tenant's
knowledge base, asks a text model for an answer grounded in them, and
answers **only when it can stand behind the answer**.

## The rule

Every citation the model produces must name a chunk id that was actually
retrieved for the turn. One citation pointing outside the retrieved
context downgrades the answer — it can never be `answered`, whatever
confidence the model claims, because a citation that can name anything is
decoration. `answered` additionally requires at least one citation and a
confidence at or above the answer threshold. Everything else is a
`clarify` question — or a `handoff` when nothing was retrieved at all, or
the conversation has already had its quota of two clarifies. A `handoff`
marks the conversation `needs_escalation`; delivery of the escalation is
the next issue's.

## Threshold precedence

The effective answer threshold resolves, in order:

1. the tenant's stored `sg_tenant_settings.answer_threshold_pct`
   (`PUT /v1/support/admin/settings`, bearer `ADMIN_TOKEN`);
2. `SUPPORT_ANSWER_THRESHOLD` from the deployment config, a fraction in
   `0.0..=1.0`;
3. the builder's `.answer_threshold(..)`;
4. the compile-time default `0.60`.

Top-k resolves the same way over `SUPPORT_TOP_K` and `.top_k(..)`, default
6.

## The `TextModel` port

`cratefield-core` has no `TextModel` port yet (the repository README names
it as an upstream blocker), so this crate defines the smallest port that
carries the module — one `complete` method, a tier, never a vendor name.
It moves to the harness when the upstream port lands. Until then a
deployment hands the model to the builder: `Support::new().text_model(..)`.
A `Support` built with no model answers `503 text-model-not-configured` —
the same clean problem+json as an explicitly unconfigured one, never a
panic, and never a pretend answer.

## Ordering, and why failures are cheap

A turn loads the conversation, retrieves, and calls the model **before**
the first write; the write itself is one `batch_atomic`. Every failure —
not-configured, unavailable (503 with `Retry-After`), unusable answer
(502) — leaves no conversation row, no message, and no spent clarify
budget, so the same request can simply be retried.

## The v0/v1 seam

This crate carries support v1. Support v0 (issue #2) owns tenants, API
keys, sources and BM25 retrieval; migration `0001_retrieval_stub.sql` is
the minimum retrieval table v1 needs to be testable and is expected to be
dropped or folded into v0's schema, and `store::top_chunks` is a
term-overlap placeholder whose body alone is expected to be replaced.
