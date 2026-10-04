# 0001 — Staff identity: per-staff API keys

Status: adopted, 2026-10-04 · Issue #35 (epic #24) · module-support

## Context

A support conversation can now be taken over by a person: the bot falls
silent, an agent replies, and the agent's reply can be saved as a reviewed
source. That needs three things the module did not have: a way to tell a
person's credential from the tenant's own (a customer widget key, an
integration key), a stable name to hang a conversation's `assignee` on,
and a name to record against a reviewed source's `reviewed_by`.

Today every tenant credential is the same thing: an `sg_…` key minted by
`POST /admin/tenants`, with a row in `sg_api_keys`. There is no roster and
no user table, and there is no interactive sign-in anywhere in the
product — the widget is anonymous, and the only human who authenticates is
an operator holding an API key.

## Decision

Staff identity is a property of a **key**, not a new entity. Migration
`0009` adds a nullable `sg_api_keys.staff_id TEXT`. `POST /keys` accepts
an optional `staff_id` (validated like `label`); a key minted with one is
a **staff key**. `authenticate` returns a small principal —
`{tenant_id, staff_id}` — and the staff routes (`/inbox`,
`/conversations/*`) call a `require_staff` helper that answers `403
not-staff` when the key carries no `staff_id`. A key without one behaves
exactly as before, so customer and integration keys are untouched.

`staff_id` is an opaque, caller-chosen handle (`"agent-7"`, an email, an
idp subject). It is the value written to `sg_conversations.assignee` and
`sg_sources.reviewed_by`. Anything that can map a person to a handle in
the operator's own system can mint the key; the module never asks who they
are.

## Alternatives

- **Harness `Auth` port (`auth-magic-link` / `auth-oidc`).** The right
  long-term home for interactive sign-in, and the seam that would make
  `staff_id` a verified subject rather than a caller-chosen string. Not
  wired: no surface in this product has a person signing in yet (the
  widget is anonymous), so it would add a login flow nothing consumes.
  Revisit when the mobile app needs interactive sign-in; the mapping is
  additive — an `Auth`-verified subject becomes the `staff_id` on the same
  key row, or the row's `staff_id` is set from it, and no route changes.
- **A `sg_staff` roster table.** Rejected for now: it would need
  roles, invitations and its own lifecycle, and the only question the
  module has to answer is "which person owns this conversation". A handle
  on the key answers it with one column and no new write path.

## Consequences

- Revoking a person is deleting their key row — the existing
  `DELETE /keys/{kid}` path, effective on the next request.
- No roles: any staff key of a tenant may take any of that tenant's
  conversations. Ownership is enforced at the conversation (`assignee`
  must equal the caller to reply or hand back), not by capability.
- `staff_id` is not a verified identity; it is as trustworthy as who can
  mint keys for the tenant. That is the tenant's own key, which is the
  same trust boundary the tenant already had.
- Staff turns are stored as `role = 'staff'` with the author in a new
  nullable `sg_messages.author`, and are rendered into the model's history
  as `Support agent: …` after a handback.
