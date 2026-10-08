# Claims: what the site says, and what this repo has built

The site ([SupportGenius/website](https://github.com/SupportGenius/website))
states only what this repo ships. This file is the source of truth for each
chip: a capability named on the site maps either to a merged PR here (shipped)
or to an open issue (still planned). See [CONTRIBUTING.md](../CONTRIBUTING.md).

Status here: **shipped** = merged to `main`; **in progress** = an open PR, or
part of the capability has landed; **planned** = an open issue, no code.

| Site capability | Site chip | Status here | Evidence |
| :--- | :--- | :--- | :--- |
| Answers with citations | `planned` | shipped | issue #3, PR #15 |
| Confidence threshold | `planned` | shipped | issue #3, PR #15 |
| Multilingual answers | `planned` | shipped | issue #32, PR #49 |
| Duplicate detection | `planned` | shipped | issue #27, PR #58 |
| GitHub issues with repro steps (integration) | `planned` | shipped | issue #25, PR #57 |
| Signed webhook / API events | `planned` | shipped | issue #25, PR #57 |
| REST API surface | `planned` | shipped | issues #2/#3, PRs #9/#15 |
| Web widget / `w.js` embed (surface) | `planned` | shipped | issue #33, PR #59 |
| MCP server / Agent-ready (surface, integration) | `planned` | shipped | issue #34, PR #69 |
| Built-in ticketing (feature, integration) | `planned` | shipped | issue #24, PR #71 |
| Router by kind: bug / support case / lead | `planned` | shipped | issue #24, PR #71 |
| Rust core, one static self-host binary | `planned` | shipped | issue #6, PR #13 |
| Knowledge ingestion (URL, sitemap, GitHub docs) | `planned` | shipped | issue #29, PR #54 |
| Human in the loop: take over, reply, hand back | `planned` | in progress | issue #35, PR #72 open |
| Lifecycle: follow the ticket, tell the customer | `planned` | in progress | issue #26, PR #70 open |
| File ingestion: uploads, PDF text | `planned` | in progress | issue #30, PR #51 (part) |
| Built with: what it is built on (Factory Zero registry FZ-008) | (implied) | in progress | issue #66, PR open |
| Analytics: deflection and escalation metrics | `planned` | planned | issue #36 |
| Living Brain as a knowledge source | `planned` | planned | issue #64 |
| Pricing tiers, quotas and billing hooks | `planned` | planned | issue #20 |
| Observability: events, metrics, alerts | (implied) | in progress | issue #39, PR open, [docs/OPERATIONS.md](OPERATIONS.md) |
| Branded status email | (implied) | planned | issue #68 |

## Gaps: site claims with no issue here

These site chips have no issue. Open one before the chip means anything:

- Voice conversations, and the phone line.
- iOS SDK, Android SDK, and the on-call mobile app.
- Consent-based diagnostics (client stack traces, screen recording).
- SLA policies.
- Trackers beyond GitHub and the signed webhook: Jira, Linear, Zendesk,
  Intercom, Freshdesk, Salesforce, HubSpot and Slack-as-a-tracker. `README.md`
  marks each "planned — upstream adapter not published"; #25 (closed) wired
  only the published adapters, so none gets a design issue until an adapter
  ships.

## Facts in the site's `COPY.md` that are stale

- "There is no product repository in `github.com/SupportGenius` other than
  this site" — false: this repository is that product repository.
- "Nothing on this page is built yet", and `llms.txt`'s "no widget, no SDK, no
  phone line, no API, no MCP server": the widget, REST API, MCP server,
  GitHub/webhook trackers, escalation pipeline, built-in ticketing and the
  static binary are merged on `main`.
- The site README's "to live in `SupportGenius/waitlist-backend`" predates ADR
  0013: the Worker lives in this repository.

## When a capability ships

1. Update the table above in the same PR that ships it (or the release PR),
   adding the merged PR number and moving the status.
2. In [SupportGenius/website](https://github.com/SupportGenius/website), update
   `COPY.md`: move the claim from Plans to Facts, cite the PR or file that
   proves it, and flip that capability's chip from `planned` to shipped. Link
   the PR that did it.
3. Flip the waitlist form's `data-open="false"` only after the production
   deploy at `api.supportgeni.us` smokes green — see
   [docs/RELEASING.md](RELEASING.md). Until then the form stays closed.
