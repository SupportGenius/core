# Corpus provenance

Frozen, MIT-licensed documentation used as the retrieval corpus for the
issue #38 evaluation fixtures. Every `corpus/*.md` file is a verbatim copy
of the upstream file at the revision below — no edits.

The corpus is **frozen on purpose**: the questions and their gold phrases
are labelled against these exact bytes, so a later doc edit would silently
move the numbers. Refreshing the corpus means re-labelling the questions
that point at the changed files.

## Files

| Corpus file | Upstream repo | Upstream path | Revision |
| --- | --- | --- | --- |
| `supportgenius-core.md` | SupportGenius/core | `README.md` | `35f58d9686355caa357a3968fb55db3949752343` |
| `supportgenius-binary.md` | SupportGenius/core | `bin/supportgenius/README.md` | `35f58d9686355caa357a3968fb55db3949752343` |
| `module-support.md` | SupportGenius/core | `crates/module-support/README.md` | `35f58d9686355caa357a3968fb55db3949752343` |
| `supportgenius-worker.md` | SupportGenius/core | `ventures/supportgenius/README.md` | `35f58d9686355caa357a3968fb55db3949752343` |
| `harness-architecture.md` | Cratefield/harness | `docs/ARCHITECTURE.md` | `b50c1d99b6f29e11a9fd5f116a8cd31275f735c6` |
| `harness-releasing.md` | Cratefield/harness | `docs/RELEASING.md` | `b50c1d99b6f29e11a9fd5f116a8cd31275f735c6` |
| `harness-secrets.md` | Cratefield/harness | `docs/SECRETS-DESIGN.md` | `b50c1d99b6f29e11a9fd5f116a8cd31275f735c6` |

The SupportGenius/core revision is the `HEAD` of the branch these fixtures
were built on. The harness revision is the single `cratefield-*` git rev the
workspace pins (`Cargo.toml`), which lives in the cargo git checkout
`~/.cargo/git/checkouts/harness-483fd7ddc4d3e1d6/b50c1d9`.

## Licences

Both sources are MIT.

- SupportGenius/core — `/workspace/LICENSE`:
  `Copyright (c) 2026 SupportGenius, a Factory Zero venture`
- Cratefield/harness — `LICENSE` at the revision above:
  `Copyright (c) 2026 Factory Zero`

## Why these harness docs

`ARCHITECTURE.md`, `RELEASING.md` and `SECRETS-DESIGN.md` are the harness
docs an operator of a self-hosted SupportGenius most plausibly asks about:
how the modules and ports fit together, how `cratefield-*` is released, and
where secrets live. Larger harness docs (`MODULE-AUTHORING.md` at 46 KiB)
were left out to keep the whole corpus under ~110 KB.
