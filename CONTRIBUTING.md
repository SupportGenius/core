# Contributing

Small repo, small rules: keep `main` green and keep the site honest. MIT.

## Run what CI runs

```sh
cargo fmt --all --check                                          # rustfmt job
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo clippy -p supportgenius --all-targets --locked --features cli -- -D warnings
cargo clippy -p supportgenius-bin --all-targets --locked --features dev-fakes -- -D warnings
cargo test --workspace --locked
cargo test -p supportgenius-bin --locked --features dev-fakes
scripts/smoke.sh <path-to-musl-binary>                           # native job
```

The `cli` and `dev-fakes` runs are separate because the workspace run never
compiles those feature-gated targets (`.github/workflows/ci.yml`). CI also
builds the Worker to wasm, checks `migrations/` is current, and runs the evals.

## Changing what the product can do

A PR that ships, changes or removes a capability must update
[docs/CLAIMS.md](docs/CLAIMS.md) in the same PR and follow its "When a
capability ships" step. That keeps every `planned` chip mapped to a real issue.

## Elsewhere

- [docs/RELEASING.md](docs/RELEASING.md) — how a release is cut and deployed.
- [docs/SELF-HOSTING.md](docs/SELF-HOSTING.md) — running the static binary.
- [docs/OPERATIONS.md](docs/OPERATIONS.md) — the escalation pipeline's events, `GET /v1/escalation/admin/health`, and the alert thresholds they feed.
