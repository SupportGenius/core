# Releasing `cratefield-*` to crates.io

The pipeline is wired; the only things left are one-time crates.io
setup steps that need a human with the owner account. This page is the
exact list (issue #15). Everything here is **for the owner** — the
credential-gated steps below run from this repository's CI only once it
is configured. The one exception that needs nothing: the `crates.io
resolve` workflow already runs daily with no credentials, checking the
published set on its own (see "The published set" below).

## How the pipeline works

`.github/workflows/release.yml` runs on every push to `main`:

1. `release-plz release-pr` collects the conventional commits since the
   last release and opens (or updates) a release PR: per-crate version
   bumps, per-crate `CHANGELOG.md`, rewritten workspace dependency
   requirements.
2. Merging that release PR triggers `release-plz release` on the
   resulting push to `main`: it publishes every crate whose version is
   not yet on crates.io — in dependency order (`cratefield-core` first) —
   and tags `<crate>-v<version>` with a GitHub release per crate.

Configuration lives in `release-plz.toml` (per-crate versioning,
conventional-commit changelogs, `publish = false` for `examples/*` and
the CLI acceptance crate).

`cratefield-tables` is no longer held back: its first publish (0.1.1) was
done by hand, and `cratefield-manifest` and `cratefield-cli` depend on it,
so it is in the ordered list in step 2 below ahead of both. Its trusted
publisher (step 3) is still to be enabled; until it is, the release run
cannot publish it over OIDC.

`cratefield-mcp` (issue #160) carries `publish = false` as well: it is
new, nothing depends on it, and publishing it is the remaining human step
ADR 0021 records — until the crates.io setup below exists, a publishable
crate would only make the release run fail authentication over OIDC.
When that step lands its first publish is manual, the same as every other
new crate: add it to the ordered list in step 2 below, then enable its
trusted publisher and drop the `publish = false` entry.

`cratefield-push-auth`, `cratefield-adapter-fcm`,
`cratefield-adapter-webpush`, `cratefield-push-wiring`, `cratefield-i18n`,
`cratefield-module-privacy`, `cratefield-module-notifications` and
`cratefield-auth-client` were held back with `publish = false` during
their first-publish window. None carries the flag any more, all eight are
on crates.io, and release-plz publishes them like any other crate. In
particular, nothing holds back `cratefield-push-auth` (#489).

What does **not** constrain the order: `cratefield-module-privacy` is a
dev-dependency of `module-email-signup`, `module-waitlist` and
`module-notifications`, and those are path-only by the rule in step 2 below, so
they are stripped from the packaged manifests. Those three published while
`module-privacy` did not exist on crates.io, which is the proof. Only the
facade's real (optional) dependency on it constrains anything.

`fz push` (issue #184) adds three more edges into `cratefield-cli`, and the
ordered list below already satisfies all of them: it depends on
`cratefield-push-auth` and `cratefield-adapter-webpush` **not** optionally
(`fz push vapid keygen` mints the key with the same signer the adapters
present, and `fz push inspect-subscription` validates with the Web Push
adapter's own rules), and on `cratefield-runtime-native` behind the optional
`push-send` feature — which still has to resolve on crates.io when the CLI is
packaged, for the reason the facade's optional dependencies do.

Authentication is **trusted publishing**: the workflow exchanges the
GitHub Actions OIDC token (`id-token: write`) for a short-lived
crates.io token. No `CARGO_REGISTRY_TOKEN` is stored anywhere.

Both steps are **switched off** until the owner setup below is done, so
an unfinished setup leaves `main` green with an annotation instead of a
red X on every merge.

Step 1 runs only with a token GitHub lets create pull requests. The
built-in `GITHUB_TOKEN` usually may not: the API answers *"GitHub
Actions is not permitted to create or approve pull requests"* (403)
unless the repository setting allows it. Two ways to fix it, and the
first is better:

- **A `RELEASE_PLZ_TOKEN` secret** — a fine-grained PAT or GitHub App
  token with *contents: write* and *pull requests: write* on this
  repository. It sidesteps the setting, and the release PR it opens
  **runs CI**, which a `GITHUB_TOKEN`-authored PR never does (GitHub
  suppresses workflow triggers on those, so version bumps would merge
  unverified).
- **The repository setting** — *Settings* → *Actions* → *General* →
  *Workflow permissions* → tick *Allow GitHub Actions to create and
  approve pull requests*, then set the repository variable
  `ACTIONS_MAY_OPEN_PRS` to `true` so the workflow knows. Note the
  setting also lets any workflow in this repository *approve* PRs.

Step 2 runs only when the repository variable
`CRATES_IO_READY` is `true`. A crate's trusted publisher cannot be
configured before the crate exists, so until the first manual publish
every `release` run would fail on authentication and leave `main` red.
`release-pr` always runs; it needs no registry credentials.

The workflow also takes a manual run (Actions → Release → *Run
workflow*) with a `dry_run` checkbox, on by default, so the pipeline
can be exercised without publishing.

## Dependent crates do not reliably follow a breaking bump

**Assume a breaking bump of `cratefield-core` does not release its
dependents, and check that it did.** release-plz normally cascades: when
it releases a crate it rewrites that crate's version requirement in
every manifest it manages, and each crate whose requirement it rewrote
earns a patch bump and is published in the same release PR. Nothing
configures this behaviour — there is no option to turn the cascade on,
off, or up, and no option to make a breaking bump propagate as anything
larger than a patch.

**The cascade is keyed on that rewrite, which makes it easy to suppress
by accident.** release-plz asks whether a requirement has to change in
order to admit the version being released, and reads "no change needed"
as "this dependent does not need a release". A requirement of `"0.5"`
already admits 0.5.0, so releasing core as 0.5.0 against a workspace
table that already reads `"0.5"` rewrites nothing and publishes nothing
but core. That is how core reached 0.5.0 on crates.io while 23 published
dependents still required `^0.4` (issue #462): the requirement in the
root `Cargo.toml` had been edited by hand ahead of the release that was
meant to move it, so the release round that should have followed never
ran.

**So bump one place, not two.** Set the new version in
`crates/<name>/Cargo.toml` and leave the `[workspace.dependencies]`
requirement in the root `Cargo.toml` alone — moving it is release-plz's
job, and moving it first is what costs the dependent round. If the two
have already diverged, the dependents will not catch up on their own:
bump each of them in the release PR by hand, or publish them in the
dependency order given in step 2 below.

**A cascaded dependent gets a patch bump even when the dependency
broke.** release-plz does not inspect code to work out whether a
dependent re-exports a type that changed, so a crate whose public
surface exposes a core type can ship a breaking change as a patch
release, which consumers pinned with a caret take silently.
`COMPATIBILITY.md` does not catch this either: it is generated from the
workspace manifests and checked in CI for drift, so it records the
ranges this tree declares, not the ranges the published crates actually
carry.

**Nothing in this repository catches the mismatch.** Every in-tree build
resolves `cratefield-core` through the path dependency in the root
`Cargo.toml`, so the workspace compiles against the local 0.5 whatever
the published requirements say, and CI stays green while the published
set is unresolvable. Issue #466 tracks the CI job that resolves a
venture from crates.io alone, which is the check that would have caught
this.

## Owner setup (once)

Step 0 is about GitHub; the rest need the crates.io account that will
own the `cratefield-*` names.

0. **Let the release PR be opened.** Either add a `RELEASE_PLZ_TOKEN`
   secret (preferred, and the release PR then runs CI) or tick the
   repository setting and add the variable `ACTIONS_MAY_OPEN_PRS=true`,
   as described above. Verify with Actions → *Release* → *Run workflow*
   (leave `dry_run` checked): the release PR appears.
 A crate's trusted publisher can only be configured **after the
crate exists**, so the very first release of each crate is manual:

1. **Create a scoped token, on an account with a verified email.**
   Sign in to crates.io → *Account settings* → *API Tokens* → *New
   token*. Scope: **Publish new crates**. This token exists only for the
   first publishes; revoke it afterwards.

   crates.io refuses to publish from an account whose email address is
   unverified, and it refuses **at upload**, after packaging and the
   verification build have both succeeded:

   ```
   error: failed to publish cratefield-core v0.3.0 to registry at https://crates.io
   Caused by:
     the remote server responded with an error (status 400 Bad Request):
     A verified email address is required to publish crates to crates.io.
   ```

   A `--dry-run` cannot catch this, because a dry run stops before the
   upload. Check <https://crates.io/settings/profile> first: the address
   must be present *and* confirmed through the mail crates.io sends.
2. **First-publish each crate manually**, in dependency order, from a
   checkout of `main` at the version being released:

   ```sh
   export CARGO_REGISTRY_TOKEN=...   # the scoped token from step 1
   cargo publish --dry-run -p cratefield-core   # then without --dry-run
   cargo publish -p cratefield-tables         # before the manifest and the CLI
   cargo publish -p cratefield-manifest       # before the CLI and the facade
   cargo publish -p cratefield-i18n           # before module-notifications
   cargo publish -p cratefield-kms
   cargo publish -p cratefield-adapter-anthropic
   cargo publish -p cratefield-adapter-postgres
   cargo publish -p cratefield-module-email-signup
   cargo publish -p cratefield-adapter-sqlite
   cargo publish -p cratefield-testing        # before module-privacy and auth-client (versioned dev-dep)
   cargo publish -p cratefield-module-privacy # before the facade
   cargo publish -p cratefield-module-waitlist
   cargo publish -p cratefield-module-webhooks  # before the facade
   cargo publish -p cratefield-push-auth      # before adapter-apns, -fcm, -webpush and the CLI
   cargo publish -p cratefield-adapter-classifier-llm
   cargo publish -p cratefield-adapter-github-issues
   cargo publish -p cratefield-adapter-resend
   cargo publish -p cratefield-adapter-stripe
   cargo publish -p cratefield-adapter-turnstile
   cargo publish -p cratefield-adapter-typesafe
   cargo publish -p cratefield-adapter-webhook-tracker
   cargo publish -p cratefield-auth-client    # before module-notifications and the runtimes
   cargo publish -p cratefield-oauth-client   # before module-linkedin (which is held back)
   cargo publish -p cratefield-adapter-workers-ai
   cargo publish -p cratefield-module-changelog
   cargo publish -p cratefield-module-cms
   cargo publish -p cratefield-module-telemetry
   cargo publish -p cratefield-ui
   cargo publish -p cratefield-secrets
   cargo publish -p cratefield-adapter-apns
   cargo publish -p cratefield-adapter-fcm    # before push-wiring and the facade
   cargo publish -p cratefield-adapter-webpush  # before push-wiring, the CLI and the facade
   cargo publish -p cratefield-module-notifications  # needs i18n and auth-client
   cargo publish -p cratefield-push-wiring    # before the runtimes and the CLI
   cargo publish -p cratefield-runtime-native  # cannot package yet: path-only dep, see tools/package-check.sh
   cargo publish -p cratefield-runtime-cloudflare  # cannot package yet: path-only dep, see tools/package-check.sh
   cargo publish -p cratefield-cli  # cannot package yet: path-only dep, see tools/package-check.sh
   cargo publish -p cratefield            # the facade; cannot package yet: path-only dep, see tools/package-check.sh
   ```

   Thirty-seven crates, and the order is the dependency order: `--dry-run`
   for a crate whose upstream `cratefield-*` dependencies are not on
   crates.io yet resolves against the registry and fails until those are
   published. The `package` CI job (`tools/package-check.sh`) fails if this
   list misses a publishable crate, lists one that is not publishable, or
   puts a crate before one of its `cratefield-*` dependencies, so a new
   crate has to be added here in a valid position. Four lines cannot run
   yet: each of those crates has a path-only dependency with no version,
   which `cargo package` refuses (`UNPACKAGEABLE` in the script names
   them). An older version of this list put `cratefield-auth-client` after
   the two runtimes that depend on it; it now comes before them.

   `cargo publish` resolves **dev**-dependencies against crates.io too,
   which is why internal dev-dependencies in this workspace are declared
   path-only rather than `workspace = true` (see the note in the root
   `Cargo.toml`). Cargo strips a path-only dev-dependency from the
   packaged manifest, so `cratefield-ui` can dev-depend on the unpublished
   `module-hello` example, and the
   `adapter-postgres -> module-* -> testing -> adapter-postgres` dev cycle
   never has to be broken with `--no-verify`. If you ever find yourself
   reaching for `--no-verify`, a dev-dependency has grown a version. Two
   already have: `cratefield-module-privacy` and `cratefield-auth-client`
   dev-depend on `cratefield-testing` through `workspace = true`, so that
   requirement ships, and `cratefield-testing` has to be published before
   either of them. The order check counts versioned dev-dependencies for
   that reason.
3. **Enable trusted publishing per crate.** For each published crate:
   crates.io → crate page → *Settings* → *Trusted publishing* → add
   repository `Cratefield/harness`, workflow `release.yml`,
   environment *(leave empty)*. From then on the release workflow
   publishes that crate with OIDC and no token.
4. **Switch publishing on.** Repository → *Settings* → *Secrets and
   variables* → *Actions* → *Variables* → *New repository variable*:
   name `CRATES_IO_READY`, value `true`. Until this exists the release
   step is skipped and the run says so in an annotation. Verify with a
   manual dry run: Actions → *Release* → *Run workflow*, leave
   `dry_run` checked; the release step should now execute and report
   what it *would* publish.
5. **Revoke the token from step 1.**
6. **Add the team owner.** On each crate page → *Owners* → add a
   GitHub team from the `Cratefield` org (the org that owns this
   repository), so a second human can recover the crates.

If trusted publishing ever needs to be bypassed temporarily: create a
token with the *Update crates* scope and add it as the GitHub Actions
secret `CARGO_REGISTRY_TOKEN` — release-plz picks it up automatically
and skips OIDC. Remove the secret when done.

Two traps in the workflow itself, both commented where they bite:

- The action is pinned to an exact version (`release-plz/action@v0.5.x`).
  It moved from `MarcoIeni/release-plz-action` and publishes no floating
  major tag, so `@v0` does not resolve and the job never starts.
- Its `dry_run` input is tested with `[[ -n ... ]]`, so **any** non-empty
  value turns a dry run on, the string `false` included. The workflow
  passes an empty string for a real release; do not "simplify" it to
  `false` or nothing will ever be published.

## Prerelease flow (0.x, `-rc.N`)

While pre-1.0 a minor bump may break (that is what the caret ranges in
[COMPATIBILITY.md](COMPATIBILITY.md) guard). To cut a prerelease, e.g.
`0.2.0-rc.1`:

1. On a branch, set the candidate versions
   (`cargo set-version -p cratefield-core 0.2.0-rc.1` from `cargo-edit`,
   or by hand) and land the change on `main` with a message like
   `chore(release): prepare cratefield-core 0.2.0-rc.1`.
2. The next `release-plz release` run publishes any version that is not
   on crates.io yet — including the `rc`. (Verify the release run's
   logs; if release-plz skipped it, publish the `rc` manually as in
   step 2 above — same trusted publishing / token rules.)
3. Consumers opt in explicitly: prerelease versions never match a caret
   range, so a venture must pin
   `cratefield-core = "=0.2.0-rc.1"` while testing.
4. The final `0.2.0` follows the normal release-PR flow; the `rc`
   commits appear in its changelog.

## The published set: what runs on its own, what is still by hand

One check of the published set now runs on its own: the `crates.io
resolve` workflow (`.github/workflows/crates-io-resolve.yml` running
`tools/crates-io-resolve.sh`). It resolves this repository's crates the
way a stranger does: every publishable crate that exists on crates.io is
added — no version pin, no path override — to a fresh project in a temp
directory outside this workspace; the resolution is asserted to hold one
copy of each in-repo crate (`cargo tree --duplicates`; two copies of one
of ours is the `expected cratefield_core::Module, found
cratefield_core::Module` failure downstream); and the result is compiled
(`cargo check`). A crate whose first publish is still pending is skipped
with a notice, not failed on — a pending first publish is a gap being
filled (Owner setup above), not a broken published set.

It runs daily on a schedule, after a real release (the Release workflow
calls it once a publishing run actually happened, dry runs excepted), and
on manual dispatch. The schedule is the point: crates.io can become
incoherent without a single commit here — one publish that half-succeeds
is enough — and only a scheduled run is both guaranteed to notice and
guaranteed to rerun after it. It is deliberately not on push or
pull_request: a broken published set should redden the release pipeline,
not every unrelated pull request.

This retires the old one-shot advice to re-run `cargo add
cratefield-core` from an empty project after the first release (issue #15
acceptance): the workflow now does that for every published crate,
together, every day.

The package itself is checked on every push and pull request, by the
`package` job in `.github/workflows/ci.yml` running
`tools/package-check.sh` with no credentials:

- **Publish-order drift**: the first-publish list in step 2 above must
  name exactly the publishable set (`publish` unset in the crate's
  manifest and not held back in `release-plz.toml`), each crate after its
  normal and build `cratefield-*` dependencies and its versioned
  dev-dependencies, and each only once.
- **`UNPACKAGEABLE` stays honest**: each crate the script lists as unable
  to package must still fail `cargo package` for the path-only
  dependency it records; once one packages, the job fails until its entry is removed.
- **Leak deny-list**: `cargo package --list` for every publishable crate,
  failing if a packaged path is under `target/` or is `BRIEF.md`, a
  `*-BRIEF.md` such as `BUILD-BRIEF.md`, `PROGRESS.md` or a `.env*` file.
- **Verification build**: one multi-package `cargo package` over every
  publishable crate not in `UNPACKAGEABLE`. Cargo overlays the freshly
  packaged crates as a local registry, so unpublished versions resolve,
  and builds each from its extracted tarball. That is the only build that
  notices a file `include_str!` reads but the crate's `include` list
  leaves out (#499): every other build reads the source tree.

`tools/package-check.sh --no-verify` runs everything but the verification
build, for a fast local check that takes seconds once cargo's index
is warm.

Still manual: the credentialed publish itself. Every first publish is
`cargo publish` with the scoped token (step 2 above); only after that
does the release run publish a crate on its own, over OIDC.
