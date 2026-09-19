//! `Harness::build` refuses a composition whose modules collide, and the
//! venture as composed today is collision-free (issue #5): module names
//! are pairwise unique and the tables each module claims are pairwise
//! disjoint. Route paths need no separate check — the harness mounts
//! every module's router at `/v1/<module.name()>`, so two modules cannot
//! serve the same route without first colliding on a name. The negative
//! half below exercises that name check; it says nothing about routes.

use std::sync::Arc;

use cratefield_adapter_resend::Resend;
use cratefield_core::{Harness, Mailer, Venture};
use cratefield_module_waitlist::Waitlist;
use cratefield_runtime_cloudflare::{Cloudflare, FetchClient, WorkersClock};
use supportgenius::compose;

/// A keyless Resend adapter as the test mailer: the composition only
/// needs a mailer port to build, and these tests never send. Keyless
/// means it reports `NotConfigured` rather than pretending, should a send
/// ever happen.
fn keyless_mailer() -> Arc<dyn Mailer> {
    Arc::new(Resend::new(
        Arc::new(FetchClient),
        Arc::new(WorkersClock),
        None,
        "no-reply@send.supportgeni.us",
        None,
    ))
}

/// The guard half. Whatever modules the venture composes — `waitlist`
/// today, the `support` and `escalation` modules as sibling branches land
/// them — no two may share a name or claim the same table. The checks run
/// pairwise over the built harness rather than against a fixed module
/// list, so the day a third module is composed in `src/lib.rs` it is
/// covered here with no edit to this file.
#[test]
fn composed_modules_have_unique_names_and_disjoint_tables() {
    let (harness, _runtime) = compose(keyless_mailer(), None).expect("harness builds");
    let modules = harness.modules();
    assert!(
        !modules.is_empty(),
        "nothing composed: the pairwise checks below would be vacuous"
    );

    let names: Vec<&str> = modules.iter().map(|module| module.name()).collect();
    for (index, name) in names.iter().enumerate() {
        assert!(
            !names[index + 1..].contains(name),
            "module `{name}` is composed more than once: {names:?}"
        );
    }

    for (index, module) in modules.iter().enumerate() {
        for other in &modules[index + 1..] {
            for table in module.tables() {
                assert!(
                    !other.tables().contains(table),
                    "table `{table}` is claimed by both modules `{}` and `{}`",
                    module.name(),
                    other.name(),
                );
            }
        }
    }
}

/// The problems `build()` reports for a deliberately invalid composition:
/// `waitlist` composed twice.
///
/// Hand-written stub modules — the shape upstream's own build-failure
/// tests use (`crates/core/tests/build.rs`) — cannot compile in this
/// crate: `Module::router` returns `axum::Router`, axum is only reachable
/// through `cratefield-core` (Rust does not name transitive
/// dependencies). Adding axum to `[dev-dependencies]` would work —
/// updating `Cargo.lock` in the same change keeps `--locked` green — but
/// the lockfile is a high-conflict file while sibling branches edit this
/// repo, and the stub must name the exact axum version `cratefield-core`
/// 0.4.3 was compiled against or the `Router` types will not unify.
/// Brittle coupling for little test value, so two `Waitlist`s are the
/// smallest invalid composition available — and both owners in the
/// duplicate-table message are necessarily the same name.
///
/// The runtime is the real `Cloudflare` one carrying every port
/// `waitlist` requires (`Db`, `Mailer`, `Signer`), and the venture is the
/// one `compose` builds, so the only problems `build()` can report are
/// the collision ones — the failure is not a missing port or an invalid
/// venture.
fn duplicate_waitlist_pair_problems() -> Vec<String> {
    // `let-else` rather than `expect_err`: `Harness` is not `Debug`, and
    // `expect_err` formats the `Ok` half when it goes red.
    let Err(error) = Harness::builder()
        .venture(
            Venture::new("supportgenius", "supportgeni.us")
                .cors_origins(["https://supportgeni.us"]),
        )
        .module(Waitlist::new())
        .module(Waitlist::new())
        .runtime(Cloudflare::new().db("DB").mailer_arc(keyless_mailer()))
        .build()
    else {
        panic!("composing waitlist twice must fail to build");
    };
    error.problems
}

#[test]
fn duplicate_module_names_are_reported() {
    let problems = duplicate_waitlist_pair_problems();
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("duplicate module name `waitlist`")),
        "problems: {problems:?}"
    );
}

/// The assertion pins the exact message, but both owner slots
/// necessarily read `waitlist` here — the negative half composes one
/// module twice — so only the same-name formatting is exercised.
#[test]
fn duplicate_tables_are_reported() {
    let problems = duplicate_waitlist_pair_problems();
    assert!(
        problems.iter().any(|problem| problem.contains(
            "duplicate table `waitlist_entries` claimed by modules `waitlist` and `waitlist`"
        )),
        "problems: {problems:?}"
    );
}
