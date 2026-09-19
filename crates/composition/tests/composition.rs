//! The composition's safety property (issue #6): the module list composes
//! against the exact port set a Redis-less native boot offers. Without
//! Redis the native runtime provides no `RateLimiter` and no `KeyValue`,
//! and a module that hard-fails without a limiter turns the self-hosted
//! binary into a brick — so that shape is tested for, here, where the
//! module list is written down.

use cratefield_core::{Harness, Port, Runtime};
use supportgenius_composition::{modules, venture};

/// A runtime that provides exactly the ports it is handed: `Harness::build`
/// validates every module's `requires()` against `provides()` and nothing
/// else, which is the whole point.
struct FixedPorts(Vec<Port>);

impl Runtime for FixedPorts {
    fn provides(&self) -> Vec<Port> {
        self.0.clone()
    }
}

/// The ports a Redis-less native boot offers: `Db` from SQLite, `Mailer`
/// from the Resend adapter, `Signer` from `HARNESS_SECRET`, plus `Clock`
/// and `IdGen`, which the native runtime provides unconditionally.
///
/// Those two used to be left out as shorthand — "never the missing ones,
/// so omit them and keep this the worst case". That held only while no
/// mounted module asked for them. `Support` does, so omitting them made
/// this fail on the two ports a Redis-less boot always has, which is not
/// the question being asked. The question is whether a module
/// hard-requires `RateLimiter` or `KeyValue`; those stay out.
fn redis_less_native_ports() -> FixedPorts {
    FixedPorts(vec![
        Port::Db,
        Port::Mailer,
        Port::Signer,
        Port::Clock,
        Port::IdGen,
    ])
}

#[test]
fn composes_without_rate_limiter_or_key_value() {
    let built = modules(Harness::builder().venture(venture()))
        .runtime(redis_less_native_ports())
        .build();
    // `Err` carries every problem, so `Debug` is the useful half. If this
    // goes red, a module now hard-requires a port a Redis-less boot does
    // not offer: `Harness::build` would refuse, and the static
    // self-hosted binary would be a brick for every operator without
    // Redis — the exact failure issue #6 forbids.
    if let Err(error) = &built {
        panic!(
            "composition failed to build on a Redis-less native boot \
             (a module hard-requires RateLimiter or KeyValue? the \
             self-hosted binary would be a brick): {error:?}"
        );
    }
}

#[test]
fn no_module_requires_a_limiter_or_key_value() {
    let harness = modules(Harness::builder().venture(venture()))
        .runtime(redis_less_native_ports())
        .build()
        .expect("composition builds on a Redis-less native boot");
    for module in harness.modules() {
        for port in [Port::RateLimiter, Port::KeyValue] {
            assert!(
                !module.requires().contains(&port),
                "module `{}` requires {} — a Redis-less self-hosted boot \
                 provides neither, so the static binary would refuse to \
                 build and brick for operators without Redis. Declare the \
                 port in `optional()` and fail open at the call site \
                 (`RateLimitFailure::FailOpen`) instead.",
                module.name(),
                port.name(),
            );
        }
    }
}

#[test]
fn composes_a_non_empty_module_set_on_a_valid_venture() {
    // `Harness::build` runs core's venture validation (non-empty domain,
    // CORS origins that are never `*`), so a successful build here is
    // also the statement that `venture()` is valid on its own.
    let harness = modules(Harness::builder().venture(venture()))
        .runtime(redis_less_native_ports())
        .build()
        .expect("venture() passes core's validation: CORS origins are required");
    assert!(
        !harness.modules().is_empty(),
        "composition registered no modules: both link targets would serve \
         an empty venture"
    );
}
