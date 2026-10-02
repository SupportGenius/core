//! The composition compiles into a valid harness (with no secrets, the
//! state `fz` and CI see), the harness itself reports every venture
//! module, and the served surface lists them on `GET /__health`.

use std::sync::Arc;

use cratefield_adapter_resend::Resend;
use cratefield_core::Mailer;
use cratefield_runtime_cloudflare::{FetchClient, WorkersClock};
use supportgenius::compose;

/// A keyless Resend adapter as the test mailer: the composition only
/// needs a mailer port to build, and the tests never send. Keyless means
/// it reports `NotConfigured` rather than pretending, should a send ever
/// happen. The trailing `None`s of the `compose` calls are the captcha
/// and the text model — like every port here, mounted only when a secret
/// asks for one.
fn keyless_mailer() -> Arc<dyn Mailer> {
    Arc::new(Resend::new(
        Arc::new(FetchClient),
        Arc::new(WorkersClock),
        None,
        "no-reply@send.supportgeni.us",
        None,
    ))
}

#[test]
fn harness_builds() {
    let built = compose(keyless_mailer(), None, None, None);
    // The `ConfigError` lists every problem, so `Debug` on `Err` is the
    // useful output when this goes red. (`Cloudflare` is not `Debug`, so
    // the whole `Ok` half cannot be formatted.)
    if let Err(error) = &built {
        panic!("harness failed to build: {error:?}");
    }
}

#[test]
fn harness_reports_every_module() {
    let (harness, _runtime) = compose(keyless_mailer(), None, None, None).expect("harness builds");
    let names: Vec<&str> = harness
        .modules()
        .iter()
        .map(|module| module.name())
        .collect();
    for module in ["waitlist", "support", "escalation"] {
        assert!(
            names.contains(&module),
            "{module} missing from harness modules: {names:?}"
        );
    }
}

/// The served surface, through the test harness standing in for the
/// Worker runtime with the same modules the composition registers.
#[pollster::test]
async fn health_lists_every_module() {
    let kit = cratefield_testing::TestHarness::new(vec![
        // The wrapped waitlist the composition registers, so this exercises
        // the module list the Worker actually serves.
        Box::new(supportgenius_composition::waitlist()),
        Box::new(supportgenius_composition::support()),
        Box::new(supportgenius_composition::escalation()),
    ]);
    let response =
        cratefield_testing::request(&kit.router, http::Method::GET, "/__health", None).await;
    assert_eq!(response.status, http::StatusCode::OK);
    let health = response.json();
    let modules = health["modules"]
        .as_array()
        .expect("health carries a modules array");
    for module in ["waitlist", "support", "escalation"] {
        assert!(
            modules.iter().any(|entry| entry["name"] == module),
            "{module} missing from /__health modules: {modules:?}"
        );
    }
}
