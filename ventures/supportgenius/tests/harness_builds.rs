//! The composition compiles into a valid harness (with no secrets, the
//! state `fz` and CI see), the harness itself reports the `waitlist`
//! module, and the served surface lists it on `GET /__health`.

use std::sync::Arc;

use cratefield_adapter_resend::Resend;
use cratefield_core::Mailer;
use cratefield_module_waitlist::Waitlist;
use cratefield_runtime_cloudflare::{FetchClient, WorkersClock};
use supportgenius::compose;

/// A keyless Resend adapter as the test mailer: the composition only
/// needs a mailer port to build, and the tests never send. Keyless means
/// it reports `NotConfigured` rather than pretending, should a send ever
/// happen.
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
    let built = compose(keyless_mailer(), None);
    // The `ConfigError` lists every problem, so `Debug` on `Err` is the
    // useful output when this goes red. (`Cloudflare` is not `Debug`, so
    // the whole `Ok` half cannot be formatted.)
    if let Err(error) = &built {
        panic!("harness failed to build: {error:?}");
    }
}

#[test]
fn harness_reports_waitlist_module() {
    let (harness, _runtime) = compose(keyless_mailer(), None).expect("harness builds");
    let names: Vec<&str> = harness
        .modules()
        .iter()
        .map(|module| module.name())
        .collect();
    assert!(
        names.contains(&"waitlist"),
        "waitlist missing from harness modules: {names:?}"
    );
}

/// The served surface, through the test harness standing in for the
/// Worker runtime with the same `Waitlist` configuration.
#[pollster::test]
async fn health_lists_waitlist() {
    let kit = cratefield_testing::TestHarness::new(vec![Box::new(
        Waitlist::new()
            .products(["supportgenius"])
            .status_redirect("https://supportgeni.us/"),
    )]);
    let response =
        cratefield_testing::request(&kit.router, http::Method::GET, "/__health", None).await;
    assert_eq!(response.status, http::StatusCode::OK);
    let health = response.json();
    let modules = health["modules"]
        .as_array()
        .expect("health carries a modules array");
    assert!(
        modules.iter().any(|module| module["name"] == "waitlist"),
        "waitlist missing from /__health modules: {modules:?}"
    );
}
