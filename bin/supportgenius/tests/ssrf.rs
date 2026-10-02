//! Issue #29's SSRF acceptance for the native binary: the hardened
//! `ReqwestClient` the bin mounts as the `HttpClient` port refuses the
//! destinations a connector must never be able to reach — the cloud
//! metadata address and loopback — with `HttpError::BlockedDestination`,
//! before any socket is opened (the checks run on the request URI, so
//! these tests make no network calls).
//!
//! The Workers runtime's equivalent guarantee is the platform's own
//! private-network refusal plus the connectors' allowlist
//! (`module_support`'s `ConnectorConfig::allows`).

use bytes::Bytes;
use cratefield_core::{HttpClient, HttpError};

fn get(uri: &str) -> http::Request<Bytes> {
    http::Request::builder()
        .method(http::Method::GET)
        .uri(uri)
        .body(Bytes::new())
        .expect("request builds")
}

#[pollster::test]
async fn the_metadata_address_is_refused() {
    let client = cratefield_runtime_native::ReqwestClient::new();
    let err = client
        .send(get("http://169.254.169.254/latest/meta-data/"))
        .await
        .expect_err("the metadata address is refused");
    assert!(matches!(err, HttpError::BlockedDestination(_)), "{err}");
}

#[pollster::test]
async fn loopback_is_refused() {
    let client = cratefield_runtime_native::ReqwestClient::new();
    for uri in ["http://localhost/", "http://127.0.0.1/"] {
        let err = client
            .send(get(uri))
            .await
            .expect_err("loopback is refused");
        assert!(
            matches!(err, HttpError::BlockedDestination(_)),
            "{uri}: {err}"
        );
    }
}
