//! Issue #23 follow-up: escalation's destination routes refuse a
//! suspended tenant the way `module-support` does, through the real
//! composition — support's `sg_tenants` row is the one source of truth,
//! adapted into escalation by `SupportTenants`.

use std::sync::Arc;

use axum::http::{Method, StatusCode};
use cratefield_core::{MapConfig, Statement};
use cratefield_testing::{TestHarness, request_as};
use serde_json::json;

const ADMIN_TOKEN: &str = "test-admin-token-0123456789abcdef";
const TENANTS: &str = "/v1/support/admin/tenants";
const SOURCES: &str = "/v1/support/sources";
const DESTINATIONS: &str = "/v1/escalation/destinations";

fn kit() -> TestHarness {
    TestHarness::with_ports(
        vec![
            Box::new(supportgenius_composition::support()),
            Box::new(supportgenius_composition::escalation()),
        ],
        |ports| {
            ports.config = Arc::new(MapConfig::from_pairs([("ADMIN_TOKEN", ADMIN_TOKEN)]));
        },
    )
}

async fn mint_tenant(kit: &TestHarness) -> (String, String) {
    let reply = request_as(
        &kit.router,
        Method::POST,
        TENANTS,
        ADMIN_TOKEN,
        Some(&json!({ "name": "Acme" }).to_string()),
    )
    .await;
    assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.json());
    let body = reply.json();
    (
        body["tenant_id"].as_str().expect("tenant_id").to_owned(),
        body["api_key"].as_str().expect("api_key").to_owned(),
    )
}

#[pollster::test]
async fn a_suspended_tenant_is_refused_by_support_and_escalation_alike() {
    let kit = kit();
    let (tenant_id, key) = mint_tenant(&kit).await;
    let admin = format!("/v1/escalation/admin/tenants/{tenant_id}/destinations");

    // Active: the key reaches both modules (no destination yet is a 404 on
    // GET; DELETE of nothing is a 204).
    let reply = request_as(&kit.router, Method::GET, DESTINATIONS, &key, None).await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND);
    let reply = request_as(&kit.router, Method::DELETE, DESTINATIONS, &key, None).await;
    assert_eq!(reply.status, StatusCode::NO_CONTENT);
    let reply = request_as(&kit.router, Method::DELETE, &admin, ADMIN_TOKEN, None).await;
    assert_eq!(reply.status, StatusCode::NO_CONTENT);

    // Suspend the tenant in support's own table.
    pollster::block_on(kit.db.batch_atomic(&[Statement::new(format!(
        "UPDATE sg_tenants SET status = 'suspended' WHERE id = '{tenant_id}'"
    ))]))
    .expect("the tenant is suspended");

    // Support refuses the key...
    let reply = request_as(&kit.router, Method::GET, SOURCES, &key, None).await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "support");
    let refused = reply.status;

    // ...and escalation refuses it the same way on every method.
    let body = json!({
        "destination": { "git_hub": { "owner": "acme", "repo": "api" } },
        "credential": "token",
    })
    .to_string();
    for (method, payload) in [
        (Method::GET, None),
        (Method::PUT, Some(body.as_str())),
        (Method::DELETE, None),
    ] {
        let reply = request_as(&kit.router, method.clone(), DESTINATIONS, &key, payload).await;
        assert_eq!(reply.status, refused, "tenant {method}");
        let reply = request_as(&kit.router, method.clone(), &admin, ADMIN_TOKEN, payload).await;
        assert_eq!(reply.status, StatusCode::NOT_FOUND, "admin {method}");
    }
}
