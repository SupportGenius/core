//! The destination routes (issue #23): a tenant — or the operator acting
//! for one — names where its escalations are filed and the token to file
//! there, and the token is stored **encrypted**, never as a Worker secret
//! an operator has to edit by hand.
//!
//! `/v1/escalation/destinations` is guarded by the tenant's own `sg_…` API
//! key (the one `module-support` issues); the tenant id is whatever the
//! key's signed payload names. `/v1/escalation/admin/tenants/{tenant_id}/
//! destinations` is guarded by the harness admin token, for a tenant that
//! cannot do it itself.
//!
//! `PUT` asks the tracker once about the credential (see [`PROBE_ID`]) and
//! only then seals the credential — and, for a webhook, its URL, which is
//! itself a bearer capability — into the tenant's `cratefield-secrets`
//! store. The `sg_destinations` row keeps the destination (a webhook URL
//! replaced by a marker) and a `secret:` reference, never the secret. No
//! response, error body or log line carries either.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::put;
use serde::Deserialize;
use serde_json::json;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use cratefield_core::{
    Credential, Database, Destination, Json, ModuleContext, Problem, ProblemDef, Scope,
    SystemClock, TrackerError, require_admin,
};
use cratefield_kms::Kms;
use cratefield_secrets::{Actor, SecretBytes};

use crate::secrets::{
    CREDENTIAL_REF, CREDENTIAL_SECRET, SECRET_REF_PREFIX, WEBHOOK_SECRET, WEBHOOK_URL_MARKER,
    audited_secrets,
};
use crate::tenants::TenantDirectory;

/// The external id the validation probe asks the tracker about — never a
/// real ticket. `Rejected` ("no such ticket") is a *healthy* answer, so the
/// probe catches a refused credential (`Unauthorized`) or a tracker with no
/// adapter for the destination (`NotConfigured`); it does **not** prove the
/// destination itself is right, and a bad repo or project comes back
/// `Rejected` and is accepted.
const PROBE_ID: &str = "escalation-destination-probe";

/// The config key the shared revoked-kid list lives under. `tenancy`
/// documents that both consumer modules read *one* name, so a single edit
/// revokes a key everywhere; `module-support` resolves that name through
/// its module prefix, so this is the fully-qualified key and this module
/// reads the identical one rather than a prefixed key of its own.
const REVOKED_KIDS_CONFIG_KEY: &str = "SUPPORT_REVOKED_KIDS";

/// Every way tenant-key authentication can fail answers with this one
/// indistinguishable `401`, the same shape `module-support` uses.
const UNAUTHORIZED: ProblemDef = ProblemDef {
    slug: "escalation-unauthorized",
    status: StatusCode::UNAUTHORIZED,
    title: "Escalation API key unauthorized",
    description: "An escalation route was reached without a valid, unrevoked tenant API key.",
};

/// `422` when the tracker refuses the credential (`401`/`403`): wrong,
/// expired or under-scoped, and no retry helps.
const CREDENTIAL_REJECTED: ProblemDef = ProblemDef {
    slug: "escalation-credential-rejected",
    status: StatusCode::UNPROCESSABLE_ENTITY,
    title: "Tracker credential rejected",
    description: "The tracker refused the credential: it was wrong, expired, or lacked the \
                  scope the call needed.",
};

/// `422` when the tracker serves no adapter for the named destination.
const DESTINATION_UNSUPPORTED: ProblemDef = ProblemDef {
    slug: "escalation-destination-unsupported",
    status: StatusCode::UNPROCESSABLE_ENTITY,
    title: "Tracker destination unsupported",
    description: "No tracker adapter is configured for this destination, so tickets filed \
                  there could never reach it.",
};

/// `503` when the tracker is unreachable right now, so the credential
/// cannot be checked.
const TRACKER_UNAVAILABLE: ProblemDef = ProblemDef {
    slug: "escalation-tracker-unavailable",
    status: StatusCode::SERVICE_UNAVAILABLE,
    title: "Tracker unavailable",
    description: "The tracker could not be reached to validate the credential; retry later.",
};

/// `503` when the deployment configured no KMS: credentials cannot be
/// stored encrypted, so the route refuses rather than storing one in the
/// clear.
const KMS_NOT_CONFIGURED: ProblemDef = ProblemDef {
    slug: "escalation-kms-not-configured",
    status: StatusCode::SERVICE_UNAVAILABLE,
    title: "Credential storage is not configured",
    description: "No KMS is configured, so tenant credentials cannot be stored encrypted.",
};

struct DestinationsState {
    ctx: Arc<ModuleContext>,
    /// The KMS resolved once at router build time, or `None` when the
    /// deployment configured none — the routes then answer `503`.
    kms: Option<Arc<dyn Kms>>,
    /// Tenant status; `None` refuses every tenant (fail closed, see
    /// [`crate::tenants`]).
    tenants: Option<Arc<dyn TenantDirectory>>,
}

/// Mounts the tenant and admin destination routes. `kms` is built from
/// config by the caller (`crate::secrets::kms_from_config`) so the provider
/// is resolved once, not per request.
pub(crate) fn router(
    ctx: Arc<ModuleContext>,
    kms: Option<Arc<dyn Kms>>,
    tenants: Option<Arc<dyn TenantDirectory>>,
) -> axum::Router {
    let state = Arc::new(DestinationsState { ctx, kms, tenants });
    axum::Router::new()
        .route(
            "/destinations",
            put(put_tenant).get(get_tenant).delete(delete_tenant),
        )
        .route(
            "/admin/tenants/{tenant_id}/destinations",
            put(put_admin).get(get_admin).delete(delete_admin),
        )
        .with_state(state)
}

/// One `PUT` body: the destination and the credential to file with, the
/// credential a non-empty string.
#[derive(Deserialize)]
struct PutBody {
    destination: Destination,
    credential: String,
}

// ---------------------------------------------------------------------------
// Tenant routes (tenant API key)
// ---------------------------------------------------------------------------

async fn put_tenant(
    scope: Scope,
    State(state): State<Arc<DestinationsState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, Problem> {
    let (tenant_id, actor) = tenant_of(&state.ctx, state.tenants.as_deref(), &headers).await?;
    put_destination(&state, &tenant_id, &actor, &scope, &body).await
}

async fn get_tenant(
    scope: Scope,
    State(state): State<Arc<DestinationsState>>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let (tenant_id, actor) = tenant_of(&state.ctx, state.tenants.as_deref(), &headers).await?;
    get_destination(&state, &tenant_id, &actor, &scope).await
}

async fn delete_tenant(
    scope: Scope,
    State(state): State<Arc<DestinationsState>>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let (tenant_id, actor) = tenant_of(&state.ctx, state.tenants.as_deref(), &headers).await?;
    delete_destination(&state, &tenant_id, &actor, &scope).await
}

// ---------------------------------------------------------------------------
// Admin routes (harness admin token, tenant named in the path)
// ---------------------------------------------------------------------------

async fn put_admin(
    scope: Scope,
    State(state): State<Arc<DestinationsState>>,
    Path(tenant_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, Problem> {
    let actor = admin_of(&state, &headers, &tenant_id, &scope).await?;
    put_destination(&state, &tenant_id, &actor, &scope, &body).await
}

async fn get_admin(
    scope: Scope,
    State(state): State<Arc<DestinationsState>>,
    Path(tenant_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let actor = admin_of(&state, &headers, &tenant_id, &scope).await?;
    get_destination(&state, &tenant_id, &actor, &scope).await
}

async fn delete_admin(
    scope: Scope,
    State(state): State<Arc<DestinationsState>>,
    Path(tenant_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let actor = admin_of(&state, &headers, &tenant_id, &scope).await?;
    delete_destination(&state, &tenant_id, &actor, &scope).await
}

// ---------------------------------------------------------------------------
// The shared handler bodies
// ---------------------------------------------------------------------------

/// `PUT` a tenant's destination and credential: validate the credential
/// against the tracker, seal it (and a webhook URL) into the encrypted
/// store, then write the row. The response names the destination and
/// whether a credential is set, never the credential itself.
async fn put_destination(
    state: &DestinationsState,
    tenant_id: &str,
    actor: &Actor,
    scope: &Scope,
    body: &Bytes,
) -> Result<Response, Problem> {
    let Some(kms) = state.kms.clone() else {
        return Err(Problem::new(&KMS_NOT_CONFIGURED).instance(&scope.request_id));
    };
    let request: PutBody = serde_json::from_slice(body).map_err(|_| {
        Problem::validation_failed(
            "body: expected {\"destination\": <destination>, \"credential\": \"<non-empty string>\"}",
        )
        .instance(&scope.request_id)
    })?;
    if request.credential.is_empty() {
        return Err(
            Problem::validation_failed("credential: required, a non-empty string")
                .instance(&scope.request_id),
        );
    }

    // One probe before anything is stored; the arms that store nothing are
    // the credential's and the destination's failure modes (see PROBE_ID).
    let Some(tracker) = state.ctx.ports.tracker.clone() else {
        return Err(required_port_missing("Tracker").instance(&scope.request_id));
    };
    let credential = Credential::new(request.credential);
    match tracker
        .status(&request.destination, &credential, PROBE_ID)
        .await
    {
        Ok(_) | Err(TrackerError::Rejected(_)) => {}
        Err(TrackerError::Unauthorized) => {
            return Err(Problem::new(&CREDENTIAL_REJECTED)
                .with_detail("the tracker refused the credential")
                .instance(&scope.request_id));
        }
        Err(TrackerError::NotConfigured) => {
            return Err(Problem::new(&DESTINATION_UNSUPPORTED)
                .with_detail(format!(
                    "no tracker adapter serves {}",
                    request.destination.kind()
                ))
                .instance(&scope.request_id));
        }
        Err(TrackerError::Transient { .. }) => {
            return Err(Problem::new(&TRACKER_UNAVAILABLE).instance(&scope.request_id));
        }
    }

    let Some(db) = state.ctx.ports.db.clone() else {
        return Err(required_port_missing("Db").instance(&scope.request_id));
    };
    // The credential's bytes, read once here from the redacting handle so
    // no second copy of the plaintext is kept around.
    let secret = SecretBytes::new(credential.expose().as_bytes().to_vec());
    drop(credential);
    let secret_store = audited_secrets(kms, db.clone())
        .tenant(tenant_id, db.clone())
        .map_err(|_| Problem::internal())?;
    secret_store
        .put(CREDENTIAL_SECRET, &secret, actor)
        .await
        .map_err(|_| Problem::internal())?;

    // A webhook URL is itself the capability, so it is sealed too and the
    // stored destination keeps only a marker in its place. Any other
    // destination retires a URL stored by an earlier webhook, so an old URL
    // does not stay decryptable behind the new row.
    let stored_destination = match &request.destination {
        Destination::Webhook { url } => {
            let url = SecretBytes::new(url.as_bytes().to_vec());
            secret_store
                .put(WEBHOOK_SECRET, &url, actor)
                .await
                .map_err(|_| Problem::internal())?;
            Destination::Webhook {
                url: WEBHOOK_URL_MARKER.to_owned(),
            }
        }
        other => {
            secret_store
                .delete(WEBHOOK_SECRET, actor)
                .await
                .map_err(|_| Problem::internal())?;
            other.clone()
        }
    };

    let now = iso_now(&state.ctx);
    let statement =
        crate::store::put_destination_stmt(tenant_id, &stored_destination, CREDENTIAL_REF, &now);
    db.batch_atomic(&[statement]).await?;

    Ok((
        StatusCode::OK,
        Json(json!({ "destination": stored_destination, "credential_set": true })),
    )
        .into_response())
}

/// `GET` a tenant's destination. `404` when none is configured; the
/// `destination` is the stored column (for a webhook, a marker in place of
/// the URL), and `credential_set` says whether the referenced credential
/// can currently be resolved.
async fn get_destination(
    state: &DestinationsState,
    tenant_id: &str,
    actor: &Actor,
    scope: &Scope,
) -> Result<Response, Problem> {
    let Some(db) = state.ctx.ports.db.clone() else {
        return Err(required_port_missing("Db").instance(&scope.request_id));
    };
    let Some((destination, credential_ref)) = crate::store::load_destination(&*db, tenant_id)
        .await
        .map_err(|_| Problem::internal())?
    else {
        return Err(Problem::not_found().instance(&scope.request_id));
    };
    let credential_set = credential_present(state, tenant_id, &credential_ref, &db, actor)
        .await
        .map_err(|_| Problem::internal())?;
    Ok((
        StatusCode::OK,
        Json(json!({ "destination": destination, "credential_set": credential_set })),
    )
        .into_response())
}

/// `DELETE` a tenant's destination and both of its secrets. `204` whether
/// or not a row existed — the target state is "no destination", and a
/// repeated delete already has it.
async fn delete_destination(
    state: &DestinationsState,
    tenant_id: &str,
    actor: &Actor,
    scope: &Scope,
) -> Result<Response, Problem> {
    let Some(db) = state.ctx.ports.db.clone() else {
        return Err(required_port_missing("Db").instance(&scope.request_id));
    };
    if let Some(kms) = state.kms.clone()
        && let Ok(secret_store) = audited_secrets(kms, db.clone()).tenant(tenant_id, db.clone())
    {
        // Deleting a name that was never set is a no-op, so this is safe
        // whether the row was a secret-backed one or a legacy config-key
        // one.
        for name in [CREDENTIAL_SECRET, WEBHOOK_SECRET] {
            secret_store
                .delete(name, actor)
                .await
                .map_err(|_| Problem::internal())?;
        }
    }
    db.batch_atomic(&[crate::store::delete_destination_stmt(tenant_id)])
        .await?;
    Ok((StatusCode::NO_CONTENT).into_response())
}

// ---------------------------------------------------------------------------
// Auth and helpers
// ---------------------------------------------------------------------------

/// The tenant a tenant-key request acts for, and the audit actor for it.
/// A valid key for a tenant that is not active (suspended, closed,
/// unknown) is refused with the same indistinguishable `401` as a bad key
/// — the answer `module-support`'s routes give the same key.
///
/// Shared by the destination and ticket routes (hence `ctx`/`tenants`
/// rather than a route state): every surface authenticated by a tenant
/// `sg_…` key takes this same path, so a key accepted on one is accepted
/// identically on the other.
pub(crate) async fn tenant_of(
    ctx: &ModuleContext,
    tenants: Option<&dyn TenantDirectory>,
    headers: &HeaderMap,
) -> Result<(String, Actor), Problem> {
    let key = authenticate(ctx, headers)?;
    let tenant_id = key.tenant_id;
    if !tenant_is_active(ctx, tenants, &tenant_id).await? {
        return Err(Problem::new(&UNAUTHORIZED));
    }
    // A valid signature is not enough: the key must still be on record, so
    // a key revoked by deleting its row is refused here too. No directory
    // means no way to tell, so no key is live (fail closed).
    let live = match tenants {
        Some(tenants) => tenants
            .key_is_live(ctx, &tenant_id, &key.key_id)
            .await
            .map_err(Problem::from)?,
        None => false,
    };
    if !live {
        return Err(Problem::new(&UNAUTHORIZED));
    }
    let actor = Actor::new(format!("tenant:{tenant_id}")).map_err(|_| Problem::internal())?;
    Ok((tenant_id, actor))
}

/// The audit actor for an admin request, once the admin token checks out
/// and the tenant named in the path is active. A tenant that is unknown,
/// suspended or closed is one `404`, the way `module-support`'s admin
/// routes for a tenant answer it.
async fn admin_of(
    state: &DestinationsState,
    headers: &HeaderMap,
    tenant_id: &str,
    scope: &Scope,
) -> Result<Actor, Problem> {
    require_admin(&*state.ctx.config, headers)?;
    if !tenant_is_active(&state.ctx, state.tenants.as_deref(), tenant_id).await? {
        return Err(Problem::not_found().instance(&scope.request_id));
    }
    Actor::new("admin").map_err(|_| Problem::internal())
}

/// Whether `tenant_id` is active, per the composed [`TenantDirectory`].
/// No directory means no way to tell, so no tenant is (fail closed). A
/// database failure is a `500`, not evidence about the tenant.
async fn tenant_is_active(
    ctx: &ModuleContext,
    tenants: Option<&dyn TenantDirectory>,
    tenant_id: &str,
) -> Result<bool, Problem> {
    let Some(tenants) = tenants else {
        return Ok(false);
    };
    tenants
        .is_active(ctx, tenant_id)
        .await
        .map_err(Problem::from)
}

/// Whether the credential the row references can be resolved. A `secret:`
/// reference is checked against the encrypted store (no KMS, and no route
/// that could have written a secret, means "not set"); a bare reference
/// against config (the legacy form).
async fn credential_present(
    state: &DestinationsState,
    tenant_id: &str,
    credential_ref: &str,
    db: &Arc<dyn Database>,
    actor: &Actor,
) -> Result<bool, cratefield_secrets::SecretsError> {
    let Some(name) = credential_ref.strip_prefix(SECRET_REF_PREFIX) else {
        return Ok(state.ctx.config.get(credential_ref).is_some());
    };
    let Some(kms) = state.kms.clone() else {
        return Ok(false);
    };
    let secret_store = audited_secrets(kms, db.clone()).tenant(tenant_id, db.clone())?;
    match secret_store.get(name, actor).await {
        Ok(found) => Ok(found.is_some()),
        // No data key means nothing was ever sealed in this store, so no
        // credential is set — a deliberate state, not a failure.
        Err(cratefield_secrets::SecretsError::NoKey(_)) => Ok(false),
        Err(err) => Err(err),
    }
}

/// Verifies the `Authorization` bearer as a tenant API key and returns the
/// tenant and key it names. Every failure collapses into [`UNAUTHORIZED`].
/// The module keeps no tenant or key table of its own, so tenant *status*
/// and the key's own row (revocation) are checked separately, through the
/// composed [`TenantDirectory`] (see [`tenant_of`]).
pub(crate) fn authenticate(
    ctx: &ModuleContext,
    headers: &HeaderMap,
) -> Result<tenancy::TenantKey, Problem> {
    let unauthorized = || Problem::new(&UNAUTHORIZED);
    let Some(raw) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return Err(unauthorized());
    };
    let Some(presented) = tenancy::bearer(raw) else {
        return Err(unauthorized());
    };
    let Some(signer) = ctx.ports.signer.as_deref() else {
        return Err(unauthorized());
    };
    let revoked =
        tenancy::parse_revoked_kids(&ctx.config.get(REVOKED_KIDS_CONFIG_KEY).unwrap_or_default());
    let Ok(key) = tenancy::verify(signer, presented, &revoked) else {
        return Err(unauthorized());
    };
    Ok(key)
}

/// The `internal` problem for a required port that `requires()` promised
/// but the context does not carry: a harness bug, answered rather than
/// panicked.
pub(crate) fn required_port_missing(name: &str) -> Problem {
    Problem::internal().with_detail(format!("required port {name} is missing"))
}

/// The current instant, RFC 3339, from the `Clock` port when the runtime
/// resolved one and the system clock otherwise.
pub(crate) fn iso_now(ctx: &ModuleContext) -> String {
    let clock = ctx
        .ports
        .clock
        .clone()
        .unwrap_or_else(|| Arc::new(SystemClock));
    clock.now().format(&Rfc3339).unwrap_or_else(|_| {
        OffsetDateTime::UNIX_EPOCH
            .format(&Rfc3339)
            .unwrap_or_default()
    })
}
