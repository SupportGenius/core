//! The built-in ticketing routes (issue #24, part 2): a tenant lists and
//! reads the tickets the file stage filed into the module's own ticketing
//! — the ones with no tracker route, filed as `local:<ticket id>` — and
//! closes or reopens one that has been filed.
//!
//! `/v1/escalation/tickets` is guarded by the tenant's own `sg_…` API key,
//! the same key the `destinations` routes take: the tenant id is whatever
//! the key's signed payload names, and a suspended tenant or a revoked key
//! is refused with the identical `401` (see
//! [`crate::destinations::tenant_of`], shared by both surfaces).
//!
//! Every read is tenant-scoped and restricted to built-in tickets: a
//! ticket that belongs to another tenant, or one filed to an external
//! tracker, is a `404` — indistinguishable from one that does not exist,
//! so the routes never leak the shape of another tenant's workspace.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use serde_json::json;

use cratefield_core::{Json, ModuleContext, Problem, ProblemDef, Scope, Severity, UlidIdGen};

use crate::destinations::{iso_now, required_port_missing, tenant_of};
use crate::model::{EventKind, Kind, Status, Ticket, stage_seq};
use crate::store;
use crate::tenants::TenantDirectory;

/// `409` when a status change is not a transition this route owns — an
/// already-closed ticket closed again, or a ticket in no state its two
/// moves could start from.
const TICKET_STATUS_CONFLICT: ProblemDef = ProblemDef {
    slug: "escalation-ticket-status-conflict",
    status: StatusCode::CONFLICT,
    title: "Ticket status transition not allowed",
    description: "A built-in ticket can only move between `filed` and `closed`; the request \
                  asked for a move from some other state.",
};

struct TicketsState {
    ctx: Arc<ModuleContext>,
    /// Tenant status; `None` refuses every tenant (fail closed, see
    /// [`crate::tenants`]) — the same directory the destination routes use.
    tenants: Option<Arc<dyn TenantDirectory>>,
}

/// Mounts the tenant ticket routes, beside the destination routes in the
/// same module router.
pub(crate) fn router(
    ctx: Arc<ModuleContext>,
    tenants: Option<Arc<dyn TenantDirectory>>,
) -> axum::Router {
    let state = Arc::new(TicketsState { ctx, tenants });
    axum::Router::new()
        .route("/tickets", get(list_tickets))
        .route("/tickets/{id}", get(get_ticket))
        .route("/tickets/{id}/status", post(set_status))
        .with_state(state)
}

/// The small view of a ticket the routes return: the fields a support
/// engineer reads to triage, and the ids that tie it back to the
/// conversation and the funnel. The transcript, the tracker reference and
/// the judge's reasons stay off the wire.
#[derive(Debug, Serialize)]
struct TicketView {
    id: String,
    kind: Kind,
    status: Status,
    title: Option<String>,
    severity: Option<Severity>,
    body_markdown: Option<String>,
    conversation_id: String,
    created_at: String,
    updated_at: String,
}

impl From<&Ticket> for TicketView {
    fn from(ticket: &Ticket) -> Self {
        Self {
            id: ticket.id.clone(),
            kind: ticket.kind,
            status: ticket.status,
            title: ticket.title.clone(),
            severity: ticket.severity,
            body_markdown: ticket.body_markdown.clone(),
            conversation_id: ticket.conversation_id.clone(),
            created_at: ticket.created_at.clone(),
            updated_at: ticket.updated_at.clone(),
        }
    }
}

/// `GET /tickets`'s query: an optional status filter. Any other parameter
/// is ignored, and the status is parsed after auth, so a malformed filter
/// is never answered to an unauthenticated caller.
#[derive(Deserialize)]
struct ListQuery {
    status: Option<String>,
}

/// The `POST /tickets/{id}/status` body.
#[derive(Deserialize)]
struct StatusBody {
    status: String,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Lists the authenticated tenant's built-in tickets, newest first.
async fn list_tickets(
    scope: Scope,
    State(state): State<Arc<TicketsState>>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Response, Problem> {
    let (tenant_id, _actor) = tenant_of(&state.ctx, state.tenants.as_deref(), &headers).await?;
    let status = match query.status.as_deref() {
        None => None,
        Some(raw) => Some(parse_status(raw, &scope)?),
    };
    let Some(db) = state.ctx.ports.db.clone() else {
        return Err(required_port_missing("Db").instance(&scope.request_id));
    };
    let tickets = store::local_tickets(&*db, &tenant_id, status)
        .await
        .map_err(|_| Problem::internal())?;
    let views: Vec<TicketView> = tickets.iter().map(TicketView::from).collect();
    Ok((StatusCode::OK, Json(views)).into_response())
}

/// Reads one built-in ticket.
async fn get_ticket(
    scope: Scope,
    State(state): State<Arc<TicketsState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let (tenant_id, _actor) = tenant_of(&state.ctx, state.tenants.as_deref(), &headers).await?;
    let ticket = load_ticket(&state, &tenant_id, &id, &scope).await?;
    Ok((StatusCode::OK, Json(TicketView::from(&ticket))).into_response())
}

/// Moves a built-in ticket between `filed` and `closed`, recording the
/// transition on the ticket's audit trail and answering with the row it
/// wrote.
async fn set_status(
    scope: Scope,
    State(state): State<Arc<TicketsState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, Problem> {
    let (tenant_id, _actor) = tenant_of(&state.ctx, state.tenants.as_deref(), &headers).await?;
    let request: StatusBody = serde_json::from_slice(&body).map_err(|_| {
        Problem::validation_failed(
            "body: expected {\"status\": \"closed\"} or {\"status\": \"filed\"}",
        )
        .instance(&scope.request_id)
    })?;
    let target = parse_status(&request.status, &scope)?;
    if !matches!(target, Status::Closed | Status::Filed) {
        return Err(Problem::validation_failed(
            "status: this route moves a ticket between `filed` and `closed` only",
        )
        .instance(&scope.request_id));
    }

    let ticket = load_ticket(&state, &tenant_id, &id, &scope).await?;
    let allowed = matches!(
        (ticket.status, target),
        (Status::Filed, Status::Closed) | (Status::Closed, Status::Filed)
    );
    if !allowed {
        return Err(Problem::new(&TICKET_STATUS_CONFLICT)
            .with_detail(format!(
                "a ticket at `{}` cannot move to `{}`",
                ticket.status.as_str(),
                target.as_str()
            ))
            .instance(&scope.request_id));
    }

    let Some(db) = state.ctx.ports.db.clone() else {
        return Err(required_port_missing("Db").instance(&scope.request_id));
    };
    let at = iso_now(&state.ctx);
    // The transition is recorded the way every other status change is: one
    // `sg_ticket_events` row at the stage the ticket occupies, in the same
    // batch as the status write, so the trail and the row cannot disagree.
    let event = store::insert_event_stmt(
        &new_id(&state.ctx),
        &ticket.id,
        stage_seq(ticket.stage, 1),
        &at,
        ticket.stage,
        match target {
            Status::Closed => EventKind::Closed,
            _ => EventKind::Reopened,
        },
        &json!({
            "status": target.as_str(),
            "previous_status": ticket.status.as_str(),
        }),
    );
    db.batch_atomic(&[
        store::update_ticket_status_stmt(&ticket.id, target, &at),
        event,
    ])
    .await
    .map_err(|_| Problem::internal())?;

    // Answer with the row the transition wrote, never a reconstruction.
    let ticket = load_ticket(&state, &tenant_id, &id, &scope).await?;
    Ok((StatusCode::OK, Json(TicketView::from(&ticket))).into_response())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// The tenant's built-in ticket with this id, or the `404` that covers
/// "no such ticket", "another tenant's ticket" and "filed to a tracker"
/// alike.
async fn load_ticket(
    state: &TicketsState,
    tenant_id: &str,
    id: &str,
    scope: &Scope,
) -> Result<Ticket, Problem> {
    let Some(db) = state.ctx.ports.db.clone() else {
        return Err(required_port_missing("Db").instance(&scope.request_id));
    };
    store::load_local_ticket(&*db, tenant_id, id)
        .await
        .map_err(|_| Problem::internal())?
        .ok_or_else(|| Problem::not_found().instance(&scope.request_id))
}

/// Parses a ticket status from the wire, or the `400` that names the bad
/// value — the same validation answer the rest of the harness gives.
fn parse_status(raw: &str, scope: &Scope) -> Result<Status, Problem> {
    raw.parse::<Status>().map_err(|_| {
        Problem::validation_failed(format!("status: unknown ticket status `{raw}`"))
            .instance(&scope.request_id)
    })
}

/// A fresh ULID from the `IdGen` port, or core's `UlidIdGen` when the
/// runtime resolved none — the same default the pipeline applies.
fn new_id(ctx: &ModuleContext) -> String {
    ctx.ports
        .id_gen
        .clone()
        .unwrap_or_else(|| Arc::new(UlidIdGen))
        .ulid()
}
