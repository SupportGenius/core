//! `GET /v1/escalation/admin/health` (issue #39): what the escalation
//! pipeline is doing, in one poll.
//!
//! Answers two questions a log line cannot: "is work piling up?" and "is
//! the drain still running?". The queue lives in the outbox and the drain
//! that empties it runs on a cron nobody watches. Guarded by the harness
//! admin token, like the destinations admin routes: this is operational
//! data about the deployment, which no tenant key may read.
//!
//! `now` is the module's `Clock` port wherever the runtime resolved one, so
//! a test can move it and get an exact age. Every age is computed in Rust
//! from the RFC 3339 the writers stamped — date arithmetic inside SQL
//! (`julianday`, `EXTRACT`) is the dialect-specific surface ADR 0004
//! forbids, and the arithmetic is two subtractions.

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde::Serialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use cratefield_core::{
    Action, Audience, Database, Json, ModuleContext, Outcome, Problem, Scope, Surface, SystemClock,
    require_admin,
};

use crate::destinations::required_port_missing;
use crate::model::Stage;
use crate::store::{self, StageDepth};

/// The window `dead_letters_24h` counts over.
const DEAD_LETTER_WINDOW_HOURS: i64 = 24;

struct HealthState {
    ctx: Arc<ModuleContext>,
}

pub(crate) fn router(ctx: Arc<ModuleContext>) -> axum::Router {
    let state = Arc::new(HealthState { ctx });
    axum::Router::new()
        .route("/admin/health", get(health))
        .with_state(state)
}

/// The module's declared surface (ADR 0010): one action per route
/// [`router`] mounts, so `GET /__surface` and the `OpenAPI` document
/// describe exactly what exists.
pub(crate) fn surface() -> Surface {
    Surface::new().action(
        Action::get("get-admin-health", "/admin/health")
            .audience(Audience::Admin)
            .outcome(Outcome::Json),
    )
}

#[derive(Debug, Serialize)]
struct TopicHealth {
    /// The stage's outbox topic: `draft`, `judge`, `file`, `notify`,
    /// `follow`.
    topic: &'static str,
    /// Rows queued for the topic, leased or not.
    depth: i64,
    /// Rows due now — the predicate core's `Outbox::claim_due` claims
    /// with, so this is exactly what the next sweep will take.
    due: i64,
    /// How long the oldest due row has waited, or `null` when nothing is
    /// due. The number that says a stage is stuck: a deep queue that is
    /// draining is fine, a due row nobody claimed is not.
    oldest_due_age_secs: Option<i64>,
}

#[derive(Debug, Serialize)]
struct Health {
    /// Every stage, always — a stage absent and a stage with an empty
    /// queue are one fact reported two ways, and only one is fixed-shape.
    topics: Vec<TopicHealth>,
    /// Tickets dead-lettered in the last 24 hours, across every stage.
    /// See [`store::dead_letters_since`] for exactly what this counts.
    dead_letters_24h: i64,
    /// When the last drain finished without error, RFC 3339, or `null` if
    /// no drain has ever completed.
    last_drain_ok_at: Option<String>,
    /// How long ago that was, in seconds, or `null` with the timestamp.
    last_drain_age_secs: Option<i64>,
}

async fn health(
    scope: Scope,
    State(state): State<Arc<HealthState>>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    // Auth before anything else, exactly like every other admin route.
    require_admin(&*state.ctx.config, &headers)?;
    let Some(db) = state.ctx.ports.db.clone() else {
        return Err(required_port_missing("Db").instance(&scope.request_id));
    };
    // One clock reading for every predicate and every age, so two
    // statements cannot disagree about what "now" was.
    let now = now(&state.ctx);
    let now_at = rfc3339(now);
    let topics = stage_topics(&*db, now, &now_at).await?;
    let cutoff = rfc3339(
        now.checked_sub(time::Duration::hours(DEAD_LETTER_WINDOW_HOURS))
            // A clock before 1970 minus a day is not a thing; reporting
            // "no dead letters" would be a guess, so the read fails
            // instead. Unreachable on any real clock.
            .ok_or_else(Problem::internal)?,
    );
    let dead_letters_24h = store::dead_letters_since(&*db, &cutoff)
        .await
        .map_err(|_| Problem::internal())?;
    let last_drain_ok_at = store::last_drain_ok_at(&*db)
        .await
        .map_err(|_| Problem::internal())?;

    let document = Health {
        topics,
        dead_letters_24h,
        last_drain_age_secs: last_drain_ok_at.as_deref().and_then(|at| age_secs(now, at)),
        last_drain_ok_at,
    };
    Ok((StatusCode::OK, Json(document)).into_response())
}

/// Every stage's depth, in pipeline order, empty stages as zeroes. The
/// query groups by topic, so an empty stage is absent from its answer;
/// listing all five regardless keeps the document fixed-shape for a
/// dashboard, which a chart that drops an emptied stage would lie about.
async fn stage_topics(
    db: &dyn Database,
    now: OffsetDateTime,
    now_at: &str,
) -> Result<Vec<TopicHealth>, Problem> {
    let stages = [
        Stage::Draft,
        Stage::Judge,
        Stage::File,
        Stage::Notify,
        Stage::Follow,
    ];
    let topics: Vec<&str> = stages.iter().map(|stage| stage.as_topic()).collect();
    let depths = store::stage_depths(db, now_at, &topics)
        .await
        .map_err(|_| Problem::internal())?;
    Ok(stages
        .iter()
        .map(|stage| {
            let topic = stage.as_topic();
            let found: Option<&StageDepth> = depths.iter().find(|row| row.topic == topic);
            TopicHealth {
                topic,
                depth: found.map_or(0, |row| row.depth),
                due: found.map_or(0, |row| row.due),
                oldest_due_age_secs: found
                    .and_then(|row| row.oldest_due_at.as_deref())
                    .and_then(|at| age_secs(now, at)),
            }
        })
        .collect())
}

/// How many seconds ago `at` was, or `None` if it does not parse. A stamp
/// this module wrote always parses; one that does not is written by
/// something else or truncated, and a wrong age is worse than none — the
/// raw timestamp beside it still tells an operator what is in the column.
fn age_secs(now: OffsetDateTime, at: &str) -> Option<i64> {
    OffsetDateTime::parse(at, &Rfc3339)
        .ok()
        .map(|then| (now - then).whole_seconds())
}

/// The module's `Clock` port where the runtime resolved one, the system
/// clock otherwise — the same fallback the destinations routes use.
fn now(ctx: &ModuleContext) -> OffsetDateTime {
    let clock = ctx
        .ports
        .clock
        .clone()
        .unwrap_or_else(|| Arc::new(SystemClock));
    clock.now()
}

fn rfc3339(at: OffsetDateTime) -> String {
    at.format(&Rfc3339).unwrap_or_default()
}
