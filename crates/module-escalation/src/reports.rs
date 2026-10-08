//! Error and bug intake (issue #65): what the application and its users
//! report becomes deduplicated, routed tracker issues.
//!
//! `POST /v1/escalation/reports` takes one of two bodies, tagged by `kind`:
//! an **error** the app caught (its type, message, stack frames, release and
//! occurrence count) or a **bug** a user described. It is guarded by the
//! tenant's own `sg_…` API key, exactly as the destination routes are (see
//! [`crate::destinations::tenant_of`]); the body is parsed only after that,
//! so a malformed report is never answered to an unauthenticated caller.
//!
//! Three properties the route holds to:
//!
//! 1. **Deduplicated.** An error is grouped by a *server*-computed
//!    fingerprint — the error type plus its top frames, with no line
//!    numbers — so the thousandth report of the same failure bumps a counter
//!    on one issue rather than filing a thousand of them. A client-supplied
//!    fingerprint is ignored, not rejected: the server value is the one that
//!    groups, so two SDKs that disagree cannot split a group in two.
//! 2. **Redacted.** Every free-text field is cut to
//!    [`MAX_FIELD_CHARS`] and goes through [`crate::redact`] before it is
//!    stored or sent, a report carries at most [`MAX_FRAMES`] frames, and
//!    the body itself is capped at core's `MAX_BODY_BYTES` (`413` past it)
//!    — a report is an input, and an unbounded one is a cost. A report that
//!    reads like a prompt
//!    injection is **held** — stored, answered `202`, never filed, and never
//!    read by anything downstream. Nothing on this path calls a
//!    [`TextModel`](cratefield_core::TextModel) at all: the work is
//!    deterministic grouping, and a report is an input, not an instruction.
//! 3. **Bounded.** A tenant may make at most `REPORTS_MAX_ISSUES_PER_HOUR`
//!    tracker writes per hour. Past the cap the count still rises, nothing
//!    is written to the tracker, and the first cap-hit in the window
//!    publishes one `report.spike` webhook event.

use std::fmt::Write as _;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use sea_query::{Alias, Expr, OnConflict, Query};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use time::Duration;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use cratefield_core::{
    Action, Credential, Database, Destination, Json, ModuleConfig, ModuleContext, Outcome, Problem,
    RoutePolicy, Scope, Severity, Statement, Surface, TicketComment, TicketDraft, TicketState,
    Tracker, UlidIdGen,
};
use cratefield_module_webhooks::{PublishError, Webhooks};
use cratefield_secrets::Actor;

use crate::destinations::{iso_now, required_port_missing, tenant_of};
use crate::redact::{Screen, redact, screen};
use crate::store;
use crate::tenants::TenantDirectory;

/// How many tracker writes (files and comments) a tenant may make per hour
/// window when nothing is configured.
const DEFAULT_MAX_WRITES_PER_HOUR: u32 = 20;

/// How long a tenant's raw occurrences and spent budget windows are kept
/// when nothing is configured (the `ESCALATION_REPORTS_RETENTION_DAYS`
/// key). Group rows are never pruned: a group is the deduplication key
/// and the issue it filed into, and its count is the number that matters.
const DEFAULT_RETENTION_DAYS: u32 = 30;

/// How many frames go into the fingerprint. Deep stacks differ between two
/// callers of one defect; the first few do not.
const FINGERPRINT_FRAMES: usize = 5;

/// How many frames a report may carry into this module at all. A stack deep
/// enough to matter is already past [`FINGERPRINT_FRAMES`]; the rest is a
/// client sending memory, not a defect worth grouping on.
const MAX_FRAMES: usize = 50;

/// How many characters of free text one field may carry. Enough for a real
/// stack message or a user's steps to reproduce, far short of what an
/// unbounded body would let into the tracker.
const MAX_FIELD_CHARS: usize = 2_000;

/// How long a report's own words may be inside an issue title.
const TITLE_CHARS: usize = 120;

/// The `sg_reports.status` of a report taken in but not yet acted on.
const RECEIVED: &str = "received";
/// The `sg_reports.status` of a report that reads like a prompt injection:
/// stored for a human, never filed, never read again.
const HELD: &str = "held";
/// The `sg_report_groups.status` of a group whose issue is believed open —
/// including the span in which one report has claimed the group and is
/// filing its issue.
const GROUP_OPEN: &str = "open";
/// The `sg_report_groups.status` of a group that has come back after the
/// tracker reported its issue finished.
const GROUP_REGRESSED: &str = "regressed";
/// The `sg_report_groups.status` of a group with no issue filed against it
/// yet: the tenant has configured no destination, or has spent this hour's
/// write cap. A later report for the group files it.
const GROUP_RECORDED: &str = "recorded";

/// The webhook event a rate-capped intake publishes, once per window.
/// Declared here rather than in [`crate::model::webhook_events`] because
/// only this route publishes it.
const REPORT_SPIKE: &str = "report.spike";

struct ReportsState {
    ctx: Arc<ModuleContext>,
    /// Tenant status; `None` refuses every tenant (fail closed, see
    /// [`crate::tenants`]).
    tenants: Option<Arc<dyn TenantDirectory>>,
}

/// Mounts the intake route, beside the destination and ticket routes in the
/// same module router.
pub(crate) fn router(
    ctx: Arc<ModuleContext>,
    tenants: Option<Arc<dyn TenantDirectory>>,
) -> axum::Router {
    let state = Arc::new(ReportsState { ctx, tenants });
    axum::Router::new()
        .route(
            "/reports",
            // The harness's API plane already caps every body at core's
            // `MAX_BODY_BYTES`; naming it here makes this route's own
            // contract (and the 413 it answers with) explicit rather than
            // inherited, and holds if the module is ever mounted alone.
            post(post_report).layer(axum::extract::DefaultBodyLimit::max(
                cratefield_core::MAX_BODY_BYTES,
            )),
        )
        .with_state(state)
}

/// The one action [`router`] mounts (ADR 0010), so `GET /__surface` and the
/// `OpenAPI` document describe exactly what exists.
pub(crate) fn surface() -> Surface {
    Surface::new().action(
        Action::new("post-reports", Method::POST, "/reports")
            .policy(RoutePolicy::ApiKey)
            .outcome(Outcome::Json),
    )
}

// ---------------------------------------------------------------------------
// The wire
// ---------------------------------------------------------------------------

/// The `POST /reports` body, tagged by `kind`. An unknown `kind` is a `400`
/// naming both shapes.
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Report {
    Error(ErrorBody),
    Bug(BugBody),
}

/// An error the application caught. `fingerprint` is accepted and ignored —
/// the server computes the grouping key — so an SDK that sends a wrong one
/// still groups correctly.
#[derive(Deserialize)]
struct ErrorBody {
    error_type: String,
    message: String,
    #[serde(default)]
    frames: Vec<FrameBody>,
    release: Option<String>,
    environment: Option<String>,
    route: Option<String>,
    trace_id: Option<String>,
    #[serde(default)]
    count: Option<i64>,
    /// Read and dropped: grouping is the server's to compute.
    #[serde(default)]
    #[allow(dead_code)]
    fingerprint: Option<String>,
}

/// One stack frame. The line number is kept for the filed body but never
/// hashed: the same failure two lines apart is one defect.
#[derive(Deserialize)]
struct FrameBody {
    file: String,
    #[serde(default)]
    function: String,
    #[serde(default)]
    line: Option<i64>,
}

/// A bug a user described, in their own words.
#[derive(Deserialize)]
struct BugBody {
    description: String,
    steps: Option<String>,
    expected: Option<String>,
    actual: Option<String>,
    contact: Option<String>,
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// Takes one report. A report that parses is answered `202` — the route
/// has taken it in, redacted it and routed what it could, and a held
/// report is stored without being filed. The one thing that is **not**
/// `202` is a tracker that could not take the filing: that answers `500`,
/// which is the caller's cue to resend rather than lose the report.
async fn post_report(
    scope: Scope,
    State(state): State<Arc<ReportsState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, Problem> {
    let (tenant_id, _actor) = tenant_of(&state.ctx, state.tenants.as_deref(), &headers).await?;
    let report: Report = serde_json::from_slice(&body).map_err(|_| {
        Problem::validation_failed(
            "body: expected {\"kind\":\"error\", \"error_type\": ..., \"message\": ...} or \
             {\"kind\":\"bug\", \"description\": ...}",
        )
        .instance(&scope.request_id)
    })?;
    let Some(db) = state.ctx.ports.db.clone() else {
        return Err(required_port_missing("Db").instance(&scope.request_id));
    };
    let Some(tracker) = state.ctx.ports.tracker.clone() else {
        return Err(required_port_missing("Tracker").instance(&scope.request_id));
    };
    let at = iso_now(&state.ctx);
    let id = new_id(&state.ctx);
    prune(db.as_ref(), &state, &tenant_id, &at).await?;
    match report {
        Report::Error(body) => intake_error(&state, db, &tracker, &tenant_id, &id, body, &at).await,
        Report::Bug(body) => intake_bug(&state, db, &tracker, &tenant_id, &id, body, &at).await,
    }
}

/// An app-reported error: group it, then file it or count it onto it.
async fn intake_error(
    state: &Arc<ReportsState>,
    db: Arc<dyn Database>,
    tracker: &Arc<dyn Tracker>,
    tenant_id: &str,
    id: &str,
    body: ErrorBody,
    at: &str,
) -> Result<Response, Problem> {
    let report = ErrorReport {
        fingerprint: fingerprint(&body.error_type, &body.frames),
        error_type: scrubbed(&body.error_type),
        message: scrubbed(&body.message),
        frames: body
            .frames
            .iter()
            .take(MAX_FRAMES)
            .map(|frame| Frame {
                file: scrubbed(&frame.file),
                function: scrubbed(&frame.function),
                line: frame.line,
            })
            .collect(),
        release: body.release.as_deref().map(scrubbed),
        environment: body.environment.as_deref().map(scrubbed),
        route: body.route.as_deref().map(scrubbed),
        trace_id: body.trace_id.as_deref().map(scrubbed),
        // One report stands for at most a thousand occurrences: the field
        // is an SDK's batched total, not a claim on the tenant's tracker.
        occurrences: body.count.unwrap_or(1).clamp(1, 1000),
    };
    let release = report.release.as_deref();
    record(
        db.as_ref(),
        tenant_id,
        id,
        at,
        Audit {
            kind: "error",
            fingerprint: Some(&report.fingerprint),
            release,
            status: RECEIVED,
        },
    )
    .await?;

    // The claim decides who files: the first report of a fingerprint
    // inserts the group row — unfiled, `open`, a filer at work on it — and
    // a twin that raced it loses the insert and counts onto the issue the
    // winner is filing, rather than filing a second one.
    if claim_group(db.as_ref(), tenant_id, &report.fingerprint, &report, at).await? {
        return file_group(state, db, tracker, tenant_id, &report, at, None).await;
    }
    let Some(group) = load_group(db.as_ref(), tenant_id, &report.fingerprint).await? else {
        // Unreachable — a group row is never pruned — but a row that
        // vanished between the two statements is safest claimed by filing.
        return file_group(state, db, tracker, tenant_id, &report, at, None).await;
    };
    // A group that never got an issue — no destination, or the write cap
    // spent, when it was last reported — is filed by the first report that
    // finds budget. An issueless row still `open` is a winner mid-filing,
    // and this report only counts onto it.
    if group.issue_external_id.is_none() && group.status == GROUP_RECORDED {
        return file_group(state, db, tracker, tenant_id, &report, at, Some(&group)).await;
    }
    // The true running total, saturating: `occurrences` is clamped on
    // the way in, and a stored count is not this module's to trust.
    let count = group.count.saturating_add(report.occurrences);
    let newer = is_newer(release, group.max_release.as_deref());
    let mut status = None;
    let mut regressed = false;
    // Only a *new* release against a group whose issue the tracker
    // reported finished is a regression. The tracker is asked only
    // then: it is the one question whose answer changes what happens
    // next, and a needless call per report is what the cap is for.
    if newer {
        let finished = tracker_is_closed(state, &db, tracker, tenant_id, &group).await?;
        if finished {
            regressed = true;
            status = Some(GROUP_REGRESSED);
        }
    }
    // A note is worth making at a power of ten, on a new release, or
    // when the fixed issue has come back. Anything else is a counter
    // row the route already wrote.
    let milestone = count > group.count && count.reaches_milestone();
    if (regressed || milestone || newer) && spend(db.as_ref(), state, tenant_id, at).await? {
        let prefix = if regressed {
            format!(
                "Regressed in release {} — the issue this was filed as is finished.\n\n",
                release.unwrap_or("unknown")
            )
        } else {
            String::new()
        };
        comment(
            state,
            &db,
            tracker,
            tenant_id,
            &group,
            &format!("report:{tenant_id}:{}:{count}", report.fingerprint),
            &format!(
                "{prefix}{} has now been reported {count} times (first seen {}).",
                report.error_type, group.first_seen,
            ),
        )
        .await;
    }
    bump_group(
        db.as_ref(),
        tenant_id,
        &report.fingerprint,
        report.occurrences,
        if newer { release } else { None },
        status,
        at,
    )
    .await?;
    Ok(accepted(json!({
        "status": if regressed { GROUP_REGRESSED } else { "counted" },
        "count": count,
        "issue_url": group.issue_url,
    })))
}

/// A user-described bug: screened, then filed as one issue.
async fn intake_bug(
    state: &Arc<ReportsState>,
    db: Arc<dyn Database>,
    tracker: &Arc<dyn Tracker>,
    tenant_id: &str,
    id: &str,
    body: BugBody,
    at: &str,
) -> Result<Response, Problem> {
    let bug = Bug {
        description: scrubbed(&body.description),
        steps: body.steps.as_deref().map(scrubbed),
        expected: body.expected.as_deref().map(scrubbed),
        actual: body.actual.as_deref().map(scrubbed),
        contact: body.contact.as_deref().map(scrubbed),
    };
    // Screening reads the *redacted* text, so a marker split across a
    // redaction is not one.
    if let Screen::Held { .. } = screen(&bug_text(&bug)) {
        record(
            db.as_ref(),
            tenant_id,
            id,
            at,
            Audit {
                kind: "bug",
                fingerprint: None,
                release: None,
                status: HELD,
            },
        )
        .await?;
        return Ok(accepted(json!({ "status": HELD })));
    }
    record(
        db.as_ref(),
        tenant_id,
        id,
        at,
        Audit {
            kind: "bug",
            fingerprint: None,
            release: None,
            status: RECEIVED,
        },
    )
    .await?;

    let Some((destination, credential)) = resolve(db.clone(), state, tenant_id).await? else {
        return Ok(accepted(json!({ "status": GROUP_RECORDED })));
    };
    if !spend(db.as_ref(), state, tenant_id, at).await? {
        return Ok(accepted(json!({ "status": "rate_capped" })));
    }
    // One issue per description: a user who reports the same bug twice gets
    // the same idempotency key, so a retry after a timeout is one issue.
    let draft = TicketDraft::new(
        format!("report:{tenant_id}:{}", short_hash(&bug.description)),
        truncate(&format!("Bug report: {}", bug.description), TITLE_CHARS),
        bug_body(&bug),
        Severity::Info,
    )
    .labels(vec!["bug".to_owned(), "triage".to_owned()]);
    let filed = tracker
        .file(&destination, &credential, &draft)
        .await
        .map_err(|_| Problem::internal())?;
    Ok(accepted(json!({
        "status": "filed",
        "issue_url": filed.url,
    })))
}

// ---------------------------------------------------------------------------
// What the route works in
// ---------------------------------------------------------------------------

/// An error report, redacted and grouped.
struct ErrorReport {
    fingerprint: String,
    error_type: String,
    message: String,
    frames: Vec<Frame>,
    release: Option<String>,
    environment: Option<String>,
    route: Option<String>,
    trace_id: Option<String>,
    occurrences: i64,
}

/// One redacted stack frame.
struct Frame {
    file: String,
    function: String,
    line: Option<i64>,
}

/// A bug report, redacted.
struct Bug {
    description: String,
    steps: Option<String>,
    expected: Option<String>,
    actual: Option<String>,
    contact: Option<String>,
}

/// One deduplication group: the row that says "these reports are one
/// defect, filed as this one issue".
struct Group {
    title: String,
    issue_external_id: Option<String>,
    issue_url: Option<String>,
    count: i64,
    max_release: Option<String>,
    status: String,
    first_seen: String,
}

/// The grouping key: sha256 over the error's type and its top frames'
/// `file:function`, with no line numbers. The client-sent `fingerprint` is
/// deliberately not part of it — two SDKs that compute it differently must
/// still land in one group, so the server computes the only value it
/// trusts.
fn fingerprint(error_type: &str, frames: &[FrameBody]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(error_type.trim().to_lowercase());
    for frame in frames.iter().take(FINGERPRINT_FRAMES) {
        hasher.update(b"\n");
        hasher.update(frame.file.trim());
        hasher.update(b":");
        hasher.update(frame.function.trim());
    }
    short_hex(&hasher.finalize())
}

/// Whether an incoming release is later than the newest one the group has
/// been seen in. Dotted numeric versions compare segment by segment; a
/// release string that does not parse falls back to inequality, which is
/// right often enough for a regression check and never claims a release is
/// older on the strength of a parse it did not understand.
fn is_newer(incoming: Option<&str>, seen: Option<&str>) -> bool {
    let (Some(incoming), Some(seen)) = (incoming, seen) else {
        return incoming.is_some();
    };
    let numeric = |value: &str| -> Option<Vec<u64>> {
        value
            .split(['.', '-', '+'])
            .map(str::parse::<u64>)
            .collect::<Result<Vec<_>, _>>()
            .ok()
    };
    match (numeric(incoming), numeric(seen)) {
        (Some(incoming), Some(seen)) => (0..incoming.len().max(seen.len())).any(|at| {
            let mine = incoming.get(at).copied().unwrap_or(0);
            let theirs = seen.get(at).copied().unwrap_or(0);
            mine != theirs && mine > theirs
        }),
        _ => incoming != seen,
    }
}

/// Whether a running count is a power of ten from ten up — the only
/// occasions a repeating error is worth a note about. A thousand identical
/// reports are one issue and three notes, not a thousand issues.
trait Milestone {
    fn reaches_milestone(self) -> bool;
}

impl Milestone for i64 {
    fn reaches_milestone(self) -> bool {
        if self < 10 {
            return false;
        }
        // A power of ten is a trailing run of zeros and nothing else: 10,
        // 100, 1000. Twenty is a multiple of ten and not a milestone.
        let mut rest = self;
        while rest % 10 == 0 {
            rest /= 10;
        }
        rest == 1
    }
}

/// Files a group that has no issue yet as a tracker issue, or records it
/// against a tenant that has configured no destination or has spent this
/// hour's write cap. The row is written either way: a group that was never
/// stored is a group whose count never rises, and the count is the whole
/// point of the route.
///
/// `existing` is the row a prior report left behind, when there was one —
/// its count and first sighting are carried forward rather than restarted.
/// A `None` speaks for the row [`claim_group`] has just written, which
/// already carries this report's own count and first sighting.
async fn file_group(
    state: &Arc<ReportsState>,
    db: Arc<dyn Database>,
    tracker: &Arc<dyn Tracker>,
    tenant_id: &str,
    report: &ErrorReport,
    at: &str,
    existing: Option<&Group>,
) -> Result<Response, Problem> {
    let count = existing
        .map_or(0, |group| group.count)
        .saturating_add(report.occurrences);
    let first_seen = existing.map_or_else(|| at.to_owned(), |group| group.first_seen.clone());
    let title = title(report);
    let unfiled = Group {
        title: title.clone(),
        issue_external_id: None,
        issue_url: None,
        count,
        max_release: report.release.clone(),
        status: GROUP_RECORDED.to_owned(),
        first_seen: first_seen.clone(),
    };
    let Some((destination, credential)) = resolve(db.clone(), state, tenant_id).await? else {
        put_group(db.as_ref(), tenant_id, &report.fingerprint, &unfiled, at).await?;
        return Ok(accepted(json!({ "status": GROUP_RECORDED })));
    };
    if !spend(db.as_ref(), state, tenant_id, at).await? {
        put_group(db.as_ref(), tenant_id, &report.fingerprint, &unfiled, at).await?;
        return Ok(accepted(json!({ "status": "rate_capped" })));
    }
    let mut draft = TicketDraft::new(
        format!("report:{tenant_id}:{}", report.fingerprint),
        title.clone(),
        error_body(report),
        Severity::Error,
    )
    .labels(vec!["bug".to_owned(), "auto-reported".to_owned()]);
    if let Some(environment) = report.environment.clone() {
        draft = draft.environment(environment);
    }
    // A tracker that refused the filing releases the claim back to
    // `recorded` rather than leaving it `open`: the occurrences already
    // counted stand, and the next report for the group retries the filing
    // instead of finding a filer that is never coming back.
    let Ok(filed) = tracker.file(&destination, &credential, &draft).await else {
        put_group(db.as_ref(), tenant_id, &report.fingerprint, &unfiled, at).await?;
        return Err(Problem::internal());
    };
    let group = Group {
        title,
        issue_external_id: Some(filed.external_id),
        issue_url: Some(filed.url.clone()),
        count,
        max_release: report.release.clone(),
        status: GROUP_OPEN.to_owned(),
        first_seen,
    };
    put_group(db.as_ref(), tenant_id, &report.fingerprint, &group, at).await?;
    Ok(accepted(json!({
        "status": "filed",
        "issue_url": filed.url,
    })))
}

/// The tracker issue title: the error's type and what it said.
fn title(report: &ErrorReport) -> String {
    truncate(
        &format!("{}: {}", report.error_type, report.message),
        TITLE_CHARS,
    )
}

/// The body filed with a new group.
fn error_body(report: &ErrorReport) -> String {
    let mut body = format!(
        "{}\n\nOccurrences: {}\nRelease: {}\nEnvironment: {}\nRoute: {}\n",
        report.message,
        report.occurrences,
        report.release.as_deref().unwrap_or("unknown"),
        report.environment.as_deref().unwrap_or("unknown"),
        report.route.as_deref().unwrap_or("none"),
    );
    if let Some(trace) = &report.trace_id {
        let _ = writeln!(body, "Trace: {trace}");
    }
    if !report.frames.is_empty() {
        body.push_str("\nTop frames:\n");
        for frame in report.frames.iter().take(FINGERPRINT_FRAMES) {
            let line = frame
                .line
                .map_or_else(String::new, |line| format!(":{line}"));
            let _ = writeln!(body, "- {} in {}{}", frame.file, frame.function, line);
        }
    }
    body
}

/// The body filed with a bug report.
fn bug_body(bug: &Bug) -> String {
    let mut body = format!("{}\n", bug.description);
    for (label, value) in [
        ("Steps to reproduce", bug.steps.as_deref()),
        ("Expected", bug.expected.as_deref()),
        ("Actual", bug.actual.as_deref()),
        ("Reported by", bug.contact.as_deref()),
    ] {
        if let Some(value) = value {
            let _ = writeln!(body, "\n**{label}:** {value}");
        }
    }
    body
}

/// Everything a bug report says, as one string — what screening reads.
/// `contact` is in it: it is filed into the body like the rest, so an
/// instruction hidden in it is screened like the rest.
fn bug_text(bug: &Bug) -> String {
    [
        Some(bug.description.as_str()),
        bug.steps.as_deref(),
        bug.expected.as_deref(),
        bug.actual.as_deref(),
        bug.contact.as_deref(),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join("\n")
}

// ---------------------------------------------------------------------------
// Tracker access
// ---------------------------------------------------------------------------

/// The tenant's tracker destination and credential, or `None` when the
/// tenant has configured neither — a report is still counted in that case,
/// it simply has nowhere to file.
///
/// The resolution mirrors the file stage's ([`crate::pipeline`]): a
/// `secret:` reference names the tenant's encrypted store, anything else a
/// Config key. Duplicated rather than shared because the file stage's copy
/// lives on `Pipeline`, which this route does not own.
async fn resolve(
    db: Arc<dyn Database>,
    state: &ReportsState,
    tenant_id: &str,
) -> Result<Option<(Destination, Credential)>, Problem> {
    let Some((destination, credential_ref)) = store::load_destination(&*db, tenant_id)
        .await
        .map_err(|_| Problem::internal())?
    else {
        return Ok(None);
    };
    let config = &*state.ctx.ports.config;
    let Some(name) = credential_ref.strip_prefix(crate::secrets::SECRET_REF_PREFIX) else {
        let Some(secret) = config.get(&credential_ref) else {
            return Err(Problem::internal());
        };
        return Ok(Some((destination, Credential::new(secret))));
    };
    let Some(kms) = crate::secrets::kms_from_config(config) else {
        return Err(Problem::internal());
    };
    let store = crate::secrets::audited_secrets(kms, db.clone())
        .tenant(tenant_id, db.clone())
        .map_err(|_| Problem::internal())?;
    let actor = Actor::new("escalation.reports").map_err(|_| Problem::internal())?;
    let read = async |name: &str| -> Result<String, Problem> {
        let secret = store
            .get(name, &actor)
            .await
            .map_err(|_| Problem::internal())?
            .ok_or_else(Problem::internal)?;
        secret
            .expose_str()
            .map_err(|_| Problem::internal())
            .map(str::to_owned)
    };
    let credential = Credential::new(read(name).await?);
    // A webhook URL is credential material; the stored destination keeps a
    // marker, and the real URL is read back here.
    let destination = match destination {
        Destination::Webhook { url } => match url.strip_prefix(crate::secrets::SECRET_REF_PREFIX) {
            Some(url_name) => Destination::Webhook {
                url: read(url_name).await?,
            },
            None => Destination::Webhook { url },
        },
        other => other,
    };
    Ok(Some((destination, credential)))
}

/// Adds a note to a group's issue.
///
/// Non-fatal by design: the shipped GitHub and webhook adapters do not
/// implement `comment` and answer `Rejected` by name, and a note that could
/// not be taken is not a reason to lose the count.
async fn comment(
    state: &ReportsState,
    db: &Arc<dyn Database>,
    tracker: &Arc<dyn Tracker>,
    tenant_id: &str,
    group: &Group,
    key: &str,
    body: &str,
) {
    let (Ok(Some((destination, credential))), Some(external_id)) = (
        resolve(db.clone(), state, tenant_id).await,
        group.issue_external_id.clone(),
    ) else {
        return;
    };
    let note = TicketComment::new(key, body);
    if let Err(err) = tracker
        .comment(&destination, &credential, &external_id, &note)
        .await
    {
        tracing::warn!(
            %err,
            tenant_id,
            external_id,
            "escalation: a report note was refused (this adapter serves no comments)"
        );
    }
}

/// Whether the tracker says the group's issue is finished. A tracker that
/// cannot answer is treated as "still open": a missed regression note costs
/// one comment, a false one costs a team's trust in the flag.
async fn tracker_is_closed(
    state: &ReportsState,
    db: &Arc<dyn Database>,
    tracker: &Arc<dyn Tracker>,
    tenant_id: &str,
    group: &Group,
) -> Result<bool, Problem> {
    let (Ok(Some((destination, credential))), Some(external_id)) = (
        resolve(db.clone(), state, tenant_id).await,
        group.issue_external_id.clone(),
    ) else {
        return Ok(false);
    };
    let Ok(status) = tracker
        .status(&destination, &credential, &external_id)
        .await
    else {
        return Ok(false);
    };
    Ok(matches!(
        status.state,
        TicketState::Closed | TicketState::Resolved
    ))
}

// ---------------------------------------------------------------------------
// The hourly write cap
// ---------------------------------------------------------------------------

/// Spends one tracker write against the tenant's hourly budget, or answers
/// `false` when the cap is already reached. The read-then-bump is
/// deliberate and honest: two reports racing at the boundary can both spend
/// the last write. That is one extra issue per tenant per hour, and it is
/// not worth a serializable transaction on the intake path.
async fn spend(
    db: &dyn Database,
    state: &ReportsState,
    tenant_id: &str,
    at: &str,
) -> Result<bool, Problem> {
    let window = window_start(at);
    let cap = ModuleConfig::new("escalation", &*state.ctx.ports.config)
        .get_u32("REPORTS_MAX_ISSUES_PER_HOUR", DEFAULT_MAX_WRITES_PER_HOUR);
    let budget = load_budget(db, tenant_id, &window).await?;
    // No row yet means nothing spent this window, which is under any cap.
    if budget
        .as_ref()
        .is_some_and(|budget| budget.filed_count >= i64::from(cap))
    {
        notify_spike(db, tenant_id, &window, at, budget, cap).await?;
        return Ok(false);
    }
    db.execute(&bump_budget_stmt(tenant_id, &window)).await?;
    Ok(true)
}

/// Publishes `report.spike` the first time a window is capped, and only
/// then. Fail-safe exactly as the pipeline's publish is: a venture that
/// mounts no `Webhooks` module has no tables, and a warning is the honest
/// answer — refusing the reports themselves over a notification is not.
async fn notify_spike(
    db: &dyn Database,
    tenant_id: &str,
    window: &str,
    at: &str,
    budget: Option<Budget>,
    cap: u32,
) -> Result<(), Problem> {
    if budget.and_then(|budget| budget.spike_notified_at).is_some() {
        return Ok(());
    }
    // The guarded update is the claim: of two capped reports racing, only
    // the one whose update moved the row publishes, and the other answers
    // having announced nothing.
    let claimed = db.execute(&mark_spike_stmt(tenant_id, window, at)).await?;
    if claimed == 0 {
        return Ok(());
    }
    let data = json!({
        "tenant_id": tenant_id,
        "window_start": window,
        "cap": cap,
    });
    match Webhooks::new()
        .publish(db, tenant_id, REPORT_SPIKE, &data, at)
        .await
    {
        Ok(published) => {
            let statements = published.into_statements();
            if !statements.is_empty() {
                db.batch_atomic(&statements).await?;
            }
        }
        Err(PublishError::Database(err)) => tracing::warn!(
            %err,
            tenant_id,
            "escalation: the report-spike fan-out was skipped (is `Webhooks` composed?)"
        ),
        Err(PublishError::InvalidEventType(event_type)) => {
            return Err(Problem::internal()
                .with_detail(format!("webhooks refused the event type `{event_type}`")));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

fn iden(name: &str) -> Alias {
    Alias::new(name)
}

/// Prunes what has aged out for this tenant, on every intake: the raw
/// `sg_reports` occurrences and the `sg_report_budget` windows they
/// spent.
///
/// A group row is deliberately **not** pruned. It is the deduplication
/// key and the issue that key filed into; dropping it would make the
/// thousandth report of one defect file a second issue for it. An
/// occurrence, by contrast, is one row of audit trail that nothing ever
/// reads again once the count has been bumped, and a budget row is a
/// window that stopped being matched the moment the hour turned.
///
/// The window is `ESCALATION_REPORTS_RETENTION_DAYS` (default
/// [`DEFAULT_RETENTION_DAYS`]); zero disables the sweep, which is the
/// honest way to say "keep everything".
async fn prune(
    db: &dyn Database,
    state: &Arc<ReportsState>,
    tenant_id: &str,
    at: &str,
) -> Result<(), Problem> {
    let days = ModuleConfig::new("escalation", &*state.ctx.ports.config)
        .get_u32("REPORTS_RETENTION_DAYS", DEFAULT_RETENTION_DAYS);
    if days == 0 {
        return Ok(());
    }
    let Some(cutoff) = days_before(at, days) else {
        return Ok(());
    };
    db.batch_atomic(&[
        prune_before_stmt("sg_reports", "created_at", tenant_id, &cutoff),
        prune_before_stmt("sg_report_budget", "window_start", tenant_id, &cutoff),
    ])
    .await
    .map_err(|_| Problem::internal())
}

/// The RFC 3339 instant `days` before `at`, or `None` when the harness
/// clock's own value will not parse — a sweep that cannot name its own
/// cut-off skips rather than guessing at one.
fn days_before(at: &str, days: u32) -> Option<String> {
    OffsetDateTime::parse(at, &Rfc3339)
        .ok()?
        .checked_sub(Duration::days(i64::from(days)))
        .and_then(|cutoff| cutoff.format(&Rfc3339).ok())
}

/// `DELETE FROM <table> WHERE tenant_id = ? AND <column> < ?` — the one
/// shape both prunes share, since both tables key their age on a single
/// timestamp column written from the same clock in UTC. A string
/// comparison is correct here precisely because both sides are RFC 3339
/// UTC: they sort as times.
fn prune_before_stmt(table: &str, column: &str, tenant_id: &str, cutoff: &str) -> Statement {
    let mut delete = Query::delete();
    delete
        .from_table(iden(table))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden(column)).lt(cutoff));
    Statement::render(&delete)
}

/// Everything one `sg_reports` row carries about the report beyond who it
/// was for and when it arrived.
struct Audit<'a> {
    kind: &'a str,
    fingerprint: Option<&'a str>,
    release: Option<&'a str>,
    status: &'a str,
}

/// Appends the intake audit row. It holds no free text by construction (see
/// the migration's header): what was said lives in the tracker issue,
/// scrubbed, and the row says only what became of it.
async fn record(
    db: &dyn Database,
    tenant_id: &str,
    id: &str,
    at: &str,
    audit: Audit<'_>,
) -> Result<(), Problem> {
    let Audit {
        kind,
        fingerprint,
        release,
        status,
    } = audit;
    let mut insert = Query::insert();
    insert
        .into_table(iden("sg_reports"))
        .columns([
            "id",
            "tenant_id",
            "kind",
            "fingerprint",
            "release",
            "status",
            "created_at",
        ])
        .values_panic([
            id.to_owned().into(),
            tenant_id.to_owned().into(),
            kind.to_owned().into(),
            fingerprint.map(str::to_owned).into(),
            release.map(str::to_owned).into(),
            status.to_owned().into(),
            at.to_owned().into(),
        ]);
    db.execute(&Statement::render(&insert))
        .await
        .map(|_| ())
        .map_err(|_| Problem::internal())
}

/// Writes a group's row, replacing any. Only a report that claimed the
/// group or read it reaches here, so a concurrent first report cannot
/// overwrite the winner's issue with its own.
fn put_group_stmt(tenant_id: &str, fingerprint: &str, group: &Group, at: &str) -> Statement {
    let mut insert = Query::insert();
    insert
        .into_table(iden("sg_report_groups"))
        .columns([
            "tenant_id",
            "fingerprint",
            "title",
            "issue_external_id",
            "issue_url",
            "count",
            "max_release",
            "status",
            "first_seen_at",
            "last_seen_at",
        ])
        .values_panic([
            tenant_id.to_owned().into(),
            fingerprint.to_owned().into(),
            group.title.clone().into(),
            group.issue_external_id.clone().into(),
            group.issue_url.clone().into(),
            group.count.into(),
            group.max_release.clone().into(),
            group.status.clone().into(),
            group.first_seen.clone().into(),
            at.to_owned().into(),
        ])
        .on_conflict(
            OnConflict::columns([iden("tenant_id"), iden("fingerprint")])
                .update_columns([
                    "title",
                    "issue_external_id",
                    "issue_url",
                    "count",
                    "max_release",
                    "status",
                    "last_seen_at",
                ])
                .to_owned(),
        );
    Statement::render(&insert)
}

/// Adds `occurrences` to a group's count and moves `last_seen_at` (and
/// `max_release` or `status` when this report changed them). The increment
/// rides the statement rather than a value read earlier, so two reports
/// racing cannot each write the same total.
fn bump_group_stmt(
    tenant_id: &str,
    fingerprint: &str,
    occurrences: i64,
    max_release: Option<&str>,
    status: Option<&str>,
    at: &str,
) -> Statement {
    let mut update = Query::update();
    update
        .table(iden("sg_report_groups"))
        .value(iden("count"), Expr::col(iden("count")).add(occurrences))
        .value(iden("last_seen_at"), at);
    if let Some(release) = max_release {
        update.value(iden("max_release"), release);
    }
    if let Some(status) = status {
        update.value(iden("status"), status);
    }
    update
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden("fingerprint")).eq(fingerprint));
    Statement::render(&update)
}

/// Counts one tracker write against the tenant's current window. The
/// increment rides the conflict action, so the read-then-bump in
/// [`spend`] cannot reset the count to one.
fn bump_budget_stmt(tenant_id: &str, window: &str) -> Statement {
    let mut insert = Query::insert();
    insert
        .into_table(iden("sg_report_budget"))
        .columns(["tenant_id", "window_start", "filed_count"])
        .values_panic([
            tenant_id.to_owned().into(),
            window.to_owned().into(),
            1_i32.into(),
        ])
        .on_conflict(
            OnConflict::columns([iden("tenant_id"), iden("window_start")])
                .value(iden("filed_count"), Expr::col(iden("filed_count")).add(1))
                .to_owned(),
        );
    Statement::render(&insert)
}

/// Claims the one spike notification this window is allowed: the
/// `spike_notified_at IS NULL` guard is what makes exactly one capped
/// report the publisher, even when two hit the cap together.
fn mark_spike_stmt(tenant_id: &str, window: &str, at: &str) -> Statement {
    let mut update = Query::update();
    update
        .table(iden("sg_report_budget"))
        .value(iden("spike_notified_at"), at)
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden("window_start")).eq(window))
        .and_where(Expr::col(iden("spike_notified_at")).is_null());
    Statement::render(&update)
}

/// One window's spend, if the tenant has a row for it.
struct Budget {
    filed_count: i64,
    spike_notified_at: Option<String>,
}

async fn load_budget(
    db: &dyn Database,
    tenant_id: &str,
    window: &str,
) -> Result<Option<Budget>, Problem> {
    let mut query = Query::select();
    query
        .columns(["filed_count", "spike_notified_at"])
        .from(iden("sg_report_budget"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden("window_start")).eq(window))
        .limit(1);
    let rows = db.query(&Statement::render(&query)).await?;
    Ok(rows.first().map(|row| Budget {
        filed_count: row.get::<i64>("filed_count").unwrap_or(0),
        spike_notified_at: row
            .get::<Option<String>>("spike_notified_at")
            .unwrap_or(None),
    }))
}

async fn load_group(
    db: &dyn Database,
    tenant_id: &str,
    fingerprint: &str,
) -> Result<Option<Group>, Problem> {
    let mut query = Query::select();
    query
        .columns([
            "title",
            "issue_external_id",
            "issue_url",
            "count",
            "max_release",
            "status",
            "first_seen_at",
        ])
        .from(iden("sg_report_groups"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden("fingerprint")).eq(fingerprint))
        .limit(1);
    let rows = db.query(&Statement::render(&query)).await?;
    Ok(rows.first().map(|row| Group {
        title: row.get::<String>("title").unwrap_or_default(),
        issue_external_id: row
            .get::<Option<String>>("issue_external_id")
            .unwrap_or(None),
        issue_url: row.get::<Option<String>>("issue_url").unwrap_or(None),
        count: row.get::<i64>("count").unwrap_or(0),
        max_release: row.get::<Option<String>>("max_release").unwrap_or(None),
        status: row
            .get::<Option<String>>("status")
            .unwrap_or(None)
            .unwrap_or_else(|| GROUP_OPEN.to_owned()),
        first_seen: row.get::<String>("first_seen_at").unwrap_or_default(),
    }))
}

async fn put_group(
    db: &dyn Database,
    tenant_id: &str,
    fingerprint: &str,
    group: &Group,
    at: &str,
) -> Result<(), Problem> {
    db.execute(&put_group_stmt(tenant_id, fingerprint, group, at))
        .await
        .map(|_| ())
        .map_err(|_| Problem::internal())
}

/// Claims the right to file a fingerprint's issue: an insert of the group
/// row that does nothing when a row is already there, answered with whether
/// this report's insert is the one that landed. Only a landed insert files;
/// a report that raced one and lost finds the row [`load_group`] reads and
/// counts onto it. The claimed row carries this report's own occurrences
/// and sighting, `open` with no issue — the mark of a filer at work, and
/// what the losers of the race read.
async fn claim_group(
    db: &dyn Database,
    tenant_id: &str,
    fingerprint: &str,
    report: &ErrorReport,
    at: &str,
) -> Result<bool, Problem> {
    let claim = Group {
        title: title(report),
        issue_external_id: None,
        issue_url: None,
        count: report.occurrences,
        max_release: report.release.clone(),
        status: GROUP_OPEN.to_owned(),
        first_seen: at.to_owned(),
    };
    let mut insert = Query::insert();
    insert
        .into_table(iden("sg_report_groups"))
        .columns([
            "tenant_id",
            "fingerprint",
            "title",
            "issue_external_id",
            "issue_url",
            "count",
            "max_release",
            "status",
            "first_seen_at",
            "last_seen_at",
        ])
        .values_panic([
            tenant_id.to_owned().into(),
            fingerprint.to_owned().into(),
            claim.title.into(),
            claim.issue_external_id.into(),
            claim.issue_url.into(),
            claim.count.into(),
            claim.max_release.into(),
            claim.status.into(),
            claim.first_seen.into(),
            at.to_owned().into(),
        ])
        .on_conflict(
            OnConflict::columns([iden("tenant_id"), iden("fingerprint")])
                .do_nothing()
                .to_owned(),
        );
    let landed = db
        .execute(&Statement::render(&insert))
        .await
        .map_err(|_| Problem::internal())?;
    Ok(landed > 0)
}

async fn bump_group(
    db: &dyn Database,
    tenant_id: &str,
    fingerprint: &str,
    occurrences: i64,
    max_release: Option<&str>,
    status: Option<&str>,
    at: &str,
) -> Result<(), Problem> {
    db.execute(&bump_group_stmt(
        tenant_id,
        fingerprint,
        occurrences,
        max_release,
        status,
        at,
    ))
    .await
    .map(|_| ())
    .map_err(|_| Problem::internal())
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

/// Every `202` this route answers with.
fn accepted(value: Value) -> Response {
    (StatusCode::ACCEPTED, Json(value)).into_response()
}

/// The current hour — the budget window. Every writer stamps UTC RFC 3339,
/// so the hour is the leading thirteen characters and the rest is constant.
fn window_start(at: &str) -> String {
    format!("{}:00:00Z", at.chars().take(13).collect::<String>())
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

/// The first eight bytes of a sha256, in hex: a fingerprint and an
/// idempotency key never need more, and a group key a person could read is
/// a group key a person could correlate.
fn short_hash(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value);
    short_hex(&hasher.finalize())
}

fn short_hex(bytes: &[u8]) -> String {
    let mut hex = String::with_capacity(16);
    for byte in bytes.iter().take(8) {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

/// One report field, cut to [`MAX_FIELD_CHARS`] and then redacted. The cut
/// comes first so redaction reads the same text the tracker will be filed,
/// and so an oversized field cannot cost the route a second full pass over
/// it.
fn scrubbed(text: &str) -> String {
    redact(&truncate(text, MAX_FIELD_CHARS))
}

/// `text` cut to at most `limit` characters, on a character boundary and
/// with an ellipsis where the cut fell.
fn truncate(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        return text.to_owned();
    }
    let cut: String = text.chars().take(limit).collect();
    format!("{cut}…")
}
