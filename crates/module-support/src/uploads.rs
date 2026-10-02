//! Chunked uploads (issue #30): a document larger than the 64 KiB
//! `/v1/*` body cap arrives in parts, and a durable `extract` job turns
//! it into a source through the same upsert path as `POST /sources`.
//!
//! The shape, and why each piece is where it is:
//!
//! - `POST /uploads` opens an upload — filename, content type and the
//!   declared size only, no bytes. `PUT /uploads/{id}/parts/{n}` moves
//!   them: one part per request, up to [`handlers::MAX_TEXT_BYTES`] each,
//!   straight into the `Blob` port. There is no presigned upload to hand
//!   out (`Blob::signed_url` is GET-only), so the API itself is the
//!   transport. `POST /uploads/{id}/complete` checks the parts add up and
//!   enqueues the `extract` job in the same `batch_atomic` as the status
//!   change — a completed upload always has its job and never has two
//!   (the job id is derived from the upload id, so a duplicate `complete`
//!   loses to the outbox's primary key rather than enqueueing twice).
//! - The extract job is drained as soon as the response is on its way —
//!   the request-scoped [`Defer`] port, the same hand-off the escalation
//!   module uses — and, durably, by [`crate::Support::scheduled`]. Cron
//!   is what makes the work real even where nothing drains it inline.
//! - An upload abandoned before `complete` is garbage-collected by the
//!   same cron sweep: an `open` upload past [`UPLOAD_TTL`] has its part
//!   blobs and rows deleted. `Blob` has no list operation, so both the
//!   GC and the quota read are driven by the `sg_uploads` /
//!   `sg_upload_parts` rows — which is why those rows exist at all.
//!
//! Every route here is guarded exactly like the rest of the module: the
//! tenant API key first, then the per-tenant rate budget, and only then
//! anything about the request.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::json;
use time::Duration as TimeDuration;
use time::format_description::well_known::Rfc3339;

use cratefield_core::{
    AnyError, Blob, Clock, Database, DbError, IdGen, Json, ModuleConfig, ModuleContext, Outbox,
    OutboxRecord, Problem, ProblemDef, Scope, SystemClock, UlidIdGen,
};

use crate::chunk::Chunker;
use crate::extract;
use crate::handlers::{ModuleState, authenticate, clean_title, guard_rate_limit, required_port};
use crate::store::{self, UPLOAD_COMPLETE, UPLOAD_OPEN, UploadOutcome, UploadPartRow, UploadRow};

/// One part's ceiling: the same 48 KiB the inline `POST /sources` form
/// accepts, so a part always fits the 64 KiB `/v1/*` request body with
/// room for auth and framing, and a client can size parts from one
/// constant. It bounds the request, not the document — the document's
/// bound is [`MAX_UPLOAD_BYTES`].
pub(crate) const PART_BYTES: usize = crate::handlers::MAX_TEXT_BYTES;

/// The largest document an upload may declare, in bytes. Not arbitrary:
/// 4 MiB is far past any manual or policy PDF that belongs in a support
/// index, still short of core's per-object `MAX_BLOB_BYTES` bound (10 MiB)
/// so a whole-file object would fit an R2 bucket if one ever existed, and
/// it keeps the worst-case part count ([`MAX_PARTS`]) a two-digit number.
/// A constant, not configuration: raising it is a deploy-time decision
/// about blob storage and isolate memory, not a runtime knob.
pub(crate) const MAX_UPLOAD_BYTES: usize = 4 * 1024 * 1024;

/// Part ordinals are contiguous from 0, and a legal upload never needs
/// more than [`MAX_UPLOAD_BYTES`]/[`PART_BYTES`] of them; the bound is
/// what turns a hostile `n` into a 400 before any arithmetic runs.
pub(crate) const MAX_PARTS: usize = MAX_UPLOAD_BYTES.div_ceil(PART_BYTES);

/// The largest extracted text the chunker is handed, in bytes. A
/// document assembled from parts never passed the 64 KiB request body,
/// so the only thing standing between a pathological one (a PDF that
/// expands past its source, an HTML page of a million tags) and the
/// index is this ceiling; extraction past it fails the upload.
pub(crate) const MAX_EXTRACTED_TEXT_BYTES: usize = 2 * 1024 * 1024;

/// How long an `open` upload may sit untouched before cron collects it.
/// A day covers a client that stalls mid-upload and comes back; after
/// that the parts are storage nobody is coming back for.
const UPLOAD_TTL: TimeDuration = TimeDuration::hours(24);

/// The default per-tenant retained-upload budget, in bytes, overridable
/// with `SUPPORT_UPLOAD_QUOTA_BYTES` (see [`QUOTA_KEY`]). It counts `open`
/// and `complete` uploads only — the ones whose part blobs still exist —
/// so extracting a document returns its bytes to the budget and keeps
/// only the indexed text.
pub(crate) const DEFAULT_QUOTA_BYTES: usize = 50 * 1024 * 1024;

/// The config key suffix for the per-tenant retained-upload budget; the
/// full key is `SUPPORT_UPLOAD_QUOTA_BYTES`.
pub(crate) const QUOTA_KEY: &str = "UPLOAD_QUOTA_BYTES";

/// The outbox table this module owns; migration 0003 creates it from
/// `Outbox::new(OUTBOX_TABLE).create_table_sql()`.
pub(crate) const OUTBOX_TABLE: &str = "sg_support_outbox";

/// The one topic this module enqueues. Routing on it keeps a future
/// topic from being run through the extract path by accident.
const TOPIC_EXTRACT: &str = "extract";

/// How long an extract job holds its claim: far longer than an extract
/// can take (a blob read, a parse, one batch), short enough that a
/// crashed drainer's job comes back within minutes.
const LEASE_SECS: i64 = 300;

/// How many runs a failing extract job gets before the upload is failed
/// for good. A run fails on infrastructure only (a database error — a
/// bad document fails the upload on its first run), so hitting this cap
/// means a drain is persistently broken and a `complete` that can never
/// finish is storage the quota counts and nobody can read. The claim
/// never counts an attempt — only [`Outbox::retry_later`] does — so the
/// count lives where the reschedule does.
const MAX_EXTRACT_ATTEMPTS: i64 = 5;

/// The wait between extract retries: the claim lease, so a retry never
/// lands before the failed run's lease could have lapsed anyway.
const EXTRACT_RETRY_SECS: i64 = LEASE_SECS;

/// How many due jobs one sweep claims, and how many sweeps `scheduled`
/// runs before leaving the rest to the next tick — the escalation
/// module's bound against a tick that never ends.
const SWEEP_LIMIT: u64 = 25;
const MAX_SWEEPS: u32 = 8;

/// 413: the upload declared more than [`MAX_UPLOAD_BYTES`], or its parts
/// have grown past the declaration.
const UPLOAD_TOO_LARGE: ProblemDef = ProblemDef {
    slug: "upload-too-large",
    status: StatusCode::PAYLOAD_TOO_LARGE,
    title: "Upload is too large",
    description: "The declared size exceeds the upload maximum, or the parts received so far \
                  would not add up to it.",
};

/// 403: opening this upload would put the tenant past its retained
/// upload budget. Policy, not payload size — the same request can be
/// accepted once the tenant's other uploads are extracted or collected.
const UPLOAD_QUOTA_EXCEEDED: ProblemDef = ProblemDef {
    slug: "upload-quota-exceeded",
    status: StatusCode::FORBIDDEN,
    title: "Upload quota exceeded",
    description: "The tenant's retained upload storage is at its budget. Finish extracting, or \
                  let the abandoned uploads be collected, then try again.",
};

/// 415: a content type outside the four the extract job can read.
const UNSUPPORTED_MEDIA_TYPE: ProblemDef = ProblemDef {
    slug: "unsupported-media-type",
    status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
    title: "Unsupported content type",
    description: "The upload's content type is not one this service can extract text from.",
};

/// 409: parts were sent to, or `complete` was called on, an upload that
/// is no longer `open`.
const UPLOAD_CLOSED: ProblemDef = ProblemDef {
    slug: "upload-closed",
    status: StatusCode::CONFLICT,
    title: "Upload is no longer open",
    description: "The upload already completed (or failed extraction); parts are only accepted \
                  while an upload is open.",
};

/// 409: `complete` was called, but the parts do not add up — a missing
/// ordinal, or fewer bytes than declared.
const UPLOAD_INCOMPLETE: ProblemDef = ProblemDef {
    slug: "upload-incomplete",
    status: StatusCode::CONFLICT,
    title: "Upload parts are incomplete",
    description: "The parts received so far are not a contiguous series from 0 adding up to \
                  the declared size, so `complete` was refused.",
};

/// `POST /uploads` body: identity and the declared size, nothing else.
#[derive(Deserialize)]
struct UploadBody {
    filename: String,
    content_type: String,
    bytes: usize,
}

/// `PUT /uploads/{id}/parts/{n}` path. `n` stays a string until the
/// guards have run — a numeric path extractor would reject a malformed
/// part number before the handler could authenticate, the same
/// extractor leak `POST /sources` was built to avoid.
#[derive(Deserialize)]
pub(crate) struct PartPath {
    upload_id: String,
    n: String,
}

/// The blob key for one part, under the module scope the harness applies
/// (`support/uploads/…`). Ordinals are zero-padded so a store listing
/// reads in order.
fn part_key(tenant_id: &str, upload_id: &str, n: i64) -> String {
    format!("uploads/{tenant_id}/{upload_id}/{n:05}")
}

/// The outbox job id for one upload's extract. Deterministic on purpose:
/// `complete` inserts this id in the same batch as the status flip, so a
/// second `complete` of the same upload — two racing requests, or a
/// client retrying what only looked lost — loses to the primary key and
/// cannot enqueue a second extract.
fn extract_job_id(upload_id: &str) -> String {
    format!("{upload_id}-extract")
}

/// The `Blob` port, or the `503 not-ready` every upload route answers
/// when the deployment has no object storage. [`required_port`] is wrong
/// here: `Blob` is in `optional()`, so a missing one is deployment
/// posture, not the harness bug a 500 would claim.
fn blob_port(ctx: &ModuleContext) -> Result<&dyn Blob, Problem> {
    ctx.ports.blob.as_deref().ok_or_else(|| {
        Problem::not_ready(
            "Chunked upload needs the Blob port; this deployment did not configure object \
             storage, so parts have nowhere to land.",
        )
    })
}

/// The tenant's retained-upload budget in bytes, from module config
/// (`SUPPORT_UPLOAD_QUOTA_BYTES`), defaulting to [`DEFAULT_QUOTA_BYTES`].
/// `validate_config` refuses a value that does not parse, so this read
/// falls back only on absence.
fn retention_quota(ctx: &ModuleContext) -> i64 {
    let module = ModuleConfig::new(crate::MODULE_NAME, ctx.config.as_ref());
    i64::from(module.get_u32(
        QUOTA_KEY,
        u32::try_from(DEFAULT_QUOTA_BYTES).unwrap_or(u32::MAX),
    ))
}

/// `POST /uploads` — `{"filename", "content_type", "bytes"}` →
/// `201 {id, part_bytes, …}`. Validates the declaration (size against
/// [`MAX_UPLOAD_BYTES`], type against the four extractable ones, tenant
/// quota) before anything is stored: an upload that could never be
/// accepted should not hold a row.
pub(crate) async fn create_upload(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, Problem> {
    let ctx = &state.ctx;
    let tenant_id = authenticate(ctx, &headers).await?;
    if let Some(rate_limited) = guard_rate_limit(ctx, &tenant_id).await {
        return Ok(rate_limited);
    }
    let body: UploadBody = serde_json::from_slice(&body).map_err(|_| {
        Problem::validation_failed(
            "body: expected a JSON object with string \"filename\", string \"content_type\" \
             and integer \"bytes\"",
        )
        .instance(&scope.request_id)
    })?;

    let filename = body.filename.trim();
    if filename.is_empty() || filename.len() > crate::handlers::MAX_NAME_BYTES {
        return Err(Problem::validation_failed(format!(
            "filename: required, 1..={} bytes",
            crate::handlers::MAX_NAME_BYTES
        ))
        .instance(&scope.request_id));
    }
    let content_type = body.content_type.to_ascii_lowercase();
    if !extract::is_supported(&content_type) {
        return Err(Problem::new(&UNSUPPORTED_MEDIA_TYPE)
            .with_detail(format!(
                "content_type {content_type:?} is not one of {}",
                extract::supported_list()
            ))
            .instance(&scope.request_id));
    }
    if body.bytes == 0 {
        return Err(
            Problem::validation_failed("bytes: must be at least 1").instance(&scope.request_id)
        );
    }
    if body.bytes > MAX_UPLOAD_BYTES {
        return Err(Problem::new(&UPLOAD_TOO_LARGE)
            .with_detail(format!(
                "declared {} bytes; the maximum is {MAX_UPLOAD_BYTES} (as {MAX_PARTS} parts of \
                 at most {PART_BYTES} bytes each)",
                body.bytes
            ))
            .instance(&scope.request_id));
    }

    let db: &dyn Database = required_port(ctx.ports.db.as_deref(), "Db")?;
    let clock: &dyn Clock = required_port(ctx.ports.clock.as_deref(), "Clock")?;
    let id_gen: &dyn IdGen = required_port(ctx.ports.id_gen.as_deref(), "IdGen")?;
    // The Blob port is where the parts will land; without it an upload
    // could open and then strand (every PUT refused). Answer before any
    // row exists.
    blob_port(ctx)?;

    // Best-effort, not bulletproof: two concurrent opens can both pass
    // this read. The quota is a budget against accidents, not an
    // adversarial limit, and closing that window would cost a write
    // transaction around every open.
    let quota = retention_quota(ctx);
    let retained = store::tenant_retained_upload_bytes(db, &tenant_id).await?;
    if retained.saturating_add(i64::try_from(body.bytes).unwrap_or(i64::MAX)) > quota {
        return Err(Problem::new(&UPLOAD_QUOTA_EXCEEDED)
            .with_detail(format!(
                "retained {retained} bytes + declared {} would pass the {quota}-byte budget",
                body.bytes
            ))
            .instance(&scope.request_id));
    }

    let upload_id = id_gen.ulid();
    let upload = UploadRow {
        id: upload_id.clone(),
        tenant_id: tenant_id.clone(),
        filename: filename.to_owned(),
        content_type,
        declared_bytes: i64::try_from(body.bytes).unwrap_or(i64::MAX),
        received_bytes: 0,
        status: UPLOAD_OPEN.to_owned(),
        source_id: None,
        error: None,
        created_at: store::iso_now(clock),
        completed_at: None,
    };
    store::insert_upload(db, &upload).await?;

    Ok((
        StatusCode::CREATED,
        Json(json!({
            "id": upload_id,
            "filename": upload.filename,
            "content_type": upload.content_type,
            "declared_bytes": upload.declared_bytes,
            "received_bytes": 0,
            "status": upload.status,
            "part_bytes": PART_BYTES,
        })),
    )
        .into_response())
}

/// `PUT /uploads/{id}/parts/{n}` — the raw part bytes (1..=[`PART_BYTES`])
/// as the request body, no JSON. Idempotent per `n`: a re-`PUT` replaces
/// the part and the running total swaps the old bytes out. A foreign or
/// unknown upload is the same 404; a non-open one is a 409.
pub(crate) async fn put_part(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    Path(path): Path<PartPath>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, Problem> {
    let ctx = &state.ctx;
    let tenant_id = authenticate(ctx, &headers).await?;
    if let Some(rate_limited) = guard_rate_limit(ctx, &tenant_id).await {
        return Ok(rate_limited);
    }

    let n: u32 = path.n.parse().map_err(|_| {
        Problem::validation_failed("n: part ordinal must be a whole number, contiguous from 0")
            .instance(&scope.request_id)
    })?;
    if n >= u32::try_from(MAX_PARTS).unwrap_or(u32::MAX) {
        return Err(Problem::validation_failed(format!(
            "n: part ordinal must be below {MAX_PARTS} (a {MAX_UPLOAD_BYTES}-byte upload needs \
             no more)"
        ))
        .instance(&scope.request_id));
    }
    if body.is_empty() {
        return Err(Problem::validation_failed(
            "body: a part carries 1..=48 KiB of bytes; an empty part is never sent",
        )
        .instance(&scope.request_id));
    }
    if body.len() > PART_BYTES {
        return Err(Problem::validation_failed(format!(
            "body: {PART_BYTES} bytes maximum per part, got {} bytes — send more, smaller parts",
            body.len()
        ))
        .instance(&scope.request_id));
    }

    let db: &dyn Database = required_port(ctx.ports.db.as_deref(), "Db")?;
    let blob: &dyn Blob = blob_port(ctx)?;
    let upload_id = path.upload_id;
    let upload = store::find_upload(db, &tenant_id, &upload_id)
        .await?
        .ok_or_else(|| Problem::not_found().instance(&scope.request_id))?;
    if upload.status != UPLOAD_OPEN {
        return Err(Problem::new(&UPLOAD_CLOSED)
            .with_detail(format!("upload is {}", upload.status))
            .instance(&scope.request_id));
    }

    let part_bytes = i64::try_from(body.len()).unwrap_or(i64::MAX);
    let n = i64::from(n);
    // A re-PUT of an existing ordinal replaces it, so the running total
    // swaps the old part's bytes out before the new ones are counted.
    let prior = store::upload_parts(db, &upload_id)
        .await?
        .into_iter()
        .find(|part| part.n == n)
        .map_or(0, |part| part.bytes);
    let next_total = upload
        .received_bytes
        .saturating_sub(prior)
        .saturating_add(part_bytes);
    if next_total > upload.declared_bytes {
        return Err(Problem::new(&UPLOAD_TOO_LARGE)
            .with_detail(format!(
                "parts would total {next_total} bytes against the {} declared",
                upload.declared_bytes
            ))
            .instance(&scope.request_id));
    }

    blob.put(
        &part_key(&tenant_id, &upload_id, n),
        &body,
        "application/octet-stream",
    )
    .await
    .map_err(|err| Problem::internal().with_detail(format!("blob write failed: {err}")))?;
    store::upsert_upload_part(db, &upload_id, n, part_bytes).await?;
    let received = store::upload_parts(db, &upload_id)
        .await?
        .iter()
        .map(|part| part.bytes)
        .sum::<i64>();
    // Progress a polling GET upload can see between parts.
    store::set_upload_received(db, &upload_id, received).await?;

    Ok((
        StatusCode::OK,
        Json(json!({
            "id": upload_id,
            "n": n,
            "bytes": part_bytes,
            "received_bytes": received,
            "declared_bytes": upload.declared_bytes,
        })),
    )
        .into_response())
}

/// `POST /uploads/{id}/complete` — checks the parts form a contiguous
/// series adding up to the declared size, then flips the upload to
/// `complete` and enqueues the `extract` job in one batch. The job runs
/// right after the response when the `Defer` port is mounted, and
/// otherwise (or as well — the job is idempotent on status) at the next
/// cron tick.
pub(crate) async fn complete_upload(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    Path(upload_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let ctx = &state.ctx;
    let tenant_id = authenticate(ctx, &headers).await?;
    if let Some(rate_limited) = guard_rate_limit(ctx, &tenant_id).await {
        return Ok(rate_limited);
    }
    let db: &dyn Database = required_port(ctx.ports.db.as_deref(), "Db")?;
    let clock: &dyn Clock = required_port(ctx.ports.clock.as_deref(), "Clock")?;
    // Completing without the Blob port would strand the upload: the job
    // it enqueues has nothing to assemble, and the GC only collects
    // `open` uploads. Same 503 the other two routes answer.
    blob_port(ctx)?;

    let upload = store::find_upload(db, &tenant_id, &upload_id)
        .await?
        .ok_or_else(|| Problem::not_found().instance(&scope.request_id))?;
    if upload.status != UPLOAD_OPEN {
        return Err(Problem::new(&UPLOAD_CLOSED)
            .with_detail(format!("upload is {}", upload.status))
            .instance(&scope.request_id));
    }

    // The parts must read back as 0, 1, 2 … summing to exactly the
    // declaration. `received_bytes` is informational; this is the check
    // that decides.
    let parts = store::upload_parts(db, &upload_id).await?;
    let total: i64 = parts.iter().map(|part| part.bytes).sum();
    let contiguous = parts
        .iter()
        .enumerate()
        .all(|(index, part)| usize::try_from(part.n) == Ok(index));
    if parts.is_empty() || !contiguous || total != upload.declared_bytes {
        return Err(Problem::new(&UPLOAD_INCOMPLETE)
            .with_detail(format!(
                "received {total} of {} declared bytes across {} parts (contiguous: \
                 {contiguous})",
                upload.declared_bytes,
                parts.len()
            ))
            .instance(&scope.request_id));
    }

    let now = store::iso_now(clock);
    let payload = serde_json::to_string(&ExtractPayload {
        upload_id: upload_id.clone(),
        tenant_id: tenant_id.clone(),
    })
    .unwrap_or_else(|_| format!("{{\"upload_id\":\"{upload_id}\",\"tenant_id\":\"{tenant_id}\"}}"));
    let statements = [
        store::close_upload_stmt(&upload_id, total, &now),
        Outbox::new(OUTBOX_TABLE).enqueue_statement(
            &extract_job_id(&upload_id),
            TOPIC_EXTRACT,
            &payload,
            Some(upload_id.as_str()),
            &now,
        ),
    ];
    if let Err(err) = db.batch_atomic(&statements).await {
        // A retried `complete` — the response lost, the client came back
        // — can race the first one into this batch and lose to the
        // outbox's primary key. By then the winner has flipped the upload
        // out of `open`, so a re-read that finds it closed is the winner
        // having committed: answer the same 409 the pre-check above
        // would have. Still `open`, or the re-read itself failing, and
        // the original error stands.
        if let Ok(Some(closed)) = store::find_upload(db, &tenant_id, &upload_id).await
            && closed.status != UPLOAD_OPEN
        {
            return Err(Problem::new(&UPLOAD_CLOSED)
                .with_detail(format!("upload is {}", closed.status))
                .instance(&scope.request_id));
        }
        return Err(err.into());
    }

    // Drain inline once the response is on its way. The job is durable
    // first — this is a latency optimisation, not what makes the work
    // happen; a dropped deferred future still runs at cron.
    if let Some(defer) = ctx.ports.defer.as_ref() {
        let state = Arc::clone(&state);
        defer.wait_until(Box::pin(async move {
            if let Err(err) = drain(&state.ctx, SWEEP_LIMIT).await {
                tracing::error!(error = %err, "deferred upload extract failed");
            }
        }));
    }

    Ok((
        StatusCode::ACCEPTED,
        Json(json!({
            "id": upload_id,
            "status": UPLOAD_COMPLETE,
            "received_bytes": total,
        })),
    )
        .into_response())
}

/// `GET /uploads/{id}` — the upload's progress: which status it is in
/// (open / complete / extracted / failed), how many bytes have landed,
/// and, once extracted, the `source_id` it was indexed under. A failed
/// extract's `error` is shown to the uploader, who is the one who can
/// fix the document.
pub(crate) async fn get_upload(
    scope: Scope,
    State(state): State<Arc<ModuleState>>,
    Path(upload_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let ctx = &state.ctx;
    let tenant_id = authenticate(ctx, &headers).await?;
    if let Some(rate_limited) = guard_rate_limit(ctx, &tenant_id).await {
        return Ok(rate_limited);
    }
    let db: &dyn Database = required_port(ctx.ports.db.as_deref(), "Db")?;
    let upload = store::find_upload(db, &tenant_id, &upload_id)
        .await?
        .ok_or_else(|| Problem::not_found().instance(&scope.request_id))?;
    Ok((
        StatusCode::OK,
        Json(json!({
            "id": upload.id,
            "filename": upload.filename,
            "content_type": upload.content_type,
            "declared_bytes": upload.declared_bytes,
            "received_bytes": upload.received_bytes,
            "status": upload.status,
            "source_id": upload.source_id,
            "error": upload.error,
            "created_at": upload.created_at,
            "completed_at": upload.completed_at,
        })),
    )
        .into_response())
}

/// The outbox payload: two ids, never content — the same discipline as
/// the escalation module's stage payloads, so the queue stays out of the
/// export surface.
#[derive(Deserialize, serde::Serialize)]
struct ExtractPayload {
    upload_id: String,
    tenant_id: String,
}

/// The whole extract drain, inline (after `complete`, via `Defer`) and
/// from cron: claim due jobs, run each to a terminal upload state. Every
/// terminal batch retires its own job row, so a job is never run twice
/// through two paths — the second finder finds no row. Returns how many
/// jobs reached a terminal state.
///
/// # Errors
///
/// [`DbError`] when a claim, read or batch fails — including the
/// reschedule after a failed run, so an error out of here means the
/// retry could not be written either; the claim lease expires and the
/// job comes back to the next sweep.
pub(crate) async fn drain(ctx: &ModuleContext, limit: u64) -> Result<usize, DbError> {
    // No database, no outbox; no blob, nothing to assemble. Both are
    // declared (Db required, Blob optional), so `None` here means a
    // hand-rolled context, and draining nothing beats failing a cron
    // tick.
    let (Some(db), Some(blob)) = (ctx.ports.db.as_deref(), ctx.ports.blob.as_deref()) else {
        return Ok(0);
    };
    let clock: Arc<dyn Clock> = ctx
        .ports
        .clock
        .clone()
        .unwrap_or_else(|| Arc::new(SystemClock));
    let id_gen: Arc<dyn IdGen> = ctx
        .ports
        .id_gen
        .clone()
        .unwrap_or_else(|| Arc::new(UlidIdGen));

    let now = store::iso_now(clock.as_ref());
    let lease_until = rfc3339_later(clock.now(), LEASE_SECS);
    let due = Outbox::new(OUTBOX_TABLE)
        .claim_due(db, &now, &lease_until, limit)
        .await?;

    let mut processed = 0;
    for record in due {
        if record.topic != TOPIC_EXTRACT {
            tracing::warn!(topic = %record.topic, "unknown outbox topic left to its lease");
            continue;
        }
        match run_extract(db, blob, clock.as_ref(), id_gen.as_ref(), &record).await {
            Ok(()) => processed += 1,
            // A failed run counts an attempt and comes back later — up to
            // [`MAX_EXTRACT_ATTEMPTS`], then the upload is failed for
            // good.
            Err(err) => reschedule_failed_extract(db, blob, clock.as_ref(), &record, &err).await?,
        }
    }
    Ok(processed)
}

/// A failed extract run. The claim never counts an attempt, so without
/// this a job failing against a broken drain would be redelivered
/// forever with `attempts` stuck at zero: reschedule with the outbox's
/// own [`Outbox::retry_later`], and once the run that just failed was the
/// [`MAX_EXTRACT_ATTEMPTS`]th, fail the upload instead.
async fn reschedule_failed_extract(
    db: &dyn Database,
    blob: &dyn Blob,
    clock: &dyn Clock,
    record: &OutboxRecord,
    err: &DbError,
) -> Result<(), DbError> {
    if record.attempts + 1 < MAX_EXTRACT_ATTEMPTS {
        tracing::warn!(
            job = %record.id,
            attempts = record.attempts + 1,
            %err,
            "extract failed; rescheduled"
        );
        let next = rfc3339_later(clock.now(), EXTRACT_RETRY_SECS);
        return Outbox::new(OUTBOX_TABLE)
            .retry_later(db, &record.id, &next)
            .await;
    }
    // The last allowed run failed. A payload that cannot name an upload
    // gets `resolve_job`'s retirement policy instead.
    let Ok(payload) = serde_json::from_str::<ExtractPayload>(&record.payload) else {
        tracing::error!(job = %record.id, "extract payload unparseable; retiring job");
        return retire_job(db, &record.id).await;
    };
    let Some(upload) = store::find_upload(db, &payload.tenant_id, &payload.upload_id).await? else {
        return Ok(()); // the GC or the operator got there first; nothing to fail
    };
    let parts = store::upload_parts(db, &upload.id).await?;
    fail_upload(
        db,
        blob,
        &upload,
        &parts,
        &record.id,
        format!("extraction failed after {MAX_EXTRACT_ATTEMPTS} attempts: {err}"),
        store::iso_now(clock),
    )
    .await
}

/// Retires a job whose work no longer exists: an unparseable payload (a
/// bug, not a retryable state), an upload row that is gone, or an upload
/// already terminal (a stale double job).
async fn retire_job(db: &dyn Database, job_id: &str) -> Result<(), DbError> {
    db.execute(&store::complete_outbox_stmt(job_id))
        .await
        .map(|_| ())
}

/// The upload a claimed job is for, or `None` — and the job retired —
/// when the work no longer exists: an unparseable payload (a bug, not a
/// retryable state), an upload row that is gone (the GC won the race), or
/// an upload already terminal (a stale double job).
async fn resolve_job(
    db: &dyn Database,
    record: &OutboxRecord,
) -> Result<Option<UploadRow>, DbError> {
    let Ok(payload) = serde_json::from_str::<ExtractPayload>(&record.payload) else {
        tracing::error!(job = %record.id, "extract payload unparseable; retiring job");
        retire_job(db, &record.id).await?;
        return Ok(None);
    };
    match store::find_upload(db, &payload.tenant_id, &payload.upload_id).await? {
        None => {
            retire_job(db, &record.id).await?;
            Ok(None)
        }
        // Not sitting at `complete` (already extracted or failed by
        // another path): nothing to do, which is a successful no-op.
        Some(upload) if upload.status != UPLOAD_COMPLETE => {
            retire_job(db, &record.id).await?;
            Ok(None)
        }
        Some(upload) => Ok(Some(upload)),
    }
}

/// Reads every part back, in ordinal order. A missing object or a failed
/// read is an upload-shaped failure — the upload turns `failed` with the
/// error text as its `why`, the parts dropped — not a retry, so it comes
/// back as the error variant.
async fn assemble(
    blob: &dyn Blob,
    upload: &UploadRow,
    parts: &[UploadPartRow],
) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::with_capacity(usize::try_from(upload.declared_bytes).unwrap_or(0));
    for part in parts {
        match blob
            .get(&part_key(&upload.tenant_id, &upload.id, part.n))
            .await
        {
            Ok(Some(object)) => bytes.extend_from_slice(&object.bytes),
            Ok(None) => return Err(format!("part {} is missing from blob storage", part.n)),
            Err(err) => return Err(format!("part {} could not be read: {err}", part.n)),
        }
    }
    Ok(bytes)
}

/// One claimed job, from claim to a terminal upload state. Terminal means
/// the batch committed — `extracted` or `failed`, part rows and the job
/// row gone together. A [`DbError`] leaves the claim to expire and the
/// job to come back.
async fn run_extract(
    db: &dyn Database,
    blob: &dyn Blob,
    clock: &dyn Clock,
    id_gen: &dyn IdGen,
    record: &OutboxRecord,
) -> Result<(), DbError> {
    let Some(upload) = resolve_job(db, record).await? else {
        return Ok(());
    };
    let now = store::iso_now(clock);

    // Assemble the document: contiguous part rows, each read back from
    // the store. A gap or a missing object is upload-shaped failure, not
    // a retry.
    let parts = store::upload_parts(db, &upload.id).await?;
    let contiguous = parts
        .iter()
        .enumerate()
        .all(|(index, part)| usize::try_from(part.n) == Ok(index));
    if parts.is_empty() || !contiguous {
        return fail_upload(
            db,
            blob,
            &upload,
            &parts,
            &record.id,
            "parts are not contiguous".to_owned(),
            now,
        )
        .await;
    }
    // Bytes → text → the same upsert path `POST /sources` uses. The
    // request-body ceiling does not apply here — a document assembled
    // from parts never passed through it — so the only bound is
    // [`MAX_EXTRACTED_TEXT_BYTES`], about the index, not the transport.
    let bytes = match assemble(blob, &upload, &parts).await {
        Ok(bytes) => bytes,
        Err(why) => return fail_upload(db, blob, &upload, &parts, &record.id, why, now).await,
    };
    let text = match extract::text_from(&upload.content_type, &bytes) {
        Ok(text) => text,
        Err(why) => return fail_upload(db, blob, &upload, &parts, &record.id, why, now).await,
    };
    if text.len() > MAX_EXTRACTED_TEXT_BYTES {
        return fail_upload(
            db,
            blob,
            &upload,
            &parts,
            &record.id,
            format!(
                "extracted text is {} bytes, past the {MAX_EXTRACTED_TEXT_BYTES}-byte ceiling",
                text.len()
            ),
            now,
        )
        .await;
    }

    let source_id = id_gen.ulid();
    let chunks = Chunker::default().split(&upload.tenant_id, &source_id, &text);
    // An uploaded document is an anonymous text source: no URL, no
    // caller-side `external_id` (the upload routes do not take one), and
    // `byte_len` is the indexed text's size — the same measure
    // `POST /sources` records and `GET /sources` reports as `bytes` — not
    // the size of the PDF it was read out of.
    let created_at = store::iso_now(clock);
    let source = store::SourceRow {
        id: source_id.clone(),
        tenant_id: upload.tenant_id.clone(),
        title: clean_title(Some(upload.filename.clone())),
        url: None,
        external_id: None,
        byte_len: i64::try_from(text.len()).unwrap_or(i64::MAX),
        created_at: created_at.clone(),
        updated_at: created_at,
    };
    // The source, its chunks and postings, the terminal status, the part
    // rows and the job row — one batch, the same atomicity
    // `insert_source_with_chunks` gives the inline form: a half-indexed
    // source must never be able to exist.
    let mut statements = store::source_with_chunks_statements(&source, &chunks);
    statements.extend(store::upload_outcome_statements(
        &UploadOutcome::Extracted {
            upload_id: upload.id.clone(),
            source_id,
            completed_at: now,
        },
        &record.id,
    ));
    db.batch_atomic(&statements).await?;

    // Committed: the parts are rows-no-more, so the blobs are only
    // storage. Best effort — a store outage here leaves orphans behind,
    // which no later GC can find (the rows are gone) and which the
    // operator's storage cleanup is for.
    for part in &parts {
        if let Err(err) = blob
            .delete(&part_key(&upload.tenant_id, &upload.id, part.n))
            .await
        {
            tracing::error!(upload = %upload.id, part = part.n, %err, "part blob delete failed");
        }
    }
    Ok(())
}

/// Marks an upload `failed` with `why`, drops its part rows and retires
/// the job in one batch, then deletes the part blobs (best effort — the
/// terminal state is already committed, so a missed delete here leaks
/// storage, never state).
async fn fail_upload(
    db: &dyn Database,
    blob: &dyn Blob,
    upload: &UploadRow,
    parts: &[UploadPartRow],
    job_id: &str,
    why: String,
    now: String,
) -> Result<(), DbError> {
    db.batch_atomic(&store::upload_outcome_statements(
        &UploadOutcome::Failed {
            upload_id: upload.id.clone(),
            error: why,
            completed_at: now,
        },
        job_id,
    ))
    .await?;
    for part in parts {
        if let Err(err) = blob
            .delete(&part_key(&upload.tenant_id, &upload.id, part.n))
            .await
        {
            tracing::error!(upload = %upload.id, part = part.n, %err, "part blob delete failed");
        }
    }
    Ok(())
}

/// Cron's half: drain whatever jobs were left, then collect the uploads
/// nobody finished. Bounded sweeps, as in the escalation module, so a
/// pathological outbox cannot spin one tick forever.
pub(crate) async fn scheduled(ctx: &ModuleContext, cron: &str) -> Result<(), AnyError> {
    for _ in 0..MAX_SWEEPS {
        let processed = drain(ctx, SWEEP_LIMIT)
            .await
            .map_err(|err| Box::new(err) as AnyError)?;
        if processed == 0 {
            break;
        }
    }
    let collected = gc_abandoned(ctx)
        .await
        .map_err(|err| Box::new(err) as AnyError)?;
    if collected > 0 {
        tracing::info!(collected, cron, "collected abandoned uploads");
    }
    Ok(())
}

/// Deletes every `open` upload created more than [`UPLOAD_TTL`] ago: part
/// blobs first, rows second. If a blob delete fails, the rows stay — an
/// upload the rows still name is one the next cron tick retries, the
/// only retry mechanism a list-less `Blob` port leaves.
async fn gc_abandoned(ctx: &ModuleContext) -> Result<usize, DbError> {
    let (Some(db), Some(blob)) = (ctx.ports.db.as_deref(), ctx.ports.blob.as_deref()) else {
        return Ok(0);
    };
    let clock: Arc<dyn Clock> = ctx
        .ports
        .clock
        .clone()
        .unwrap_or_else(|| Arc::new(SystemClock));
    let cutoff = rfc3339_later(clock.now(), -UPLOAD_TTL.whole_seconds());
    let stale = store::open_uploads_before(db, &cutoff).await?;

    let mut collected = 0;
    for upload in stale {
        let parts = store::upload_parts(db, &upload.id).await?;
        let mut blobs_gone = true;
        for part in &parts {
            match blob
                .delete(&part_key(&upload.tenant_id, &upload.id, part.n))
                .await
            {
                Ok(()) => {}
                Err(err) => {
                    blobs_gone = false;
                    tracing::error!(
                        upload = %upload.id,
                        part = part.n,
                        %err,
                        "part blob delete failed"
                    );
                }
            }
        }
        if !blobs_gone {
            continue;
        }
        db.batch_atomic(&store::delete_upload_statements(&upload.id))
            .await?;
        collected += 1;
    }
    Ok(collected)
}

/// `now + seconds`, RFC 3339 at whole-second precision — the string form
/// every `next_attempt_at` comparison in the outbox and the GC cutoff
/// are written against. An unrepresentable instant keeps `now`.
fn rfc3339_later(now: time::OffsetDateTime, seconds: i64) -> String {
    let shifted = now
        .checked_add(TimeDuration::seconds(seconds))
        .unwrap_or(now);
    shifted
        .replace_nanosecond(0)
        .unwrap_or(shifted)
        .format(&Rfc3339)
        .unwrap_or_default()
}
