//! The durable stage runner: claims due outbox rows and drives each one
//! through its stage — draft, judge, file, notify — so that a stage's
//! effect, its audit row, the next stage's outbox row and the completion of
//! its own row all commit in **one** [`Database::batch_atomic`]. The
//! `follow` stage is the one exception to "one row, one run": its row
//! reschedules itself and polls the tracker until the ticket closes.
//!
//! The invariants this module exists to keep (issue #4):
//!
//! - **A stage makes exactly one outbound port call.** The model and
//!   tracker ports cap a call at 30 s; a stage is one call plus local
//!   work, never a loop.
//! - **A stage completes by writing the next stage's row in the same
//!   batch as its own `Outbox::complete`.** A crash can therefore lose a
//!   stage's work entirely (the row comes back, the work re-runs) but can
//!   never strand a ticket between stages — either the whole handoff
//!   committed or none of it did.
//! - **Draining twice files once.** Every stage guards itself with an
//!   [`Inbox`] claim keyed `ticket:stage`. A claim that is already held
//!   means either "this stage already committed" (the ticket's progress
//!   says so — retire the row, touch no port) or "an earlier attempt
//!   claimed the key and died before its batch" (re-run; the key is
//!   already ours).
//! - **Retries actually re-run.** A retryable failure reschedules the
//!   outbox row *and releases the inbox claim in the same batch* — see
//!   [`crate::store::inbox_release_stmt`] for why the release has to be
//!   in that batch.
//! - **Terminal failures stop.** A rejected draft, an unauthorized
//!   tracker, undecodable bytes: the ticket parks as `dead_letter` and the
//!   row completes. A terminal failure is never `retry_later`-ed; the
//!   bounded [`RetryPolicy`] converts a retryable failure that has spent
//!   its budget into the same dead-letter outcome.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use lexical::{bm25, tokenize};
use serde_json::{Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::error::Error;
use crate::intake::OUTBOX_TABLE;
use crate::model::{
    Drafted, EventKind, Judgment, Kind, Stage, StagePayload, Status, Ticket, Verdict, stage_seq,
    webhook_events,
};
use crate::router::OwnerRouter;
use crate::store;

use cratefield_core::{
    Clock, Completion, Config, Credential, Database, Defer, Destination, Filed, HttpClient, IdGen,
    Inbox, Mailer, Message, ModelTier, Outbox, OutboxRecord, Prompt, SendOutcome, Severity,
    Statement, TextModel, TicketDraft, TicketState, TicketStatus, Tracker, scrub_text,
};
use cratefield_module_webhooks::{PublishError, Webhooks};
use cratefield_secrets::Actor;

/// The `Config` key naming the base URL of a support conversation
/// (`<base>/<conversation_id>`). When set, the filed ticket's body carries a
/// link back to the conversation it came from — see
/// [`with_conversation_link`]. Deployment config, not tenant data: it is the
/// venture's own site.
pub(crate) const CONVERSATION_URL_KEY: &str = "ESCALATION_CONVERSATION_URL";

/// The inbox table the migration creates; the stages' claim keys live in
/// it, one `ticket:stage` per stage attempt. Reachable as
/// [`Pipeline::INBOX_TABLE`].
pub(crate) const INBOX_TABLE: &str = "sg_escalation_inbox";

/// How long one drainer's lease on an outbox row lasts. It only marks the
/// row as taken; long enough to cover a stage's one 30-s-capped port call
/// several times over, short enough that a crashed drainer does not wedge
/// the row for long.
const LEASE_SECS: u64 = 300;

/// How many candidate tickets the judge is shown. Enough to cover a real
/// cluster of reports about one defect, short enough that the brief stays
/// a brief.
const CANDIDATE_LIMIT: usize = 5;

/// The follow stage's first poll: fifteen minutes after filing, when a
/// new ticket is most likely to have moved.
const FOLLOW_FIRST_DELAY: Duration = Duration::from_mins(15);

/// A fresh ticket's follow poll waits this long between attempts, while
/// the tracker is likely to keep changing on its own.
const FOLLOW_SHORT_INTERVAL: Duration = Duration::from_hours(1);

/// A ticket older than [`FOLLOW_SHORT_WINDOW`] has usually settled; its
/// poll slows to a daily check so a stable ticket costs one call a day.
const FOLLOW_LONG_INTERVAL: Duration = Duration::from_hours(24);

/// How long after filing a ticket's follow poll keeps the short interval.
const FOLLOW_SHORT_WINDOW: Duration = Duration::from_hours(24);

// ---------------------------------------------------------------------------
// RetryPolicy

/// Bounded exponential backoff for a stage's retryable failures, and the
/// attempt budget that turns a retryable failure into a dead-letter.
///
/// The schedule is pure and injectable: [`RetryPolicy::new`] carries the
/// issue's defaults (30 s base, 15 min cap, five attempts), and the
/// builder methods shrink it for tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    base: Duration,
    cap: Duration,
    max_attempts: u32,
}

impl RetryPolicy {
    /// The default base delay in seconds: the first retry waits this long.
    pub const DEFAULT_BASE_SECS: u32 = 30;
    /// The default ceiling in seconds: however many failures stack up, a
    /// retry is never scheduled further out than fifteen minutes.
    pub const DEFAULT_CAP_SECS: u32 = 900;
    /// The default attempt budget: five runs (the first plus four
    /// retries), then the stage dead-letters.
    pub const DEFAULT_MAX_ATTEMPTS: u32 = 5;

    /// The issue's defaults: 30 s base, doubling, capped at 15 min, five
    /// attempts.
    #[must_use]
    pub fn new() -> Self {
        Self {
            base: Duration::from_secs(u64::from(Self::DEFAULT_BASE_SECS)),
            cap: Duration::from_secs(u64::from(Self::DEFAULT_CAP_SECS)),
            max_attempts: Self::DEFAULT_MAX_ATTEMPTS,
        }
    }

    /// Sets the base delay (the first retry's wait; every later retry
    /// doubles it).
    #[must_use]
    pub fn base(mut self, base: Duration) -> Self {
        self.base = base;
        self
    }

    /// Sets the ceiling any single delay is capped at.
    #[must_use]
    pub fn cap(mut self, cap: Duration) -> Self {
        self.cap = cap;
        self
    }

    /// Sets the attempt budget: the `max_attempts`-th run of a stage is
    /// the last; a failure on it dead-letters instead of rescheduling.
    #[must_use]
    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// The delay before the retry that follows `attempts` already-recorded
    /// failures (an [`OutboxRecord`]'s `attempts` field): the base,
    /// doubling per failure, never past the cap.
    #[must_use]
    pub fn backoff(&self, attempts: i64) -> Duration {
        let step = u32::try_from(attempts.max(0)).unwrap_or(u32::MAX).min(63);
        let factor = 1_u64.checked_shl(step).unwrap_or(u64::MAX);
        Duration::from_secs(self.base.as_secs().saturating_mul(factor)).min(self.cap)
    }

    /// Whether `attempts_used` failed runs (the failed run counted) have
    /// spent the budget. A retryable failure past this point dead-letters
    /// — the issue's "retried with bounded backoff" has to be bounded.
    #[must_use]
    pub fn exhausted(&self, attempts_used: i64) -> bool {
        attempts_used >= i64::from(self.max_attempts)
    }

    /// When the retry after `attempts` recorded failures should run: `now`
    /// plus the **larger** of the schedule's backoff and the port's
    /// `retry_after` hint, where the port gave one — a provider that names
    /// a longer wait is honoured, and one that names a shorter wait does
    /// not shrink the schedule below its own floor.
    #[must_use]
    pub fn next_attempt_at(
        &self,
        attempts: i64,
        now: OffsetDateTime,
        retry_after: Option<Duration>,
    ) -> OffsetDateTime {
        let wait = retry_after.map_or_else(
            || self.backoff(attempts),
            |hint| hint.max(self.backoff(attempts)),
        );
        let secs = i64::try_from(wait.as_secs()).unwrap_or(i64::MAX);
        now.checked_add(time::Duration::seconds(secs))
            .unwrap_or(now)
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Pipeline

/// Where the file stage sent a ticket: to the `Tracker` port, or into the
/// module's own built-in ticketing (no route configured, or a route naming
/// [`store::RouteTarget::Local`]).
enum FileOutcome {
    /// Handed to the `Tracker` port: its reference and the destination's
    /// [`kind`](cratefield_core::Destination::kind).
    Tracker(Filed, &'static str),
    /// Filed into built-in ticketing: no tracker call was made and the
    /// reference is the synthetic `local:<ticket id>`.
    Local,
}

/// The four-stage runner. Owned and `Clone` (every field is an `Arc` or
/// smaller), so a stage can hand a clone of the whole pipeline into a
/// [`Defer`](cratefield_core::Defer) future — which must be `'static` —
/// and keep draining its own sweep.
#[derive(Clone)]
pub struct Pipeline {
    db: Arc<dyn Database>,
    model: Arc<dyn TextModel>,
    tracker: Arc<dyn Tracker>,
    mailer: Option<Arc<dyn Mailer>>,
    config: Arc<dyn Config>,
    clock: Arc<dyn Clock>,
    idgen: Arc<dyn IdGen>,
    defer: Option<Arc<dyn Defer>>,
    /// The outbound-webhook publisher for the `escalation.*` lifecycle
    /// events. `None` (the [`Pipeline::new`] default) publishes nothing; a
    /// composed venture wires it with [`Pipeline::with_webhooks`], whose
    /// tables must exist in this database.
    webhooks: Option<Webhooks>,
    /// The owner lookup the file stage asks. `None` (the default) files
    /// with the tenant's configured destination alone.
    router: Option<Arc<dyn OwnerRouter>>,
    /// The HTTP port [`Pipeline::router`] calls through, resolved from the
    /// runtime. `None` means a wired router is asked with no client and
    /// answers `None`.
    http: Option<Arc<dyn HttpClient>>,
    outbox: Outbox,
    inbox: Inbox,
    policy: RetryPolicy,
}

impl Pipeline {
    /// How many claim/drain sweeps `Module::scheduled` runs at most before
    /// it leaves the rest to the next cron tick — the bound that stops a
    /// row that keeps re-enqueueing work from spinning the scheduler.
    pub const MAX_SWEEPS: u32 = 8;

    /// How many rows one sweep claims at once.
    pub const SWEEP_LIMIT: u64 = 25;

    /// The inbox table the stages claim `ticket:stage` keys in — the same
    /// constant as the module-level one, reachable from the type step 4's
    /// tests construct.
    pub const INBOX_TABLE: &str = INBOX_TABLE;

    /// Assembles a runner over one database, the `TextModel`/`Tracker`
    /// ports the module declared, and whatever optional ports the runtime
    /// resolved. `config` is needed by the file stage, which
    /// resolves the tenant's stored `credential_ref` against it at
    /// file-time (never a secret out of the database).
    ///
    /// Every port except the database, the model and the tracker is
    /// degradeable: `mailer` gates the notify send, `defer` gates the
    /// run-the-next-stage-immediately handoff (without it the next stage
    /// waits for the next drain), and `clock`/`idgen` are the row
    /// timestamps and ids.
    #[allow(clippy::too_many_arguments)] // one flat constructor for callers; every argument is a distinct port
    #[must_use]
    pub fn new(
        db: Arc<dyn Database>,
        model: Arc<dyn TextModel>,
        tracker: Arc<dyn Tracker>,
        mailer: Option<Arc<dyn Mailer>>,
        config: Arc<dyn Config>,
        clock: Arc<dyn Clock>,
        idgen: Arc<dyn IdGen>,
        defer: Option<Arc<dyn Defer>>,
    ) -> Self {
        Self {
            db,
            model,
            tracker,
            mailer,
            config,
            clock,
            idgen,
            defer,
            // Publishing is opt-in: a bare pipeline (every test fixture that
            // does not build the webhooks tables) publishes nothing.
            webhooks: None,
            // Routing is opt-in too: a bare pipeline files where the
            // tenant's destination says, exactly as before the port.
            router: None,
            http: None,
            outbox: Outbox::new(OUTBOX_TABLE),
            inbox: Inbox::new(INBOX_TABLE),
            policy: RetryPolicy::new(),
        }
    }

    /// Replaces the default [`RetryPolicy`] (builder-style; the issue's
    /// tests shrink it to make retries immediate).
    #[must_use]
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Wires the outbound-webhook publisher, so every outcome that changes
    /// a ticket's life also publishes its `escalation.*` event **into the
    /// same atomic batch** that records the audit row. The webhooks module's
    /// tables must exist in this pipeline's database — the venture that
    /// mounts escalation registers the `Webhooks` module for that.
    #[must_use]
    pub fn with_webhooks(mut self, webhooks: Webhooks) -> Self {
        self.webhooks = Some(webhooks);
        self
    }

    /// Wires the owner lookup the file stage asks (see [`OwnerRouter`]).
    /// `None` — the [`Pipeline::new`] default — files with the tenant's
    /// configured destination alone.
    #[must_use]
    pub fn with_router(mut self, router: Option<Arc<dyn OwnerRouter>>) -> Self {
        self.router = router;
        self
    }

    /// Wires the HTTP port [`Pipeline::with_router`]'s lookup calls through.
    /// `None` is a runtime with no HTTP port: a wired router is asked with
    /// no client and answers `None`, so filing falls back unchanged.
    #[must_use]
    pub fn with_http(mut self, http: Option<Arc<dyn HttpClient>>) -> Self {
        self.http = http;
        self
    }

    /// Claims up to `limit` due outbox rows and runs each one through its
    /// stage. Returns how many rows it processed — including rows it
    /// retired as dead or duplicate: a count of *work done*, not of
    /// successes. A row whose failure could not even be recorded (the
    /// database is down) fails the sweep; the lease expires and the row
    /// comes back.
    ///
    /// # Errors
    ///
    /// [`Error::Db`] when the claim sweep or a stage's recording batch
    /// fails. A stage's own port failure is *not* an error here: it is
    /// classified into a retry or a dead-letter and recorded durably.
    pub async fn drain(&self, limit: u64) -> Result<usize, Error> {
        let now = self.now();
        let lease_until = rfc3339_after(now, Duration::from_secs(LEASE_SECS));
        let due = self
            .outbox
            .claim_due(&*self.db, &rfc3339(now), &lease_until, limit)
            .await?;
        let mut processed = 0;
        for record in due {
            self.process_record(&record).await?;
            processed += 1;
        }
        Ok(processed)
    }

    /// Routes one claimed record. Every terminal outcome inside is
    /// recorded durably (retry, dead-letter or plain retirement), so the
    /// only errors that escape are database failures.
    async fn process_record(&self, record: &OutboxRecord) -> Result<(), Error> {
        let now = self.now();
        let at = rfc3339(now);

        // 1. Route by topic. A topic this module does not know came from a
        //    different version of this pipeline; nothing will ever route
        //    it, so it must not sit in the queue retrying forever.
        let Some(stage) = Stage::from_topic(&record.topic) else {
            return self.retire_unknown_topic(record, &at).await;
        };

        // 2. Decode the payload. Two ids; bytes that do not carry them
        //    will not heal on a retry, and the ticket id they fail to name
        //    is exactly what an audit row would need — so the row is
        //    simply retired.
        let Ok(payload) = serde_json::from_str::<StagePayload>(&record.payload) else {
            return self.complete_row(&record.id).await;
        };

        // 3. Claim `ticket:stage`. First sight proceeds; a held key needs
        //    the ticket's progress to say which of two worlds this is.
        //    The follow stage takes no claim: its row is *meant* to run
        //    again (it reschedules itself), so a claim keyed `ticket:follow`
        //    would either block the poll forever or make every poll look
        //    like a double-drain. A status-update notify is keyed by its
        //    event id instead, so two different updates never collide on one
        //    key. In both exempt cases `claimed` is `true` — the
        //    double-drain guard below must not touch them.
        let event_id = payload.event_id.as_deref();
        let claimed = if stage == Stage::Follow {
            true
        } else {
            self.inbox
                .claim(
                    &*self.db,
                    &claim_key_with(&payload.ticket_id, stage, event_id),
                    &at,
                )
                .await?
        };

        // 4. Load the ticket. Both the stage handlers and the
        //    double-drain check below read it; a read failure rides the
        //    ordinary retry path.
        let ticket = match store::load_ticket(&*self.db, &payload.ticket_id).await {
            Ok(ticket) => ticket,
            Err(err) => {
                return self
                    .fail(
                        record,
                        stage,
                        &payload.ticket_id,
                        &payload.tenant_id,
                        event_id,
                        err,
                        &at,
                    )
                    .await;
            }
        };
        let Some(ticket) = ticket else {
            // The row names a ticket that does not exist. For follow this
            // is terminal for the row — there is nothing left to poll, and
            // a follow row never dead-letters; every other stage records
            // the terminal miss (an audit row survives on its own id and
            // the status update is a harmless no-op).
            if stage == Stage::Follow {
                return self.complete_row(&record.id).await;
            }
            let err = Error::Decode(format!("ticket `{}` is missing", payload.ticket_id));
            return self
                .fail(
                    record,
                    stage,
                    &payload.ticket_id,
                    &payload.tenant_id,
                    event_id,
                    err,
                    &at,
                )
                .await;
        };

        // The crash-window branch. `claim` returned false, so the key is
        // already held — but by *which* attempt? If the ticket's recorded
        // progress shows this stage already committed (its stage has
        // advanced past this one, or its status is terminal), this row is
        // a genuine double-drain of finished work: complete it and touch
        // no port, or a re-delivered outbox row would file the ticket a
        // second time. Otherwise the previous attempt claimed the key and
        // died *before* its batch committed — its work is nowhere — so
        // this run proceeds on the already-held key exactly as if it had
        // won it. (The claim's remaining job in that case is to keep a
        // concurrent drainer out until this attempt's batch lands.) A
        // status-update notify is exempt: its key is unique per event, so
        // a held key cannot mean "already committed".
        if !claimed && event_id.is_none() && stage_already_committed(&ticket, stage) {
            return self.complete_row(&record.id).await;
        }

        // 5-6. One port call, then one all-or-nothing batch.
        match stage {
            Stage::Draft => self.run_draft(record, &ticket, &at).await,
            Stage::Judge => self.run_judge(record, &ticket, &at).await,
            Stage::File => self.run_file(record, &ticket, &at).await,
            Stage::Notify => self.run_notify(record, &ticket, event_id, &at).await,
            Stage::Follow => self.run_follow(record, &ticket, &at).await,
        }
    }

    // ------------------------------------------------------------------
    // Stage: draft

    /// One fast-model call with the [`Drafted`] schema; the transcript is
    /// the user message. On success the draft, the `Drafting` status, the
    /// move to the judge stage, the judge's outbox row and this row's
    /// completion all commit together.
    async fn run_draft(
        &self,
        record: &OutboxRecord,
        ticket: &Ticket,
        at: &str,
    ) -> Result<(), Error> {
        // The transcript is customer-authored text: it may carry an email
        // address, a signed link, a bearer token. `scrub_text` redacts those
        // before the text leaves for the model, so the drafter cannot copy a
        // secret or an address into the draft it writes.
        let prompt = Prompt::new(ModelTier::Fast)
            .system(DRAFT_SYSTEM)
            .user(scrub_text(&ticket.transcript))
            .json_schema(Drafted::json_schema());

        let completion = match self.model.complete(&prompt).await {
            Ok(completion) => completion,
            Err(err) => {
                return self
                    .fail(
                        record,
                        Stage::Draft,
                        &ticket.id,
                        &ticket.tenant_id,
                        None,
                        Error::from(err),
                        at,
                    )
                    .await;
            }
        };
        // `Completion::json` first; a provider that cannot honour the
        // schema answers with text and `json: None`, and text that does
        // not parse is a terminal failure, not a retry.
        let drafted = match decode_completion::<Drafted>(&completion) {
            Ok(drafted) => drafted,
            Err(err) => {
                return self
                    .fail(
                        record,
                        Stage::Draft,
                        &ticket.id,
                        &ticket.tenant_id,
                        None,
                        err,
                        at,
                    )
                    .await;
            }
        };

        // A draft that left out a field its kind requires is parked as
        // `needs_info` here, deterministically, before the judge is even
        // asked: an incomplete lead or support case must never reach a
        // tracker, and no model gets to overrule that (issue #24).
        let missing = drafted.missing_fields();
        if !missing.is_empty() {
            return self
                .commit_draft_needs_info(record, ticket, &drafted, &missing, at)
                .await;
        }

        let body = render_body_markdown(&drafted);
        let mut detail = serde_json::to_value(&drafted).map_err(Error::from)?;
        insert_detail(&mut detail, "body_markdown", json!(body));
        insert_detail(&mut detail, "model", json!(completion.model));

        // The customer's own question (set at intake, when the caller knew
        // one) is kept, not overwritten with an invention of the drafter's.
        let batch = [
            store::insert_event_stmt(
                &self.idgen.ulid(),
                &ticket.id,
                stage_seq(Stage::Draft, 0),
                at,
                Stage::Draft,
                EventKind::DraftCompleted,
                &detail,
            ),
            store::update_ticket_draft_stmt(
                &ticket.id,
                &drafted,
                &body,
                ticket.customer_question.as_deref(),
                at,
            ),
            store::update_ticket_status_stmt(&ticket.id, Status::Drafting, at),
            store::update_ticket_stage_stmt(&ticket.id, Stage::Judge, at),
            store::enqueue_stage_stmt(
                &self.outbox,
                &self.idgen.ulid(),
                &ticket.id,
                &ticket.tenant_id,
                Stage::Judge,
                None,
                at,
            ),
            store::outbox_complete_stmt(OUTBOX_TABLE, &record.id),
        ];
        self.commit(&batch).await?;
        self.defer_next();
        Ok(())
    }

    /// The deterministic completion gate's outcome (issue #24): a draft
    /// missing a field its kind requires is committed as-is and parked as
    /// `needs_info`, with a question naming what is missing, then handed to
    /// the notify stage — never to the judge, never to a tracker. The
    /// batch mirrors the `needs_info` verdict's (see `commit_judgment`),
    /// but there is no judgment yet to record.
    async fn commit_draft_needs_info(
        &self,
        record: &OutboxRecord,
        ticket: &Ticket,
        drafted: &Drafted,
        missing: &[&str],
        at: &str,
    ) -> Result<(), Error> {
        let question = compose_missing_fields_question(drafted.kind, missing);
        let body = render_body_markdown(drafted);
        let mut detail = serde_json::to_value(drafted).map_err(Error::from)?;
        insert_detail(&mut detail, "body_markdown", json!(body));
        let asked = json!({
            "kind": drafted.kind.as_str(),
            "missing_fields": missing,
            "question": question,
        });
        let batch = [
            store::insert_event_stmt(
                &self.idgen.ulid(),
                &ticket.id,
                stage_seq(Stage::Draft, 0),
                at,
                Stage::Draft,
                EventKind::DraftCompleted,
                &detail,
            ),
            store::insert_event_stmt(
                &self.idgen.ulid(),
                &ticket.id,
                stage_seq(Stage::Draft, 1),
                at,
                Stage::Draft,
                EventKind::NeedsInfo,
                &asked,
            ),
            store::update_ticket_draft_stmt(
                &ticket.id,
                drafted,
                &body,
                ticket.customer_question.as_deref(),
                at,
            ),
            store::update_ticket_question_stmt(&ticket.id, &question, at),
            store::update_ticket_status_stmt(&ticket.id, Status::NeedsInfo, at),
            store::update_ticket_stage_stmt(&ticket.id, Stage::Notify, at),
            store::enqueue_stage_stmt(
                &self.outbox,
                &self.idgen.ulid(),
                &ticket.id,
                &ticket.tenant_id,
                Stage::Notify,
                None,
                at,
            ),
            store::outbox_complete_stmt(OUTBOX_TABLE, &record.id),
        ];
        self.commit(&batch).await?;
        self.defer_next();
        Ok(())
    }

    // ------------------------------------------------------------------
    // Stage: judge

    /// One strong-model call with the [`Judgment`] schema, carrying the
    /// drafted ticket *and* the original transcript — the judge checks
    /// `reproducible` and `pii_clean` against the source, not against the
    /// draft's word. The brief also lists the already-filed tickets the
    /// draft might duplicate (see [`Pipeline::candidates`]), so the judge
    /// picks from names rather than recalling them; the ids shown ride in
    /// the `judge_completed` audit row so `commit_judgment` can validate
    /// the judge's `duplicate_of` against exactly what it saw. That row
    /// carries the full judgment, reasons and all, in every branch.
    async fn run_judge(
        &self,
        record: &OutboxRecord,
        ticket: &Ticket,
        at: &str,
    ) -> Result<(), Error> {
        let candidates = self.candidates(ticket).await?;
        let prompt = Prompt::new(ModelTier::Strong)
            .system(JUDGE_SYSTEM)
            .user(judge_brief(ticket, &candidates))
            .json_schema(Judgment::json_schema());

        let completion = match self.model.complete(&prompt).await {
            Ok(completion) => completion,
            Err(err) => {
                return self
                    .fail(
                        record,
                        Stage::Judge,
                        &ticket.id,
                        &ticket.tenant_id,
                        None,
                        Error::from(err),
                        at,
                    )
                    .await;
            }
        };
        let judgment = match decode_completion::<Judgment>(&completion) {
            Ok(judgment) => judgment,
            Err(err) => {
                return self
                    .fail(
                        record,
                        Stage::Judge,
                        &ticket.id,
                        &ticket.tenant_id,
                        None,
                        err,
                        at,
                    )
                    .await;
            }
        };

        let mut detail = serde_json::to_value(&judgment).map_err(Error::from)?;
        insert_detail(&mut detail, "model", json!(completion.model));
        // The candidate ids, in the order they were shown: the record of
        // what the judge could legitimately name, and the set
        // `commit_judgment` validates `duplicate_of` against.
        let shown: Vec<&str> = candidates
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect();
        insert_detail(&mut detail, "candidates", json!(shown));
        let judge_completed = store::insert_event_stmt(
            &self.idgen.ulid(),
            &ticket.id,
            stage_seq(Stage::Judge, 0),
            at,
            Stage::Judge,
            EventKind::JudgeCompleted,
            &detail,
        );

        self.commit_judgment(record, ticket, &judgment, &candidates, judge_completed, at)
            .await
    }

    /// The already-filed tickets the draft might duplicate, scored by
    /// BM25 against the draft and capped at [`CANDIDATE_LIMIT`]. Reads
    /// the tenant's filed tickets back (see
    /// [`store::candidate_tickets`]) and ranks them in memory — the
    /// corpus is one tenant's most recently active tickets, so there is
    /// nothing to index server-side.
    async fn candidates(&self, ticket: &Ticket) -> Result<Vec<Candidate>, Error> {
        let tickets = store::candidate_tickets(&*self.db, &ticket.tenant_id, &ticket.id).await?;
        Ok(rank_candidates(&draft_text(ticket), &tickets))
    }

    /// Commits the judge's verdict — one all-or-nothing batch per
    /// outcome: on to the file stage, linked to an existing filed ticket,
    /// parked as rejected, blocked for PII (a verdict that would file a
    /// draft the judge found unclean), or parked with a question for the
    /// customer and on to the notify stage.
    ///
    /// The judge's `duplicate_of` is validated here, the way a citation
    /// is: only an id from `candidates` (the exact list the brief showed,
    /// also recorded in `judge_completed`) is honoured. A `duplicate`
    /// verdict naming anything else — or nothing — falls back to filing,
    /// because a duplicate report that files is a duplicate ticket, but a
    /// report dropped on a bad id is a defect lost.
    async fn commit_judgment(
        &self,
        record: &OutboxRecord,
        ticket: &Ticket,
        judgment: &Judgment,
        candidates: &[Candidate],
        judge_completed: Statement,
        at: &str,
    ) -> Result<(), Error> {
        match judgment.verdict {
            // `commit_file` refuses a draft the judge found carrying
            // customer PII (`pii_clean: false`): it is blocked, never filed.
            Verdict::File => {
                self.commit_file(record, ticket, judgment, judge_completed, None, at)
                    .await
            }
            Verdict::Duplicate => {
                self.commit_duplicate_verdict(
                    record,
                    ticket,
                    judgment,
                    candidates,
                    judge_completed,
                    at,
                )
                .await
            }
            Verdict::Reject => {
                // The rejection's audit row records *why*: the judge's own
                // words and every flag it set. Nothing is enqueued and the
                // tracker is never touched — a rejection ends here.
                let why = json!({
                    "reasons": judgment.reasons,
                    "flags": {
                        "is_defect": judgment.is_defect,
                        "kind_ok": judgment.kind_ok,
                        "reproducible": judgment.reproducible,
                        "severity_ok": judgment.severity_ok,
                        "pii_clean": judgment.pii_clean,
                        "duplicate_of": judgment.duplicate_of,
                    },
                });
                let rejected = store::insert_event_stmt(
                    &self.idgen.ulid(),
                    &ticket.id,
                    stage_seq(Stage::Judge, 1),
                    at,
                    Stage::Judge,
                    EventKind::Rejected,
                    &why,
                );
                let batch = [
                    judge_completed,
                    rejected,
                    store::update_ticket_judgment_stmt(&ticket.id, judgment, at),
                    store::update_ticket_status_stmt(&ticket.id, Status::Rejected, at),
                    store::outbox_complete_stmt(OUTBOX_TABLE, &record.id),
                ];
                self.commit(&batch).await
            }
            Verdict::NeedsInfo => {
                // Not a filing: a question for the customer, phrased from
                // the judge's reasons and stored on the ticket, then the
                // *notify* stage (not file) delivers it.
                self.commit_needs_info(
                    record,
                    ticket,
                    judgment,
                    vec![judge_completed],
                    &judgment.reasons,
                    at,
                )
                .await
            }
        }
    }

    /// Parks a ticket as `needs_info` at the judge stage: a question
    /// phrased from `reasons` is stored on the ticket, and the notify
    /// stage (not file) delivers it. Shared by the `needs_info` verdict
    /// and the kind-completion gate (`kind_ok: false`) so both answer the
    /// customer the same way. `leading` carries the statements that come
    /// first in the batch — the `judge_completed` row, plus the
    /// `duplicate_ignored` event when a `duplicate` verdict fell back to
    /// needing more information.
    async fn commit_needs_info(
        &self,
        record: &OutboxRecord,
        ticket: &Ticket,
        judgment: &Judgment,
        leading: Vec<Statement>,
        reasons: &[String],
        at: &str,
    ) -> Result<(), Error> {
        let question = compose_needs_info_question(reasons);
        let asked = json!({ "question": question, "reasons": reasons });
        let needs_info = store::insert_event_stmt(
            &self.idgen.ulid(),
            &ticket.id,
            stage_seq(Stage::Judge, 1),
            at,
            Stage::Judge,
            EventKind::NeedsInfo,
            &asked,
        );
        let mut batch = leading;
        batch.extend([
            needs_info,
            store::update_ticket_judgment_stmt(&ticket.id, judgment, at),
            store::update_ticket_question_stmt(&ticket.id, &question, at),
            store::update_ticket_status_stmt(&ticket.id, Status::NeedsInfo, at),
            store::update_ticket_stage_stmt(&ticket.id, Stage::Notify, at),
            store::enqueue_stage_stmt(
                &self.outbox,
                &self.idgen.ulid(),
                &ticket.id,
                &ticket.tenant_id,
                Stage::Notify,
                None,
                at,
            ),
            store::outbox_complete_stmt(OUTBOX_TABLE, &record.id),
        ]);
        self.commit(&batch).await?;
        self.defer_next();
        Ok(())
    }

    /// Resolves a `duplicate` verdict: link when the judge named a shown
    /// candidate that still exists, otherwise file — always recording a
    /// `duplicate_ignored` event, so the trail shows the verdict was a
    /// duplicate and *what* it named (a rejected id, or none at all). A
    /// report dropped on a bad id is a defect lost, so a rejected link
    /// falls back to filing.
    async fn commit_duplicate_verdict(
        &self,
        record: &OutboxRecord,
        ticket: &Ticket,
        judgment: &Judgment,
        candidates: &[Candidate],
        judge_completed: Statement,
        at: &str,
    ) -> Result<(), Error> {
        let target = judgment
            .duplicate_of
            .as_deref()
            .filter(|id| candidates.iter().any(|candidate| candidate.id == *id));
        // Re-read the row rather than trusting the candidate snapshot:
        // link rows name it, so it must still exist.
        let existing = match target {
            Some(id) => store::load_ticket(&*self.db, id).await?,
            None => None,
        };
        if let Some(existing) = existing {
            self.commit_duplicate(record, ticket, judgment, judge_completed, &existing, at)
                .await
        } else {
            let ignored = self.duplicate_ignored_stmt(
                ticket,
                judgment.duplicate_of.as_deref(),
                candidates,
                at,
            );
            self.commit_file(record, ticket, judgment, judge_completed, Some(ignored), at)
                .await
        }
    }

    /// The `file` path, shared by a plain `file` verdict and by a
    /// `duplicate` verdict that had to fall back (invalid or missing
    /// target). `ignored` is the `duplicate_ignored` event for that
    /// fallback — so the trail shows the duplicate verdict was seen and
    /// what (if anything) it named. Two gates stand between the verdict and
    /// the tracker: the PII gate (never file a draft the judge found
    /// unclean) and the kind gate (never file a draft whose kind the judge
    /// could not confirm), the latter parking the ticket for more
    /// information (issue #24).
    async fn commit_file(
        &self,
        record: &OutboxRecord,
        ticket: &Ticket,
        judgment: &Judgment,
        judge_completed: Statement,
        ignored: Option<Statement>,
        at: &str,
    ) -> Result<(), Error> {
        // The one place the file stage is set and enqueued, so the PII
        // gate lives here: whichever verdict led here — a plain `file`, or
        // a `duplicate` that fell back to filing — a draft the judge found
        // carrying customer PII never reaches the tracker.
        if !judgment.pii_clean {
            return self
                .block_for_pii(record, ticket, judgment, judge_completed, ignored, at)
                .await;
        }
        // The judge's kind gate (issue #24): a draft whose kind does not
        // fit, or that is missing a field its kind requires, must not be
        // filed — but it is not a rejection either, so it is parked as
        // `needs_info` for the customer to complete, exactly as a
        // `needs_info` verdict is. Checked after the PII gate so blocking
        // for PII keeps its own distinguishable trail.
        if !judgment.kind_ok {
            let fallback = vec![format!(
                "we could not confirm this is a complete {} as written",
                ticket.kind.as_str()
            )];
            let reasons = if judgment.reasons.is_empty() {
                &fallback
            } else {
                &judgment.reasons
            };
            let mut leading = vec![judge_completed];
            leading.extend(ignored);
            return self
                .commit_needs_info(record, ticket, judgment, leading, reasons, at)
                .await;
        }
        let mut batch = vec![judge_completed];
        batch.extend(ignored);
        batch.extend([
            store::update_ticket_judgment_stmt(&ticket.id, judgment, at),
            store::update_ticket_status_stmt(&ticket.id, Status::Filing, at),
            store::update_ticket_stage_stmt(&ticket.id, Stage::File, at),
            store::enqueue_stage_stmt(
                &self.outbox,
                &self.idgen.ulid(),
                &ticket.id,
                &ticket.tenant_id,
                Stage::File,
                None,
                at,
            ),
            store::outbox_complete_stmt(OUTBOX_TABLE, &record.id),
        ]);
        self.commit(&batch).await?;
        self.defer_next();
        Ok(())
    }

    /// The `duplicate` path: this ticket is not filed. Instead it is
    /// linked to the existing filed ticket, that ticket's `match_count`
    /// is bumped, and the customer is notified (through the notify stage,
    /// exactly as a filing is). The duplicate's own `external_id`/
    /// `external_url` are copied from the existing ticket so the notify
    /// stage can name and link it without loading the other row. No
    /// `Tracker::file` call happens anywhere on this path.
    async fn commit_duplicate(
        &self,
        record: &OutboxRecord,
        ticket: &Ticket,
        judgment: &Judgment,
        judge_completed: Statement,
        existing: &Ticket,
        at: &str,
    ) -> Result<(), Error> {
        let detail = json!({
            "duplicate_of": existing.id,
            "external_id": existing.external_id,
            "url": existing.external_url,
        });
        let linked = store::insert_event_stmt(
            &self.idgen.ulid(),
            &ticket.id,
            stage_seq(Stage::Judge, 1),
            at,
            Stage::Judge,
            EventKind::Linked,
            &detail,
        );
        let batch = [
            judge_completed,
            linked,
            store::update_ticket_judgment_stmt(&ticket.id, judgment, at),
            store::update_ticket_duplicate_stmt(&ticket.id, existing, at),
            store::update_ticket_status_stmt(&ticket.id, Status::Duplicate, at),
            store::update_ticket_stage_stmt(&ticket.id, Stage::Notify, at),
            store::insert_ticket_link_stmt(existing, &ticket.id, &ticket.conversation_id, at),
            store::increment_match_count_stmt(&existing.id, at),
            store::enqueue_stage_stmt(
                &self.outbox,
                &self.idgen.ulid(),
                &ticket.id,
                &ticket.tenant_id,
                Stage::Notify,
                None,
                at,
            ),
            store::outbox_complete_stmt(OUTBOX_TABLE, &record.id),
        ];
        self.commit(&batch).await?;
        self.defer_next();
        Ok(())
    }

    /// The audit row for a `duplicate` verdict that fell back to filing:
    /// the row records the id the judge named (JSON `null` when it named
    /// none) and the candidates that were on offer.
    fn duplicate_ignored_stmt(
        &self,
        ticket: &Ticket,
        duplicate_of: Option<&str>,
        candidates: &[Candidate],
        at: &str,
    ) -> Statement {
        let shown: Vec<&str> = candidates
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect();
        let detail = json!({ "duplicate_of": duplicate_of, "candidates": shown });
        store::insert_event_stmt(
            &self.idgen.ulid(),
            &ticket.id,
            stage_seq(Stage::Judge, 1),
            at,
            Stage::Judge,
            EventKind::DuplicateIgnored,
            &detail,
        )
    }

    /// Parks a ticket the judge would file but found carrying customer PII.
    /// It is a hard stop, not a fixable verdict: nothing is enqueued, the
    /// tracker is never touched, and the ticket is parked exactly as a
    /// rejection is. The audit says why, so this block is distinguishable
    /// from a plain not-a-defect rejection. `ignored` is the
    /// `duplicate_ignored` event when a `duplicate` verdict fell back here.
    async fn block_for_pii(
        &self,
        record: &OutboxRecord,
        ticket: &Ticket,
        judgment: &Judgment,
        judge_completed: Statement,
        ignored: Option<Statement>,
        at: &str,
    ) -> Result<(), Error> {
        let why = json!({
            "blocked_for_pii": true,
            "reason": "the judge found customer PII in the draft; it must not reach a tracker",
            "reasons": judgment.reasons,
        });
        let blocked = store::insert_event_stmt(
            &self.idgen.ulid(),
            &ticket.id,
            stage_seq(Stage::Judge, 1),
            at,
            Stage::Judge,
            EventKind::Rejected,
            &why,
        );
        let mut batch = vec![judge_completed];
        batch.extend(ignored);
        batch.extend([
            blocked,
            store::update_ticket_judgment_stmt(&ticket.id, judgment, at),
            store::update_ticket_status_stmt(&ticket.id, Status::Rejected, at),
            store::outbox_complete_stmt(OUTBOX_TABLE, &record.id),
        ]);
        self.commit(&batch).await
    }

    // ------------------------------------------------------------------
    // Stage: file

    /// One tracker call — or none at all. Everything before it (route
    /// lookup, credential resolution, the draft) is local resolution; the
    /// call is the stage's one HttpClient-bound port call. The idempotency
    /// key is derived from the ticket id alone, so a retry after a lost
    /// response presents the same key and the tracker collapses it instead
    /// of filing a second ticket. A ticket with no route — or one whose
    /// route is the built-in ticketing — files locally and never touches
    /// the port (issue #24), so a tenant that has not configured a tracker
    /// still gets a filed ticket rather than a dead-lettered one.
    async fn run_file(
        &self,
        record: &OutboxRecord,
        ticket: &Ticket,
        at: &str,
    ) -> Result<(), Error> {
        match self.file_ticket(ticket).await {
            Ok(outcome) => {
                // Only a tracker-filed ticket has a tracker state to follow;
                // built-in tickets change status through their own route.
                let tracked = matches!(outcome, FileOutcome::Tracker(..));
                let (filed, kind, url_is_public) = match outcome {
                    FileOutcome::Tracker(filed, kind) => {
                        // A tracker's `Filed::url` is its public issue link
                        // (useful in a notification); a `webhook`
                        // destination's is the endpoint *itself* — credential
                        // material (issue #23) — so it reaches neither the
                        // audit row nor the event payload.
                        let url_is_public = kind != "webhook";
                        (filed, kind, url_is_public)
                    }
                    // Built-in ticketing: a synthetic reference, no URL
                    // (there is none to link to, and one is never invented).
                    FileOutcome::Local => (
                        Filed {
                            external_id: format!("{}:{}", store::LOCAL_ROUTE, ticket.id),
                            url: String::new(),
                        },
                        store::LOCAL_ROUTE,
                        false,
                    ),
                };
                let mut detail = json!({
                    "external_id": filed.external_id.clone(),
                });
                if url_is_public {
                    detail["url"] = json!((!filed.url.is_empty()).then(|| filed.url.clone()));
                }
                let mut filed_data = json!({
                    "ticket_id": ticket.id,
                    "tenant_id": ticket.tenant_id,
                    "destination_kind": kind,
                    "external_id": filed.external_id,
                });
                if url_is_public {
                    filed_data["url"] = json!(filed.url);
                }
                // A tracker-filed ticket is followed (see `follow_stmts`); a
                // built-in one is not polled, so nothing more is enqueued.
                let mut batch = vec![
                    store::insert_event_stmt(
                        &self.idgen.ulid(),
                        &ticket.id,
                        stage_seq(Stage::File, 0),
                        at,
                        Stage::File,
                        EventKind::Filed,
                        &detail,
                    ),
                    store::update_ticket_filed_stmt(&ticket.id, &filed, at),
                    store::update_ticket_status_stmt(&ticket.id, Status::Filed, at),
                    store::update_ticket_stage_stmt(&ticket.id, Stage::Notify, at),
                    store::enqueue_stage_stmt(
                        &self.outbox,
                        &self.idgen.ulid(),
                        &ticket.id,
                        &ticket.tenant_id,
                        Stage::Notify,
                        None,
                        at,
                    ),
                ];
                if tracked {
                    batch.extend(self.follow_stmts(ticket, at));
                }
                batch.push(store::outbox_complete_stmt(OUTBOX_TABLE, &record.id));
                // The `escalation.filed` fan-out joins the same batch, so a
                // subscriber is told exactly when the ticket is filed — never
                // for a file that rolled back, never missing one that landed.
                batch.extend(
                    self.webhook_stmts(
                        &ticket.tenant_id,
                        webhook_events::ESCALATION_FILED,
                        &filed_data,
                        at,
                    )
                    .await?,
                );
                self.commit(&batch).await?;
                self.defer_next();
                Ok(())
            }
            // A transient tracker failure retries under the policy; a
            // rejection, an unauthorized credential or an undecodable
            // draft dead-letters — `fail` sorts one from the other, and
            // both write their reason into a `file_dead_lettered`
            // / `file_retry_scheduled` row.
            Err(err) => {
                self.fail(
                    record,
                    Stage::File,
                    &ticket.id,
                    &ticket.tenant_id,
                    None,
                    err,
                    at,
                )
                .await
            }
        }
    }

    /// The statements that start following a tracker-filed ticket: its
    /// baseline `open` state and the follow poll's own row, enqueued and
    /// pushed out to its first poll time — a quarter of an hour, so a
    /// brand-new ticket is checked once it has had a chance to move. It
    /// reschedules itself from there (see `run_follow`).
    fn follow_stmts(&self, ticket: &Ticket, at: &str) -> [Statement; 3] {
        let follow_job = self.idgen.ulid();
        let follow_first_at = rfc3339_after(self.now(), FOLLOW_FIRST_DELAY);
        [
            store::update_ticket_tracker_state_stmt(&ticket.id, TicketState::Open, at),
            store::enqueue_stage_stmt(
                &self.outbox,
                &follow_job,
                &ticket.id,
                &ticket.tenant_id,
                Stage::Follow,
                None,
                at,
            ),
            store::outbox_reschedule_stmt(OUTBOX_TABLE, &follow_job, &follow_first_at),
        ]
    }

    /// The file stage's decision and, for an external route, its one port
    /// call. The route is resolved first (issue #24): a `(tenant, kind)`
    /// row from `sg_routes` wins, a defect falls back to the legacy
    /// `sg_destinations` row, and a kind with no route at all files
    /// locally — never a dead-letter, so an unconfigured tenant still
    /// files. A drafted-title/body/severity gap is still a terminal decode
    /// error (dead-letter). A webhook's filed URL is blanked: it is the
    /// destination's own secret.
    async fn file_ticket(&self, ticket: &Ticket) -> Result<FileOutcome, Error> {
        let title = ticket
            .title
            .clone()
            .ok_or_else(|| Error::Decode(format!("ticket `{}` has no drafted title", ticket.id)))?;
        let body = ticket
            .body_markdown
            .clone()
            .ok_or_else(|| Error::Decode(format!("ticket `{}` has no drafted body", ticket.id)))?;
        let severity = ticket.severity.ok_or_else(|| {
            Error::Decode(format!("ticket `{}` has no drafted severity", ticket.id))
        })?;
        let Some(route) = self.resolve_route(&ticket.tenant_id, ticket.kind).await? else {
            return Ok(FileOutcome::Local);
        };
        let store::RouteTarget::Tracker(destination) = route.target else {
            return Ok(FileOutcome::Local);
        };
        let (destination, credential) = self
            .resolve_credential(&ticket.tenant_id, destination, &route.credential_ref)
            .await?;
        let idempotency_key = format!("escalation:{}", ticket.id);
        let kind = destination.kind();
        let mut body = with_conversation_link(body, &*self.config, &ticket.conversation_id);
        let mut labels = escalation_labels(ticket.kind, severity, &route.priority);
        // Who owns this topic, asked of the module's router when one is
        // wired (issue #64). A target becomes an `owner:<target>` label and
        // a body line; with no router, no HTTP port, or no owner, the
        // route's destination is the routing, exactly as before. `topic` is
        // the drafted title, scrubbed of emails, tokens and links.
        if let Some(router) = self.router.as_deref()
            && let Some(owner) = router
                .route(
                    &*self.config,
                    self.http.as_deref(),
                    &ticket.tenant_id,
                    &scrub_text(&title),
                )
                .await
        {
            labels.push(format!("owner:{owner}"));
            body = with_owner(&body, &owner);
        }
        let mut draft = TicketDraft::new(idempotency_key, title, body, severity).labels(labels);
        if let Some(environment) = &ticket.environment {
            draft = draft.environment(environment.clone());
        }
        let filed = self.tracker.file(&destination, &credential, &draft).await?;
        // A webhook reports the URL it was POSTed to as `Filed::url`, and
        // that URL is the secret this module stores encrypted. Drop it,
        // before it can reach the ticket row, the audit detail or the
        // customer's notification.
        let filed = if matches!(destination, Destination::Webhook { .. }) {
            Filed {
                url: String::new(),
                ..filed
            }
        } else {
            filed
        };
        Ok(FileOutcome::Tracker(filed, kind))
    }

    /// Resolves where a ticket of `kind` files, in this order: a
    /// `(tenant, kind)` row from `sg_routes`; for a defect only, the legacy
    /// `sg_destinations` row (which predates kinds and carried every
    /// escalation); otherwise no route, which files locally. A route's
    /// `credential_ref` is never resolved here — only at file-time, so a
    /// secret is read for exactly one call.
    async fn resolve_route(
        &self,
        tenant_id: &str,
        kind: Kind,
    ) -> Result<Option<store::Route>, Error> {
        if let Some(route) = store::load_route(&*self.db, tenant_id, kind).await? {
            return Ok(Some(route));
        }
        if kind == Kind::Defect
            && let Some((destination, credential_ref)) =
                store::load_destination(&*self.db, tenant_id).await?
        {
            return Ok(Some(store::Route {
                target: store::RouteTarget::Tracker(destination),
                credential_ref,
                priority: BTreeMap::new(),
            }));
        }
        Ok(None)
    }

    /// Resolves the tenant's credential and destination from the stored
    /// `credential_ref`.
    ///
    /// Two forms. A `secret:` reference (issue #23) names a value in the
    /// tenant's encrypted `cratefield-secrets` store: the KMS comes from
    /// config, the store opens on the same database, and the credential —
    /// and, for a webhook, the URL, which is itself the secret — is read
    /// for this one call and dropped. A bare reference is the older
    /// Config-key form, resolved from the Config port.
    ///
    /// Every failure here is a terminal [`Error::Decode`] (dead-letter),
    /// and none of them names a secret value: a missing KMS, an
    /// unreadable store or an unset secret are all dead-ends a retry
    /// cannot heal.
    async fn resolve_credential(
        &self,
        tenant_id: &str,
        destination: Destination,
        credential_ref: &str,
    ) -> Result<(Destination, Credential), Error> {
        let Some(name) = credential_ref.strip_prefix(crate::secrets::SECRET_REF_PREFIX) else {
            // Legacy: the ref names a Config key, never the secret.
            let Some(secret) = self.config.get(credential_ref) else {
                return Err(Error::Decode(format!(
                    "config key `{credential_ref}` (tenant `{tenant_id}`) is not set"
                )));
            };
            return Ok((destination, Credential::new(secret)));
        };

        let kms = crate::secrets::kms_from_config(&*self.config).ok_or_else(|| {
            Error::Decode(format!(
                "tenant `{tenant_id}` stores its tracker credential encrypted, but no KMS is \
                 configured"
            ))
        })?;
        let store = crate::secrets::audited_secrets(kms, self.db.clone())
            .tenant(tenant_id, self.db.clone())
            .map_err(|_| {
                Error::Decode(format!("tenant `{tenant_id}` has no usable secret store"))
            })?;
        let actor = Actor::new("escalation.pipeline")
            .map_err(|_| Error::Decode("the pipeline actor name is empty".to_owned()))?;
        let secret = Self::read_secret(&store, &actor, name, tenant_id).await?;
        let credential = Credential::new(
            secret
                .expose_str()
                .map_err(|_| {
                    Error::Decode(format!(
                        "secret `{name}` (tenant `{tenant_id}`) is not valid UTF-8"
                    ))
                })?
                .to_owned(),
        );

        // A webhook URL is credential material; the stored destination
        // keeps a marker, and the real URL is read back here.
        let destination = match destination {
            Destination::Webhook { url } => {
                match url.strip_prefix(crate::secrets::SECRET_REF_PREFIX) {
                    Some(url_name) => {
                        let real = Self::read_secret(&store, &actor, url_name, tenant_id).await?;
                        let url = real
                            .expose_str()
                            .map_err(|_| {
                                Error::Decode(format!(
                                    "webhook URL secret `{url_name}` (tenant `{tenant_id}`) is not \
                                 valid UTF-8"
                                ))
                            })?
                            .to_owned();
                        Destination::Webhook { url }
                    }
                    None => Destination::Webhook { url },
                }
            }
            other => other,
        };
        Ok((destination, credential))
    }

    /// Reads one named secret from the tenant store. "Not found" and
    /// "unreadable" are the same terminal decode error — neither heals on
    /// retry, and neither names a value — except a database failure
    /// (including the audit chain's append), which retries.
    async fn read_secret(
        store: &cratefield_secrets::SecretStore,
        actor: &Actor,
        name: &str,
        tenant_id: &str,
    ) -> Result<cratefield_secrets::SecretBytes, Error> {
        store
            .get(name, actor)
            .await
            .map_err(|err| match err {
                // A database failure — the secret row's read or the audit
                // chain's append — is transient like any other `Db` error:
                // the outbox redelivers rather than dead-lettering a
                // ticket over a blip.
                cratefield_secrets::SecretsError::Database(db) => Error::Db(db),
                _ => Error::Decode(format!(
                    "secret `{name}` (tenant `{tenant_id}`) could not be read"
                )),
            })?
            .ok_or_else(|| {
                Error::Decode(format!("secret `{name}` (tenant `{tenant_id}`) is not set"))
            })
    }

    // ------------------------------------------------------------------
    // Stage: notify

    /// The customer-facing update, terminal by construction: it enqueues
    /// nothing, and its batch is the event plus the row's completion. The
    /// recipient is a database read of the contact `module-support` stored
    /// in `sg_contacts`, never a logged address: no contact is
    /// `no_recipient`, no `Mailer` is `no_mailer`, and the address reaches
    /// the send call and nowhere else.
    ///
    /// Two shapes: the ordinary notify (no `event_id`) announces the filing
    /// or the `NeedsInfo` question; a status-update notify (`event_id` set)
    /// announces a tracker state change the follow stage found, composed
    /// from the `status_changed` event's `to` state and keyed by the event
    /// id, so a redelivery collapses instead of mailing twice.
    async fn run_notify(
        &self,
        record: &OutboxRecord,
        ticket: &Ticket,
        event_id: Option<&str>,
        at: &str,
    ) -> Result<(), Error> {
        // The message and its idempotency key depend on which shape this
        // is. A status update needs the event's `to` state, a database
        // read; the ordinary notices never do.
        let (subject, message, idempotency_key) = if let Some(event_id) = event_id {
            let to = self.status_update_state(event_id).await?;
            let (subject, message) = compose_status_update_message(ticket, to);
            (subject, message, format!("escalation:{event_id}"))
        } else {
            let (subject, message) = compose_notify_message(ticket);
            (subject, message, format!("escalation:{}:notify", ticket.id))
        };

        // The recipient, from the contact `module-support` stored for this
        // conversation. Absent in a venture that composes escalation alone,
        // which is exactly the `no_recipient` skip.
        let recipient =
            store::contact_email(&*self.db, &ticket.tenant_id, &ticket.conversation_id).await?;
        let event = match (&self.mailer, recipient) {
            (Some(mailer), Some(to)) => {
                // The one send call. The notification is plain text; the
                // same text goes out as both parts. The key makes a
                // redelivered update a no-op at the provider.
                let outbound = Message::new(
                    to.as_str(),
                    self.notify_from(),
                    subject.as_str(),
                    message.as_str(),
                    message.as_str(),
                )
                .idempotency_key(idempotency_key);
                match mailer.send(outbound).await {
                    Ok(SendOutcome::Sent { id }) => {
                        // `to` (the address) is deliberately not recorded.
                        let detail = json!({
                            "subject": subject,
                            "message": message,
                            "provider_id": id,
                        });
                        self.notify_event(
                            ticket,
                            at,
                            EventKind::Notified,
                            &with_event_id(detail, event_id),
                        )
                    }
                    Ok(SendOutcome::NotConfigured) => {
                        let detail = json!({
                            "reason": "mailer_not_configured",
                            "subject": subject,
                            "message": message,
                        });
                        self.notify_event(
                            ticket,
                            at,
                            EventKind::NotifySkipped,
                            &with_event_id(detail, event_id),
                        )
                    }
                    Err(err) => {
                        return self
                            .fail(
                                record,
                                Stage::Notify,
                                &ticket.id,
                                &ticket.tenant_id,
                                event_id,
                                Error::from(err),
                                at,
                            )
                            .await;
                    }
                }
            }
            (None, _) => {
                let detail = json!({
                    "reason": "no_mailer",
                    "subject": subject,
                    "message": message,
                });
                self.notify_event(
                    ticket,
                    at,
                    EventKind::NotifySkipped,
                    &with_event_id(detail, event_id),
                )
            }
            (_, None) => {
                let detail = json!({
                    "reason": "no_recipient",
                    "subject": subject,
                    "message": message,
                });
                self.notify_event(
                    ticket,
                    at,
                    EventKind::NotifySkipped,
                    &with_event_id(detail, event_id),
                )
            }
        };
        self.commit(&[event, store::outbox_complete_stmt(OUTBOX_TABLE, &record.id)])
            .await
    }

    /// The tracker state a `status_changed` event announced, read back from
    /// the event's `{"from", "to"}` detail. A missing event or an
    /// unreadable `to` degrades to [`TicketState::Unknown`], which the
    /// status-update message handles with its generic wording — the notify
    /// never fails over a detail it cannot parse.
    async fn status_update_state(&self, event_id: &str) -> Result<TicketState, Error> {
        let Some(event) = store::load_event(&*self.db, event_id).await? else {
            return Ok(TicketState::Unknown);
        };
        let to = event
            .detail
            .as_ref()
            .and_then(|detail| detail.get("to"))
            .and_then(Value::as_str);
        to.map_or(Ok(TicketState::Unknown), store::ticket_state_from)
    }

    fn notify_event(
        &self,
        ticket: &Ticket,
        at: &str,
        kind: EventKind,
        detail: &Value,
    ) -> Statement {
        store::insert_event_stmt(
            &self.idgen.ulid(),
            &ticket.id,
            stage_seq(Stage::Notify, 0),
            at,
            Stage::Notify,
            kind,
            detail,
        )
    }

    /// The sending address. A deployment sets `ESCALATION_NOTIFY_FROM`;
    /// the fallback exists only so the (currently unreachable) send path
    /// is total. It is the *sender*, never a fabricated recipient.
    fn notify_from(&self) -> String {
        self.config
            .get("ESCALATION_NOTIFY_FROM")
            .unwrap_or_else(|| "supportgenius@localhost".to_owned())
    }

    // ------------------------------------------------------------------
    // Stage: follow

    /// One tracker poll. Unlike every other stage this one does **not**
    /// complete on success: it reschedules its own outbox row and runs
    /// again, so a filed ticket is watched until it closes. It makes exactly
    /// one `Tracker::status` call per run, under the same destination and
    /// credential the file stage used, and never dead-letters — a ticket
    /// that cannot be polled is not a ticket that needs a human.
    ///
    /// - `Unknown`, or the same state last recorded: reschedule, write
    ///   nothing.
    /// - A new state: one atomic batch writes a `status_changed` audit row,
    ///   remembers the new state and enqueues the status-update notify, then
    ///   either reschedules (still moving) or completes the row at `Closed`.
    /// - Any port or resolution failure: logged (never with the address or
    ///   the credential) and rescheduled.
    async fn run_follow(
        &self,
        record: &OutboxRecord,
        ticket: &Ticket,
        at: &str,
    ) -> Result<(), Error> {
        let now = self.now();

        // Nothing was filed, so there is nothing to poll. This cannot
        // happen for a row the file stage enqueued (it records the external
        // id first), but a defensive complete beats polling forever.
        let Some(external_id) = ticket.external_id.clone() else {
            return self.complete_row(&record.id).await;
        };

        let status = match self.poll_status(ticket, &external_id).await {
            Ok(status) => status,
            Err(err) => {
                tracing::warn!(
                    ticket_id = %ticket.id,
                    attempts = record.attempts,
                    error = %err,
                    "escalation: follow-up poll failed; rescheduling"
                );
                return self.reschedule_follow(record, now, err.retry_after()).await;
            }
        };

        // A state this port cannot name, or the same one last recorded:
        // no transition to report, no event, no notify. Reschedule.
        if status.state == TicketState::Unknown || Some(status.state) == ticket.tracker_state {
            return self.reschedule_follow(record, now, None).await;
        }

        // A real transition. Guard it with a permanent inbox key in the
        // *same batch*, numbered by the ticket's own transition count: a
        // concurrent poll that saw this change computes the same `n`, so
        // its claim insert conflicts and its whole batch rolls back — no
        // second `status_changed`, no second mail. The claim rides the
        // batch rather than preceding it so a crash here leaves no key
        // behind: a later poll still records the transition.
        let n = store::status_changed_count(&*self.db, &ticket.id).await?;
        let from = ticket
            .tracker_state
            .map_or_else(|| "unknown".to_owned(), store::ticket_state_text);
        let detail = json!({ "from": from, "to": store::ticket_state_text(status.state) });
        let event_id = self.idgen.ulid();
        let mut batch = vec![
            self.inbox
                .claim_statement(&format!("{}:follow:{n}", ticket.id), at),
            store::insert_event_stmt(
                &event_id,
                &ticket.id,
                stage_seq(Stage::Follow, 0),
                at,
                Stage::Follow,
                EventKind::StatusChanged,
                &detail,
            ),
            store::update_ticket_tracker_state_stmt(&ticket.id, status.state, at),
            store::enqueue_stage_stmt(
                &self.outbox,
                &self.idgen.ulid(),
                &ticket.id,
                &ticket.tenant_id,
                Stage::Notify,
                Some(&event_id),
                at,
            ),
        ];
        if status.state == TicketState::Closed {
            batch.push(store::outbox_complete_stmt(OUTBOX_TABLE, &record.id));
        } else {
            let next_at = self.follow_next_at(record, now, None).await?;
            batch.push(store::outbox_reschedule_stmt(
                OUTBOX_TABLE,
                &record.id,
                &next_at,
            ));
        }
        // A conflicting claim — a concurrent poll won this transition — or
        // any other batch failure rolls the whole batch back, ours included,
        // so this run writes nothing and queues the next poll. A real
        // database outage fails that reschedule too and propagates.
        if self.commit(&batch).await.is_err() {
            return self.reschedule_follow(record, now, None).await;
        }
        self.defer_next();
        Ok(())
    }

    /// The follow stage's one port call: resolve the tenant's destination
    /// and per-call credential exactly as [`Pipeline::file_ticket`] does,
    /// then ask the tracker for the ticket's state. The port caps the call
    /// at 30 s.
    async fn poll_status(&self, ticket: &Ticket, external_id: &str) -> Result<TicketStatus, Error> {
        let route = self.resolve_route(&ticket.tenant_id, ticket.kind).await?;
        let Some(store::Route {
            target: store::RouteTarget::Tracker(destination),
            credential_ref,
            ..
        }) = route
        else {
            return Err(Error::Decode(format!(
                "tenant `{}` has no tracker route for `{}` tickets",
                ticket.tenant_id,
                ticket.kind.as_str()
            )));
        };
        let (destination, credential) = self
            .resolve_credential(&ticket.tenant_id, destination, &credential_ref)
            .await?;
        self.tracker
            .status(&destination, &credential, external_id)
            .await
            .map_err(Error::from)
    }

    /// Reschedules the follow row (ageing the wait, honouring a tracker's
    /// `retry_after` when it asks for a later one) and commits it. A poll
    /// that found nothing to say and a poll that failed both land here.
    async fn reschedule_follow(
        &self,
        record: &OutboxRecord,
        now: OffsetDateTime,
        retry_after: Option<Duration>,
    ) -> Result<(), Error> {
        let next_at = self.follow_next_at(record, now, retry_after).await?;
        self.commit(&[store::outbox_reschedule_stmt(
            OUTBOX_TABLE,
            &record.id,
            &next_at,
        )])
        .await
    }

    /// When the next follow poll is due. Within the first day a poll runs
    /// hourly; after that, daily — a settled ticket should not cost a call
    /// an hour. A tracker's own `retry_after`, when it names a longer wait,
    /// wins: no point asking again before it said to.
    async fn follow_next_at(
        &self,
        record: &OutboxRecord,
        now: OffsetDateTime,
        retry_after: Option<Duration>,
    ) -> Result<String, Error> {
        let created = store::outbox_created_at(&*self.db, OUTBOX_TABLE, &record.id).await?;
        let age = created
            .as_deref()
            .and_then(|raw| OffsetDateTime::parse(raw, &Rfc3339).ok())
            .map_or(Duration::ZERO, |created| {
                Duration::try_from(now - created).unwrap_or(Duration::ZERO)
            });
        let base = if age < FOLLOW_SHORT_WINDOW {
            FOLLOW_SHORT_INTERVAL
        } else {
            FOLLOW_LONG_INTERVAL
        };
        let wait = retry_after.map_or(base, |hint| hint.max(base));
        Ok(rfc3339_after(now, wait))
    }

    // ------------------------------------------------------------------
    // Failure handling

    /// Records a stage failure. Retryable failures inside the attempt
    /// budget reschedule; everything else — a terminal failure, or a
    /// retryable failure that has spent the [`RetryPolicy`] budget —
    /// dead-letters. Either way: one event row carrying the reason, and
    /// one batch. `event_id` names the work when it is a status update
    /// (see [`Pipeline::run_notify`]), so the released claim is the one
    /// that attempt took, and a terminal failure of one completes the row
    /// without parking the still-moving ticket; it is `None` for every
    /// other stage.
    #[allow(clippy::too_many_arguments)] // one flat call for every failure site; each argument is distinct
    async fn fail(
        &self,
        record: &OutboxRecord,
        stage: Stage,
        ticket_id: &str,
        tenant_id: &str,
        event_id: Option<&str>,
        err: Error,
        at: &str,
    ) -> Result<(), Error> {
        let attempts_used = record.attempts + 1;
        let reason = err.to_string();

        if err.is_retryable() && !self.policy.exhausted(attempts_used) {
            let next_at = rfc3339(self.policy.next_attempt_at(
                record.attempts,
                self.now(),
                err.retry_after(),
            ));
            let detail = json!({
                "outcome": "retry_scheduled",
                "attempt": attempts_used,
                "reason": reason,
                "retry_at": next_at,
            });
            let event = store::insert_event_stmt(
                &self.idgen.ulid(),
                ticket_id,
                stage_seq(stage, 0),
                at,
                stage,
                retry_kind(stage),
                &detail,
            );
            // The claim release rides the retry batch. Released alone, a
            // crash before the retry lands wedges the stage forever (the
            // key is held, the work never re-runs); retried alone, the
            // re-run would find the key held and the ticket unadvanced and
            // walk straight through the crash-window branch — the claim
            // would be guarding nothing.
            let batch = [
                event,
                store::inbox_release_stmt(INBOX_TABLE, &claim_key_with(ticket_id, stage, event_id)),
                store::outbox_retry_later_stmt(OUTBOX_TABLE, &record.id, &next_at),
            ];
            self.commit(&batch).await
        } else {
            let detail = json!({
                "outcome": "dead_letter",
                "attempt": attempts_used,
                "reason": reason,
            });
            let event = store::insert_event_stmt(
                &self.idgen.ulid(),
                ticket_id,
                stage_seq(stage, 0),
                at,
                stage,
                dead_letter_kind(stage),
                &detail,
            );
            // Terminal. A status update (`event_id` set) is about a ticket
            // that is already filed and still moving: its failure stops at
            // the audit row and the row's completion, and never parks the
            // ticket. Anything else dead-letters — the ticket parks for a
            // human (a no-op when the row is already gone) and the outbox
            // row completes so it stops. Never `retry_later` a terminal
            // failure.
            let mut batch = vec![event, store::outbox_complete_stmt(OUTBOX_TABLE, &record.id)];
            if event_id.is_none() {
                batch.push(store::update_ticket_status_stmt(
                    ticket_id,
                    Status::DeadLetter,
                    at,
                ));
                let payload = json!({
                    "ticket_id": ticket_id,
                    "tenant_id": tenant_id,
                    "stage": stage.as_topic(),
                    "reason": err.to_string(),
                });
                // The file stage has its own dead-letter event; every
                // dead-letter — this one included — also parks the ticket
                // for a human, so it publishes `needs_human` too.
                if stage == Stage::File {
                    batch.extend(
                        self.webhook_stmts(
                            tenant_id,
                            webhook_events::ESCALATION_DEAD_LETTERED,
                            &payload,
                            at,
                        )
                        .await?,
                    );
                }
                batch.extend(
                    self.webhook_stmts(
                        tenant_id,
                        webhook_events::ESCALATION_NEEDS_HUMAN,
                        &payload,
                        at,
                    )
                    .await?,
                );
            }
            self.commit(&batch).await
        }
    }

    /// The `escalation.*` webhook fan-out for one outcome: the returned
    /// statements go into the **same** [`Database::batch_atomic`] as the
    /// outcome's audit row, so a subscriber hears about an outcome exactly
    /// when it committed. Empty when no webhooks module is wired
    /// ([`Pipeline::with_webhooks`]) or when no endpoint of the tenant
    /// matches. The publish is a database read (the tenant's endpoints),
    /// not an outbound port call — delivery happens later, in the webhooks
    /// module's own drain.
    ///
    /// **Fail-safe by design.** A publish read failure — above all a venture
    /// that mounts this module without the `Webhooks` module, so its tables
    /// do not exist — must not break filing, which is the module's job;
    /// the notification is not. So a database failure skips the fan-out and
    /// warns. When `Webhooks` *is* composed the only way to reach this arm
    /// is a database broken worse than the batch's own commit below, which
    /// then fails the stage and retries it — the event is not lost.
    /// [`PublishError::InvalidEventType`] cannot happen (our event-type
    /// constants are header-safe), so it stays terminal if it ever does.
    async fn webhook_stmts(
        &self,
        tenant_id: &str,
        event_type: &str,
        data: &Value,
        at: &str,
    ) -> Result<Vec<Statement>, Error> {
        let Some(webhooks) = &self.webhooks else {
            return Ok(Vec::new());
        };
        match webhooks
            .publish(&*self.db, tenant_id, event_type, data, at)
            .await
        {
            Ok(published) => Ok(published.into_statements()),
            Err(PublishError::InvalidEventType(event_type)) => Err(Error::Decode(format!(
                "webhooks refused the event type `{event_type}`"
            ))),
            Err(PublishError::Database(err)) => {
                tracing::warn!(
                    %err,
                    tenant_id,
                    event_type,
                    "escalation: webhook fan-out skipped (is `Webhooks` composed in this venture?)"
                );
                Ok(Vec::new())
            }
        }
    }

    /// A topic this module does not know: it can never be routed, so it
    /// must not sit in the queue re-leasing forever. Where the payload
    /// still identifies the ticket, park it and leave one dead-letter
    /// audit row in the trail it belongs to; otherwise just retire the
    /// row.
    async fn retire_unknown_topic(&self, record: &OutboxRecord, at: &str) -> Result<(), Error> {
        let mut batch = Vec::new();
        if let Ok(payload) = serde_json::from_str::<StagePayload>(&record.payload) {
            // Best effort on purpose: a read failure here should not
            // resurrect a row that cannot be routed anyway.
            let ticket = store::load_ticket(&*self.db, &payload.ticket_id)
                .await
                .ok()
                .flatten();
            let stage = ticket.as_ref().map_or(Stage::Draft, |t| t.stage);
            let detail = json!({
                "outcome": "dead_letter",
                "reason": format!("unknown outbox topic `{}`", record.topic),
            });
            batch.push(store::insert_event_stmt(
                &self.idgen.ulid(),
                &payload.ticket_id,
                stage_seq(stage, 0),
                at,
                stage,
                dead_letter_kind(stage),
                &detail,
            ));
            if ticket.is_some() {
                batch.push(store::update_ticket_status_stmt(
                    &payload.ticket_id,
                    Status::DeadLetter,
                    at,
                ));
                // A ticket parked for a human is exactly what
                // `needs_human` means; it joins the same batch.
                batch.extend(
                    self.webhook_stmts(
                        &payload.tenant_id,
                        webhook_events::ESCALATION_NEEDS_HUMAN,
                        &json!({
                            "ticket_id": payload.ticket_id,
                            "tenant_id": payload.tenant_id,
                            "stage": stage.as_topic(),
                            "reason": format!("unknown outbox topic `{}`", record.topic),
                        }),
                        at,
                    )
                    .await?,
                );
            }
        }
        batch.push(store::outbox_complete_stmt(OUTBOX_TABLE, &record.id));
        self.commit(&batch).await
    }

    /// Retires one claimed row: the plain `DELETE` a duplicate or
    /// undecodable row gets, with nothing else to say about it.
    async fn complete_row(&self, record_id: &str) -> Result<(), Error> {
        self.commit(&[store::outbox_complete_stmt(OUTBOX_TABLE, record_id)])
            .await
    }

    /// The one all-or-nothing commit every stage outcome funnels through.
    async fn commit(&self, batch: &[Statement]) -> Result<(), Error> {
        self.db.batch_atomic(batch).await.map_err(Error::from)
    }

    /// Hands a clone of this pipeline to the `Defer` port so the stage
    /// whose row was just enqueued runs immediately instead of waiting for
    /// the next drain. The future must be `'static`, so the clone moves in
    /// and owns its `Arc`s. Failures are ignored on purpose: the deferred
    /// run is an execution *opportunity* — the enqueued row is already
    /// durable, and the scheduled drain is the backstop that guarantees it
    /// runs.
    fn defer_next(&self) {
        let Some(defer) = &self.defer else {
            return;
        };
        let pipeline = self.clone();
        defer.wait_until(Box::pin(async move {
            let _ = pipeline.drain(1).await;
        }));
    }

    fn now(&self) -> OffsetDateTime {
        self.clock.now()
    }
}

// ---------------------------------------------------------------------------
// Pure helpers

/// The inbox claim key the issue specifies: `ticket_id:stage`.
fn claim_key(ticket_id: &str, stage: Stage) -> String {
    format!("{ticket_id}:{}", stage.as_topic())
}

/// The claim key for a stage, widened with an event id when one names the
/// work. A status-update notify (`stage` = `notify`, `event_id` = the
/// `status_changed` event it announces) must not share the one
/// `ticket:notify` key with the filing notice, so its key carries the
/// event id: `ticket:notify:<event_id>`. With no event id this is exactly
/// [`claim_key`].
fn claim_key_with(ticket_id: &str, stage: Stage, event_id: Option<&str>) -> String {
    match event_id {
        Some(event_id) => format!("{}:{event_id}", claim_key(ticket_id, stage)),
        None => claim_key(ticket_id, stage),
    }
}

/// Whether a ticket's recorded progress shows `stage` already committed.
/// A stage commits by moving the ticket to a later stage or to a terminal
/// status (its commit batch always does one of the two), so progress is
/// always visible on the row. `NeedsInfo` counts as terminal: the ticket
/// is parked waiting on the customer, not waiting on this pipeline.
fn stage_already_committed(ticket: &Ticket, stage: Stage) -> bool {
    ticket.stage.ordinal() > stage.ordinal()
        || matches!(
            ticket.status,
            Status::Filed
                | Status::Rejected
                | Status::NeedsInfo
                | Status::Duplicate
                | Status::DeadLetter
                | Status::Closed
        )
}

/// Decodes a stage's structured answer: [`Completion::json`] when the
/// provider honoured the schema, otherwise the text parsed as JSON — the
/// port's documented fallback for a provider that cannot do structured
/// output. Text that does not parse is a terminal [`Error::Decode`], not a
/// retry: the bytes came back, and asking again will not change them.
fn decode_completion<T: serde::de::DeserializeOwned>(completion: &Completion) -> Result<T, Error> {
    let value = match &completion.json {
        Some(value) => value.clone(),
        None => serde_json::from_str::<Value>(&completion.text).map_err(Error::from)?,
    };
    serde_json::from_value(value)
        .map_err(|err| Error::Decode(format!("completion JSON did not match the schema: {err}")))
}

/// The `file` stage's audit event for a retryable failure.
fn retry_kind(stage: Stage) -> EventKind {
    match stage {
        Stage::File => EventKind::FileRetryScheduled,
        _ => failure_kind(stage),
    }
}

/// The audit event for a stage that has stopped for good. Only the file
/// stage has a dedicated dead-letter kind; the others use their failure
/// kind, with the outcome spelled out in the detail.
fn dead_letter_kind(stage: Stage) -> EventKind {
    match stage {
        Stage::File => EventKind::FileDeadLettered,
        _ => failure_kind(stage),
    }
}

fn failure_kind(stage: Stage) -> EventKind {
    match stage {
        Stage::Draft => EventKind::DraftFailed,
        Stage::Judge => EventKind::JudgeFailed,
        Stage::File => EventKind::FileFailed,
        Stage::Notify => EventKind::NotifyFailed,
        Stage::Follow => EventKind::FollowFailed,
    }
}

/// The labels every filed escalation carries: a kind marker and the
/// drafted severity as `severity:<level>` (`severity:error`), so a
/// tracker's label filter can find escalations and rank them without
/// parsing the body. A defect keeps its long-standing `bug` marker; other
/// kinds carry `kind:<kind>`. When the route names a priority for this
/// severity it rides along as `priority:<value>`, the only shape core's
/// [`TicketDraft`] accepts (it has no priority field of its own).
fn escalation_labels(
    kind: Kind,
    severity: Severity,
    priority: &BTreeMap<String, String>,
) -> Vec<String> {
    let marker = match kind {
        Kind::Defect => "bug".to_owned(),
        other => format!("kind:{}", other.as_str()),
    };
    let mut labels = vec![marker, format!("severity:{}", severity.name())];
    if let Some(value) = priority.get(severity.name()) {
        labels.push(format!("priority:{value}"));
    }
    labels
}

/// Appends the owning target an [`OwnerRouter`] named to the filed ticket's
/// body, so the engineer who reads it sees who the ticket belongs to even
/// where the tracker drops the `owner:` label. The label is the machine-
/// readable half; this line is the human one.
fn with_owner(body: &str, owner: &str) -> String {
    format!("{body}\n\n---\n\nOwner (from Living Brain): {owner}\n")
}

/// Appends a link back to the support conversation the ticket came from,
/// when the deployment names a conversation base (`ESCALATION_CONVERSATION_URL`):
/// `<base>/<conversation_id>`. With no base configured the body is returned
/// unchanged — a link is only added when there is somewhere to point.
fn with_conversation_link(body: String, config: &dyn Config, conversation_id: &str) -> String {
    let Some(base) = config.get(CONVERSATION_URL_KEY) else {
        return body;
    };
    let base = base.trim_end_matches('/');
    format!("{body}\n\n---\n\nEscalated from the support conversation: {base}/{conversation_id}\n")
}

/// The ticket body the draft stage renders, per kind. A defect is the
/// reproduction steps as a numbered list, then expected/actual sections —
/// exactly the fields the schema guarantees, in an order an engineer can
/// act on. A support case is a summary and the customer's ask; a lead is
/// the company, the seat count and the intent — each rendered from the
/// fields that kind's schema requires.
#[must_use]
pub(crate) fn render_body_markdown(drafted: &Drafted) -> String {
    use std::fmt::Write as _;

    match drafted.kind {
        Kind::Defect => {
            let mut body = String::from("## Repro steps\n");
            for (index, step) in drafted.repro_steps.iter().enumerate() {
                let _ = writeln!(body, "{}. {step}", index + 1);
            }
            body.push_str("\n## Expected\n");
            let _ = writeln!(body, "{}", drafted.expected.trim());
            body.push_str("\n## Actual\n");
            let _ = writeln!(body, "{}", drafted.actual.trim());
            body
        }
        Kind::SupportCase => {
            let mut body = String::from("## Summary\n");
            let _ = writeln!(body, "{}", drafted.summary.as_deref().unwrap_or("").trim());
            body.push_str("\n## Customer ask\n");
            let _ = writeln!(
                body,
                "{}",
                drafted.customer_ask.as_deref().unwrap_or("").trim()
            );
            body
        }
        Kind::Lead => {
            let mut body = String::from("## Company\n");
            let _ = writeln!(body, "{}", drafted.company.as_deref().unwrap_or("").trim());
            body.push_str("\n## Seats\n");
            if let Some(seats) = drafted.seats {
                let _ = writeln!(body, "{seats}");
            } else {
                body.push('\n');
            }
            body.push_str("\n## Intent\n");
            let _ = writeln!(body, "{}", drafted.intent.as_deref().unwrap_or("").trim());
            body
        }
    }
}

/// The question parked against a draft that is missing a field its kind
/// requires (see [`Drafted::missing_fields`]): one bullet per field, named
/// in plain words, so the customer can supply what the drafter could not
/// read out of the conversation.
#[must_use]
pub(crate) fn compose_missing_fields_question(kind: Kind, missing: &[&str]) -> String {
    use std::fmt::Write as _;

    let mut question = format!(
        "Before we pass this on, the {} is missing some detail. Could you provide:\n",
        kind.as_str()
    );
    for field in missing {
        let _ = writeln!(question, "- {}", field.replace('_', " "));
    }
    question
}

/// The customer-facing question the judge's `NeedsInfo` verdict asks, from
/// the judge's own reasons — the audit carries them anyway, and a question
/// that quotes them is one the customer can actually answer.
#[must_use]
pub(crate) fn compose_needs_info_question(reasons: &[String]) -> String {
    use std::fmt::Write as _;

    let mut question =
        String::from("Before we pass this to engineering, could you help us with the following?\n");
    for reason in reasons {
        let _ = writeln!(question, "- {reason}");
    }
    question
}

/// The notify stage's message, from the ticket alone: a filed ticket
/// points at its tracker reference and link, a `NeedsInfo` ticket carries
/// the stored question.
#[must_use]
pub(crate) fn compose_notify_message(ticket: &Ticket) -> (String, String) {
    let title = ticket.title.as_deref().unwrap_or("your support request");
    match ticket.status {
        Status::Filed => {
            let subject = format!("Update on your support request: {title}");
            // The ticket is escalated to the team that owns its kind: a
            // defect to engineering, a support case to the support team, a
            // lead to the sales team (issue #24).
            let team = match ticket.kind {
                Kind::Defect => "engineering",
                Kind::SupportCase => "the support team",
                Kind::Lead => "the sales team",
            };
            let message = match (&ticket.external_id, &ticket.external_url) {
                (Some(id), Some(url)) => format!(
                    "We escalated this to {team} and it was accepted as {id}.\n\nTracker link: {url}\n\nWe will follow up here when there is news."
                ),
                (Some(id), None) => format!(
                    "We escalated this to {team} and it was accepted as {id}.\n\nWe will follow up here when there is news."
                ),
                _ => format!(
                    "We escalated this to {team} and will follow up here when there is news."
                ),
            };
            (subject, message)
        }
        Status::Duplicate => {
            // Linking only ever targets a candidate, and a candidate is a
            // `status = 'filed'` ticket (see `store::candidate_tickets`),
            // so the existing ticket is filed and that is the status the
            // customer is shown, alongside the same reference and link a
            // filing would have given them.
            let subject = format!("Update on your support request: {title}");
            let message = match (&ticket.external_id, &ticket.external_url) {
                (Some(id), Some(url)) => format!(
                    "This is already tracked by our engineering team as {id} (status: {}).\n\nTracker link: {url}\n\nWe will follow up here when there is news.",
                    Status::Filed.as_str()
                ),
                (Some(id), None) => format!(
                    "This is already tracked by our engineering team as {id} (status: {}).\n\nWe will follow up here when there is news.",
                    Status::Filed.as_str()
                ),
                _ => "This is already tracked by our engineering team; we will follow up here \
                      when there is news."
                    .to_owned(),
            };
            (subject, message)
        }
        Status::NeedsInfo => (
            "We need a little more information".to_owned(),
            ticket
                .customer_question
                .clone()
                .unwrap_or_else(|| "Could you add more detail about what went wrong?".to_owned()),
        ),
        _ => (
            format!("Update on your support request: {title}"),
            "There is an update on your support request; a support engineer will follow up with \
             details."
                .to_owned(),
        ),
    }
}

/// The status-update notify's message, phrased from the state a
/// `status_changed` event announced: the ticket title and, in plain words,
/// what the new state means for the customer. The `Unknown` state — a
/// detail that would not parse, or a state this port cannot name — still
/// says something honest rather than nothing.
#[must_use]
pub(crate) fn compose_status_update_message(ticket: &Ticket, to: TicketState) -> (String, String) {
    use std::fmt::Write as _;

    let title = ticket.title.as_deref().unwrap_or("your report");
    let subject = format!("Update on your report: {title}");
    let state = match to {
        TicketState::Open => "is open with our engineering team",
        TicketState::InProgress => "is being worked on by our engineering team",
        TicketState::Resolved => "has been fixed and is ready for you to check",
        TicketState::Closed => "has been closed",
        TicketState::Unknown => "has an update from our engineering team",
    };
    let mut message = format!("Your report \"{title}\" {state}.");
    if let Some(url) = &ticket.external_url {
        let _ = write!(message, "\n\nTracker link: {url}");
    }
    if to == TicketState::Closed {
        message.push_str(
            "\n\nThanks for your patience. If the problem comes back, report it again and we \
             will take another look.",
        );
    }
    (subject, message)
}

/// Merges an event id into a notify event's `detail`, when there is one.
/// The ordinary notices carry no id, so their `detail` is unchanged; a
/// status update records the `status_changed` event it announces, which is
/// how the idempotency key and the audit trail line up.
#[must_use]
fn with_event_id(mut detail: Value, event_id: Option<&str>) -> Value {
    if let (Value::Object(map), Some(event_id)) = (&mut detail, event_id) {
        map.insert("event_id".to_owned(), json!(event_id));
    }
    detail
}

/// The draft prompt's standing instruction. The drafter first classifies
/// the conversation — a `defect`, a `support_case` (a how-to or question
/// that needs a person) or a `lead` (buying intent) — and fills only that
/// kind's fields.
const DRAFT_SYSTEM: &str = "You draft a ticket from a customer support conversation. First \
     classify it as one of `defect`, `support_case` or `lead`, then fill that kind's fields. A \
     `defect` is a bug in our product: give a one-line title, the reproduction steps in order, \
     what the customer expected to happen and what actually happened. A `support_case` is a \
     how-to or account question that needs a person: give the `summary` and the customer's \
     actual `customer_ask`. A `lead` is buying or upgrade intent: give the `company`, the \
     `intent`, and the `seats` if the transcript names a number. Always give a title and a \
     severity. Answer only with JSON matching the given schema; leave the other kinds' fields \
     out.";

/// The judge prompt's standing instruction. The judge is deliberately
/// independent of the drafter and checks the draft against the source.
const JUDGE_SYSTEM: &str = "You are an independent judge of a drafted support ticket; you did \
     not write it. First check that the draft's `kind` fits the conversation — a how-to or \
     account question is a `support_case`, buying or upgrade intent is a `lead`, and only a bug \
     in our product is a `defect`. Do not reject a support case or a lead merely because it is \
     not a defect. Then check the draft against the original transcript: is the kind's required \
     fields complete (a defect needs its reproduction steps; a support case its summary and ask; \
     a lead its company and intent), and does the draft carry customer personal data that must \
     not reach a tracker? Set `kind_ok` to `false` when the kind is wrong or its required fields \
     are incomplete. If the brief lists existing tickets and this draft is the same defect as \
     one of them, set `verdict` to `duplicate` and `duplicate_of` to that ticket's id from the \
     list — never any other id, and never when it is not the same defect. Answer only with JSON \
     matching the given schema, and always give your reasons.";

/// One duplicate candidate as the judge's brief shows it: the filed
/// ticket's id (the only value a valid `duplicate_of` may carry) and its
/// title.
struct Candidate {
    id: String,
    title: String,
}

/// What the judge's prompt carries: the draft, the candidate tickets it
/// might duplicate, and the transcript it must be checked against.
fn judge_brief(ticket: &Ticket, candidates: &[Candidate]) -> String {
    use std::fmt::Write as _;

    let mut brief = String::from("Drafted ticket:\n");
    if let Some(title) = &ticket.title {
        let _ = writeln!(brief, "Title: {title}");
    }
    if let Some(body) = &ticket.body_markdown {
        brief.push('\n');
        brief.push_str(body);
        brief.push('\n');
    }
    if let Some(severity) = ticket.severity {
        let _ = writeln!(brief, "\nDrafted severity: {}", severity.name());
    }
    if !candidates.is_empty() {
        brief.push_str("\nExisting filed tickets (name one in `duplicate_of` only if it is the same defect):\n");
        for candidate in candidates {
            let _ = writeln!(brief, "[{}] {}", candidate.id, candidate.title);
        }
    }
    brief.push_str("\nOriginal transcript:\n");
    // The same customer-authored transcript the drafter saw, scrubbed the
    // same way: no un-redacted address or token rides into the judge prompt
    // either.
    brief.push_str(&scrub_text(&ticket.transcript));
    brief
}

/// The text a ticket is tokenized on for duplicate scoring: its title
/// and body, the two drafted fields that say what it is about.
fn draft_text(ticket: &Ticket) -> String {
    let title = ticket.title.as_deref().unwrap_or_default();
    let body = ticket.body_markdown.as_deref().unwrap_or_default();
    format!("{title}\n{body}")
}

/// Ranks `candidates` against `draft` by BM25 over title+body, keeping
/// the top [`CANDIDATE_LIMIT`] with a positive score (best first).
///
/// The corpus is the candidate set itself — `N` its size, `avg_length`
/// the mean token count — so the ranking is self-contained and needs no
/// server-side index. Pure: no database, no clock.
fn rank_candidates(draft: &str, candidates: &[Ticket]) -> Vec<Candidate> {
    let query_terms = tokenize(draft);
    if query_terms.is_empty() || candidates.is_empty() {
        return Vec::new();
    }

    let mut postings: Vec<bm25::Posting> = Vec::new();
    let mut total_length: u64 = 0;
    for candidate in candidates {
        let (terms, length) = term_stats(&draft_text(candidate));
        total_length += u64::from(length);
        for (term, tf) in terms {
            postings.push(bm25::Posting {
                chunk_id: candidate.id.clone(),
                term,
                tf,
                length,
            });
        }
    }

    // A candidate set is a handful of tickets, nowhere near 2^53, so the
    // precision these casts lose cannot surface in a score.
    #[expect(clippy::cast_precision_loss)]
    let avg_length = total_length as f64 / candidates.len() as f64;
    let corpus = bm25::Corpus {
        chunk_count: candidates.len() as u64,
        avg_length,
    };

    // Document frequency over the candidate set: `term_stats` yields each
    // term once per candidate, so a term's posting count is its df. The
    // set is the whole corpus here, so nothing is truncated and these are
    // exact (support's persisted-statistics path is the bounded one).
    let mut df: HashMap<String, u64> = HashMap::new();
    for posting in &postings {
        *df.entry(posting.term.clone()).or_default() += 1;
    }

    let mut ranked = bm25::rank(
        &query_terms,
        &postings,
        &df,
        &corpus,
        &bm25::Params::default(),
    );
    ranked.retain(|scored| scored.score > 0.0);
    ranked.truncate(CANDIDATE_LIMIT);
    ranked
        .into_iter()
        .filter_map(|scored| {
            let candidate = candidates.iter().find(|c| c.id == scored.chunk_id)?;
            Some(Candidate {
                id: candidate.id.clone(),
                title: candidate
                    .title
                    .clone()
                    .unwrap_or_else(|| "(untitled)".to_owned()),
            })
        })
        .collect()
}

/// Terms and their frequencies for one string, sorted by term — the
/// per-document half of the in-memory postings [`rank_candidates`]
/// builds.
fn term_stats(text: &str) -> (Vec<(String, u32)>, u32) {
    let mut counts: BTreeMap<String, u32> = BTreeMap::new();
    for term in tokenize(text) {
        *counts.entry(term).or_default() += 1;
    }
    let length = counts.values().sum();
    (counts.into_iter().collect(), length)
}

/// Inserts a key into an event `detail` that serialised from a struct —
/// always an object, but the guard keeps this total instead of trusting it.
fn insert_detail(detail: &mut Value, key: &str, value: Value) {
    if let Value::Object(map) = detail {
        map.insert(key.to_owned(), value);
    }
}

/// RFC 3339 of `at`; a clock that cannot format (none exists) degrades to
/// the empty string rather than panicking, as the intake handoff does.
fn rfc3339(at: OffsetDateTime) -> String {
    at.format(&Rfc3339).unwrap_or_default()
}

/// RFC 3339 of `now + wait`, total at both edges: an unrepresentable delay
/// clamps, an unrepresentable instant falls back to `now`.
fn rfc3339_after(now: OffsetDateTime, wait: Duration) -> String {
    let secs = i64::try_from(wait.as_secs()).unwrap_or(i64::MAX);
    rfc3339(
        now.checked_add(time::Duration::seconds(secs))
            .unwrap_or(now),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use cratefield_core::Severity;

    /// One failure's delay, and the doubling-then-capping shape.
    #[test]
    fn the_backoff_schedule_doubles_and_caps() {
        let policy = RetryPolicy::new().base(Duration::from_secs(10));
        assert_eq!(policy.backoff(0), Duration::from_secs(10));
        assert_eq!(policy.backoff(1), Duration::from_secs(20));
        assert_eq!(policy.backoff(2), Duration::from_secs(40));
        assert_eq!(policy.backoff(3), Duration::from_secs(80));

        // The issue's defaults cap at fifteen minutes however many
        // failures pile up, and an absurd attempt count saturates instead
        // of overflowing.
        let defaults = RetryPolicy::new();
        assert_eq!(defaults.backoff(0), Duration::from_secs(30));
        assert_eq!(
            defaults.backoff(6),
            Duration::from_mins(15),
            "30s * 2^6 exceeds the 15 min cap"
        );
        assert_eq!(defaults.backoff(1_000), Duration::from_mins(15));
        assert_eq!(
            RetryPolicy::new().cap(Duration::from_secs(60)).backoff(50),
            Duration::from_secs(60)
        );
    }

    /// A provider's `retry_after` is honoured when it is the larger of the
    /// two, and never lets a short hint shrink the schedule.
    #[test]
    fn a_retry_after_hint_is_honoured_only_when_it_is_larger() {
        let policy = RetryPolicy::new().base(Duration::from_secs(30));
        let epoch = time::OffsetDateTime::from_unix_timestamp(1_789_000_000).expect("epoch");

        // Hint shorter than the schedule: the schedule wins.
        let at = policy.next_attempt_at(0, epoch, Some(Duration::from_secs(10)));
        assert_eq!((at - epoch).whole_seconds(), 30);

        // Hint longer than the schedule: the provider is honoured.
        let at = policy.next_attempt_at(0, epoch, Some(Duration::from_secs(120)));
        assert_eq!((at - epoch).whole_seconds(), 120);

        // No hint at all: pure schedule.
        let at = policy.next_attempt_at(2, epoch, None);
        assert_eq!((at - epoch).whole_seconds(), 120);
    }

    /// Five attempts are the budget: the fifth failure dead-letters, the
    /// fourth still retries.
    #[test]
    fn the_attempt_budget_dead_letters_at_max() {
        let policy = RetryPolicy::new();
        assert!(policy.exhausted(5));
        assert!(policy.exhausted(6));
        assert!(!policy.exhausted(4));
        assert!(!policy.exhausted(1));
        // A shrunken policy (what step 4's tests will use) spends
        // itself just as fast as it says.
        let short = RetryPolicy::new().max_attempts(1);
        assert!(short.exhausted(1));
    }

    #[test]
    fn the_draft_body_renders_steps_and_sections() {
        let drafted = Drafted {
            title: "Checkout 500s on a used gift card".to_owned(),
            kind: Kind::Defect,
            repro_steps: vec![
                "Add an item to the cart".to_owned(),
                "Pay with a part-used gift card".to_owned(),
            ],
            expected: "The order completes".to_owned(),
            actual: "HTTP 500 from /checkout".to_owned(),
            environment: Some("production".to_owned()),
            severity: Severity::Error,
            summary: None,
            customer_ask: None,
            company: None,
            seats: None,
            intent: None,
        };
        assert_eq!(
            render_body_markdown(&drafted),
            "## Repro steps\n\
             1. Add an item to the cart\n\
             2. Pay with a part-used gift card\n\
             \n\
             ## Expected\n\
             The order completes\n\
             \n\
             ## Actual\n\
             HTTP 500 from /checkout\n"
        );

        // No steps: the section stays, honestly empty.
        let empty = Drafted {
            repro_steps: Vec::new(),
            ..drafted
        };
        assert!(render_body_markdown(&empty).starts_with("## Repro steps\n\n## Expected\n"));
    }

    /// A support case and a lead render their own sections, and never the
    /// defect's — the body matches the kind, not a fixed defect shape.
    #[test]
    fn the_body_renders_the_support_case_and_lead_sections() {
        let support = Drafted {
            title: "How do I add a seat?".to_owned(),
            kind: Kind::SupportCase,
            repro_steps: Vec::new(),
            expected: String::new(),
            actual: String::new(),
            environment: None,
            severity: Severity::Info,
            summary: Some("Billing question about adding a seat".to_owned()),
            customer_ask: Some("How do I add a seat to my plan?".to_owned()),
            company: None,
            seats: None,
            intent: None,
        };
        assert_eq!(
            render_body_markdown(&support),
            "## Summary\n\
             Billing question about adding a seat\n\
             \n\
             ## Customer ask\n\
             How do I add a seat to my plan?\n"
        );

        let lead = Drafted {
            title: "Acme wants 50 seats".to_owned(),
            kind: Kind::Lead,
            severity: Severity::Info,
            company: Some("Acme".to_owned()),
            seats: Some(50),
            intent: Some("wants to buy the enterprise plan".to_owned()),
            summary: None,
            customer_ask: None,
            repro_steps: Vec::new(),
            expected: String::new(),
            actual: String::new(),
            environment: None,
        };
        assert_eq!(
            render_body_markdown(&lead),
            "## Company\n\
             Acme\n\
             \n\
             ## Seats\n\
             50\n\
             \n\
             ## Intent\n\
             wants to buy the enterprise plan\n"
        );

        // A lead with no seat count leaves the section empty, not invented.
        let no_seats = Drafted {
            seats: None,
            ..lead
        };
        assert!(render_body_markdown(&no_seats).contains("## Seats\n\n"));
    }

    #[test]
    fn the_needs_info_question_quotes_the_judges_reasons() {
        let question = compose_needs_info_question(&[
            "the build number is not in the transcript".to_owned(),
            "the steps skip the login step".to_owned(),
        ]);
        assert!(question.starts_with("Before we pass this to engineering"));
        assert!(question.contains("- the build number is not in the transcript\n"));
        assert!(question.contains("- the steps skip the login step\n"));
        // A judge with no reasons still asks something coherent.
        assert!(compose_needs_info_question(&[]).ends_with("following?\n"));
    }

    fn notify_ticket(status: Status) -> Ticket {
        Ticket {
            id: "01JTICKET".to_owned(),
            tenant_id: "acme".to_owned(),
            conversation_id: "conv-1".to_owned(),
            kind: Kind::Defect,
            status,
            stage: Stage::Notify,
            transcript: "customer: it broke".to_owned(),
            title: Some("Checkout 500s".to_owned()),
            body_markdown: Some("## Repro steps\n1. Pay\n".to_owned()),
            severity: Some(Severity::Error),
            environment: None,
            verdict: None,
            judge_reasons: None,
            customer_question: Some("which build is this?".to_owned()),
            external_id: Some("acme/api#7".to_owned()),
            external_url: Some("https://github.test/acme/api/7".to_owned()),
            match_count: 0,
            tracker_state: None,
            created_at: "2026-09-19T00:00:00Z".to_owned(),
            updated_at: "2026-09-19T00:00:00Z".to_owned(),
        }
    }

    #[test]
    fn the_notify_message_uses_the_filed_reference_or_the_question() {
        let (subject, message) = compose_notify_message(&notify_ticket(Status::Filed));
        assert!(subject.contains("Checkout 500s"));
        assert!(message.contains("acme/api#7"));
        assert!(message.contains("https://github.test/acme/api/7"));

        let (subject, message) = compose_notify_message(&notify_ticket(Status::NeedsInfo));
        assert_eq!(subject, "We need a little more information");
        assert_eq!(message, "which build is this?");

        // Without a stored question the ask degrades to a plain one.
        let mut ticket = notify_ticket(Status::NeedsInfo);
        ticket.customer_question = None;
        let (_, message) = compose_notify_message(&ticket);
        assert!(message.contains("more detail"));
    }

    /// `json` wins when the provider honoured the schema; the text is the
    /// documented fallback; text that is not JSON is terminal.
    #[test]
    fn completions_decode_from_json_then_text() {
        let drafted = json!({
            "title": "Checkout 500s",
            "repro_steps": ["pay"],
            "expected": "an order",
            "actual": "a 500",
            "severity": "error",
        });

        let from_json = decode_completion::<Drafted>(
            &Completion::new("prose around it", "m").json(drafted.clone()),
        )
        .expect("json decodes");
        assert_eq!(from_json.title, "Checkout 500s");

        let from_text = decode_completion::<Drafted>(&Completion::new(drafted.to_string(), "m"))
            .expect("text decodes as the fallback");
        assert_eq!(from_text.repro_steps, vec!["pay".to_owned()]);

        let err = decode_completion::<Drafted>(&Completion::new("definitely not json", "m"))
            .expect_err("unparseable text is terminal");
        assert!(!err.is_retryable(), "a parse failure never heals on retry");

        let err =
            decode_completion::<Drafted>(&Completion::new("{}", "m")).expect_err("missing fields");
        assert!(!err.is_retryable());
    }

    #[test]
    fn committed_stages_are_recognised_and_fresh_ones_are_not() {
        let fresh = Ticket {
            status: Status::Intake,
            stage: Stage::Draft,
            ..notify_ticket(Status::Intake)
        };
        assert!(
            !stage_already_committed(&fresh, Stage::Draft),
            "a fresh ticket has not run its draft"
        );

        // Draft committed: the ticket moved on to the judge.
        let drafted = Ticket {
            status: Status::Drafting,
            stage: Stage::Judge,
            ..fresh.clone()
        };
        assert!(stage_already_committed(&drafted, Stage::Draft));
        assert!(!stage_already_committed(&drafted, Stage::Judge));

        // A terminal status is committed from any earlier stage's point
        // of view.
        for status in [
            Status::Filed,
            Status::Rejected,
            Status::NeedsInfo,
            Status::DeadLetter,
            Status::Closed,
        ] {
            let parked = Ticket {
                status,
                stage: Stage::Judge,
                ..fresh.clone()
            };
            assert!(
                stage_already_committed(&parked, Stage::Judge),
                "{status:?} is terminal"
            );
        }
    }

    #[test]
    fn each_stage_names_its_retry_and_dead_letter_kind() {
        assert_eq!(retry_kind(Stage::File), EventKind::FileRetryScheduled);
        assert_eq!(retry_kind(Stage::Draft), EventKind::DraftFailed);
        assert_eq!(retry_kind(Stage::Judge), EventKind::JudgeFailed);
        assert_eq!(retry_kind(Stage::Notify), EventKind::NotifyFailed);
        assert_eq!(retry_kind(Stage::Follow), EventKind::FollowFailed);

        assert_eq!(dead_letter_kind(Stage::File), EventKind::FileDeadLettered);
        assert_eq!(dead_letter_kind(Stage::Draft), EventKind::DraftFailed);
        assert_eq!(dead_letter_kind(Stage::Judge), EventKind::JudgeFailed);
        assert_eq!(dead_letter_kind(Stage::Notify), EventKind::NotifyFailed);
        assert_eq!(dead_letter_kind(Stage::Follow), EventKind::FollowFailed);
    }

    /// Pins the status-update wording: a plain-language sentence per state,
    /// the tracker link when there is one, and a closing note only at
    /// `Closed`.
    #[test]
    fn the_status_update_message_names_the_new_state_in_words() {
        let ticket = notify_ticket(Status::Filed);

        let (subject, message) = compose_status_update_message(&ticket, TicketState::Resolved);
        assert!(subject.contains("Checkout 500s"));
        assert!(message.contains("has been fixed"));
        assert!(message.contains("https://github.test/acme/api/7"));

        let (_, closed) = compose_status_update_message(&ticket, TicketState::Closed);
        assert!(closed.contains("has been closed"));
        assert!(closed.contains("report it again"));

        // An unknown state still says something honest.
        let (_, unknown) = compose_status_update_message(&ticket, TicketState::Unknown);
        assert!(unknown.contains("an update from our engineering team"));
    }

    #[test]
    fn status_update_claim_keys_carry_the_event_id() {
        assert_eq!(
            claim_key_with("01JT", Stage::Notify, Some("01JEVENT")),
            "01JT:notify:01JEVENT"
        );
        assert_eq!(
            claim_key_with("01JT", Stage::Notify, None),
            claim_key("01JT", Stage::Notify)
        );
    }

    #[test]
    fn claim_keys_are_ticket_and_stage() {
        assert_eq!(claim_key("01JT", Stage::Draft), "01JT:draft");
        assert_eq!(claim_key("01JT", Stage::Notify), "01JT:notify");
    }
}
