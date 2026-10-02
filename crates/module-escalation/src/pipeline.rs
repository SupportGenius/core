//! The durable stage runner: claims due outbox rows and drives each one
//! through its stage — draft, judge, file, notify — so that a stage's
//! effect, its audit row, the next stage's outbox row and the completion of
//! its own row all commit in **one** [`Database::batch_atomic`].
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
    Drafted, EventKind, Judgment, Stage, StagePayload, Status, Ticket, Verdict, stage_seq,
    webhook_events,
};
use crate::store;

use cratefield_core::{
    Clock, Completion, Config, Credential, Database, Defer, Destination, Filed, IdGen, Inbox,
    Mailer, Message, ModelTier, Outbox, OutboxRecord, Prompt, SendOutcome, Severity, Statement,
    TextModel, TicketDraft, Tracker,
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
        let key = claim_key(&payload.ticket_id, stage);
        let claimed = self.inbox.claim(&*self.db, &key, &at).await?;

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
                        err,
                        &at,
                    )
                    .await;
            }
        };
        let Some(ticket) = ticket else {
            // The row names a ticket that does not exist. Terminal — the
            // audit row still lands (an event survives on its own id) and
            // the status update is a harmless no-op.
            let err = Error::Decode(format!("ticket `{}` is missing", payload.ticket_id));
            return self
                .fail(
                    record,
                    stage,
                    &payload.ticket_id,
                    &payload.tenant_id,
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
        // concurrent drainer out until this attempt's batch lands.)
        if !claimed && stage_already_committed(&ticket, stage) {
            return self.complete_row(&record.id).await;
        }

        // 5-6. One port call, then one all-or-nothing batch.
        match stage {
            Stage::Draft => self.run_draft(record, &ticket, &at).await,
            Stage::Judge => self.run_judge(record, &ticket, &at).await,
            Stage::File => self.run_file(record, &ticket, &at).await,
            Stage::Notify => self.run_notify(record, &ticket, &at).await,
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
        let prompt = Prompt::new(ModelTier::Fast)
            .system(DRAFT_SYSTEM)
            .user(&ticket.transcript)
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
                    .fail(record, Stage::Draft, &ticket.id, &ticket.tenant_id, err, at)
                    .await;
            }
        };

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
                    .fail(record, Stage::Judge, &ticket.id, &ticket.tenant_id, err, at)
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
    /// parked as rejected, or parked with a question for the customer and
    /// on to the notify stage.
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
        // Two of the branches enqueue the stage the verdict leads to.
        let enqueue = |stage: Stage| {
            store::enqueue_stage_stmt(
                &self.outbox,
                &self.idgen.ulid(),
                &ticket.id,
                &ticket.tenant_id,
                stage,
                at,
            )
        };

        match judgment.verdict {
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
                let question = compose_needs_info_question(&judgment.reasons);
                let asked = json!({ "question": question, "reasons": judgment.reasons });
                let needs_info = store::insert_event_stmt(
                    &self.idgen.ulid(),
                    &ticket.id,
                    stage_seq(Stage::Judge, 1),
                    at,
                    Stage::Judge,
                    EventKind::NeedsInfo,
                    &asked,
                );
                let batch = [
                    judge_completed,
                    needs_info,
                    store::update_ticket_judgment_stmt(&ticket.id, judgment, at),
                    store::update_ticket_question_stmt(&ticket.id, &question, at),
                    store::update_ticket_status_stmt(&ticket.id, Status::NeedsInfo, at),
                    store::update_ticket_stage_stmt(&ticket.id, Stage::Notify, at),
                    enqueue(Stage::Notify),
                    store::outbox_complete_stmt(OUTBOX_TABLE, &record.id),
                ];
                self.commit(&batch).await?;
                self.defer_next();
                Ok(())
            }
        }
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
    /// what (if anything) it named.
    async fn commit_file(
        &self,
        record: &OutboxRecord,
        ticket: &Ticket,
        judgment: &Judgment,
        judge_completed: Statement,
        ignored: Option<Statement>,
        at: &str,
    ) -> Result<(), Error> {
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

    // ------------------------------------------------------------------
    // Stage: file

    /// One tracker call. Everything before it — destination lookup,
    /// credential resolution from the Config port, the draft — is local
    /// resolution; the call is the stage's one HttpClient-bound port call.
    /// The idempotency key is derived from the ticket id alone, so a retry
    /// after a lost response presents the same key and the tracker
    /// collapses it instead of filing a second ticket.
    async fn run_file(
        &self,
        record: &OutboxRecord,
        ticket: &Ticket,
        at: &str,
    ) -> Result<(), Error> {
        match self.file_ticket(ticket).await {
            Ok((filed, kind)) => {
                // A tracker's `Filed::url` is its public issue link (useful in
                // a notification); a `webhook` destination's is the endpoint
                // *itself* — credential material (issue #23) — so it reaches
                // neither the audit row nor the event payload.
                let url_is_public = kind != "webhook";
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
                        at,
                    ),
                    store::outbox_complete_stmt(OUTBOX_TABLE, &record.id),
                ];
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
            // rejection, an unauthorized credential or a missing
            // destination dead-letters — `fail` sorts one from the other,
            // and both write their reason into a `file_dead_lettered`
            // / `file_retry_scheduled` row.
            Err(err) => {
                self.fail(record, Stage::File, &ticket.id, &ticket.tenant_id, err, at)
                    .await
            }
        }
    }

    /// The file stage's port call, with the tenant's destination and
    /// per-call credential resolved first. Any gap — no drafted ticket to
    /// file, no destination row, no credential behind the stored ref — is
    /// a terminal decode error, which dead-letters. A webhook's filed URL
    /// is blanked: it is the destination's own secret.
    ///
    /// Returns the tracker's [`Filed`] answer and the destination's
    /// [`kind`](cratefield_core::Destination::kind) (never the destination
    /// itself — a `Webhook` variant is credential material), which the
    /// `escalation.filed` event reports.
    async fn file_ticket(&self, ticket: &Ticket) -> Result<(Filed, &'static str), Error> {
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
        let Some((destination, credential_ref)) =
            store::load_destination(&*self.db, &ticket.tenant_id).await?
        else {
            return Err(Error::Decode(format!(
                "tenant `{}` has no tracker destination configured",
                ticket.tenant_id
            )));
        };
        let (destination, credential) = self
            .resolve_credential(&ticket.tenant_id, destination, &credential_ref)
            .await?;
        let idempotency_key = format!("escalation:{}", ticket.id);
        let body = with_conversation_link(body, &*self.config, &ticket.conversation_id);
        let kind = destination.kind();
        let mut draft = TicketDraft::new(idempotency_key, title, body, severity)
            .labels(escalation_labels(severity));
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
        Ok((filed, kind))
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
    /// nothing, and its batch is the event plus the row's completion.
    ///
    /// **There is no recipient in v0, and the code says so rather than
    /// inventing one.** Issue #4's intake signature is
    /// `(tenant_id, conversation_id, transcript)` — it carries no customer
    /// address — and the conversation/contact tables belong to
    /// `module-support` (issue #2), which does not exist yet. So the
    /// message is composed and *recorded* in the audit trail, and the send
    /// is skipped with an explicit reason: `no_mailer` when no `Mailer`
    /// port is wired, `no_recipient` when there is no address to send to
    /// (always, today — see [`notify_recipient`]). No recipient column was
    /// invented and no fake address was fabricated; when module-support
    /// lands, teaching [`notify_recipient`] to find the real address is
    /// the whole change.
    async fn run_notify(
        &self,
        record: &OutboxRecord,
        ticket: &Ticket,
        at: &str,
    ) -> Result<(), Error> {
        let (subject, message) = compose_notify_message(ticket);
        let event = match (&self.mailer, notify_recipient(ticket)) {
            (Some(mailer), Some(to)) => {
                // The one send call. The notification is plain text; the
                // same text goes out as both parts.
                let outbound = Message::new(
                    to.as_str(),
                    self.notify_from(),
                    subject.as_str(),
                    message.as_str(),
                    message.as_str(),
                );
                match mailer.send(outbound).await {
                    Ok(SendOutcome::Sent { id }) => {
                        let detail = json!({
                            "to": to,
                            "subject": subject,
                            "message": message,
                            "provider_id": id,
                        });
                        self.notify_event(ticket, at, EventKind::Notified, &detail)
                    }
                    Ok(SendOutcome::NotConfigured) => {
                        let detail = json!({
                            "reason": "mailer_not_configured",
                            "subject": subject,
                            "message": message,
                        });
                        self.notify_event(ticket, at, EventKind::NotifySkipped, &detail)
                    }
                    Err(err) => {
                        return self
                            .fail(
                                record,
                                Stage::Notify,
                                &ticket.id,
                                &ticket.tenant_id,
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
                self.notify_event(ticket, at, EventKind::NotifySkipped, &detail)
            }
            (_, None) => {
                let detail = json!({
                    "reason": "no_recipient",
                    "subject": subject,
                    "message": message,
                });
                self.notify_event(ticket, at, EventKind::NotifySkipped, &detail)
            }
        };
        self.commit(&[event, store::outbox_complete_stmt(OUTBOX_TABLE, &record.id)])
            .await
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
    // Failure handling

    /// Records a stage failure. Retryable failures inside the attempt
    /// budget reschedule; everything else — a terminal failure, or a
    /// retryable failure that has spent the [`RetryPolicy`] budget —
    /// dead-letters. Either way: one event row carrying the reason, and
    /// one batch.
    async fn fail(
        &self,
        record: &OutboxRecord,
        stage: Stage,
        ticket_id: &str,
        tenant_id: &str,
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
                store::inbox_release_stmt(INBOX_TABLE, &claim_key(ticket_id, stage)),
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
            // Terminal: the ticket parks for a human (a no-op when the
            // row is already gone) and the outbox row completes so it
            // stops. Never `retry_later` a terminal failure.
            let mut batch = vec![
                event,
                store::update_ticket_status_stmt(ticket_id, Status::DeadLetter, at),
                store::outbox_complete_stmt(OUTBOX_TABLE, &record.id),
            ];
            let reason = err.to_string();
            let payload = json!({
                "ticket_id": ticket_id,
                "tenant_id": tenant_id,
                "stage": stage.as_topic(),
                "reason": reason,
            });
            // The file stage has its own dead-letter event; every
            // dead-letter — this one included — also parks the ticket for a
            // human, so it publishes `needs_human` too.
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
    }
}

/// The labels every filed escalation carries: a `bug` marker, and the
/// drafted severity as `severity:<level>` (`severity:error`), so a tracker's
/// label filter can find escalations and rank them without parsing the body.
fn escalation_labels(severity: Severity) -> Vec<String> {
    vec!["bug".to_owned(), format!("severity:{}", severity.name())]
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

/// The ticket body the draft stage renders: the reproduction steps as a
/// numbered list, then expected/actual sections — exactly the fields the
/// schema guarantees, in an order an engineer can act on.
#[must_use]
pub(crate) fn render_body_markdown(drafted: &Drafted) -> String {
    use std::fmt::Write as _;

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
            let message = match (&ticket.external_id, &ticket.external_url) {
                (Some(id), Some(url)) => format!(
                    "We escalated this to engineering and it was accepted as {id}.\n\nTracker link: {url}\n\nWe will follow up here when there is news."
                ),
                (Some(id), None) => format!(
                    "We escalated this to engineering and it was accepted as {id}.\n\nWe will follow up here when there is news."
                ),
                _ => "We escalated this to engineering and will follow up here when there is \
                      news."
                    .to_owned(),
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

/// The notify recipient, when one exists. **It does not, in v0** — issue
/// #4's intake carries no customer address and `module-support` (issue #2)
/// owns the contact tables that would — so this is `None` and the notify
/// stage records its message instead of sending it. The signature is the
/// seam: when module-support lands, this is the one function that changes.
#[allow(clippy::unnecessary_wraps)] // deliberately a seam: always None today, Option when module-support lands
#[must_use]
pub(crate) fn notify_recipient(_ticket: &Ticket) -> Option<String> {
    None
}

/// The draft prompt's standing instruction.
const DRAFT_SYSTEM: &str = "You draft a defect ticket from a customer support conversation. \
     Read the transcript and answer only with JSON matching the given schema: a one-line ticket \
     title, the reproduction steps in order, what the customer expected to happen, what actually \
     happened, the deployment the transcript names (if any), and how urgent this is.";

/// The judge prompt's standing instruction. The judge is deliberately
/// independent of the drafter and checks the draft against the source.
const JUDGE_SYSTEM: &str = "You are an independent judge of a drafted defect ticket; you did \
     not write it. Check the draft against the original transcript: is this a defect in our \
     product, do the reproduction steps actually reproduce something, is the severity \
     proportionate, does the draft carry customer personal data that must not reach a tracker? \
     If the brief lists existing tickets and this draft is the same defect as one of them, set \
     `verdict` to `duplicate` and `duplicate_of` to that ticket's id from the list — never any \
     other id, and never when it is not the same defect. Answer only with JSON matching the \
     given schema, and always give your reasons.";

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
    brief.push_str(&ticket.transcript);
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
            repro_steps: vec![
                "Add an item to the cart".to_owned(),
                "Pay with a part-used gift card".to_owned(),
            ],
            expected: "The order completes".to_owned(),
            actual: "HTTP 500 from /checkout".to_owned(),
            environment: Some("production".to_owned()),
            severity: Severity::Error,
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

        assert_eq!(dead_letter_kind(Stage::File), EventKind::FileDeadLettered);
        assert_eq!(dead_letter_kind(Stage::Draft), EventKind::DraftFailed);
        assert_eq!(dead_letter_kind(Stage::Judge), EventKind::JudgeFailed);
        assert_eq!(dead_letter_kind(Stage::Notify), EventKind::NotifyFailed);
    }

    /// Pins the v0 answer: there is no recipient, by construction, until
    /// module-support exists to supply one.
    #[test]
    fn v0_has_no_notify_recipient() {
        assert_eq!(notify_recipient(&notify_ticket(Status::Filed)), None);
    }

    #[test]
    fn claim_keys_are_ticket_and_stage() {
        assert_eq!(claim_key("01JT", Stage::Draft), "01JT:draft");
        assert_eq!(claim_key("01JT", Stage::Notify), "01JT:notify");
    }
}
