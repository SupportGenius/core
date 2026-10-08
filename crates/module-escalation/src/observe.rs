//! The two spans an operator queries when the pipeline goes quiet:
//! `model.call` and `pipeline.stage` (issue #39).
//!
//! Two harness facts shape this module. A span's fields are not a log
//! line — both runtimes visit only what an *event* carries and neither
//! emits span-close, so every span closes with exactly one [`info!`].
//! And the sink redacts any field name containing `secret`, `token`,
//! `key`, `authorization` or `password`, which is why the token counts
//! are `tok_in`/`tok_out`: `input_tokens` would arrive `[redacted]`.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU8, Ordering};

use cratefield_core::{Completion, ModelTier, subject_hash};
use time::OffsetDateTime;
use tracing::{Span, info, info_span};

/// One in-flight model call. `started` is the module clock's reading from
/// immediately before the call, so `latency_ms` covers the provider round
/// trip and not the bookkeeping around it.
pub(crate) struct ModelCall {
    span: Span,
    tier: &'static str,
    tenant: String,
    started: OffsetDateTime,
    usage: Option<(u64, u64)>,
}

impl ModelCall {
    /// Open the span. Not `span.enter()`: that guard is `!Send`, so holding
    /// one across `complete().await` would make every future here `!Send`.
    pub(crate) fn start(tier: ModelTier, tenant_id: &str, started: OffsetDateTime) -> Self {
        let tier = tier.name();
        let tenant = subject_hash(tenant_id);
        let span = info_span!(
            "model.call",
            tier = tier,
            tenant = %tenant,
            latency_ms = tracing::field::Empty,
            tok_in = tracing::field::Empty,
            tok_out = tracing::field::Empty,
            outcome = tracing::field::Empty,
        );
        Self {
            span,
            tier,
            tenant,
            started,
            usage: None,
        }
    }

    pub(crate) fn span(&self) -> &Span {
        &self.span
    }

    /// Note the token counts. A call that errors never gets here and logs
    /// zeroes, which is honest: nothing was billed.
    pub(crate) fn answered(&mut self, completion: &Completion) {
        self.usage = Some((completion.input_tokens, completion.output_tokens));
    }

    /// Record every field, then emit the one event that carries them.
    pub(crate) fn finish(self, now: OffsetDateTime, outcome: &'static str) {
        // `Instant::now` panics on wasm32 and the `Clock` port has no
        // monotonic reading, so latency is a delta between two `Clock`
        // readings, clamped at zero against an NTP step back.
        let latency_ms = (now - self.started).whole_milliseconds().max(0);
        let (tok_in, tok_out) = self.usage.unwrap_or((0, 0));
        self.span.record("latency_ms", latency_ms);
        self.span.record("tok_in", tok_in);
        self.span.record("tok_out", tok_out);
        self.span.record("outcome", outcome);
        self.span.in_scope(|| {
            info!(
                tier = self.tier,
                tenant = %self.tenant,
                latency_ms,
                tok_in,
                tok_out,
                outcome,
                "escalation: model call"
            );
        });
    }
}

/// What a stage did with one outbox record: its own work committed, a
/// retryable failure, an exhausted attempt budget, a record retired
/// without running, a self-reschedule, or a sweep that failed before the
/// outcome could be recorded. The strings are the log field values, so
/// they are pinned here rather than spelled at each call site.
#[derive(Clone, Copy, Debug)]
pub(crate) enum StageResult {
    Done,
    Retry,
    /// The attempt budget ran out; the ticket was marked for a human.
    DeadLetter,
    /// Retired without running: an unknown topic, an unparseable
    /// payload, or a commit another sweep had already made.
    Skipped,
    Rescheduled,
    /// A database error escaped the stage, so nothing about this row was
    /// recorded. Its lease expires and the row comes back — which is why
    /// this is not `Retry`: no retry was scheduled, the row was merely
    /// not consumed.
    Aborted,
}

impl StageResult {
    /// The stable wire/log spelling.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Done => "done",
            Self::Retry => "retry",
            Self::DeadLetter => "dead_letter",
            Self::Skipped => "skipped",
            Self::Rescheduled => "rescheduled",
            Self::Aborted => "aborted",
        }
    }

    fn code(self) -> u8 {
        match self {
            Self::Done => 1,
            Self::Retry => 2,
            Self::DeadLetter => 3,
            Self::Skipped => 4,
            Self::Rescheduled => 5,
            Self::Aborted => 6,
        }
    }
}

/// One in-flight stage run. `result` is first-write-wins, so a path that
/// both reschedules and returns early cannot report twice.
pub(crate) struct StageRun {
    span: Span,
    stage: String,
    attempt: i64,
    tenant: OnceLock<String>,
    result: AtomicU8,
}

impl StageRun {
    /// Open the span for one outbox record. `attempt` is 1-based: the try
    /// about to run, so a first try logs `1`. The tenant is unnamed until
    /// the payload decodes (see [`StageRun::name_tenant`]), because the
    /// span is opened before the decode.
    pub(crate) fn start(stage: &str, attempt: i64) -> Self {
        let stage = stage.to_owned();
        let tenant = OnceLock::new();
        let span = info_span!(
            "pipeline.stage",
            stage = %stage,
            attempt,
            tenant = tracing::field::Empty,
            result = tracing::field::Empty,
        );
        Self {
            span,
            stage,
            attempt,
            tenant,
            result: AtomicU8::new(0),
        }
    }

    /// Record the tenant pseudonym, once. A record that never decoded has
    /// no tenant to name, which is when a blank tenant is honest.
    pub(crate) fn name_tenant(&self, tenant_id: &str) {
        if self.tenant.set(subject_hash(tenant_id)).is_ok() {
            self.span
                .record("tenant", self.tenant.get().map_or("", String::as_str));
        }
    }

    /// The tenant pseudonym, for the settling event.
    fn tenant_pseudonym(&self) -> &str {
        self.tenant.get().map_or("", String::as_str)
    }

    /// The span, for [`tracing::Instrument`] on the future this describes.
    pub(crate) fn span(&self) -> &Span {
        &self.span
    }

    /// Record `result` and emit the one event carrying it. The first call
    /// wins; later ones are ignored.
    pub(crate) fn settle(&self, result: StageResult) {
        if self.result.swap(result.code(), Ordering::Relaxed) != 0 {
            return;
        }
        let outcome = result.as_str();
        self.span.record("result", outcome);
        self.span.in_scope(|| {
            info!(
                stage = %self.stage,
                attempt = self.attempt,
                tenant = self.tenant_pseudonym(),
                result = outcome,
                "escalation: pipeline stage"
            );
        });
    }
}
