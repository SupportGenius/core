//! Per-tenant quotas and usage metering (issue #20).
//!
//! Two meters in the one [`Usage`] table core ships the machinery for:
//! `conversations`, per UTC calendar month, bounded by the tenant's plan
//! and counted on the turn that *creates* a conversation; and
//! `model_tokens`, per UTC day, bounded by the tenant's own ceiling or,
//! absent that, [`DEFAULT_DAILY_TOKEN_CEILING`].
//!
//! **The check is a read, the spend is a statement.** [`admit`] reads
//! both meters before the model is asked, so a refused turn costs
//! nothing. The conversation increment then rides the turn's own
//! `batch_atomic` through core's guarded upsert, which cannot let two
//! concurrent turns both spend the last slot — a batch the guard
//! refused is re-read back into the same `402` the check would have
//! given ([`Admission::refusal_after_failure`]).
//!
//! The token increment does **not** ride that batch. Tokens are recorded
//! the moment the model answers, in their own write
//! ([`record_tokens`]), because the spend is real whether or not the
//! turn that caused it ever commits: a reply the schema rejects, a
//! takeover that arrives before the write, any batch rollback. Metering
//! it in the batch would let a runaway loop spend without ever moving
//! the ceiling that stops it.

use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use time::OffsetDateTime;

use cratefield_core::{
    Completion, Database, Exhausted, Period, Problem, ProblemDef, Statement, Usage,
};

use crate::messages::TurnFailure;

/// The usage table; `migrations/sqlite/0011_quotas.sql` ships core's
/// [`Usage::create_table_sql`] for it verbatim (a test at the foot of
/// this file asserts it).
const USAGE_TABLE: &str = "sg_usage";

/// Conversations opened in the UTC calendar month.
const METER_CONVERSATIONS: &str = "conversations";

/// Input plus output model tokens in the UTC day.
const METER_MODEL_TOKENS: &str = "model_tokens";

/// The model tokens a tenant may spend in one UTC day when it has set no
/// ceiling of its own: hundreds of ordinary turns, but finite — the
/// point of the meter is that a loop cannot spend without end.
pub(crate) const DEFAULT_DAILY_TOKEN_CEILING: u64 = 2_000_000;

/// 402: the plan's monthly conversation allowance is spent. Not retryable
/// within the month and not a rate limit, so it carries no
/// `Retry-After` — the way out is another plan, not a later request.
const QUOTA_EXCEEDED: ProblemDef = ProblemDef {
    slug: "quota-exceeded",
    status: StatusCode::PAYMENT_REQUIRED,
    title: "Plan quota exceeded",
    description: "This workspace has opened as many conversations this month as its plan \
                  allows. The turn was not run; the allowance resets next month, or another \
                  plan allows more.",
};

/// 429: the tenant's daily model-token ceiling is spent. Retryable, and
/// the response carries `Retry-After` to the next UTC midnight, the
/// moment the ceiling's day resets.
const TOKEN_CEILING_REACHED: ProblemDef = ProblemDef {
    slug: "token-ceiling-reached",
    status: StatusCode::TOO_MANY_REQUESTS,
    title: "Daily model token ceiling reached",
    description: "This workspace has spent the model tokens its daily ceiling allows. The \
                  turn was not run and no tokens were consumed; the ceiling resets at the \
                  next UTC midnight.",
};

/// What a turn was admitted to spend.
pub(crate) struct Admission {
    at: OffsetDateTime,
    /// `Some` only when the turn opens a conversation and the plan
    /// bounds how many a month; `None` is unbounded, which is also what
    /// a tenant with no plan row gets.
    conversation_limit: Option<u64>,
    /// Whether *this* turn creates the `sg_conversations` row, which is
    /// what decides whether the conversation meter is spent at all.
    new_conversation: bool,
}

impl Admission {
    /// The guarded conversation increment for the turn's own atomic
    /// batch — empty on a turn that opens no conversation.
    pub(crate) fn statements(&self, tenant_id: &str) -> Vec<Statement> {
        if !self.new_conversation {
            return Vec::new();
        }
        vec![Usage::new(USAGE_TABLE).consume_statement(
            tenant_id,
            METER_CONVERSATIONS,
            Period::CalendarMonthUtc,
            self.at,
            1,
            self.conversation_limit,
        )]
    }

    /// The `402` a failed turn batch was refused with, told apart from a
    /// genuine database failure by re-reading the meter: a concurrent
    /// turn took the last conversation slot between [`admit`]'s read and
    /// this write. `None` — including when the re-read itself fails —
    /// means the caller reports its own error instead, because a quota
    /// answer for a broken database would be a lie about why the turn
    /// did not happen.
    pub(crate) async fn refusal_after_failure(
        &self,
        db: &dyn Database,
        tenant_id: &str,
        request_id: &str,
    ) -> Option<Problem> {
        let limit = self.conversation_limit.filter(|_| self.new_conversation)?;
        let Ok(used) = read_meter(
            db,
            tenant_id,
            METER_CONVERSATIONS,
            Period::CalendarMonthUtc,
            self.at,
        )
        .await
        else {
            return None;
        };
        (used >= limit).then(|| quota_exceeded(request_id, used, limit))
    }
}

/// Records what one completion cost, in its own write, the moment the
/// model answered.
///
/// Unbounded by construction: the tokens are already paid for, and a
/// meter that refused its own record would under-bill an overrun
/// instead of showing it. A failure here is propagated, not logged —
/// continuing would be exactly the unmetered spend this exists to
/// prevent, and every other database call on this path propagates too.
pub(crate) async fn record_tokens(
    db: &dyn Database,
    tenant_id: &str,
    at: OffsetDateTime,
    tokens: u64,
) -> Result<(), Problem> {
    if tokens == 0 {
        return Ok(());
    }
    let statement = Usage::new(USAGE_TABLE).consume_statement(
        tenant_id,
        METER_MODEL_TOKENS,
        Period::Day,
        at,
        tokens,
        None,
    );
    Ok(db.batch_atomic(&[statement]).await?)
}

/// The model tokens one completion was paid for: input plus output. The
/// cached subset is already inside `input_tokens`, so it is not added
/// again.
pub(crate) fn completion_tokens(completion: &Completion) -> u64 {
    completion
        .input_tokens
        .saturating_add(completion.output_tokens)
}

/// Reads the tenant's plan and ceiling, and refuses the turn before the
/// model is ever asked when a meter it is subject to is spent.
/// `new_conversation` says whether this turn would open one — a
/// continuing turn is never stopped by the conversation allowance — and
/// `spends_tokens` whether it will ask the model at all: a turn answered
/// from a customer-safe source spends no tokens and is not stopped by
/// their ceiling.
pub(crate) async fn admit(
    db: &dyn Database,
    tenant_id: &str,
    now: OffsetDateTime,
    default_ceiling: u64,
    new_conversation: bool,
    spends_tokens: bool,
    request_id: &str,
) -> Result<Admission, TurnFailure> {
    let (conversations_per_month, daily_token_ceiling) = allowances(db, tenant_id).await?;
    if new_conversation && let Some(limit) = conversations_per_month {
        let used = read_meter(
            db,
            tenant_id,
            METER_CONVERSATIONS,
            Period::CalendarMonthUtc,
            now,
        )
        .await?;
        if used >= limit {
            return Err(TurnFailure::Problem(quota_exceeded(
                request_id, used, limit,
            )));
        }
    }
    if spends_tokens {
        let ceiling = daily_token_ceiling.unwrap_or(default_ceiling);
        let tokens = read_meter(db, tenant_id, METER_MODEL_TOKENS, Period::Day, now).await?;
        if tokens >= ceiling {
            let spent = Exhausted::spent(tokens, ceiling, Period::Day, now);
            return Err(TurnFailure::Response(Box::new(ceiling_reached(
                request_id, &spent,
            ))));
        }
    }
    Ok(Admission {
        at: now,
        conversation_limit: conversations_per_month,
        new_conversation,
    })
}

/// The tenant's `(conversations_per_month, daily_token_ceiling)`, both
/// `None` when it has no plan row. The `LEFT JOIN` is the plan's: a
/// tenant whose `sg_plans` row was deleted keeps its ceiling — a spend
/// guard must not vanish with a catalog entry.
async fn allowances(
    db: &dyn Database,
    tenant_id: &str,
) -> Result<(Option<u64>, Option<u64>), Problem> {
    let rows = db
        .query(&Statement::with_values(
            "SELECT p.conversations_per_month AS conversations_per_month, \
             t.daily_token_ceiling AS daily_token_ceiling \
             FROM sg_tenant_plan t LEFT JOIN sg_plans p ON p.plan_id = t.plan_id \
             WHERE t.tenant_id = ?",
            vec![tenant_id.to_owned().into()],
        ))
        .await?;
    let Some(row) = rows.rows.first() else {
        return Ok((None, None));
    };
    // A NULL column and a column this read cannot make sense of are the
    // same answer here: no bound was recorded.
    let count = |column: &str| {
        row.get::<i64>(column)
            .and_then(|value| u64::try_from(value).ok())
    };
    Ok((
        count("conversations_per_month"),
        count("daily_token_ceiling"),
    ))
}

/// One meter's spend in the window containing `at`.
async fn read_meter(
    db: &dyn Database,
    tenant_id: &str,
    meter: &str,
    period: Period,
    at: OffsetDateTime,
) -> Result<u64, Problem> {
    Ok(Usage::new(USAGE_TABLE)
        .read(db, tenant_id, meter, period, at)
        .await?)
}

/// The 402. The three numbers ride as RFC 9457 §3.2 extension members so
/// a billing integration reads the refusal as data, not prose.
fn quota_exceeded(request_id: &str, used: u64, limit: u64) -> Problem {
    Problem::new(&QUOTA_EXCEEDED)
        .with_extension("meter", METER_CONVERSATIONS)
        .with_extension("used", used)
        .with_extension("limit", limit)
        .instance(request_id)
}

/// The 429. `Retry-After` is the delta-seconds to the day reset, core's
/// own rounding — rounded up, so a client never waits less than the
/// truth — added on the built `Response` because a `Problem` carries no
/// headers, exactly as `messages::unavailable` does.
fn ceiling_reached(request_id: &str, spent: &Exhausted) -> Response {
    let mut response = Problem::new(&TOKEN_CEILING_REACHED)
        .with_extension("meter", METER_MODEL_TOKENS)
        .with_extension("used", spent.used)
        .with_extension("limit", spent.limit)
        .instance(request_id)
        .into_response();
    let millis = u64::try_from(spent.retry_after.as_millis()).unwrap_or(u64::MAX);
    let secs = millis.div_ceil(1_000).max(1);
    if let Ok(value) = HeaderValue::from_str(&secs.to_string()) {
        response.headers_mut().insert(header::RETRY_AFTER, value);
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use cratefield_core::lint_portable_sql;

    /// The migration must ship core's [`Usage`] DDL to the character: the
    /// guarded upserts bind by column name, and a hand-edited table would
    /// fail at runtime rather than at compile time.
    #[test]
    fn the_migration_ships_core_s_usage_ddl() {
        let sql = include_str!("../migrations/sqlite/0011_quotas.sql");
        assert!(
            sql.contains(&Usage::new(USAGE_TABLE).create_table_sql()),
            "0011_quotas.sql must carry Usage::create_table_sql() verbatim",
        );
        assert_eq!(
            lint_portable_sql(sql),
            Vec::new(),
            "the migration must be portable SQL on both engines",
        );
    }

    /// `completion_tokens` counts the cached subset once: core already
    /// reports it inside `input_tokens`.
    #[test]
    fn cached_input_tokens_are_counted_once() {
        let plain = Completion::new("a", "fake-fast").usage(100, 20);
        let cached = Completion::new("a", "fake-fast")
            .usage(100, 20)
            .cached_input_tokens(80);
        assert_eq!(completion_tokens(&plain), 120);
        assert_eq!(
            completion_tokens(&cached),
            completion_tokens(&plain),
            "caching is a discount, not extra spend",
        );
    }
}
