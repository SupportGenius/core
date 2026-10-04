//! Daily analytics rollups (issue #36): the deflection and escalation
//! numbers `/v1/support/analytics` reads.
//!
//! The rollup is a **recompute**, not an accumulator, and that is the
//! whole design. Reading a day's numbers straight from the live tables
//! would mean a scan whose cost grows with a workspace's history on every
//! dashboard load, and an incident that changed a fact after the fact (a
//! rejected ticket, a re-indexed source) would never reach a counter that
//! had already been incremented. So the scheduled sweep recomputes the
//! trailing [`ROLLUP_DAYS`] days from the source tables and installs each
//! day's rows with a delete-then-insert in **one** `batch_atomic`: running
//! it twice, or running it after a backfill, converges on the same
//! numbers. The routes thus read three small rollup tables and nothing
//! else — the cost of a dashboard is the range, never the corpus.
//!
//! Bucketing is by **UTC day**, taken from the stored timestamp's date
//! part (every timestamp here is written from a `Clock` in UTC, so the
//! first ten characters of the RFC 3339 value are its UTC date).
//! Conversations are bucketed by the day they were *created*; turns by
//! the day the turn was written. One conversation counter reaches across
//! days: `handed_off` counts, among the conversations created on the day,
//! those that have *any* assistant `handoff` turn as of rollup time — a
//! conversation created today and escalated tomorrow is a deflection
//! failure of today's cohort, not tomorrow's.
//!
//! The ticket columns come from `module-escalation`, which this module
//! may not depend on; they arrive through the [`TicketStats`] port, the
//! composition wires the two together (see `crates/composition`), and an
//! unwired deployment reports zeros there rather than refusing to roll
//! up its own numbers.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use cratefield_core::{
    AnyError, BoxFuture, Clock, Database, DbError, ModuleContext, Statement, SystemClock,
};
use sea_query::{Alias, Expr, Func, JoinType, Order, Query};
use serde::Serialize;

use crate::handlers::query_terms;
use crate::store::{self, ROLE_ASSISTANT, ROLE_USER};

/// How many days one scheduled recompute refreshes: today and the six
/// before it. Long enough that a kick or a late write is picked up on the
/// next tick, short enough that a cron tick stays bounded — the daily
/// sweep rewrites a fixed window, not the whole history.
///
/// The window is also why a cohort day's `handed_off` eventually freezes:
/// whether a conversation created on `day` escalated is re-derived only
/// while `day` is in the window, so a handoff more than [`ROLLUP_DAYS`] − 1
/// days after the conversation opened is never folded back in.
pub const ROLLUP_DAYS: i64 = 7;

/// The most chunk ids one `sg_chunks` lookup asks about at once. A busy
/// day's cited chunks can outnumber a backend's bind parameters (SQLite's
/// historical limit is 999), so the ids are resolved in batches this
/// size rather than one unbounded `IN`.
const CHUNK_LOOKUP_BATCH: usize = 500;

/// One day's ticket counts for one tenant, as the [`TicketStats`] port
/// hands them over. The escalation module owns the ticket and event
/// tables; this is the whole shape support needs from them, so nothing
/// else crosses the seam.
///
/// `duplicates` counts the accepted-duplicate event (`linked` — the judge
/// recognising an escalation as a duplicate and linking it to an existing
/// ticket), *not* `duplicate_ignored`, which records the opposite: a named
/// duplicate the judge was not shown, so nothing was linked. Which event
/// kind each field maps to is the adapter's business (see
/// `crates/module-escalation`), since the kinds are that module's
/// vocabulary.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TicketCounts {
    pub tenant_id: String,
    pub filed: i64,
    pub rejected: i64,
    pub needs_info: i64,
    pub duplicates: i64,
    pub dead_lettered: i64,
}

/// The seam support reaches escalation's ticket events through, and why
/// it is a port rather than a dependency (the same reasoning as
/// [`crate::HandoffSink`]): modules never depend on each other, so support
/// declares the smallest query it needs and the composition adapts
/// escalation to it. A `Support` built with no port keeps working: the
/// five ticket columns roll up as zero.
pub trait TicketStats: Send + Sync {
    /// Per-tenant counts of the day's ticket events, one entry per tenant
    /// with any on `day` (a tenant absent from the result counts zero).
    /// `day` is a UTC `YYYY-MM-DD`; the events are matched on the date
    /// part of their timestamp.
    fn day_counts<'a>(
        &'a self,
        db: &'a dyn Database,
        day: &'a str,
    ) -> BoxFuture<'a, Result<Vec<TicketCounts>, DbError>>;
}

/// One tenant's running totals while a day is recomputed, before they
/// become the three rollup tables' rows.
#[derive(Default)]
struct Accumulator {
    conversations: i64,
    answered: i64,
    clarify: i64,
    handoff: i64,
    handed_off: i64,
    filed: i64,
    rejected: i64,
    needs_info: i64,
    duplicates: i64,
    dead_lettered: i64,
    /// The day's assistant-turn confidences, sorted into a median at the
    /// end; kept raw because the median of an even count is the middle
    /// pair's mean, which no incremental sum can produce.
    confidences: Vec<i64>,
    /// Normalized term → hits, for this tenant's unanswered turns.
    gaps: HashMap<String, i64>,
    /// Source id → the day's answered turns that cited it at least once.
    citations: HashMap<String, i64>,
}

/// One `sg_daily_stats` row, as the `/analytics` route serializes it: the
/// field names are the column names, so the API and the schema read alike.
#[derive(Serialize)]
pub(crate) struct DayRow {
    pub day: String,
    pub conversations: i64,
    pub answered: i64,
    pub clarify: i64,
    pub handoff: i64,
    pub handed_off: i64,
    pub filed: i64,
    pub rejected: i64,
    pub needs_info: i64,
    pub duplicates: i64,
    pub dead_lettered: i64,
    pub median_confidence: Option<i64>,
}

/// A `sg_daily_citations` row joined to its source's title, for the
/// most-cited list.
#[derive(Serialize)]
pub(crate) struct CitedSource {
    pub source_id: String,
    pub title: String,
    pub cites: i64,
}

/// A tenant's current source that earned no citation in the range.
#[derive(Serialize)]
pub(crate) struct SourceRef {
    pub source_id: String,
    pub title: String,
}

/// Recomputes `day` (UTC `YYYY-MM-DD`) for every tenant and installs the
/// result: the day's rows in all three rollup tables are deleted and the
/// freshly computed ones inserted in one `batch_atomic`, so a reader sees
/// either the old day or the new one, never a half-rewritten one. Pure
/// with respect to the day — running it again after the underlying data
/// changed recomputes from the source tables, and running it unchanged is
/// a no-op in effect.
///
/// `ticket_stats` is the optional escalation seam; without it the ticket
/// columns are zero.
///
/// # Errors
///
/// The database's error from any read or the final batch.
pub async fn rollup_day(
    db: &dyn Database,
    ticket_stats: Option<&dyn TicketStats>,
    day: &str,
) -> Result<(), DbError> {
    let mut tenants: BTreeMap<String, Accumulator> = BTreeMap::new();

    // The day's conversations, counted per tenant.
    read_conversations(db, day, &mut tenants).await?;

    // Every assistant turn of the day, in one read: the outcome counters,
    // the confidences the median comes from, the gap candidates, and the
    // answered turns whose citations become the day's citation counts.
    let mut gap_candidates: Vec<(String, String, i64)> = Vec::new();
    let mut cited_turns: Vec<(String, Vec<String>)> = Vec::new();
    read_turns(db, day, &mut tenants, &mut gap_candidates, &mut cited_turns).await?;

    // The day's user messages, so a gap candidate can recover the question
    // it answered (its own turn's user message is `seq - 1`, written at
    // the same instant, hence the same day).
    let user_bodies = read_user_bodies(db, day).await?;
    for (tenant_id, conversation_id, assistant_seq) in gap_candidates {
        let Some(body) = user_bodies.get(&(tenant_id.clone(), conversation_id, assistant_seq - 1))
        else {
            continue;
        };
        let acc = tenants.entry(tenant_id).or_default();
        for term in query_terms(body) {
            *acc.gaps.entry(term).or_default() += 1;
        }
    }

    read_handed_off(db, day, &mut tenants).await?;
    resolve_citations(db, &cited_turns, &mut tenants).await?;

    // The escalation seam, last so a failure to reach it fails the whole
    // recompute before anything is written.
    if let Some(stats) = ticket_stats {
        for counts in stats.day_counts(db, day).await? {
            let acc = tenants.entry(counts.tenant_id).or_default();
            acc.filed = counts.filed;
            acc.rejected = counts.rejected;
            acc.needs_info = counts.needs_info;
            acc.duplicates = counts.duplicates;
            acc.dead_lettered = counts.dead_lettered;
        }
    }

    write_day(db, day, tenants).await
}

/// Bumps each tenant's `conversations` counter for the day.
async fn read_conversations(
    db: &dyn Database,
    day: &str,
    tenants: &mut BTreeMap<String, Accumulator>,
) -> Result<(), DbError> {
    let mut select = Query::select();
    select
        .columns(["id", "tenant_id"])
        .from(iden("sg_conversations"))
        .and_where(Expr::col(iden("created_at")).like(day_pattern(day)));
    for row in &db.query(&Statement::render(&select)).await?.rows {
        let tenant_id = row.get::<String>("tenant_id").unwrap_or_default();
        tenants.entry(tenant_id).or_default().conversations += 1;
    }
    Ok(())
}

/// Reads the day's assistant turns once, for every turn-derived number:
/// the answered/clarify/handoff counters, the confidences the median comes
/// from, the gap candidates, and the chunks each answered turn cited.
async fn read_turns(
    db: &dyn Database,
    day: &str,
    tenants: &mut BTreeMap<String, Accumulator>,
    gap_candidates: &mut Vec<(String, String, i64)>,
    cited_turns: &mut Vec<(String, Vec<String>)>,
) -> Result<(), DbError> {
    let mut select = Query::select();
    select
        .columns([
            "tenant_id",
            "conversation_id",
            "seq",
            "outcome",
            "confidence_pct",
            "citations",
            "retrieved_chunks",
        ])
        .from(iden("sg_messages"))
        .and_where(Expr::col(iden("role")).eq(ROLE_ASSISTANT))
        .and_where(Expr::col(iden("created_at")).like(day_pattern(day)));
    for row in &db.query(&Statement::render(&select)).await?.rows {
        let tenant_id = row.get::<String>("tenant_id").unwrap_or_default();
        let conversation_id = row.get::<String>("conversation_id").unwrap_or_default();
        let seq = row.get::<i64>("seq").unwrap_or_default();
        let outcome = row.get::<String>("outcome").unwrap_or_default();
        let acc = tenants.entry(tenant_id.clone()).or_default();
        match outcome.as_str() {
            "answered" => acc.answered += 1,
            "clarify" => acc.clarify += 1,
            "handoff" => acc.handoff += 1,
            _ => {}
        }
        if let Some(confidence) = row.get::<i64>("confidence_pct") {
            acc.confidences.push(confidence);
        }
        // Nothing retrieved *and* no answer: the question retrieval could
        // not serve, so its terms are the day's gap. A turn that retrieved
        // nothing but still answered (the model did not cite) is not a
        // corpus gap in the same sense.
        if matches!(outcome.as_str(), "handoff" | "clarify")
            && row.get::<i64>("retrieved_chunks") == Some(0)
        {
            gap_candidates.push((tenant_id.clone(), conversation_id, seq));
        }
        if outcome == "answered"
            && let Some(raw) = row.get::<String>("citations")
        {
            let mut chunks = citation_chunk_ids(&raw);
            // One turn, one entry: a passage cited twice in one answer is
            // still that turn citing the source once.
            chunks.sort_unstable();
            chunks.dedup();
            if !chunks.is_empty() {
                cited_turns.push((tenant_id.clone(), chunks));
            }
        }
    }
    Ok(())
}

/// The day's user messages, keyed by tenant/conversation/seq, so a gap
/// candidate can recover the question it answered.
async fn read_user_bodies(
    db: &dyn Database,
    day: &str,
) -> Result<HashMap<(String, String, i64), String>, DbError> {
    let mut bodies = HashMap::new();
    let mut select = Query::select();
    select
        .columns(["tenant_id", "conversation_id", "seq", "body"])
        .from(iden("sg_messages"))
        .and_where(Expr::col(iden("role")).eq(ROLE_USER))
        .and_where(Expr::col(iden("created_at")).like(day_pattern(day)));
    for row in &db.query(&Statement::render(&select)).await?.rows {
        let key = (
            row.get::<String>("tenant_id").unwrap_or_default(),
            row.get::<String>("conversation_id").unwrap_or_default(),
            row.get::<i64>("seq").unwrap_or_default(),
        );
        bodies.insert(key, row.get::<String>("body").unwrap_or_default());
    }
    Ok(bodies)
}

/// `handed_off`: among the day's conversations, those the conversation
/// itself escalated — any assistant `handoff` turn, whenever it ran. One
/// join from the day's conversations to their handoff turns, rather than
/// carrying a list of ids into an `IN` that a busy day could outgrow.
async fn read_handed_off(
    db: &dyn Database,
    day: &str,
    tenants: &mut BTreeMap<String, Accumulator>,
) -> Result<(), DbError> {
    let conv = Alias::new("c");
    let msg = Alias::new("m");
    let mut select = Query::select();
    select
        .distinct()
        .columns([
            (conv.clone(), Alias::new("tenant_id")),
            (msg.clone(), Alias::new("conversation_id")),
        ])
        .from_as(iden("sg_conversations"), conv.clone())
        .join_as(
            JoinType::InnerJoin,
            iden("sg_messages"),
            msg.clone(),
            Expr::col((conv.clone(), iden("id"))).equals((msg.clone(), iden("conversation_id"))),
        )
        .and_where(Expr::col((conv, iden("created_at"))).like(day_pattern(day)))
        .and_where(Expr::col((msg.clone(), iden("role"))).eq(ROLE_ASSISTANT))
        .and_where(Expr::col((msg, iden("outcome"))).eq("handoff"));
    let mut counts: HashMap<String, i64> = HashMap::new();
    for row in &db.query(&Statement::render(&select)).await?.rows {
        let tenant_id = row.get::<String>("tenant_id").unwrap_or_default();
        *counts.entry(tenant_id).or_default() += 1;
    }
    for (tenant_id, handed_off) in counts {
        tenants.entry(tenant_id).or_default().handed_off = handed_off;
    }
    Ok(())
}

/// Counts, per tenant and source, the day's answered turns that cited at
/// least one of that source's chunks — a turn quoting two passages of one
/// source counts once for it, not twice. Chunk ids are resolved to their
/// sources in bounded batches, since a day's cited chunks can outnumber a
/// backend's bind parameters.
async fn resolve_citations(
    db: &dyn Database,
    cited_turns: &[(String, Vec<String>)],
    tenants: &mut BTreeMap<String, Accumulator>,
) -> Result<(), DbError> {
    // Every chunk the day cited, per tenant, deduplicated.
    let mut wanted: HashMap<String, Vec<String>> = HashMap::new();
    for (tenant_id, chunks) in cited_turns {
        wanted
            .entry(tenant_id.clone())
            .or_default()
            .extend(chunks.iter().cloned());
    }

    // Chunk id → source id, per tenant.
    let mut sources: HashMap<String, HashMap<String, String>> = HashMap::new();
    for (tenant_id, mut chunk_ids) in wanted {
        chunk_ids.sort_unstable();
        chunk_ids.dedup();
        for batch in chunk_ids.chunks(CHUNK_LOOKUP_BATCH) {
            let mut select = Query::select();
            select
                .columns(["id", "source_id"])
                .from(iden("sg_chunks"))
                .and_where(Expr::col(iden("tenant_id")).eq(tenant_id.as_str()))
                .and_where(Expr::col(iden("id")).is_in(batch.to_vec()));
            for row in &db.query(&Statement::render(&select)).await?.rows {
                if let (Some(id), Some(source_id)) =
                    (row.get::<String>("id"), row.get::<String>("source_id"))
                {
                    sources
                        .entry(tenant_id.clone())
                        .or_default()
                        .insert(id, source_id);
                }
            }
        }
    }

    for (tenant_id, chunks) in cited_turns {
        let tenant_sources = sources.get(tenant_id);
        let mut cited: Vec<&str> = chunks
            .iter()
            .filter_map(|chunk_id| tenant_sources?.get(chunk_id).map(String::as_str))
            .collect();
        cited.sort_unstable();
        cited.dedup();
        let acc = tenants.entry(tenant_id.clone()).or_default();
        for source_id in cited {
            *acc.citations.entry(source_id.to_owned()).or_default() += 1;
        }
    }
    Ok(())
}

/// Recomputes `days` days ending at `today` (inclusive), newest first.
/// [`rollup_day`]'s idempotence is what makes the overlap harmless: a day
/// recomputed again from unchanged data rewrites the same rows.
///
/// # Errors
///
/// The database's error, or a malformed `today`.
pub async fn rollup_recent(
    db: &dyn Database,
    ticket_stats: Option<&dyn TicketStats>,
    today: &str,
    days: i64,
) -> Result<(), DbError> {
    let base = parse_day(today)?;
    for offset in 0..days {
        let day = format_day(base - time::Duration::days(offset))?;
        rollup_day(db, ticket_stats, &day).await?;
    }
    Ok(())
}

/// [`crate::Support::scheduled`]'s analytics third: recompute the trailing
/// [`ROLLUP_DAYS`] for whatever tenants the database holds. A context with
/// no database has nothing to roll up (the same honest no-op the other
/// sweeps give a port they lack).
pub(crate) async fn scheduled(
    ctx: &ModuleContext,
    ticket_stats: Option<&dyn TicketStats>,
) -> Result<(), AnyError> {
    let Some(db) = ctx.ports.db.clone() else {
        return Ok(());
    };
    let clock: Arc<dyn Clock> = ctx
        .ports
        .clock
        .clone()
        .unwrap_or_else(|| Arc::new(SystemClock));
    let now = store::iso_now(clock.as_ref());
    // Every stored timestamp is UTC RFC 3339, so its first ten characters
    // are today's UTC date.
    let Some(today) = now.get(..10) else {
        return Ok(());
    };
    rollup_recent(db.as_ref(), ticket_stats, today, ROLLUP_DAYS)
        .await
        .map_err(|err| Box::new(err) as AnyError)
}

/// Installs a recomputed day: delete the day from all three rollup tables,
/// then insert the fresh rows — one `batch_atomic`, so the day flips as a
/// unit.
async fn write_day(
    db: &dyn Database,
    day: &str,
    tenants: BTreeMap<String, Accumulator>,
) -> Result<(), DbError> {
    let mut statements = Vec::new();
    for table in ["sg_daily_stats", "sg_daily_gaps", "sg_daily_citations"] {
        let mut delete = Query::delete();
        delete
            .from_table(iden(table))
            .and_where(Expr::col(iden("day")).eq(day));
        statements.push(Statement::render(&delete));
    }

    if !tenants.is_empty() {
        let mut stats = Query::insert();
        stats.into_table(iden("sg_daily_stats")).columns([
            "tenant_id",
            "day",
            "conversations",
            "answered",
            "clarify",
            "handoff",
            "handed_off",
            "filed",
            "rejected",
            "needs_info",
            "duplicates",
            "dead_lettered",
            "median_confidence",
        ]);
        let mut gaps = Query::insert();
        gaps.into_table(iden("sg_daily_gaps"))
            .columns(["tenant_id", "day", "term", "hits"]);
        let mut citations = Query::insert();
        citations.into_table(iden("sg_daily_citations")).columns([
            "tenant_id",
            "day",
            "source_id",
            "cites",
        ]);
        // Whether the two sparse tables ended up with any row at all: a
        // `values()` insert with no values is not a statement.
        let mut has_gaps = false;
        let mut has_citations = false;

        for (tenant_id, mut acc) in tenants {
            stats.values_panic([
                tenant_id.clone().into(),
                day.to_owned().into(),
                acc.conversations.into(),
                acc.answered.into(),
                acc.clarify.into(),
                acc.handoff.into(),
                acc.handed_off.into(),
                acc.filed.into(),
                acc.rejected.into(),
                acc.needs_info.into(),
                acc.duplicates.into(),
                acc.dead_lettered.into(),
                median(std::mem::take(&mut acc.confidences)).into(),
            ]);
            // Sorted so a day's gap and citation rows insert in a stable
            // order — the rows are a set, but a reproducible write makes
            // the idempotence test's row comparison trivial.
            let mut terms: Vec<(String, i64)> = acc.gaps.into_iter().collect();
            terms.sort();
            for (term, hits) in terms {
                has_gaps = true;
                gaps.values_panic([
                    tenant_id.clone().into(),
                    day.to_owned().into(),
                    term.into(),
                    hits.into(),
                ]);
            }
            let mut sources: Vec<(String, i64)> = acc.citations.into_iter().collect();
            sources.sort();
            for (source_id, cites) in sources {
                has_citations = true;
                citations.values_panic([
                    tenant_id.clone().into(),
                    day.to_owned().into(),
                    source_id.into(),
                    cites.into(),
                ]);
            }
        }
        statements.push(Statement::render(&stats));
        if has_gaps {
            statements.push(Statement::render(&gaps));
        }
        if has_citations {
            statements.push(Statement::render(&citations));
        }
    }

    db.batch_atomic(&statements).await
}

/// A day's `sg_daily_stats` rows for one tenant, in day order.
pub(crate) async fn day_stats(
    db: &dyn Database,
    tenant_id: &str,
    from: &str,
    to: &str,
) -> Result<Vec<DayRow>, DbError> {
    let mut select = Query::select();
    select
        .columns([
            "day",
            "conversations",
            "answered",
            "clarify",
            "handoff",
            "handed_off",
            "filed",
            "rejected",
            "needs_info",
            "duplicates",
            "dead_lettered",
            "median_confidence",
        ])
        .from(iden("sg_daily_stats"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden("day")).gte(from))
        .and_where(Expr::col(iden("day")).lte(to))
        .order_by(iden("day"), Order::Asc);
    Ok(db
        .query(&Statement::render(&select))
        .await?
        .rows
        .iter()
        .map(|row| DayRow {
            day: row.get("day").unwrap_or_default(),
            conversations: row.get("conversations").unwrap_or_default(),
            answered: row.get("answered").unwrap_or_default(),
            clarify: row.get("clarify").unwrap_or_default(),
            handoff: row.get("handoff").unwrap_or_default(),
            handed_off: row.get("handed_off").unwrap_or_default(),
            filed: row.get("filed").unwrap_or_default(),
            rejected: row.get("rejected").unwrap_or_default(),
            needs_info: row.get("needs_info").unwrap_or_default(),
            duplicates: row.get("duplicates").unwrap_or_default(),
            dead_lettered: row.get("dead_lettered").unwrap_or_default(),
            median_confidence: row.get("median_confidence"),
        })
        .collect())
}

/// The range's gap terms summed per term, most hits first (`term`
/// ascending breaks a tie), capped at `limit`.
pub(crate) async fn gap_terms(
    db: &dyn Database,
    tenant_id: &str,
    from: &str,
    to: &str,
    limit: usize,
) -> Result<Vec<(String, i64)>, DbError> {
    let mut select = Query::select();
    select
        .expr_as(Expr::col(iden("term")), Alias::new("term"))
        .expr_as(Func::sum(Expr::col(iden("hits"))), Alias::new("hits"))
        .from(iden("sg_daily_gaps"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden("day")).gte(from))
        .and_where(Expr::col(iden("day")).lte(to))
        .group_by_col(iden("term"))
        .order_by(iden("hits"), Order::Desc)
        .order_by(iden("term"), Order::Asc)
        .limit(u64::try_from(limit).unwrap_or(u64::MAX));
    Ok(db
        .query(&Statement::render(&select))
        .await?
        .rows
        .iter()
        .filter_map(|row| Some((row.get::<String>("term")?, row.get::<i64>("hits")?)))
        .collect())
}

/// The range's most-cited sources, most cites first (`source_id`
/// ascending breaks a tie), capped at `limit`, each with its title — one
/// grouped read joined to `sg_sources`, so a title costs no second
/// round trip. A source deleted since the rollup left a `NULL` title and
/// reads as the empty string.
pub(crate) async fn most_cited(
    db: &dyn Database,
    tenant_id: &str,
    from: &str,
    to: &str,
    limit: usize,
) -> Result<Vec<CitedSource>, DbError> {
    let cites = Alias::new("dc");
    let sources = Alias::new("s");
    let mut select = Query::select();
    select
        .expr_as(
            Expr::col((cites.clone(), iden("source_id"))),
            Alias::new("source_id"),
        )
        .expr_as(
            Func::sum(Expr::col((cites.clone(), iden("cites")))),
            Alias::new("cites"),
        )
        .expr_as(
            Expr::col((sources.clone(), iden("title"))),
            Alias::new("title"),
        )
        .from_as(iden("sg_daily_citations"), cites.clone())
        .join_as(
            JoinType::LeftJoin,
            iden("sg_sources"),
            sources.clone(),
            Expr::col((cites.clone(), iden("source_id"))).equals((sources.clone(), iden("id"))),
        )
        .and_where(Expr::col((cites.clone(), iden("tenant_id"))).eq(tenant_id))
        .and_where(Expr::col((cites.clone(), iden("day"))).gte(from))
        .and_where(Expr::col((cites.clone(), iden("day"))).lte(to))
        .group_by_col((cites.clone(), iden("source_id")))
        .group_by_col((sources, iden("title")))
        .order_by(iden("cites"), Order::Desc)
        .order_by(iden("source_id"), Order::Asc)
        .limit(u64::try_from(limit).unwrap_or(u64::MAX));
    Ok(db
        .query(&Statement::render(&select))
        .await?
        .rows
        .iter()
        .filter_map(|row| {
            Some(CitedSource {
                source_id: row.get("source_id")?,
                title: row.get("title").unwrap_or_default(),
                cites: row.get("cites")?,
            })
        })
        .collect())
}

/// The tenant's current sources that earned no citation in the range,
/// id order, capped at `limit`. The not-cited test is a `NOT IN`
/// subquery over the range's citation rows, so the read is the source
/// list and one index probe, never a scan of the citation table per
/// source.
pub(crate) async fn never_cited(
    db: &dyn Database,
    tenant_id: &str,
    from: &str,
    to: &str,
    limit: usize,
) -> Result<Vec<SourceRef>, DbError> {
    let mut cited = Query::select();
    cited
        .column(iden("source_id"))
        .from(iden("sg_daily_citations"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden("day")).gte(from))
        .and_where(Expr::col(iden("day")).lte(to));

    let mut select = Query::select();
    select
        .columns(["id", "title"])
        .from(iden("sg_sources"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden("id")).not_in_subquery(cited))
        .order_by(iden("id"), Order::Asc)
        .limit(u64::try_from(limit).unwrap_or(u64::MAX));
    Ok(db
        .query(&Statement::render(&select))
        .await?
        .rows
        .iter()
        .filter_map(|row| {
            Some(SourceRef {
                source_id: row.get("id")?,
                title: row.get("title")?,
            })
        })
        .collect())
}

/// The median of `values`, or `None` when there are none: the middle value
/// for an odd count, and the floor of the middle pair's mean for an even
/// one — confidence is a non-negative integer percentage, so the integer
/// division is a floor.
fn median(mut values: Vec<i64>) -> Option<i64> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let mid = values.len() / 2;
    Some(if values.len() % 2 == 1 {
        values[mid]
    } else {
        // `midpoint` rounds toward zero, which the comment above notes is
        // a floor for the non-negative confidences stored here.
        i64::midpoint(values[mid - 1], values[mid])
    })
}

/// The chunk ids named by one assistant row's stored `citations` JSON — a
/// `[{chunk_id, quote}]` array. Anything malformed or foreign is skipped:
/// a rollup is not the place to fail a day over one bad row.
fn citation_chunk_ids(raw: &str) -> Vec<String> {
    let Ok(serde_json::Value::Array(items)) = serde_json::from_str(raw) else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|item| item.get("chunk_id")?.as_str().map(str::to_owned))
        .collect()
}

/// The UTC day as a `LIKE` prefix: `YYYY-MM-DD` times a stored RFC 3339
/// date part is that day. `day` is validated `YYYY-MM-DD` upstream, so the
/// only wildcards in play are the one we add.
fn day_pattern(day: &str) -> String {
    format!("{day}%")
}

fn iden(name: &str) -> Alias {
    Alias::new(name)
}

/// Parses a UTC `YYYY-MM-DD` into a date. Shared with the handlers, which
/// validate a request's `from`/`to` with exactly the parser the rollup
/// buckets by.
pub(crate) fn parse_day(day: &str) -> Result<time::Date, DbError> {
    time::Date::parse(day, &time::format_description::well_known::Iso8601::DATE)
        .map_err(|err| DbError::Query(format!("analytics: bad day {day:?}: {err}")))
}

/// Formats a date back to UTC `YYYY-MM-DD`.
pub(crate) fn format_day(date: time::Date) -> Result<String, DbError> {
    date.format(&time::format_description::well_known::Iso8601::DATE)
        .map_err(|err| DbError::Query(format!("analytics: {err}")))
}

#[cfg(test)]
mod tests {
    use super::median;

    #[test]
    fn median_is_the_middle_value_or_the_floor_of_the_middle_pair() {
        assert_eq!(median(Vec::new()), None);
        assert_eq!(median(vec![90]), Some(90));
        // Odd: the middle of the sorted values, whatever order they came
        // in.
        assert_eq!(median(vec![90, 20, 50]), Some(50));
        // Even: the floor of the middle pair's mean.
        assert_eq!(median(vec![20, 40]), Some(30));
        assert_eq!(median(vec![21, 40]), Some(30));
        assert_eq!(median(vec![0, 100]), Some(50));
    }
}
