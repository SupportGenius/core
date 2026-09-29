//! Every query the support module runs, built with `sea_query` and
//! rendered through [`cratefield_core::Statement::render`] (ADR 0004:
//! module code never writes raw SQL strings — the migrations are the only
//! hand-written SQL in this crate, and the `fz doctor` lint keeps them
//! portable).
//!
//! The isolation boundary is [`tenant_id`]: every statement in this file
//! filters on it, whatever credential was verified upstream. Nothing here
//! is reachable without a tenant id, and nothing here ignores one.
//!
//! Most of this module is private to the crate. The public surface is
//! the retrieval bounds (so callers can reason about a query's cost) and
//! [`insert_source_with_chunks`], the one ingest primitive, which bulk
//! importers and the tests drive directly — with the same atomic-batch
//! statistics maintenance the HTTP route gets.

use std::collections::HashMap;

use cratefield_core::{Database, DbError, Statement};
use sea_query::{Alias, Expr, Func, OnConflict, Query};
use time::format_description::well_known::Rfc3339;

use crate::bm25::{Corpus, Posting};
use crate::chunk::Chunk;

/// The only tenant status that authenticates. Anything else (a future
/// `suspended`, `closed`) fails key verification with the same
/// indistinguishable 401 as an unknown key.
pub(crate) const STATUS_ACTIVE: &str = "active";

/// The label recorded for the first key `POST /admin/tenants` mints.
pub(crate) const FIRST_KEY_LABEL: &str = "primary";

/// Distinct terms one retrieval may look up and fetch postings for:
/// after deduping, the first [`MAX_QUERY_TERMS`] terms of the query, the
/// rest dropped. A support question with more than 32 distinct words is
/// not a question, and the cap is what keeps a pathological query
/// (`?q=` a novel's worth of text) from turning into that many database
/// reads. 32 query terms cost at most 32 statistics lookups and
/// 32 postings fetches — a query's cost is bounded by the query, never
/// by the corpus.
pub const MAX_QUERY_TERMS: usize = 32;

/// Postings fetched per term: the term's chunks by tf, highest first
/// (`chunk_id` ascending breaks ties deterministically). A chunk outside a
/// term's top [`MAX_POSTINGS_PER_TERM`] is scored without that term's
/// contribution. 128 because the ranker's output is capped far below it
/// anyway (`/search` shows at most 50, `/messages` grounds in 6), so a
/// chunk with a term's top tf almost always outranks the cut; because it
/// keeps a full [`MAX_QUERY_TERMS`]-term query under ~4k posting rows;
/// and because it is small enough that the per-term read runs straight
/// off the `(tenant_id, term, tf DESC, chunk_id)` index with no sort.
pub const MAX_POSTINGS_PER_TERM: usize = 128;

/// A term carried by more than this fraction of a tenant's chunks is a
/// stopword by statistics and is dropped before any postings are
/// fetched. Language-neutral and self-maintaining: whatever the corpus
/// treats as filler (`the`, `and`, …) crosses the line on its own, and
/// the line moves with the corpus rather than a word list. The cost is
/// the point — such a term's postings fetch alone would scale with the
/// whole corpus, for a contribution BM25's idf already weights to almost
/// nothing (`ln(1 + 0.5/df)` shrinks toward zero as df grows).
pub const STOPWORD_DF_RATIO: f64 = 0.5;

/// Smallest corpus the stopword ratio above is trusted in. Below a
/// handful of chunks the ratio carries no information — in a
/// one-chunk corpus every term scores df/N = 1, so the strict rule
/// would drop every word of the only document and a two-word query
/// would retrieve nothing from a two-document index. BM25's idf
/// already discounts common terms at this scale, so until there are
/// enough chunks for "in more than half the corpus" to mean
/// something, nothing is dropped for being common.
pub const STOPWORD_MIN_CHUNKS: u64 = 8;

pub(crate) struct TenantRow {
    pub id: String,
    pub name: String,
    pub status: String,
    pub created_at: String,
}

pub(crate) struct ApiKeyRow {
    pub id: String,
    pub tenant_id: String,
    pub kid: String,
    pub label: String,
    pub created_at: String,
}

/// A source document's row: what `sg_sources` holds. Public because
/// [`insert_source_with_chunks`] is the bulk-ingest primitive — callers
/// outside the crate (the benchmark suite, future import tools) construct
/// it directly rather than going through the HTTP handler.
pub struct SourceRow {
    pub id: String,
    pub tenant_id: String,
    pub title: String,
    pub url: Option<String>,
    pub byte_len: i64,
    pub created_at: String,
}

/// What `chunks_by_id` returns for one ranked hit: enough to quote the
/// chunk back with its source, or to show it to the model.
pub(crate) struct ChunkRow {
    pub id: String,
    pub source_id: String,
    pub title: String,
    pub body: String,
}

/// Renders and runs one statement.
async fn execute(db: &dyn Database, stmt: &Statement) -> Result<(), DbError> {
    db.execute(stmt).await.map(|_| ())
}

/// Postings and the per-batch statistic rows derived from them travel to
/// `batch_atomic` in statements of this many rows. Three or four bound
/// values per row keeps each statement far under the conservative
/// 999-parameter limit some sqlite builds still enforce.
const POSTINGS_PER_STATEMENT: usize = 120;

fn iden(name: &str) -> Alias {
    Alias::new(name)
}

pub(crate) async fn insert_tenant(db: &dyn Database, tenant: &TenantRow) -> Result<(), DbError> {
    let mut insert = Query::insert();
    insert
        .into_table(iden("sg_tenants"))
        .columns(["id", "name", "status", "created_at"])
        .values_panic([
            tenant.id.clone().into(),
            tenant.name.clone().into(),
            tenant.status.clone().into(),
            tenant.created_at.clone().into(),
        ]);
    execute(db, &Statement::render(&insert)).await
}

/// The tenant row for `id`, status included — the caller decides whether
/// the status authenticates, so the read stays single-purpose.
pub(crate) async fn find_tenant(db: &dyn Database, id: &str) -> Result<Option<TenantRow>, DbError> {
    let mut select = Query::select();
    select
        .columns(["id", "name", "status", "created_at"])
        .from(iden("sg_tenants"))
        .and_where(Expr::col(iden("id")).eq(id));
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows.rows.first().map(|row| TenantRow {
        id: row.get("id").unwrap_or_default(),
        name: row.get("name").unwrap_or_default(),
        status: row.get("status").unwrap_or_default(),
        created_at: row.get("created_at").unwrap_or_default(),
    }))
}

pub(crate) async fn insert_api_key(db: &dyn Database, key: &ApiKeyRow) -> Result<(), DbError> {
    let mut insert = Query::insert();
    insert
        .into_table(iden("sg_api_keys"))
        .columns(["id", "tenant_id", "kid", "label", "created_at"])
        .values_panic([
            key.id.clone().into(),
            key.tenant_id.clone().into(),
            key.kid.clone().into(),
            key.label.clone().into(),
            key.created_at.clone().into(),
        ]);
    execute(db, &Statement::render(&insert)).await
}

/// The source row, its chunks, the chunks' postings and the corpus
/// statistics derived from them land in **one** `batch_atomic`: a
/// half-indexed source must never be able to exist, because a source
/// whose postings are missing some terms is worse than no source —
/// searches quietly return wrong answers instead of nothing. A stats row
/// drifting from the index it describes is the same class of lie (see
/// [`corpus_stats`]), so the increments are written by the same batch
/// that writes what they count.
///
/// Exposed for bulk ingest: this is the one way to put documents into
/// the index, and it carries the whole statistics contract with it.
///
/// # Errors
/// When the batch cannot be applied in full — a duplicate source id, a
/// database outage — `batch_atomic` rolls everything back, so the source,
/// its chunks, its postings and the statistics increments all land or
/// none do.
pub async fn insert_source_with_chunks(
    db: &dyn Database,
    source: &SourceRow,
    chunks: &[Chunk],
) -> Result<(), DbError> {
    let mut statements: Vec<Statement> = Vec::new();

    let mut insert_source = Query::insert();
    insert_source
        .into_table(iden("sg_sources"))
        .columns(["id", "tenant_id", "title", "url", "byte_len", "created_at"])
        .values_panic([
            source.id.clone().into(),
            source.tenant_id.clone().into(),
            source.title.clone().into(),
            source.url.clone().into(),
            source.byte_len.into(),
            source.created_at.clone().into(),
        ]);
    statements.push(Statement::render(&insert_source));

    // The statistics increments, guarded per chunk and therefore placed
    // **before** the chunk insert: the guards ask sg_chunks whether each
    // incoming chunk already exists, and while the batch's own chunks
    // are not yet in the table, "no row" means exactly "new". A chunk
    // cannot exist at this point through any caller today — chunk ids
    // are content addresses of (tenant, source, text), the chunker
    // dedupes within a document, and a second call with the same source
    // id fails the sg_sources primary key above and rolls the whole
    // batch back — but the guard makes the increments exact even if a
    // future caller replays a source, so the stats can never be
    // double-counted into a lie.
    statements.extend(df_statements(&source.tenant_id, chunks));
    statements.extend(tenant_stats_statements(&source.tenant_id, chunks));

    let mut insert_chunks = Query::insert();
    insert_chunks.into_table(iden("sg_chunks")).columns([
        "id",
        "tenant_id",
        "source_id",
        "ordinal",
        "body",
        "term_count",
        "created_at",
    ]);
    for chunk in chunks {
        insert_chunks.values_panic([
            chunk.id.clone().into(),
            source.tenant_id.clone().into(),
            source.id.clone().into(),
            chunk.ordinal.into(),
            chunk.text.clone().into(),
            chunk.length.into(),
            source.created_at.clone().into(),
        ]);
    }
    if !chunks.is_empty() {
        statements.push(Statement::render(&insert_chunks));
    }

    // (tenant_id, term, chunk_id, tf) rows, in the chunker's term-sorted
    // order so the batch is deterministic for a given document.
    let postings: Vec<(&str, &str, &str, u32)> = chunks
        .iter()
        .flat_map(|chunk| {
            chunk.terms.iter().map(move |(term, tf)| {
                (
                    source.tenant_id.as_str(),
                    term.as_str(),
                    chunk.id.as_str(),
                    *tf,
                )
            })
        })
        .collect();
    for group in postings.chunks(POSTINGS_PER_STATEMENT) {
        let mut insert_postings = Query::insert();
        insert_postings.into_table(iden("sg_postings")).columns([
            "tenant_id",
            "term",
            "chunk_id",
            "tf",
        ]);
        for (tenant_id, term, chunk_id, tf) in group {
            insert_postings.values_panic([
                (*tenant_id).into(),
                (*term).into(),
                (*chunk_id).into(),
                (*tf).into(),
            ]);
        }
        statements.push(Statement::render(&insert_postings));
    }

    db.batch_atomic(&statements).await
}

/// The `sg_terms` upserts for one ingest batch: for every (term, chunk)
/// pair of the incoming chunks, one VALUES row, from which the statement
/// counts — grouped by term — only the pairs whose chunk does not exist
/// yet, and adds that count to whatever df the tenant already carries:
///
/// ```sql
/// INSERT INTO sg_terms (tenant_id, term, df)
/// SELECT v.column1, v.column2, COUNT(*) FROM (VALUES …) AS v
/// WHERE NOT EXISTS (SELECT 1 FROM sg_chunks WHERE …)
/// GROUP BY v.column1, v.column2
/// ON CONFLICT (tenant_id, term)
///   DO UPDATE SET df = sg_terms.df + excluded.df
/// ```
///
/// The grouping matters twice over: it merges the batch's many rows per
/// term into one out-row, so a single statement never upserts the same
/// key twice (Postgres refuses to affect one row twice in a statement),
/// and a term all of whose chunks already exist yields no row at all, so
/// its df is left exactly as it was. The count is done in SQL, not
/// trusted from Rust, because the guard is per chunk.
///
/// The VALUES rows carry `(tenant_id, chunk_id, term)` in that column
/// order (`column1..column3`, how both SQLite and Postgres name an
/// unaliased VALUES list).
fn df_statements(tenant_id: &str, chunks: &[Chunk]) -> Vec<Statement> {
    let rows: Vec<(String, String, String)> = chunks
        .iter()
        .flat_map(|chunk| {
            chunk
                .terms
                .iter()
                .map(move |(term, _)| (tenant_id.to_owned(), chunk.id.clone(), term.clone()))
        })
        .collect();
    rows.chunks(POSTINGS_PER_STATEMENT)
        .map(|group| {
            let mut select = Query::select();
            select
                .expr(Expr::col((values_alias(), Alias::new("column1"))))
                .expr(Expr::col((values_alias(), Alias::new("column3"))))
                .expr(Func::count(Expr::col((
                    values_alias(),
                    Alias::new("column2"),
                ))))
                .from_values(
                    group.iter().map(|(tenant, chunk_id, term)| {
                        (tenant.clone(), chunk_id.clone(), term.clone())
                    }),
                    values_alias(),
                )
                .and_where(Expr::exists(chunk_exists("column1", "column2")).not())
                .group_by_col((values_alias(), Alias::new("column1")))
                .group_by_col((values_alias(), Alias::new("column3")));
            let mut insert = Query::insert();
            insert
                .into_table(iden("sg_terms"))
                .columns(["tenant_id", "term", "df"])
                .select_from(select)
                .expect("select feeds exactly the three insert columns")
                .on_conflict(
                    OnConflict::columns([iden("tenant_id"), iden("term")])
                        .value(
                            iden("df"),
                            Expr::col((iden("sg_terms"), iden("df")))
                                .add(Expr::col((iden("excluded"), iden("df")))),
                        )
                        .to_owned(),
                );
            Statement::render(&insert)
        })
        .collect()
}

/// The `sg_tenant_stats` upsert for one ingest batch, the chunk-level
/// twin of [`df_statements`]: count and total term length of the
/// batch's chunks that do not exist yet, added to the tenant's row —
/// or creating it, on a tenant's first indexed chunk.
///
/// The length column is summed through `CAST(… AS BIGINT)` (portable
/// SQL both engines parse): a bare placeholder inside a VALUES list has
/// no type context, so a client that leaves parameter typing to the
/// engine would hand Postgres a `text` column and `SUM(text)` has no
/// overload. Casting makes the statement's arithmetic exact whatever
/// the adapter binds.
fn tenant_stats_statements(tenant_id: &str, chunks: &[Chunk]) -> Vec<Statement> {
    let rows: Vec<(String, String, i64)> = chunks
        .iter()
        .map(|chunk| {
            (
                tenant_id.to_owned(),
                chunk.id.clone(),
                i64::from(chunk.length),
            )
        })
        .collect();
    rows.chunks(POSTINGS_PER_STATEMENT)
        .map(|group| {
            let mut select = Query::select();
            select
                .expr(Expr::col((values_alias(), Alias::new("column1"))))
                .expr(Func::count(Expr::col((
                    values_alias(),
                    Alias::new("column2"),
                ))))
                .expr(Func::sum(Func::cast_as(
                    Expr::col((values_alias(), Alias::new("column3"))),
                    Alias::new("BIGINT"),
                )))
                .from_values(group.iter().cloned(), values_alias())
                .and_where(Expr::exists(chunk_exists("column1", "column2")).not())
                .group_by_col((values_alias(), Alias::new("column1")));
            let mut insert = Query::insert();
            insert
                .into_table(iden("sg_tenant_stats"))
                .columns(["tenant_id", "n_chunks", "total_len"])
                .select_from(select)
                .expect("select feeds exactly the three insert columns")
                .on_conflict(
                    OnConflict::column(iden("tenant_id"))
                        .value(
                            iden("n_chunks"),
                            Expr::col((iden("sg_tenant_stats"), iden("n_chunks")))
                                .add(Expr::col((iden("excluded"), iden("n_chunks")))),
                        )
                        .value(
                            iden("total_len"),
                            Expr::col((iden("sg_tenant_stats"), iden("total_len")))
                                .add(Expr::col((iden("excluded"), iden("total_len")))),
                        )
                        .to_owned(),
                );
            Statement::render(&insert)
        })
        .collect()
}

/// The alias for the ingest guards' VALUES list, and the unqualified
/// name both engines give its columns without an explicit column list.
fn values_alias() -> Alias {
    Alias::new("v")
}

/// Does a chunk with the VALUES row's tenant (`tenant_col`) and id
/// (`chunk_col`) exist? The EXISTS form keeps the guard inside the one
/// statement, so the increment can never race the check.
fn chunk_exists(tenant_col: &str, chunk_col: &str) -> sea_query::SelectStatement {
    let mut select = Query::select();
    select
        .expr(Expr::val(1_i32))
        .from(iden("sg_chunks"))
        .and_where(
            Expr::col(iden("tenant_id")).eq(Expr::col((values_alias(), Alias::new(tenant_col)))),
        )
        .and_where(Expr::col(iden("id")).eq(Expr::col((values_alias(), Alias::new(chunk_col)))));
    select
}

/// Chunk count and mean document length for one tenant's index — the two
/// numbers Okapi BM25 needs before it can score anything — read from the
/// tenant's single `sg_tenant_stats` row, which
/// [`insert_source_with_chunks`] keeps in step with `sg_chunks`. An empty
/// index (no row yet) is `Corpus { chunk_count: 0, avg_length: 0.0 }`,
/// not an error, and one primary-key read whatever the corpus size —
/// never a `COUNT`/`AVG` over the chunks themselves.
pub(crate) async fn corpus_stats(db: &dyn Database, tenant_id: &str) -> Result<Corpus, DbError> {
    let mut select = Query::select();
    select
        .columns(["n_chunks", "total_len"])
        .from(iden("sg_tenant_stats"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id));
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows.rows.first().map_or_else(
        || Corpus {
            chunk_count: 0,
            avg_length: 0.0,
        },
        |row| {
            let n_chunks: u64 = row.get("n_chunks").unwrap_or(0);
            let total_len: u64 = row.get("total_len").unwrap_or(0);
            #[expect(clippy::cast_precision_loss)]
            let avg_length = if n_chunks > 0 {
                total_len as f64 / n_chunks as f64
            } else {
                0.0
            };
            Corpus {
                chunk_count: n_chunks,
                avg_length,
            }
        },
    ))
}

/// The persisted document frequencies for `terms` — one `sg_terms` row
/// per term at most, all primary-key reads. Terms with no row have never
/// been indexed by this tenant and simply come back absent; the caller
/// treats an absent term as df 0 and drops it before fetching postings.
pub(crate) async fn term_dfs(
    db: &dyn Database,
    tenant_id: &str,
    terms: &[String],
) -> Result<HashMap<String, u64>, DbError> {
    if terms.is_empty() {
        return Ok(HashMap::new());
    }
    let mut select = Query::select();
    select
        .columns(["term", "df"])
        .from(iden("sg_terms"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden("term")).is_in(terms.to_vec()));
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows
        .rows
        .iter()
        .filter_map(|row| Some((row.get::<String>("term")?, row.get::<u64>("df")?)))
        .collect())
}

/// One term's index rows for one tenant, joined back to `sg_chunks` for
/// the document length: at most `limit` postings, the term's top chunks
/// by `tf` (`chunk_id` ascending breaks ties, so the fetch is
/// deterministic), served by `idx_sg_postings_tenant_term_tf` in exactly
/// that order — an index search, no sort, no full-index scan.
///
/// **The fetch is bounded, and the bound is honest, because df and N no
/// longer come from these rows.** `bm25::rank` takes each term's df from
/// `sg_terms` and N from `sg_tenant_stats`, both exact whatever subset of
/// postings is read; a truncated fetch cannot deflate an idf any more.
/// What truncation costs is a score, not a wrong one: a chunk outside a
/// term's top `limit` by tf is ranked without that term's contribution.
/// Per query, the reads are bounded by impact: 1 stats row, at most
/// [`MAX_QUERY_TERMS`] df rows, and at most [`MAX_QUERY_TERMS`] ×
/// [`MAX_POSTINGS_PER_TERM`] posting rows, however large the corpus.
pub(crate) async fn postings_for(
    db: &dyn Database,
    tenant_id: &str,
    term: &str,
    limit: usize,
) -> Result<Vec<Posting>, DbError> {
    let mut select = Query::select();
    select
        .expr_as(Expr::col((iden("p"), iden("term"))), Alias::new("term"))
        .expr_as(
            Expr::col((iden("p"), iden("chunk_id"))),
            Alias::new("chunk_id"),
        )
        .expr_as(Expr::col((iden("p"), iden("tf"))), Alias::new("tf"))
        .expr_as(
            Expr::col((iden("c"), iden("term_count"))),
            Alias::new("length"),
        )
        .from_as(iden("sg_postings"), iden("p"))
        .join_as(
            sea_query::JoinType::InnerJoin,
            iden("sg_chunks"),
            iden("c"),
            Expr::col((iden("c"), iden("id")))
                .eq(Expr::col((iden("p"), iden("chunk_id"))))
                .and(Expr::col((iden("c"), iden("tenant_id"))).eq(tenant_id)),
        )
        .and_where(Expr::col((iden("p"), iden("tenant_id"))).eq(tenant_id))
        .and_where(Expr::col((iden("p"), iden("term"))).eq(term))
        .order_by((iden("p"), iden("tf")), sea_query::Order::Desc)
        .order_by((iden("p"), iden("chunk_id")), sea_query::Order::Asc)
        .limit(u64::try_from(limit).unwrap_or(u64::MAX));
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows
        .rows
        .iter()
        .filter_map(|row| {
            Some(Posting {
                chunk_id: row.get("chunk_id")?,
                term: row.get("term")?,
                tf: row.get("tf")?,
                length: row.get("length")?,
            })
        })
        .collect())
}

/// Bodies, source ids and source titles for the top-ranked chunk ids.
/// The id list is the ranker's shortlist (at most the `limit` clamp), so
/// the `IN` here stays small — like [`postings_for`], one bounded read
/// among a query's fixed budget, never a scan of the index.
pub(crate) async fn chunks_by_id(
    db: &dyn Database,
    tenant_id: &str,
    chunk_ids: &[String],
) -> Result<Vec<ChunkRow>, DbError> {
    if chunk_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut select = Query::select();
    select
        .expr_as(Expr::col((iden("c"), iden("id"))), Alias::new("id"))
        .expr_as(
            Expr::col((iden("c"), iden("source_id"))),
            Alias::new("source_id"),
        )
        .expr_as(Expr::col((iden("s"), iden("title"))), Alias::new("title"))
        .expr_as(Expr::col((iden("c"), iden("body"))), Alias::new("body"))
        .from_as(iden("sg_chunks"), iden("c"))
        .join_as(
            sea_query::JoinType::InnerJoin,
            iden("sg_sources"),
            iden("s"),
            Expr::col((iden("s"), iden("id")))
                .eq(Expr::col((iden("c"), iden("source_id"))))
                .and(Expr::col((iden("s"), iden("tenant_id"))).eq(tenant_id)),
        )
        .and_where(Expr::col((iden("c"), iden("tenant_id"))).eq(tenant_id))
        .and_where(Expr::col((iden("c"), iden("id"))).is_in(chunk_ids.to_vec()));
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows
        .rows
        .iter()
        .filter_map(|row| {
            Some(ChunkRow {
                id: row.get("id")?,
                source_id: row.get("source_id")?,
                title: row.get("title")?,
                body: row.get("body")?,
            })
        })
        .collect())
}

/// A conversation as loaded for a turn. `status` is not loaded: in this
/// schema `status = 'escalated'` iff `needs_escalation = 1`, so the flag
/// alone carries everything the turn needs.
pub(crate) struct ConversationRow {
    pub id: String,
    pub needs_escalation: bool,
}

/// The conversation `conversation_id`, **scoped to the tenant**: a row
/// from another tenant is no row at all, which is what makes an unknown
/// and a foreign conversation the same `404`.
pub(crate) async fn find_conversation(
    db: &dyn Database,
    tenant_id: &str,
    conversation_id: &str,
) -> Result<Option<ConversationRow>, DbError> {
    let mut select = Query::select();
    select
        .columns(["id", "needs_escalation"])
        .from(iden("sg_conversations"))
        .and_where(Expr::col(iden("id")).eq(conversation_id))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id));
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows.rows.first().map(|row| ConversationRow {
        id: row.get("id").unwrap_or_default(),
        needs_escalation: row.get::<i64>("needs_escalation").unwrap_or(0) != 0,
    }))
}

/// What a turn reads from the conversation's messages before the model
/// is asked.
pub(crate) struct ConversationCounts {
    /// Every message so far — the next message's `seq`.
    pub messages: i64,
    /// Assistant messages that were clarifies — the budget
    /// [`crate::answer::MAX_CLARIFY_TURNS`] is spent against.
    pub clarifies: u32,
}

/// The conversation's message and clarify counts, scoped to the tenant.
pub(crate) async fn conversation_counts(
    db: &dyn Database,
    tenant_id: &str,
    conversation_id: &str,
) -> Result<ConversationCounts, DbError> {
    let count = |clarifies_only: bool| {
        let mut select = Query::select();
        select
            .expr_as(Func::count(Expr::col(iden("id"))), Alias::new("n"))
            .from(iden("sg_messages"))
            .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
            .and_where(Expr::col(iden("conversation_id")).eq(conversation_id));
        if clarifies_only {
            select
                .and_where(Expr::col(iden("role")).eq(ROLE_ASSISTANT))
                .and_where(Expr::col(iden("outcome")).eq(crate::answer::OUTCOME_CLARIFY));
        }
        Statement::render(&select)
    };
    let first = |rows: &cratefield_core::Rows| {
        rows.rows
            .first()
            .and_then(|row| row.get::<i64>("n"))
            .unwrap_or(0)
    };
    let messages = first(&db.query(&count(false)).await?);
    let clarifies = first(&db.query(&count(true)).await?);
    Ok(ConversationCounts {
        messages,
        clarifies: u32::try_from(clarifies).unwrap_or(u32::MAX),
    })
}

/// The tenant's stored answer threshold, in percent. `None` when the
/// tenant has never set one and [`crate::DEFAULT_ANSWER_THRESHOLD`]
/// applies.
pub(crate) async fn tenant_threshold_pct(
    db: &dyn Database,
    tenant_id: &str,
) -> Result<Option<i64>, DbError> {
    let mut select = Query::select();
    select
        .column(iden("answer_threshold_pct"))
        .from(iden("sg_tenant_settings"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id));
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows
        .rows
        .first()
        .and_then(|row| row.get::<i64>("answer_threshold_pct")))
}

/// Upserts the tenant's answer threshold. `ON CONFLICT … DO UPDATE SET …
/// = excluded.…` is the one upsert form SQLite and Postgres agree on.
pub(crate) async fn upsert_tenant_threshold(
    db: &dyn Database,
    tenant_id: &str,
    answer_threshold_pct: i64,
    updated_at: &str,
) -> Result<(), DbError> {
    let mut insert = Query::insert();
    insert
        .into_table(iden("sg_tenant_settings"))
        .columns(["tenant_id", "answer_threshold_pct", "updated_at"])
        .values_panic([
            tenant_id.into(),
            answer_threshold_pct.into(),
            updated_at.into(),
        ])
        .on_conflict(
            OnConflict::column(iden("tenant_id"))
                .update_columns([iden("answer_threshold_pct"), iden("updated_at")])
                .to_owned(),
        );
    execute(db, &Statement::render(&insert)).await
}

pub(crate) const ROLE_USER: &str = "user";
pub(crate) const ROLE_ASSISTANT: &str = "assistant";

const CONVERSATION_OPEN: &str = "open";
const CONVERSATION_ESCALATED: &str = "escalated";

/// One decided turn's write, built by the handler once the decision is
/// made.
pub(crate) struct Turn {
    pub conversation_id: String,
    pub tenant_id: String,
    /// `false` when this turn creates the conversation.
    pub conversation_existed: bool,
    /// Whether *this* turn escalates (a handoff). Escalation is monotonic:
    /// a turn that does not escalate never writes the flag, so it cannot
    /// clear one a concurrent handoff has just set.
    pub escalates: bool,
    pub now: String,
    /// The user message's `seq` (the conversation's prior message count);
    /// the assistant reply is `user_seq + 1`.
    pub user_seq: i64,
    pub user_message_id: String,
    pub user_message: String,
    pub assistant_message_id: String,
    /// What the user is shown: the model's answer when the outcome is
    /// `answered`, the canned message otherwise.
    pub assistant_body: String,
    /// What the model actually said, stored whatever the outcome. Equal
    /// to `assistant_body` on an `answered` turn; on a downgraded one the
    /// two deliberately differ, and this stays out of every user-facing
    /// path.
    pub model_answer: String,
    pub outcome: String,
    pub confidence_pct: i64,
    /// The model's raw citations as a JSON string, persisted whatever the
    /// outcome — the response may hide them, the row does not.
    pub citations_json: String,
}

/// The statements for one turn: the conversation (insert or update) plus
/// the user message and the assistant message. The caller runs them in
/// **one** `batch_atomic`, so a half turn — a conversation with no
/// messages, an answer with no question behind it — cannot survive a
/// crash between statements.
pub(crate) fn turn_statements(turn: &Turn) -> Vec<Statement> {
    let conversation = if turn.conversation_existed {
        let mut update = Query::update();
        update.table(iden("sg_conversations"));
        if turn.escalates {
            update.values([
                (iden("status"), CONVERSATION_ESCALATED.into()),
                (iden("needs_escalation"), 1_i64.into()),
            ]);
        }
        update
            .value(iden("updated_at"), turn.now.clone())
            .and_where(Expr::col(iden("id")).eq(turn.conversation_id.as_str()))
            .and_where(Expr::col(iden("tenant_id")).eq(turn.tenant_id.as_str()));
        Statement::render(&update)
    } else {
        let status = if turn.escalates {
            CONVERSATION_ESCALATED
        } else {
            CONVERSATION_OPEN
        };
        let mut insert = Query::insert();
        insert
            .into_table(iden("sg_conversations"))
            .columns([
                "id",
                "tenant_id",
                "status",
                "needs_escalation",
                "created_at",
                "updated_at",
            ])
            .values_panic([
                turn.conversation_id.clone().into(),
                turn.tenant_id.clone().into(),
                status.into(),
                i64::from(turn.escalates).into(),
                turn.now.clone().into(),
                turn.now.clone().into(),
            ]);
        Statement::render(&insert)
    };

    let mut messages = Query::insert();
    messages
        .into_table(iden("sg_messages"))
        .columns([
            "id",
            "conversation_id",
            "tenant_id",
            "role",
            "seq",
            "body",
            "model_answer",
            "outcome",
            "confidence_pct",
            "citations",
            "created_at",
        ])
        .values_panic([
            turn.user_message_id.clone().into(),
            turn.conversation_id.clone().into(),
            turn.tenant_id.clone().into(),
            ROLE_USER.into(),
            turn.user_seq.into(),
            turn.user_message.clone().into(),
            Option::<String>::None.into(),
            Option::<String>::None.into(),
            Option::<i64>::None.into(),
            Option::<String>::None.into(),
            turn.now.clone().into(),
        ])
        .values_panic([
            turn.assistant_message_id.clone().into(),
            turn.conversation_id.clone().into(),
            turn.tenant_id.clone().into(),
            ROLE_ASSISTANT.into(),
            (turn.user_seq + 1).into(),
            turn.assistant_body.clone().into(),
            turn.model_answer.clone().into(),
            turn.outcome.clone().into(),
            turn.confidence_pct.into(),
            turn.citations_json.clone().into(),
            turn.now.clone().into(),
        ]);

    vec![conversation, Statement::render(&messages)]
}

/// ISO-8601 (RFC 3339) from a `Clock` port reading — timestamps are bound
/// from code, never computed by a SQL function, so the same instant is
/// written whatever engine the statement runs on.
pub(crate) fn iso_now(clock: &dyn cratefield_core::Clock) -> String {
    clock
        .now()
        .replace_nanosecond(0)
        .unwrap_or_else(|_| clock.now())
        .format(&Rfc3339)
        .unwrap_or_default()
}
