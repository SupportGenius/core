//! Every query the support module runs, built with `sea_query` and
//! rendered through [`cratefield_core::Statement::render`] (ADR 0004:
//! module code never writes raw SQL strings — the migrations are the only
//! hand-written SQL in this crate, and the `fz doctor` lint keeps them
//! portable).
//!
//! The isolation boundary is `tenant_id`: every statement in this file
//! filters on it, whatever credential was verified upstream. Nothing here
//! is reachable without a tenant id, and nothing here ignores one — the
//! deliberate exceptions are the module's own cron sweeps over every
//! tenant it holds, never a tenant's request: the re-index's stale-chunk
//! select (`stale_chunks`) and the connector re-sync's listing
//! (`list_connectors`, `pending_subjects`, and the per-connector page
//! reads keyed by a connector id), whose rows each carry the tenant into
//! every job they produce.
//!
//! Most of this module is private to the crate. The public surface is
//! the retrieval bounds (so callers can reason about a query's cost) and
//! [`insert_source_with_chunks`], the one ingest primitive, which bulk
//! importers and the tests drive directly — with the same atomic-batch
//! statistics maintenance the HTTP route gets.

use std::collections::{HashMap, HashSet};

use cratefield_core::{Database, DbError, Row, Statement};
use sea_query::{Alias, Expr, Func, OnConflict, Order, Query, SelectStatement, SimpleExpr};
use time::format_description::well_known::Rfc3339;

use crate::bm25::{Corpus, Posting};
use crate::chunk::Chunk;

/// The only tenant status that authenticates. Anything else (a future
/// `suspended`, `closed`) fails key verification with the same
/// indistinguishable 401 as an unknown key.
pub(crate) const STATUS_ACTIVE: &str = "active";

/// The label recorded for the first key `POST /admin/tenants` mints.
pub(crate) const FIRST_KEY_LABEL: &str = "primary";

/// The label `POST /keys` records when the request body names none.
pub(crate) const DEFAULT_KEY_LABEL: &str = "key";

/// Distinct terms one retrieval may look up and fetch postings for:
/// after deduping, the first [`MAX_QUERY_TERMS`] terms of the query, the
/// rest dropped. A support question with more than 32 distinct terms is
/// not a question, and the cap is what keeps a pathological query
/// (`?q=` a novel's worth of text — or a long CJK run, which tokenizes
/// into one bigram per character) from turning into that many database
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
/// treats as filler (`the`, `and`, a CJK particle bigram, …) crosses the
/// line on its own, and the line moves with the corpus rather than a
/// word list. The cost is the point — such a term's postings fetch alone
/// would scale with the whole corpus, for a contribution BM25's idf
/// already weights to almost nothing (`ln(1 + 0.5/df)` shrinks toward
/// zero as df grows).
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
    /// The key's own id (the column predates per-key ids and keeps its
    /// name): the `tenancy` key id bound into the credential's signed
    /// subject, and what `GET`/`DELETE /keys/{kid}` address. Not the
    /// signing-key generation, which lives in config alone.
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
    /// The caller's own identity for the document, unique per tenant.
    /// `None` is an anonymous source.
    pub external_id: Option<String>,
    pub byte_len: i64,
    pub created_at: String,
    pub updated_at: String,
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

/// Rows and ids travel to `batch_atomic` in statements of this many at a
/// time. At two to seven bound values per row — the chunk insert is the
/// seven — every statement stays far under the conservative
/// 999-parameter limit some sqlite builds still enforce, including a
/// 48 KiB document that chunks to over a hundred one-word windows.
const ROWS_PER_STATEMENT: usize = 120;

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

/// The tenant's key row carrying `kid`, scoped to the tenant. This is the
/// read that makes a row — not merely a valid signature — the thing that
/// authenticates: a kid with no row, or a row held by another tenant,
/// returns `None` and the caller answers the same indistinguishable `401`
/// it answers every other key failure.
pub(crate) async fn find_api_key(
    db: &dyn Database,
    tenant_id: &str,
    kid: &str,
) -> Result<Option<ApiKeyRow>, DbError> {
    let mut select = Query::select();
    select
        .columns(["id", "tenant_id", "kid", "label", "created_at"])
        .from(iden("sg_api_keys"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden("kid")).eq(kid));
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows.rows.first().map(api_key_from))
}

/// The tenant's key rows, oldest first — the order `created_at` gives,
/// with the ULID `id` as a tiebreaker so two keys minted in the same
/// truncated second still list deterministically.
pub(crate) async fn list_api_keys(
    db: &dyn Database,
    tenant_id: &str,
) -> Result<Vec<ApiKeyRow>, DbError> {
    let mut select = Query::select();
    select
        .columns(["id", "tenant_id", "kid", "label", "created_at"])
        .from(iden("sg_api_keys"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .order_by(iden("created_at"), sea_query::Order::Asc)
        .order_by(iden("id"), sea_query::Order::Asc);
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows.rows.iter().map(api_key_from).collect())
}

/// Atomically deletes the tenant's key row carrying `kid`, refusing in the
/// same statement when it is the tenant's last key.
///
/// The "another key remains" test is an `EXISTS` clause *inside* the
/// `DELETE`, not a `SELECT` the caller can lose a race against: a separate
/// list-then-delete lets two concurrent deletes — each authenticated with
/// the key the other is deleting — both pass the check and leave the
/// tenant with no key at all. Here the row is removed only if a sibling
/// row exists at delete time.
///
/// Returns the rows removed: `1` on success, `0` when the row is absent
/// (unknown or another tenant's key) or is the tenant's only key. The
/// caller tells those two `0`s apart with a follow-up existence read.
pub(crate) async fn delete_api_key(
    db: &dyn Database,
    tenant_id: &str,
    kid: &str,
) -> Result<u64, DbError> {
    // `SELECT 1 FROM sg_api_keys WHERE tenant_id = ? AND kid <> ?`: does a
    // sibling key remain? A correlated `EXISTS` in the delete condition,
    // so the whole decision is one atomic statement.
    let sibling = Query::select()
        .expr(Expr::value(1))
        .from(iden("sg_api_keys"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden("kid")).ne(kid))
        .take();
    let mut delete = Query::delete();
    delete
        .from_table(iden("sg_api_keys"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden("kid")).eq(kid))
        .and_where(Expr::exists(sibling));
    db.execute(&Statement::render(&delete)).await
}

/// One `sg_api_keys` row from a result row, columns as
/// [`ApiKeyRow`] names them.
fn api_key_from(row: &cratefield_core::Row) -> ApiKeyRow {
    ApiKeyRow {
        id: row.get("id").unwrap_or_default(),
        tenant_id: row.get("tenant_id").unwrap_or_default(),
        kid: row.get("kid").unwrap_or_default(),
        label: row.get("label").unwrap_or_default(),
        created_at: row.get("created_at").unwrap_or_default(),
    }
}

/// The `sg_chunks` insert statements for `chunks` — the source's windows,
/// stamped with the tenant, the source, its `created_at` and the
/// tokenizer that produced their terms, batched
/// [`ROWS_PER_STATEMENT`] rows per statement (eight values per row).
fn chunk_insert_statements(
    tenant_id: &str,
    source_id: &str,
    created_at: &str,
    chunks: &[&Chunk],
) -> Vec<Statement> {
    chunks
        .chunks(ROWS_PER_STATEMENT)
        .map(|group| {
            let mut insert_chunks = Query::insert();
            insert_chunks.into_table(iden("sg_chunks")).columns([
                "id",
                "tenant_id",
                "source_id",
                "ordinal",
                "body",
                "term_count",
                "tokenizer_version",
                "created_at",
            ]);
            for chunk in group {
                insert_chunks.values_panic([
                    chunk.id.clone().into(),
                    tenant_id.into(),
                    source_id.into(),
                    chunk.ordinal.into(),
                    chunk.text.clone().into(),
                    chunk.length.into(),
                    crate::chunk::TOKENIZER_VERSION.into(),
                    created_at.into(),
                ]);
            }
            Statement::render(&insert_chunks)
        })
        .collect()
}

/// The inverted-index half of an ingest: the `(tenant_id, term, chunk_id,
/// tf)` rows for `chunks`, in the chunker's term-sorted order so the batch
/// is deterministic for a given document, batched
/// [`ROWS_PER_STATEMENT`] rows per statement.
fn posting_insert_statements(tenant_id: &str, chunks: &[&Chunk]) -> Vec<Statement> {
    let postings: Vec<(&str, &str, &str, u32)> = chunks
        .iter()
        .flat_map(|chunk| {
            chunk
                .terms
                .iter()
                .map(move |(term, tf)| (tenant_id, term.as_str(), chunk.id.as_str(), *tf))
        })
        .collect();
    posting_rows_statements(&postings)
}

/// The `(tenant_id, term, chunk_id, tf)` rows as insert statements, one
/// batch per [`ROWS_PER_STATEMENT`] rows — the shape both ingest and the
/// re-index write the index in.
fn posting_rows_statements(postings: &[(&str, &str, &str, u32)]) -> Vec<Statement> {
    postings
        .chunks(ROWS_PER_STATEMENT)
        .map(|group| {
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
            Statement::render(&insert_postings)
        })
        .collect()
}

/// The source row, its chunks, the chunks' postings and the corpus
/// statistics derived from them land in **one** `batch_atomic`: a
/// half-indexed source must never be able to exist, because a source
/// whose postings are missing some terms is worse than no source —
/// searches quietly return wrong answers instead of nothing. A stats row
/// drifting from the index it describes is the same class of lie (see
/// `corpus_stats`), so the increments are written by the same batch
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
    let statements = source_with_chunks_statements(source, chunks);
    db.batch_atomic(&statements).await
}

/// The statements [`insert_source_with_chunks`] executes, exposed so the
/// upload extract job can land the source *and* its terminal upload
/// update in one batch of its own — the upload must never go `extracted`
/// without the source it claims to have produced.
pub(crate) fn source_with_chunks_statements(
    source: &SourceRow,
    chunks: &[Chunk],
) -> Vec<Statement> {
    let mut statements: Vec<Statement> = Vec::new();

    let mut insert_source = Query::insert();
    insert_source
        .into_table(iden("sg_sources"))
        .columns([
            "id",
            "tenant_id",
            "title",
            "url",
            "external_id",
            "byte_len",
            "created_at",
            "updated_at",
        ])
        .values_panic([
            source.id.clone().into(),
            source.tenant_id.clone().into(),
            source.title.clone().into(),
            source.url.clone().into(),
            source.external_id.clone().into(),
            source.byte_len.into(),
            source.created_at.clone().into(),
            source.updated_at.clone().into(),
        ]);
    statements.push(Statement::render(&insert_source));

    let windows: Vec<&Chunk> = chunks.iter().collect();
    statements.extend(chunk_insert_statements(
        &source.tenant_id,
        &source.id,
        &source.created_at,
        &windows,
    ));
    statements.extend(posting_insert_statements(&source.tenant_id, &windows));
    // After the rows they count: every id here was inserted by this very
    // batch (a clash on any of them fails the chunk primary key and rolls
    // the batch back), so the increments count exactly what landed.
    statements.extend(stats_add_statements(&source.tenant_id, &windows));

    statements
}

/// The chunks a statistics adjustment covers, always read back out of
/// `sg_chunks` inside the statement itself — never trusted from Rust —
/// so an adjustment counts exactly the rows that exist when its batch
/// runs, whatever a concurrent request committed in between.
enum ChunkSet<'a> {
    /// These chunk ids, at most [`ROWS_PER_STATEMENT`] of them (the
    /// callers batch).
    Ids(&'a [String]),
    /// Every chunk of one source.
    Source(&'a str),
}

/// `SELECT id FROM sg_chunks WHERE tenant_id = ? AND <set>`: the set's
/// chunks as they exist, for an `IN (…)` over postings.
fn chunk_ids_in(tenant_id: &str, set: &ChunkSet<'_>) -> SelectStatement {
    let mut select = Query::select();
    select.column(iden("id")).from(iden("sg_chunks"));
    chunk_set_filter(&mut select, tenant_id, set);
    select
}

/// Narrows a select over `sg_chunks` to one tenant's chunks in `set`.
fn chunk_set_filter(select: &mut SelectStatement, tenant_id: &str, set: &ChunkSet<'_>) {
    select.and_where(Expr::col(iden("tenant_id")).eq(tenant_id));
    match set {
        ChunkSet::Ids(ids) => {
            select.and_where(Expr::col(iden("id")).is_in(ids.iter().map(String::as_str)));
        }
        ChunkSet::Source(source_id) => {
            select.and_where(Expr::col(iden("source_id")).eq(*source_id));
        }
    }
}

/// A scalar subquery, for arithmetic in an `UPDATE … SET`.
fn scalar(select: SelectStatement) -> SimpleExpr {
    SimpleExpr::SubQuery(None, Box::new(select.into_sub_query_statement()))
}

/// The statistics increments for chunks this batch has **already
/// written** (rows and postings): one `sg_terms` upsert adding each
/// term's count of the set's postings to its df, and one
/// `sg_tenant_stats` upsert adding the set's chunk count and total
/// length — both counted in SQL from the rows themselves, batched
/// [`ROWS_PER_STATEMENT`] ids per statement pair.
///
/// ```sql
/// INSERT INTO sg_terms (tenant_id, term, df)
/// SELECT tenant_id, term, COUNT(chunk_id) FROM sg_postings
/// WHERE tenant_id = ? AND chunk_id IN (SELECT id FROM sg_chunks WHERE …)
/// GROUP BY tenant_id, term
/// ON CONFLICT (tenant_id, term) DO UPDATE SET df = sg_terms.df + excluded.df
/// ```
///
/// The grouping merges the set's many rows per term into one out-row, so
/// a statement never upserts the same key twice (Postgres refuses to
/// affect one row twice in a statement), and the `WHERE` before
/// `GROUP BY` keeps SQLite from reading the upsert's `ON` as a join.
fn stats_add_statements(tenant_id: &str, chunks: &[&Chunk]) -> Vec<Statement> {
    let ids: Vec<String> = chunks.iter().map(|chunk| chunk.id.clone()).collect();
    ids.chunks(ROWS_PER_STATEMENT)
        .flat_map(|group| stats_add_for(tenant_id, &ChunkSet::Ids(group)))
        .collect()
}

/// [`stats_add_statements`] for one set.
fn stats_add_for(tenant_id: &str, set: &ChunkSet<'_>) -> Vec<Statement> {
    let mut term_counts = Query::select();
    term_counts
        .column(iden("tenant_id"))
        .column(iden("term"))
        .expr(Func::count(Expr::col(iden("chunk_id"))))
        .from(iden("sg_postings"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden("chunk_id")).in_subquery(chunk_ids_in(tenant_id, set)))
        .group_by_col(iden("tenant_id"))
        .group_by_col(iden("term"));
    let mut add_terms = Query::insert();
    add_terms
        .into_table(iden("sg_terms"))
        .columns([iden("tenant_id"), iden("term"), iden("df")])
        .select_from(term_counts)
        .expect("the select feeds exactly the three insert columns")
        .on_conflict(
            OnConflict::columns([iden("tenant_id"), iden("term")])
                .value(
                    iden("df"),
                    Expr::col((iden("sg_terms"), iden("df")))
                        .add(Expr::col((iden("excluded"), iden("df")))),
                )
                .to_owned(),
        );

    let mut chunk_counts = Query::select();
    chunk_counts
        .column(iden("tenant_id"))
        .expr(Func::count(Expr::col(iden("id"))))
        .expr(Func::sum(Expr::col(iden("term_count"))))
        .from(iden("sg_chunks"));
    chunk_set_filter(&mut chunk_counts, tenant_id, set);
    chunk_counts.group_by_col(iden("tenant_id"));
    let mut add_tenant = Query::insert();
    add_tenant
        .into_table(iden("sg_tenant_stats"))
        .columns([iden("tenant_id"), iden("n_chunks"), iden("total_len")])
        .select_from(chunk_counts)
        .expect("the select feeds exactly the three insert columns")
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

    vec![
        Statement::render(&add_terms),
        Statement::render(&add_tenant),
    ]
}

/// The statistics decrements for chunks this batch is **about to
/// delete** (or, for the re-index, to re-tokenize): placed before the
/// deletes, because they count the rows the deletes remove. Per set:
///
/// 1. `UPDATE sg_terms SET df = df - (the set's postings of that term)`
///    for every term the set's postings carry;
/// 2. `DELETE` those terms' rows whose df reached zero — a term nobody
///    indexes any more is absent, exactly as if it never had been;
/// 3. `UPDATE sg_tenant_stats` minus the set's chunk count and length;
/// 4. `DELETE` the tenant's row once it counts no chunk.
///
/// Every count is a subquery over the live rows in the same batch, so
/// the decrement is exactly what the following deletes take away.
fn stats_remove_for(tenant_id: &str, set: &ChunkSet<'_>) -> Vec<Statement> {
    // The terms the set's postings carry.
    let mut set_terms = Query::select();
    set_terms
        .column(iden("term"))
        .from(iden("sg_postings"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden("chunk_id")).in_subquery(chunk_ids_in(tenant_id, set)));

    // One term's share of the set, correlated to the row being updated.
    let mut term_share = Query::select();
    term_share
        .expr(Func::count(Expr::col((iden("p"), iden("chunk_id")))))
        .from_as(iden("sg_postings"), iden("p"))
        .and_where(Expr::col((iden("p"), iden("tenant_id"))).eq(tenant_id))
        .and_where(Expr::col((iden("p"), iden("term"))).equals((iden("sg_terms"), iden("term"))))
        .and_where(
            Expr::col((iden("p"), iden("chunk_id"))).in_subquery(chunk_ids_in(tenant_id, set)),
        );
    let mut lower_terms = Query::update();
    lower_terms
        .table(iden("sg_terms"))
        .value(iden("df"), Expr::col(iden("df")).sub(scalar(term_share)))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden("term")).in_subquery(set_terms.clone()));

    let mut drop_terms = Query::delete();
    drop_terms
        .from_table(iden("sg_terms"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden("df")).lte(0))
        .and_where(Expr::col(iden("term")).in_subquery(set_terms));

    let mut set_count = Query::select();
    set_count
        .expr(Func::count(Expr::col(iden("id"))))
        .from(iden("sg_chunks"));
    chunk_set_filter(&mut set_count, tenant_id, set);
    let mut set_len = Query::select();
    set_len
        .expr(Func::coalesce([
            Func::sum(Expr::col(iden("term_count"))).into(),
            Expr::value(0_i64),
        ]))
        .from(iden("sg_chunks"));
    chunk_set_filter(&mut set_len, tenant_id, set);
    let mut lower_tenant = Query::update();
    lower_tenant
        .table(iden("sg_tenant_stats"))
        .values([
            (
                iden("n_chunks"),
                Expr::col(iden("n_chunks")).sub(scalar(set_count)),
            ),
            (
                iden("total_len"),
                Expr::col(iden("total_len")).sub(scalar(set_len)),
            ),
        ])
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id));

    let mut drop_tenant = Query::delete();
    drop_tenant
        .from_table(iden("sg_tenant_stats"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden("n_chunks")).lte(0));

    vec![
        Statement::render(&lower_terms),
        Statement::render(&drop_terms),
        Statement::render(&lower_tenant),
        Statement::render(&drop_tenant),
    ]
}

/// [`stats_remove_for`] over a list of chunk ids, batched
/// [`ROWS_PER_STATEMENT`] ids per statement group.
fn stats_remove_statements(tenant_id: &str, ids: &[String]) -> Vec<Statement> {
    ids.chunks(ROWS_PER_STATEMENT)
        .flat_map(|group| stats_remove_for(tenant_id, &ChunkSet::Ids(group)))
        .collect()
}

/// One source as the source-management routes show it: the row plus how
/// many windows it was chunked into. `origin` on the wire is derived from
/// `url` (`Some` is `"url"`, `None` is `"text"`).
pub(crate) struct SourceSummary {
    pub id: String,
    pub title: String,
    pub url: Option<String>,
    pub external_id: Option<String>,
    pub byte_len: i64,
    pub updated_at: String,
    pub chunk_count: i64,
}

/// The source-list columns plus their grouped chunk count: a `LEFT JOIN`
/// so a source with no chunks (empty text) still lists, with `COUNT`
/// over the join and every selected source column in the `GROUP BY`, so
/// the grouping is unambiguous on every engine.
fn summary_select() -> sea_query::SelectStatement {
    let mut select = Query::select();
    select
        .expr_as(Expr::col((iden("s"), iden("id"))), Alias::new("id"))
        .expr_as(Expr::col((iden("s"), iden("title"))), Alias::new("title"))
        .expr_as(Expr::col((iden("s"), iden("url"))), Alias::new("url"))
        .expr_as(
            Expr::col((iden("s"), iden("external_id"))),
            Alias::new("external_id"),
        )
        .expr_as(
            Expr::col((iden("s"), iden("byte_len"))),
            Alias::new("byte_len"),
        )
        .expr_as(
            Expr::col((iden("s"), iden("updated_at"))),
            Alias::new("updated_at"),
        )
        .expr_as(
            Func::count(Expr::col((iden("c"), iden("id")))),
            Alias::new("chunk_count"),
        )
        .from_as(iden("sg_sources"), iden("s"))
        .join_as(
            sea_query::JoinType::LeftJoin,
            iden("sg_chunks"),
            iden("c"),
            Expr::col((iden("c"), iden("source_id")))
                .eq(Expr::col((iden("s"), iden("id"))))
                .and(
                    Expr::col((iden("c"), iden("tenant_id")))
                        .eq(Expr::col((iden("s"), iden("tenant_id")))),
                ),
        );
    select
}

fn summary_from(row: &cratefield_core::Row) -> Option<SourceSummary> {
    Some(SourceSummary {
        id: row.get("id")?,
        title: row.get("title")?,
        url: row.get("url")?,
        external_id: row.get("external_id")?,
        byte_len: row.get("byte_len")?,
        updated_at: row.get("updated_at")?,
        chunk_count: row.get("chunk_count")?,
    })
}

/// The tenant's sources, keyset-paginated by id, oldest ULID first: at
/// most `limit` rows starting strictly after `after` (`None` is from the
/// top). Pagination rides the primary key, so a page boundary is stable
/// no matter what ingests land between two requests.
pub(crate) async fn list_sources(
    db: &dyn Database,
    tenant_id: &str,
    limit: u32,
    after: Option<&str>,
) -> Result<Vec<SourceSummary>, DbError> {
    let mut select = summary_select();
    select.and_where(Expr::col((iden("s"), iden("tenant_id"))).eq(tenant_id));
    if let Some(after) = after {
        select.and_where(Expr::col((iden("s"), iden("id"))).gt(after));
    }
    select
        .group_by_columns([
            (iden("s"), iden("id")),
            (iden("s"), iden("title")),
            (iden("s"), iden("url")),
            (iden("s"), iden("external_id")),
            (iden("s"), iden("byte_len")),
            (iden("s"), iden("updated_at")),
        ])
        .order_by((iden("s"), iden("id")), sea_query::Order::Asc)
        .limit(u64::from(limit));
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows.rows.iter().filter_map(summary_from).collect())
}

/// One source by id, scoped to the tenant — a foreign id is no source at
/// all, which is what makes unknown and foreign the same 404.
pub(crate) async fn find_source_summary(
    db: &dyn Database,
    tenant_id: &str,
    source_id: &str,
) -> Result<Option<SourceSummary>, DbError> {
    let mut select = summary_select();
    select
        .and_where(Expr::col((iden("s"), iden("tenant_id"))).eq(tenant_id))
        .and_where(Expr::col((iden("s"), iden("id"))).eq(source_id))
        .group_by_columns([
            (iden("s"), iden("id")),
            (iden("s"), iden("title")),
            (iden("s"), iden("url")),
            (iden("s"), iden("external_id")),
            (iden("s"), iden("byte_len")),
            (iden("s"), iden("updated_at")),
        ]);
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows.rows.first().and_then(summary_from))
}

/// The tenant's source carrying this `external_id`, if any — the lookup
/// the replace-in-place ingest stands on. Scoped to the tenant, as the
/// unique index is.
pub(crate) async fn find_source_by_external_id(
    db: &dyn Database,
    tenant_id: &str,
    external_id: &str,
) -> Result<Option<SourceRow>, DbError> {
    source_by(db, tenant_id, "external_id", external_id).await
}

/// The source row for `id`, scoped to the tenant. Carries the columns the
/// replace path needs; the routes show [`SourceSummary`] instead.
pub(crate) async fn find_source(
    db: &dyn Database,
    tenant_id: &str,
    source_id: &str,
) -> Result<Option<SourceRow>, DbError> {
    source_by(db, tenant_id, "id", source_id).await
}

/// `SELECT … FROM sg_sources WHERE tenant_id = ? AND <column> = ?`, the
/// one shape both source lookups take.
async fn source_by(
    db: &dyn Database,
    tenant_id: &str,
    column: &str,
    value: &str,
) -> Result<Option<SourceRow>, DbError> {
    let mut select = Query::select();
    select
        .columns([
            "id",
            "tenant_id",
            "title",
            "url",
            "external_id",
            "byte_len",
            "created_at",
            "updated_at",
        ])
        .from(iden("sg_sources"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden(column)).eq(value));
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows.rows.first().map(|row| SourceRow {
        id: row.get("id").unwrap_or_default(),
        tenant_id: row.get("tenant_id").unwrap_or_default(),
        title: row.get("title").unwrap_or_default(),
        url: row.get("url").unwrap_or_default(),
        external_id: row.get("external_id").unwrap_or_default(),
        byte_len: row.get("byte_len").unwrap_or_default(),
        created_at: row.get("created_at").unwrap_or_default(),
        updated_at: row.get("updated_at").unwrap_or_default(),
    }))
}

/// One stored chunk's bookkeeping: its content address and its position.
pub(crate) struct StoredChunk {
    pub id: String,
    pub ordinal: u32,
}

/// The stored chunk ids and ordinals of one source, in ordinal order —
/// the old side of the replace diff.
pub(crate) async fn chunk_index(
    db: &dyn Database,
    tenant_id: &str,
    source_id: &str,
) -> Result<Vec<StoredChunk>, DbError> {
    let mut select = Query::select();
    select
        .columns(["id", "ordinal"])
        .from(iden("sg_chunks"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden("source_id")).eq(source_id))
        .order_by(iden("ordinal"), sea_query::Order::Asc);
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows
        .rows
        .iter()
        .filter_map(|row| {
            Some(StoredChunk {
                id: row.get("id")?,
                ordinal: row.get("ordinal")?,
            })
        })
        .collect())
}

/// `DELETE FROM <table> WHERE tenant_id = ? AND <column> IN (…)`, ids
/// batched [`ROWS_PER_STATEMENT`] per statement — one parameter per id,
/// for the same reason the inserts are batched.
fn delete_tenant_in(table: &str, column: &str, tenant_id: &str, ids: &[String]) -> Vec<Statement> {
    ids.chunks(ROWS_PER_STATEMENT)
        .map(|group| {
            let mut delete = Query::delete();
            delete
                .from_table(iden(table))
                .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
                .and_where(Expr::col(iden(column)).is_in(group.iter().map(String::as_str)));
            Statement::render(&delete)
        })
        .collect()
}

/// Replaces one source's whole chunk set in **one** `batch_atomic`: the
/// source row's metadata first, then the diff between the stored windows
/// and the re-chunked ones. `source` is the row as it should read after
/// the call — the stored `id` and `tenant_id`, its `created_at` carried
/// through untouched, and `updated_at` also stamping any window the
/// replacement adds. The caller chunks with the *same* source id, so a
/// window whose text survived the edit keeps its content-addressed id and
/// its row — `created_at` included — is not touched at all; only vanished
/// windows (postings first, then the rows) and added windows are written,
/// and a kept window whose ordinal moved gets an `UPDATE`.
///
/// Residual races are bounded by the batches: a DELETE that committed
/// before this batch left the row update matching nothing and the windows
/// written here orphaned, so the row is re-checked afterwards and the
/// [`delete_source`] batch sweeps them. Two concurrent replaces of one
/// source chunk identically, so the loser's insert hits the chunk
/// primary key and its whole batch rolls back — a clean `500`, never a
/// half-applied write.
pub(crate) async fn replace_source_chunks(
    db: &dyn Database,
    source: &SourceRow,
    chunks: &[Chunk],
) -> Result<(), DbError> {
    let stored = chunk_index(db, &source.tenant_id, &source.id).await?;
    let statements = replace_source_statements(source, &stored, chunks);
    db.batch_atomic(&statements).await?;

    // The re-check the doc above promises: if a DELETE committed between
    // the caller's existence read and this batch, the row is gone but the
    // windows written above are not — sweep them with the same
    // delete-by-source batch. (A DELETE committing after this check also
    // wins: it deletes by source, so it takes whatever this batch wrote.)
    sweep_if_deleted(db, &source.tenant_id, &source.id).await
}

/// The post-batch half of a replace: when the source row turned out to be
/// gone (a concurrent DELETE won), the windows a replace just wrote are
/// swept with the delete-by-source batch.
pub(crate) async fn sweep_if_deleted(
    db: &dyn Database,
    tenant_id: &str,
    source_id: &str,
) -> Result<(), DbError> {
    if find_source(db, tenant_id, source_id).await?.is_none() {
        delete_source(db, tenant_id, source_id).await?;
    }
    Ok(())
}

/// The statements [`replace_source_chunks`] executes, given the stored
/// side of the diff ([`chunk_index`]) — exposed so a connector re-sync can
/// land the replacement, its page row and its outbox completion in one
/// batch of its own.
pub(crate) fn replace_source_statements(
    source: &SourceRow,
    stored: &[StoredChunk],
    chunks: &[Chunk],
) -> Vec<Statement> {
    let new_ids: std::collections::HashSet<&str> =
        chunks.iter().map(|chunk| chunk.id.as_str()).collect();
    let vanished: Vec<String> = stored
        .iter()
        .filter(|chunk| !new_ids.contains(chunk.id.as_str()))
        .map(|chunk| chunk.id.clone())
        .collect();
    let old_ordinals: std::collections::HashMap<&str, u32> = stored
        .iter()
        .map(|chunk| (chunk.id.as_str(), chunk.ordinal))
        .collect();
    let added: Vec<&Chunk> = chunks
        .iter()
        .filter(|chunk| !old_ordinals.contains_key(chunk.id.as_str()))
        .collect();

    let mut statements: Vec<Statement> = Vec::new();

    let mut update_source = Query::update();
    update_source
        .table(iden("sg_sources"))
        .values([
            (iden("title"), source.title.as_str().into()),
            (iden("url"), source.url.as_deref().into()),
            (iden("external_id"), source.external_id.as_deref().into()),
            (iden("byte_len"), source.byte_len.into()),
            (iden("updated_at"), source.updated_at.as_str().into()),
        ])
        .and_where(Expr::col(iden("id")).eq(source.id.as_str()))
        .and_where(Expr::col(iden("tenant_id")).eq(source.tenant_id.as_str()));
    statements.push(Statement::render(&update_source));

    // The statistics leave before the rows they count: the decrements
    // read the vanished windows' postings, so they must run while those
    // still exist.
    statements.extend(stats_remove_statements(&source.tenant_id, &vanished));

    // Postings before their chunks: no foreign keys enforce the order
    // today, but the index is a projection of the rows — never the other
    // way round.
    statements.extend(delete_tenant_in(
        "sg_postings",
        "chunk_id",
        &source.tenant_id,
        &vanished,
    ));
    statements.extend(delete_tenant_in(
        "sg_chunks",
        "id",
        &source.tenant_id,
        &vanished,
    ));

    for chunk in chunks {
        // Only kept windows are renumbered: an added window is inserted
        // with its ordinal already, so an UPDATE would be a no-op against
        // a row this same batch is about to insert.
        if old_ordinals
            .get(chunk.id.as_str())
            .is_none_or(|old| *old == chunk.ordinal)
        {
            continue;
        }
        let mut renumber = Query::update();
        renumber
            .table(iden("sg_chunks"))
            .value(iden("ordinal"), chunk.ordinal)
            .and_where(Expr::col(iden("id")).eq(chunk.id.as_str()))
            .and_where(Expr::col(iden("tenant_id")).eq(source.tenant_id.as_str()));
        statements.push(Statement::render(&renumber));
    }

    // Added windows carry the replacement's instant as their created_at;
    // the windows that survived keep the instant they were first written.
    statements.extend(chunk_insert_statements(
        &source.tenant_id,
        &source.id,
        &source.updated_at,
        &added,
    ));
    statements.extend(posting_insert_statements(&source.tenant_id, &added));
    // Kept windows keep their postings, so only the added ones count in.
    statements.extend(stats_add_statements(&source.tenant_id, &added));

    statements
}

/// Deletes one source and its whole index in **one** `batch_atomic`: the
/// postings of its chunks — by subquery over `sg_chunks`, not a pre-read
/// id list, so windows a concurrent replace committed mid-flight are
/// swept too — then the chunks, then the row. Every statement is a no-op
/// if the source is already gone, so a race that removes the row first
/// still leaves nothing behind.
pub(crate) async fn delete_source(
    db: &dyn Database,
    tenant_id: &str,
    source_id: &str,
) -> Result<(), DbError> {
    db.batch_atomic(&delete_source_statements(tenant_id, source_id))
        .await
}

/// The statements [`delete_source`] executes, exposed so a connector page
/// that answered 404/410 can drop its source together with its page row.
pub(crate) fn delete_source_statements(tenant_id: &str, source_id: &str) -> Vec<Statement> {
    // The decrements first, by the same by-source subquery the deletes
    // use, so they count exactly the windows about to go — a concurrent
    // replace's windows included.
    let mut statements = stats_remove_for(tenant_id, &ChunkSet::Source(source_id));

    let mut delete_postings = Query::delete();
    delete_postings
        .from_table(iden("sg_postings"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(
            Expr::col(iden("chunk_id")).in_subquery(
                Query::select()
                    .column(iden("id"))
                    .from(iden("sg_chunks"))
                    .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
                    .and_where(Expr::col(iden("source_id")).eq(source_id))
                    .to_owned(),
            ),
        );

    let mut delete_chunks = Query::delete();
    delete_chunks
        .from_table(iden("sg_chunks"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden("source_id")).eq(source_id));

    let mut delete_row = Query::delete();
    delete_row
        .from_table(iden("sg_sources"))
        .and_where(Expr::col(iden("id")).eq(source_id))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id));

    statements.extend([
        Statement::render(&delete_postings),
        Statement::render(&delete_chunks),
        Statement::render(&delete_row),
    ]);
    statements
}

/// One chunk the re-index sweep must rewrite: who owns it, and the
/// verbatim text its terms are re-derived from.
pub(crate) struct StaleChunk {
    pub id: String,
    pub tenant_id: String,
    pub body: String,
}

/// The next `limit` chunks stamped with a `tokenizer_version` older than
/// [`crate::chunk::TOKENIZER_VERSION`], in id order. The one select in
/// this file with no `tenant_id` filter (see the module docs): the sweep
/// is module-owned maintenance over the whole index, not a tenant's
/// request, and a chunk's stamp is not readable without naming its
/// tenant anyway.
pub(crate) async fn stale_chunks(
    db: &dyn Database,
    version: u32,
    limit: u64,
) -> Result<Vec<StaleChunk>, DbError> {
    let mut select = Query::select();
    select
        .columns(["id", "tenant_id", "body"])
        .from(iden("sg_chunks"))
        .and_where(Expr::col(iden("tokenizer_version")).lt(version))
        .order_by(iden("id"), sea_query::Order::Asc)
        .limit(limit);
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows
        .rows
        .iter()
        .filter_map(|row| {
            Some(StaleChunk {
                id: row.get("id")?,
                tenant_id: row.get("tenant_id")?,
                body: row.get("body")?,
            })
        })
        .collect())
}

/// Re-tokenizes one stale chunk from its stored text and rewrites its
/// index rows in **one** `batch_atomic`: the chunk's postings replaced
/// wholesale, then `term_count` and the `tokenizer_version` stamp
/// updated. The chunk's row itself is not touched — its id is a content
/// address of (tenant, source, text) and its text a verbatim slice, and
/// neither depends on the tokenizer — so a re-indexed chunk stays exactly
/// the chunk it was, searchable by the terms the current tokenizer
/// produces.
async fn reindex_chunk(db: &dyn Database, stale: &StaleChunk) -> Result<(), DbError> {
    let (terms, length) = crate::chunk::term_frequencies(&stale.body);
    let postings: Vec<(&str, &str, &str, u32)> = terms
        .iter()
        .map(|(term, tf)| {
            (
                stale.tenant_id.as_str(),
                term.as_str(),
                stale.id.as_str(),
                *tf,
            )
        })
        .collect();

    let mut delete_postings = Query::delete();
    delete_postings
        .from_table(iden("sg_postings"))
        .and_where(Expr::col(iden("tenant_id")).eq(stale.tenant_id.as_str()))
        .and_where(Expr::col(iden("chunk_id")).eq(stale.id.as_str()));

    let mut restamp = Query::update();
    restamp
        .table(iden("sg_chunks"))
        .values([
            (iden("term_count"), length.into()),
            (
                iden("tokenizer_version"),
                crate::chunk::TOKENIZER_VERSION.into(),
            ),
        ])
        .and_where(Expr::col(iden("id")).eq(stale.id.as_str()))
        .and_where(Expr::col(iden("tenant_id")).eq(stale.tenant_id.as_str()));

    // The chunk's old contribution to the statistics leaves before its
    // postings and term count change, and its new one enters after: the
    // term set and the length both move with the tokenizer, so df and
    // the tenant's total length are re-derived from the rewritten rows.
    // A chunk a concurrent delete removed matches nothing on either side.
    let this_chunk = [stale.id.clone()];
    let set = ChunkSet::Ids(&this_chunk);
    let mut statements = stats_remove_for(&stale.tenant_id, &set);
    statements.extend([
        Statement::render(&delete_postings),
        Statement::render(&restamp),
    ]);
    // Postings before the restamp, matching ingest's order: the index is
    // a projection of the rows, never the other way round.
    statements.extend(posting_rows_statements(&postings));
    statements.extend(stats_add_for(&stale.tenant_id, &set));
    db.batch_atomic(&statements).await
}

/// The scheduled re-index body: re-tokenizes up to `limit` chunks whose
/// `tokenizer_version` stamp predates the current tokenizer and returns
/// how many it rewrote — fewer than `limit` means the index is current.
/// Public at the crate root (`pub use` in `lib.rs`) because the
/// `scheduled` hook drives it and the tests drive the same path directly:
/// a test context has no [`cratefield_core::ModuleContext`] to hand a
/// hook.
///
/// # Errors
///
/// The [`DbError`] of whichever statement failed, left uncommitted — a
/// chunk whose batch rolled back keeps its old postings and its old
/// version stamp, and the next sweep picks it again.
pub async fn reindex_stale_chunks(db: &dyn Database, limit: usize) -> Result<usize, DbError> {
    // A page bound widens losslessly: `usize` fits `u64` on every target
    // this crate compiles for.
    let limit = u64::try_from(limit).unwrap_or(u64::MAX);
    let stale = stale_chunks(db, crate::chunk::TOKENIZER_VERSION, limit).await?;
    for chunk in &stale {
        reindex_chunk(db, chunk).await?;
    }
    Ok(stale.len())
}

/// Chunk count and mean document length for one tenant's index — the two
/// numbers Okapi BM25 needs before it can score anything — read from the
/// tenant's single `sg_tenant_stats` row, which every write path keeps in
/// step with `sg_chunks` inside its own batch. An empty index (no row) is
/// `Corpus { chunk_count: 0, avg_length: 0.0 }`, not an error, and one
/// primary-key read whatever the corpus size — never a `COUNT`/`AVG` over
/// the chunks themselves.
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
            // Counts as f64: a corpus near 2^53 chunks would lose a
            // precision no score could show.
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
/// per term at most, all primary-key reads. Terms with no row are not
/// indexed by this tenant and simply come back absent; the caller treats
/// an absent term as df 0 and drops it before fetching postings.
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
        .and_where(Expr::col(iden("term")).is_in(terms.iter().map(String::as_str)));
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
/// **The fetch is bounded, and the bound is honest, because df and N do
/// not come from these rows.** `bm25::rank` takes each term's df from
/// `sg_terms` and N from `sg_tenant_stats`, both exact whatever subset of
/// postings is read; a truncated fetch cannot deflate an idf. What
/// truncation costs is a contribution, not a wrong weight: a chunk
/// outside a term's top `limit` by tf is ranked without that term.
/// Per query, the reads are bounded by the query: 1 stats row, at most
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
        .order_by((iden("p"), iden("tf")), Order::Desc)
        .order_by((iden("p"), iden("chunk_id")), Order::Asc)
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

/// One tenant's settings row as the admin settings route reads it.
/// `answer_threshold_pct` cannot be null in this schema (0002 made it
/// `NOT NULL`; a row created for the widget origins alone records the
/// documented default), while `widget_origins` is null — the widget
/// refused — until the tenant sets an allowlist.
pub(crate) struct TenantSettingsRow {
    pub answer_threshold_pct: i64,
    /// The normalized origin allowlist as the JSON text it is stored as,
    /// `None` (and an explicit `"[]"`) both meaning the widget is closed.
    pub widget_origins: Option<String>,
}

/// The tenant's settings row, or `None` when it has never set anything.
pub(crate) async fn find_tenant_settings(
    db: &dyn Database,
    tenant_id: &str,
) -> Result<Option<TenantSettingsRow>, DbError> {
    let mut select = Query::select();
    select
        .columns(["answer_threshold_pct", "widget_origins"])
        .from(iden("sg_tenant_settings"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id));
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows.rows.first().map(|row| TenantSettingsRow {
        answer_threshold_pct: row.get("answer_threshold_pct").unwrap_or_default(),
        widget_origins: row.get("widget_origins").unwrap_or_default(),
    }))
}

/// The tenant's normalized widget origin allowlist, parsed from its JSON
/// text. `None` (never set) and an empty array both mean the widget is
/// refused; a malformed stored array is treated the same way rather than
/// trusted — only the admin route writes this column and it normalizes
/// before storing.
pub(crate) fn parse_widget_origins(stored: Option<&str>) -> Vec<String> {
    stored
        .and_then(|json| serde_json::from_str::<Vec<String>>(json).ok())
        .unwrap_or_default()
}

/// One atomic statement for "set the fields this request named, leave
/// the others exactly as they stand": the `DO UPDATE` SET list is built
/// from the provided fields only, so an omitted field keeps its stored
/// value with no read-merge-write two concurrent admins could interleave
/// (and one of their writes lose). `default_threshold_pct` fills the NOT
/// NULL column only when the insert creates the row's first version — a
/// conflict never reaches it. `ON CONFLICT … DO UPDATE SET x =
/// excluded.x` is the one upsert form SQLite and Postgres agree on.
pub(crate) async fn upsert_tenant_settings(
    db: &dyn Database,
    tenant_id: &str,
    answer_threshold_pct: Option<i64>,
    widget_origins: Option<&str>,
    default_threshold_pct: i64,
    updated_at: &str,
) -> Result<(), DbError> {
    let mut insert = Query::insert();
    insert
        .into_table(iden("sg_tenant_settings"))
        .columns([
            "tenant_id",
            "answer_threshold_pct",
            "widget_origins",
            "updated_at",
        ])
        .values_panic([
            tenant_id.into(),
            answer_threshold_pct.unwrap_or(default_threshold_pct).into(),
            widget_origins.into(),
            updated_at.into(),
        ]);
    let mut conflict = OnConflict::column(iden("tenant_id")).clone();
    if let Some(pct) = answer_threshold_pct {
        conflict.value(iden("answer_threshold_pct"), pct);
    }
    if widget_origins.is_some() {
        conflict.value(iden("widget_origins"), widget_origins);
    }
    conflict.value(iden("updated_at"), updated_at);
    insert.on_conflict(conflict);
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
    /// The language the turn was conducted in, as a BCP-47 primary tag —
    /// what the prompt's `respond_in` named, what a canned body was
    /// rendered in, and the `lang` column of *both* messages of the turn:
    /// the turn is the unit, and the user's question and the answer shown
    /// for it share one language by construction.
    pub lang: Option<String>,
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
            "lang",
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
            turn.lang.clone().into(),
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
            turn.lang.clone().into(),
            turn.now.clone().into(),
        ]);

    vec![conversation, Statement::render(&messages)]
}

// ---------------------------------------------------------------------------
// Chunked uploads (issue #30). The part *bytes* live in the `Blob` port;
// these rows carry only identity, sizes and state, so every statement
// below stays in the portable table subset.
// ---------------------------------------------------------------------------

pub(crate) const UPLOAD_OPEN: &str = "open";
pub(crate) const UPLOAD_COMPLETE: &str = "complete";
pub(crate) const UPLOAD_EXTRACTED: &str = "extracted";
pub(crate) const UPLOAD_FAILED: &str = "failed";

pub(crate) struct UploadRow {
    pub id: String,
    pub tenant_id: String,
    pub filename: String,
    pub content_type: String,
    pub declared_bytes: i64,
    pub received_bytes: i64,
    pub status: String,
    pub source_id: Option<String>,
    pub error: Option<String>,
    pub created_at: String,
    pub completed_at: Option<String>,
}

pub(crate) struct UploadPartRow {
    pub n: i64,
    pub bytes: i64,
}

fn upload_row(row: &cratefield_core::Row) -> Option<UploadRow> {
    Some(UploadRow {
        id: row.get("id")?,
        tenant_id: row.get("tenant_id")?,
        filename: row.get("filename")?,
        content_type: row.get("content_type")?,
        declared_bytes: row.get("declared_bytes")?,
        received_bytes: row.get("received_bytes")?,
        status: row.get("status")?,
        source_id: row.get("source_id")?,
        error: row.get("error")?,
        created_at: row.get("created_at")?,
        completed_at: row.get("completed_at")?,
    })
}

pub(crate) async fn insert_upload(db: &dyn Database, upload: &UploadRow) -> Result<(), DbError> {
    let mut insert = Query::insert();
    insert
        .into_table(iden("sg_uploads"))
        .columns([
            "id",
            "tenant_id",
            "filename",
            "content_type",
            "declared_bytes",
            "received_bytes",
            "status",
            "source_id",
            "error",
            "created_at",
            "completed_at",
        ])
        .values_panic([
            upload.id.clone().into(),
            upload.tenant_id.clone().into(),
            upload.filename.clone().into(),
            upload.content_type.clone().into(),
            upload.declared_bytes.into(),
            upload.received_bytes.into(),
            upload.status.clone().into(),
            upload.source_id.clone().into(),
            upload.error.clone().into(),
            upload.created_at.clone().into(),
            upload.completed_at.clone().into(),
        ]);
    execute(db, &Statement::render(&insert)).await
}

/// The tenant's own upload — a row from another tenant is no row at all,
/// the same convention as [`find_conversation`].
pub(crate) async fn find_upload(
    db: &dyn Database,
    tenant_id: &str,
    upload_id: &str,
) -> Result<Option<UploadRow>, DbError> {
    let mut select = Query::select();
    select
        .columns([
            "id",
            "tenant_id",
            "filename",
            "content_type",
            "declared_bytes",
            "received_bytes",
            "status",
            "source_id",
            "error",
            "created_at",
            "completed_at",
        ])
        .from(iden("sg_uploads"))
        .and_where(Expr::col(iden("id")).eq(upload_id))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id));
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows.rows.first().and_then(upload_row))
}

/// The tenant's retained upload storage in bytes: `declared_bytes` summed
/// over uploads whose parts still exist (`open` and `complete`). A
/// terminal upload ('extracted'/'failed') has had its part blobs deleted,
/// so it no longer counts against the quota — the indexed `sg_sources`
/// row an 'extracted' upload leaves behind is the index's business, not
/// the upload quota's.
pub(crate) async fn tenant_retained_upload_bytes(
    db: &dyn Database,
    tenant_id: &str,
) -> Result<i64, DbError> {
    let mut select = Query::select();
    select
        .expr_as(
            Func::coalesce(vec![
                Func::sum(Expr::col(iden("declared_bytes"))).into(),
                Expr::value(0_i64),
            ]),
            Alias::new("retained"),
        )
        .from(iden("sg_uploads"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(
            Expr::col(iden("status")).is_in([UPLOAD_OPEN.to_owned(), UPLOAD_COMPLETE.to_owned()]),
        );
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows
        .rows
        .first()
        .and_then(|row| row.get::<i64>("retained"))
        .unwrap_or(0))
}

/// Stores (or, on a re-`PUT`, replaces) one part's size row. `ON CONFLICT
/// … DO UPDATE` is the one upsert form SQLite and Postgres agree on.
pub(crate) async fn upsert_upload_part(
    db: &dyn Database,
    upload_id: &str,
    n: i64,
    bytes: i64,
) -> Result<(), DbError> {
    let mut insert = Query::insert();
    insert
        .into_table(iden("sg_upload_parts"))
        .columns(["upload_id", "n", "bytes"])
        .values_panic([upload_id.into(), n.into(), bytes.into()])
        .on_conflict(
            OnConflict::columns([iden("upload_id"), iden("n")])
                .update_column(iden("bytes"))
                .to_owned(),
        );
    execute(db, &Statement::render(&insert)).await
}

/// `UPDATE sg_uploads SET received_bytes = ?` — the progress the GET
/// upload route reports between parts. `complete` writes the
/// authoritative total in its own guarded statement, so this is
/// bookkeeping only.
pub(crate) async fn set_upload_received(
    db: &dyn Database,
    upload_id: &str,
    received_bytes: i64,
) -> Result<(), DbError> {
    let mut update = Query::update();
    update
        .table(iden("sg_uploads"))
        .values([(iden("received_bytes"), received_bytes.into())])
        .and_where(Expr::col(iden("id")).eq(upload_id));
    execute(db, &Statement::render(&update)).await
}

/// The upload's parts in ordinal order. The extract job and `complete`
/// both derive contiguity and the running total from this list.
pub(crate) async fn upload_parts(
    db: &dyn Database,
    upload_id: &str,
) -> Result<Vec<UploadPartRow>, DbError> {
    let mut select = Query::select();
    select
        .columns(["n", "bytes"])
        .from(iden("sg_upload_parts"))
        .and_where(Expr::col(iden("upload_id")).eq(upload_id))
        .order_by(iden("n"), sea_query::Order::Asc);
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows
        .rows
        .iter()
        .filter_map(|row| {
            Some(UploadPartRow {
                n: row.get("n")?,
                bytes: row.get("bytes")?,
            })
        })
        .collect())
}

/// `UPDATE sg_uploads SET status = 'complete', …` for
/// `POST /uploads/{id}/complete`, guarded on the upload still being
/// `open` so a collected upload is never flipped back. The outbox insert
/// that pairs with it is the caller's; the job id is derived from the
/// upload id (see `uploads::extract_job_id`), so a duplicate `complete`
/// loses to the outbox's primary key instead of enqueueing a second
/// extract.
pub(crate) fn close_upload_stmt(
    upload_id: &str,
    received_bytes: i64,
    completed_at: &str,
) -> Statement {
    let mut update = Query::update();
    update
        .table(iden("sg_uploads"))
        .values([
            (iden("status"), UPLOAD_COMPLETE.into()),
            (iden("received_bytes"), received_bytes.into()),
            (iden("completed_at"), completed_at.into()),
        ])
        .and_where(Expr::col(iden("id")).eq(upload_id))
        .and_where(Expr::col(iden("status")).eq(UPLOAD_OPEN));
    Statement::render(&update)
}

/// The write that ends an extract job: the upload's terminal status, the
/// deletion of its part rows, and the retirement of the finished outbox
/// job — one batch, so an upload is never left half-decided. The part
/// *blobs* are deleted after the batch commits (see `uploads`), never
/// inside it — a blob write cannot join a database transaction.
pub(crate) enum UploadOutcome {
    /// The extract job indexed the document.
    Extracted {
        upload_id: String,
        source_id: String,
        completed_at: String,
    },
    /// The extract job could not read the document; `error` says why.
    Failed {
        upload_id: String,
        error: String,
        completed_at: String,
    },
}

impl UploadOutcome {
    /// `UPDATE sg_uploads …` for this outcome.
    fn statement(&self) -> Statement {
        let (upload_id, values) = match self {
            Self::Extracted {
                upload_id,
                source_id,
                completed_at,
            } => (
                upload_id,
                [
                    (iden("status"), UPLOAD_EXTRACTED.into()),
                    (iden("source_id"), source_id.clone().into()),
                    (iden("error"), Option::<String>::None.into()),
                    (iden("completed_at"), completed_at.clone().into()),
                ],
            ),
            Self::Failed {
                upload_id,
                error,
                completed_at,
            } => (
                upload_id,
                [
                    (iden("status"), UPLOAD_FAILED.into()),
                    (iden("source_id"), Option::<String>::None.into()),
                    (iden("error"), error.clone().into()),
                    (iden("completed_at"), completed_at.clone().into()),
                ],
            ),
        };
        let mut update = Query::update();
        update
            .table(iden("sg_uploads"))
            .values(values)
            .and_where(Expr::col(iden("id")).eq(upload_id.as_str()));
        Statement::render(&update)
    }
}

/// `DELETE FROM sg_upload_parts WHERE upload_id = ?`.
pub(crate) fn delete_upload_parts_stmt(upload_id: &str) -> Statement {
    let mut delete = Query::delete();
    delete
        .from_table(iden("sg_upload_parts"))
        .and_where(Expr::col(iden("upload_id")).eq(upload_id));
    Statement::render(&delete)
}

/// `DELETE FROM sg_support_outbox WHERE id = ?` — the same write
/// `Outbox::complete` runs, as a statement so the extract job's terminal
/// batch can retire it atomically with the upload's status change.
pub(crate) fn complete_outbox_stmt(job_id: &str) -> Statement {
    let mut delete = Query::delete();
    delete
        .from_table(iden(crate::uploads::OUTBOX_TABLE))
        .and_where(Expr::col(iden("id")).eq(job_id));
    Statement::render(&delete)
}

/// The statements that make an upload terminal (rows only; blobs after).
pub(crate) fn upload_outcome_statements(outcome: &UploadOutcome, job_id: &str) -> Vec<Statement> {
    let upload_id = match outcome {
        UploadOutcome::Extracted { upload_id, .. } | UploadOutcome::Failed { upload_id, .. } => {
            upload_id
        }
    };
    vec![
        outcome.statement(),
        delete_upload_parts_stmt(upload_id),
        complete_outbox_stmt(job_id),
    ]
}

/// Open uploads created before `cutoff` — the cron GC's work list.
pub(crate) async fn open_uploads_before(
    db: &dyn Database,
    cutoff: &str,
) -> Result<Vec<UploadRow>, DbError> {
    let mut select = Query::select();
    select
        .columns([
            "id",
            "tenant_id",
            "filename",
            "content_type",
            "declared_bytes",
            "received_bytes",
            "status",
            "source_id",
            "error",
            "created_at",
            "completed_at",
        ])
        .from(iden("sg_uploads"))
        .and_where(Expr::col(iden("status")).eq(UPLOAD_OPEN))
        .and_where(Expr::col(iden("created_at")).lt(cutoff));
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows.rows.iter().filter_map(upload_row).collect())
}

/// The statements that forget an upload and its parts. The part blobs are
/// deleted before this batch commits, so a row that survives names no
/// blob storage and a blob that survives (a delete that failed) names no
/// row — see `uploads::gc_abandoned`.
pub(crate) fn delete_upload_statements(upload_id: &str) -> Vec<Statement> {
    let mut delete = Query::delete();
    delete
        .from_table(iden("sg_uploads"))
        .and_where(Expr::col(iden("id")).eq(upload_id));
    vec![
        delete_upload_parts_stmt(upload_id),
        Statement::render(&delete),
    ]
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

// ---------------------------------------------------------------------------
// Connectors (issue #29): persistence for the crawl roots, their per-URL
// fetch state, and the ingest outbox. Same two shapes as above: pure
// statement builders that compose into the fetch job's **one**
// `batch_atomic`, and async readers over `&dyn Database`.
// ---------------------------------------------------------------------------

/// One crawl root: what `sg_connectors` stores. `config` is the
/// connector kind's own JSON (`{"url": …}`, or the GitHub owner/repo
/// triple — see `crate::connectors::ConnectorConfig`); `credential_ref`
/// names the Config key the GitHub token lives under, never the token.
pub(crate) struct ConnectorRow {
    pub id: String,
    pub tenant_id: String,
    pub kind: String,
    pub config: String,
    pub credential_ref: Option<String>,
    pub max_pages: i64,
    pub max_bytes: i64,
    pub max_depth: i64,
    pub created_at: String,
}

/// Per-URL fetch state: one `sg_ingest_pages` row per URL a connector has
/// ever fetched, carrying the conditional-GET validators and the source
/// the URL currently indexes (`None` for fetches that index nothing — a
/// sitemap or a GitHub tree listing).
pub(crate) struct PageRow {
    pub connector_id: String,
    pub url: String,
    pub tenant_id: String,
    pub role: String,
    pub depth: i64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub source_id: Option<String>,
}

/// The `insert_connector` statement, so a connector row and its seed fetch
/// job can land in one `batch_atomic` (a connector that exists but has no
/// seed job is exactly the half-state the batch exists to prevent).
#[must_use]
pub(crate) fn insert_connector_stmt(connector: &ConnectorRow) -> Statement {
    let mut insert = Query::insert();
    insert
        .into_table(iden("sg_connectors"))
        .columns([
            "id",
            "tenant_id",
            "kind",
            "config",
            "credential_ref",
            "max_pages",
            "max_bytes",
            "max_depth",
            "created_at",
        ])
        .values_panic([
            connector.id.clone().into(),
            connector.tenant_id.clone().into(),
            connector.kind.clone().into(),
            connector.config.clone().into(),
            connector.credential_ref.clone().into(),
            connector.max_pages.into(),
            connector.max_bytes.into(),
            connector.max_depth.into(),
            connector.created_at.clone().into(),
        ]);
    Statement::render(&insert)
}

/// The `sg_connectors` row shape, mapped in one place for every reader
/// (the find-by-id and the re-sync's listing).
fn connector_row(row: &Row) -> ConnectorRow {
    ConnectorRow {
        id: row.get("id").unwrap_or_default(),
        tenant_id: row.get("tenant_id").unwrap_or_default(),
        kind: row.get("kind").unwrap_or_default(),
        config: row.get("config").unwrap_or_default(),
        credential_ref: row.get("credential_ref"),
        max_pages: row.get("max_pages").unwrap_or_default(),
        max_bytes: row.get("max_bytes").unwrap_or_default(),
        max_depth: row.get("max_depth").unwrap_or_default(),
        created_at: row.get("created_at").unwrap_or_default(),
    }
}

const CONNECTOR_COLUMNS: [&str; 9] = [
    "id",
    "tenant_id",
    "kind",
    "config",
    "credential_ref",
    "max_pages",
    "max_bytes",
    "max_depth",
    "created_at",
];

pub(crate) async fn find_connector(
    db: &dyn Database,
    id: &str,
) -> Result<Option<ConnectorRow>, DbError> {
    let mut select = Query::select();
    select
        .columns(CONNECTOR_COLUMNS)
        .from(iden("sg_connectors"))
        .and_where(Expr::col(iden("id")).eq(id));
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows.rows.first().map(connector_row))
}

/// Every connector in the deployment. The scheduled re-sync sweeps all of
/// them; tenancy applies per row (`tenant_id` travels in every row and in
/// every fetch job it produces), not by pre-filtering the sweep.
pub(crate) async fn list_connectors(db: &dyn Database) -> Result<Vec<ConnectorRow>, DbError> {
    let mut select = Query::select();
    select
        .columns(CONNECTOR_COLUMNS)
        .from(iden("sg_connectors"))
        .order_by(iden("created_at"), Order::Asc);
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows.rows.iter().map(connector_row).collect())
}

/// The `sg_ingest_pages` upsert statement — the page row's validators and
/// source link land in the fetch job's batch, composed with the other
/// statements the same fetch needs.
#[must_use]
pub(crate) fn upsert_page_stmt(page: &PageRow) -> Statement {
    let mut insert = Query::insert();
    insert
        .into_table(iden("sg_ingest_pages"))
        .columns([
            "connector_id",
            "url",
            "tenant_id",
            "role",
            "depth",
            "etag",
            "last_modified",
            "source_id",
        ])
        .values_panic([
            page.connector_id.clone().into(),
            page.url.clone().into(),
            page.tenant_id.clone().into(),
            page.role.clone().into(),
            page.depth.into(),
            page.etag.clone().into(),
            page.last_modified.clone().into(),
            page.source_id.clone().into(),
        ])
        .on_conflict(
            OnConflict::columns([iden("connector_id"), iden("url")])
                .update_columns([
                    iden("role"),
                    iden("depth"),
                    iden("etag"),
                    iden("last_modified"),
                    iden("source_id"),
                ])
                .to_owned(),
        );
    Statement::render(&insert)
}

/// The `sg_ingest_pages` row shape, mapped in one place for every reader
/// (the find-by-url, the re-sync frontier and the row count).
fn page_row(row: &Row) -> PageRow {
    PageRow {
        connector_id: row.get("connector_id").unwrap_or_default(),
        url: row.get("url").unwrap_or_default(),
        tenant_id: row.get("tenant_id").unwrap_or_default(),
        role: row.get("role").unwrap_or_default(),
        depth: row.get("depth").unwrap_or_default(),
        etag: row.get("etag"),
        last_modified: row.get("last_modified"),
        source_id: row.get("source_id"),
    }
}

const PAGE_COLUMNS: [&str; 8] = [
    "connector_id",
    "url",
    "tenant_id",
    "role",
    "depth",
    "etag",
    "last_modified",
    "source_id",
];

pub(crate) async fn find_page(
    db: &dyn Database,
    connector_id: &str,
    url: &str,
) -> Result<Option<PageRow>, DbError> {
    let mut select = Query::select();
    select
        .columns(PAGE_COLUMNS)
        .from(iden("sg_ingest_pages"))
        .and_where(Expr::col(iden("connector_id")).eq(connector_id))
        .and_where(Expr::col(iden("url")).eq(url));
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows.rows.first().map(page_row))
}

/// Every URL the connector has fetched, with the role it was fetched as —
/// the re-sync frontier.
pub(crate) async fn page_rows(
    db: &dyn Database,
    connector_id: &str,
) -> Result<Vec<PageRow>, DbError> {
    let mut select = Query::select();
    select
        .columns(PAGE_COLUMNS)
        .from(iden("sg_ingest_pages"))
        .and_where(Expr::col(iden("connector_id")).eq(connector_id))
        .order_by(iden("url"), Order::Asc);
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows.rows.iter().map(page_row).collect())
}

/// How many rows the connector holds in `sg_ingest_pages` — the count the
/// page cap is measured against. **Every** row spends the cap, sources
/// and navigation rows (sitemaps, GitHub trees) alike: that is what bounds
/// a sitemap index's breadth, which no per-source count could.
pub(crate) async fn count_page_rows(db: &dyn Database, connector_id: &str) -> Result<i64, DbError> {
    let mut select = Query::select();
    select
        .expr_as(Func::count(Expr::col(iden("url"))), Alias::new("n"))
        .from(iden("sg_ingest_pages"))
        .and_where(Expr::col(iden("connector_id")).eq(connector_id));
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows
        .rows
        .first()
        .and_then(|row| row.get::<i64>("n"))
        .unwrap_or(0))
}

/// The `subject` of every un-retired outbox row — the key the enqueue
/// paths dedup against, so a re-sync tick (or two pages linking the same
/// URL) cannot stack duplicate rows, each with a fresh retry budget.
/// Completed rows are deleted, so this stays small: the in-flight and
/// backoff rows only.
pub(crate) async fn pending_subjects(db: &dyn Database) -> Result<HashSet<String>, DbError> {
    let mut select = Query::select();
    select
        .column(iden("subject"))
        .from(iden(crate::connectors::OUTBOX_TABLE))
        .and_where(Expr::col(iden("subject")).is_not_null());
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows
        .rows
        .iter()
        .filter_map(|row| row.get::<String>("subject"))
        .collect())
}

/// Deletes one fetched URL's page row — the companion of
/// [`delete_source_statements`] when a page answers 404/410.
#[must_use]
pub(crate) fn delete_page_stmt(connector_id: &str, url: &str) -> Statement {
    let mut delete = Query::delete();
    delete
        .from_table(iden("sg_ingest_pages"))
        .and_where(Expr::col(iden("connector_id")).eq(connector_id))
        .and_where(Expr::col(iden("url")).eq(url));
    Statement::render(&delete)
}

/// The ingest outbox's `enqueue_statement`, wrapped so fetch jobs cannot
/// mistype the topic. The subject is the job's dedup key —
/// `{connector_id} {url}`, from [`FetchJob::subject`]: one un-retired row
/// per connector and URL. (This table is declared `unreachable` for
/// erasure, so the subject carries no person here.)
#[must_use]
pub(crate) fn enqueue_fetch_stmt(
    outbox: &cratefield_core::Outbox,
    job_id: &str,
    payload_json: &str,
    subject: &str,
    at: &str,
) -> Statement {
    outbox.enqueue_statement(
        job_id,
        crate::connectors::TOPIC_FETCH,
        payload_json,
        Some(subject),
        at,
    )
}

/// The rendered equivalent of `Outbox::complete(db, id)` — see
/// `module-escalation`'s store for why this statement form exists (core
/// has no `complete_statement` at the rev this workspace pins).
#[must_use]
pub(crate) fn ingest_outbox_complete_stmt(id: &str) -> Statement {
    let mut delete = Query::delete();
    delete
        .from_table(iden(crate::connectors::OUTBOX_TABLE))
        .and_where(Expr::col(iden("id")).eq(id));
    Statement::render(&delete)
}

/// The rendered equivalent of `Outbox::retry_later(db, id, next_at)` —
/// same provenance as [`ingest_outbox_complete_stmt`].
#[must_use]
pub(crate) fn ingest_outbox_retry_later_stmt(id: &str, next_attempt_at: &str) -> Statement {
    let mut update = Query::update();
    update
        .table(iden(crate::connectors::OUTBOX_TABLE))
        .value(iden("attempts"), Expr::col(iden("attempts")).add(1))
        .value(iden("next_attempt_at"), next_attempt_at)
        .value(iden("locked_until"), Option::<String>::None)
        .and_where(Expr::col(iden("id")).eq(id));
    Statement::render(&update)
}

/// A conversation as the widget transcript route shows it. `status` is
/// the stored column (`'open' | 'escalated'`), redundant with
/// `needs_escalation` by this schema's invariant and sent anyway so the
/// widget reads one word instead of deriving it.
pub(crate) struct WidgetConversation {
    pub id: String,
    pub status: String,
    pub needs_escalation: bool,
}

/// The conversation `conversation_id`, scoped to the tenant, with its
/// status column — the widget transcript route's existence-and-state
/// read. Scoped exactly like [`find_conversation`]: a foreign id is no
/// conversation at all.
pub(crate) async fn find_conversation_with_status(
    db: &dyn Database,
    tenant_id: &str,
    conversation_id: &str,
) -> Result<Option<WidgetConversation>, DbError> {
    let mut select = Query::select();
    select
        .columns(["id", "status", "needs_escalation"])
        .from(iden("sg_conversations"))
        .and_where(Expr::col(iden("id")).eq(conversation_id))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id));
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows.rows.first().map(|row| WidgetConversation {
        id: row.get("id").unwrap_or_default(),
        status: row.get("status").unwrap_or_default(),
        needs_escalation: row.get::<i64>("needs_escalation").unwrap_or(0) != 0,
    }))
}

/// One message as the widget transcript shows it: the body the visitor
/// was shown — never `model_answer`, which stays out of every
/// user-facing path — plus the assistant turn's outcome and its
/// retrieved-only citations.
pub(crate) struct WidgetMessage {
    pub id: String,
    pub role: String,
    pub body: String,
    pub outcome: Option<String>,
    pub citations: Vec<(String, String)>,
    pub created_at: String,
}

/// The conversation's messages in `seq` order (the order the turn writer
/// guarantees), scoped to the tenant. A stored citations blob that does
/// not parse contributes no citations rather than failing the transcript:
/// the column is written only by this module and only as JSON, but a
/// transcript is a read path and one bad row must not blank it.
pub(crate) async fn conversation_messages(
    db: &dyn Database,
    tenant_id: &str,
    conversation_id: &str,
) -> Result<Vec<WidgetMessage>, DbError> {
    let mut select = Query::select();
    select
        .columns(["id", "role", "body", "outcome", "citations", "created_at"])
        .from(iden("sg_messages"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id))
        .and_where(Expr::col(iden("conversation_id")).eq(conversation_id))
        .order_by(iden("seq"), sea_query::Order::Asc);
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows
        .rows
        .iter()
        .filter_map(|row| {
            let citations = row
                .get::<Option<String>>("citations")
                .unwrap_or_default()
                .and_then(|json| serde_json::from_str::<Vec<ModelCitationJson>>(&json).ok())
                .map(|parsed| {
                    parsed
                        .into_iter()
                        .map(|citation| (citation.chunk_id, citation.quote))
                        .collect()
                })
                .unwrap_or_default();
            Some(WidgetMessage {
                id: row.get("id")?,
                role: row.get("role")?,
                body: row.get("body")?,
                outcome: row.get("outcome").unwrap_or_default(),
                citations,
                created_at: row.get("created_at")?,
            })
        })
        .collect())
}

/// The stored shape of one citation in the `citations` JSON column.
#[derive(serde::Deserialize)]
struct ModelCitationJson {
    chunk_id: String,
    quote: String,
}
