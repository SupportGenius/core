//! Every query the support module runs, built with `sea_query` and
//! rendered through [`cratefield_core::Statement::render`] (ADR 0004:
//! module code never writes raw SQL strings — the migrations are the only
//! hand-written SQL in this crate, and the `fz doctor` lint keeps them
//! portable).
//!
//! The isolation boundary is [`tenant_id`]: every statement in this file
//! filters on it, whatever credential was verified upstream. Nothing here
//! is reachable without a tenant id, and nothing here ignores one.

use cratefield_core::{Database, DbError, Statement};
use sea_query::{Alias, Expr, Func, Query};
use time::format_description::well_known::Rfc3339;

use crate::bm25::{Corpus, Posting};
use crate::chunk::Chunk;

/// The only tenant status that authenticates. Anything else (a future
/// `suspended`, `closed`) fails key verification with the same
/// indistinguishable 401 as an unknown key.
pub(crate) const STATUS_ACTIVE: &str = "active";

/// The label recorded for the first key `POST /admin/tenants` mints.
pub(crate) const FIRST_KEY_LABEL: &str = "primary";

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

pub(crate) struct SourceRow {
    pub id: String,
    pub tenant_id: String,
    pub title: String,
    pub url: Option<String>,
    pub byte_len: i64,
    pub created_at: String,
}

/// What `chunks_by_id` returns for one ranked hit: enough to quote the
/// chunk back with its source.
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

/// Postings travel to `batch_atomic` in statements of this many rows.
/// Four bound values per row keeps each statement far under the
/// conservative 999-parameter limit some sqlite builds still enforce.
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

/// The source row, its chunks and the chunks' postings land in **one**
/// `batch_atomic`: a half-indexed source must never be able to exist,
/// because a source whose postings are missing some terms is worse than
/// no source — searches quietly return wrong answers instead of nothing.
pub(crate) async fn insert_source_with_chunks(
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

/// Chunk count and mean document length for one tenant's index — the two
/// numbers Okapi BM25 needs before it can score anything. An empty index
/// is `Corpus { chunk_count: 0, avg_length: 0.0 }`, not an error.
pub(crate) async fn corpus_stats(db: &dyn Database, tenant_id: &str) -> Result<Corpus, DbError> {
    let mut select = Query::select();
    select
        .expr_as(
            Func::count(Expr::col(iden("id"))),
            Alias::new("chunk_count"),
        )
        .expr_as(
            Func::coalesce(vec![
                Func::avg(Expr::col(iden("term_count"))).into(),
                Expr::value(0.0_f64),
            ]),
            Alias::new("avg_length"),
        )
        .from(iden("sg_chunks"))
        .and_where(Expr::col(iden("tenant_id")).eq(tenant_id));
    let rows = db.query(&Statement::render(&select)).await?;
    Ok(rows.rows.first().map_or_else(
        || Corpus {
            chunk_count: 0,
            avg_length: 0.0,
        },
        |row| Corpus {
            chunk_count: row.get("chunk_count").unwrap_or(0),
            avg_length: row.get("avg_length").unwrap_or(0.0),
        },
    ))
}

/// The index rows for every query term of one tenant, joined back to
/// `sg_chunks` for the document length.
///
/// **No `LIMIT` on this query, ever.** `bm25::rank` derives each term's
/// document frequency from the postings it is handed, so a truncated
/// fetch silently corrupts ranking: rarer terms would look rarer than
/// they are and win they should not. If this query ever needs a bound, it
/// needs a different correctness story first.
pub(crate) async fn postings_for(
    db: &dyn Database,
    tenant_id: &str,
    terms: &[String],
) -> Result<Vec<Posting>, DbError> {
    if terms.is_empty() {
        return Ok(Vec::new());
    }
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
        .and_where(Expr::col((iden("p"), iden("term"))).is_in(terms.to_vec()));
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
/// the `IN` here stays small — unlike [`postings_for`], which must not be
/// bounded at all.
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
