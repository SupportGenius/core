//! Every query the support module runs, built with `sea_query` and
//! rendered through [`cratefield_core::Statement::render`] (ADR 0004:
//! module code never writes raw SQL strings — the migrations are the only
//! hand-written SQL in this crate, and the `fz doctor` lint keeps them
//! portable).
//!
//! The isolation boundary is [`tenant_id`]: every statement in this file
//! filters on it, whatever credential was verified upstream. Nothing here
//! is reachable without a tenant id, and nothing here ignores one — the
//! one deliberate exception is the scheduled re-index's stale-chunk
//! select ([`stale_chunks`]), which is the module's own sweep over every
//! tenant it holds, not a tenant's request.

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

    db.batch_atomic(&statements).await
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
async fn chunk_index(
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

    db.batch_atomic(&statements).await?;

    // The re-check the doc above promises: if a DELETE committed between
    // the caller's existence read and this batch, the row is gone but the
    // windows written above are not — sweep them with the same
    // delete-by-source batch. (A DELETE committing after this check also
    // wins: it deletes by source, so it takes whatever this batch wrote.)
    if find_source(db, &source.tenant_id, &source.id)
        .await?
        .is_none()
    {
        delete_source(db, &source.tenant_id, &source.id).await?;
    }
    Ok(())
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

    db.batch_atomic(&[
        Statement::render(&delete_postings),
        Statement::render(&delete_chunks),
        Statement::render(&delete_row),
    ])
    .await
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

    let mut statements = vec![
        Statement::render(&delete_postings),
        Statement::render(&restamp),
    ];
    // Postings before the restamp, matching ingest's order: the index is
    // a projection of the rows, never the other way round.
    statements.extend(posting_rows_statements(&postings));
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
