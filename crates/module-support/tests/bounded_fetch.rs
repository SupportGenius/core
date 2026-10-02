//! Issue #31 acceptance, over every available dialect: the bounded
//! fetch. Two claims are pinned here, on corpora too big for one read.
//!
//! Scoring parity: ranking on a per-term-truncated fetch returns exactly
//! what ranking the **full** index returns — same order, bit-equal
//! scores — because df and N come from the persisted statistics
//! (`sg_terms`, `sg_tenant_stats`), so the fetched subset decides which
//! chunks get a contribution and never what a contribution weighs.
//!
//! Cost flatness: a whole query reads a fixed budget of rows whatever
//! the corpus size — measured, not asserted, through a counting
//! `Database` proxy — and never runs an aggregate over the index.

use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode, header};
use cratefield_core::{Database, DbError, MapConfig, Module, Rows, Statement};
use cratefield_testing::{Dialect, TestHarness};
use module_support::Support;
use module_support::bm25;
use module_support::chunk::Chunk;
use module_support::store::{self, MAX_POSTINGS_PER_TERM, MAX_QUERY_TERMS, SourceRow};
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tower::ServiceExt;

const ADMIN_TOKEN: &str = "test-admin-token-0123456789abcdef";
const ADMIN: &str = "/v1/support/admin/tenants";
const SEARCH: &str = "/v1/support/search";

/// The rows a `/search` request reads before retrieval even starts:
/// `authenticate` resolves the bearer key with one primary-key read. The
/// budgets below measure the whole request, so they carry this row.
const AUTH_ROWS: u64 = 1;

/// `usize` as `u64`; every count here fits.
fn n(x: usize) -> u64 {
    u64::try_from(x).expect("count fits")
}

async fn send(
    router: &axum::Router,
    method: Method,
    path: &str,
    bearer: &str,
    json_body: Option<String>,
) -> (StatusCode, Value) {
    let builder = Request::builder()
        .method(method)
        .uri(path)
        .header(header::AUTHORIZATION, format!("Bearer {bearer}"));
    let (builder, body) = match json_body {
        Some(payload) => (
            builder.header(header::CONTENT_TYPE, "application/json"),
            Body::from(payload),
        ),
        None => (builder, Body::empty()),
    };
    let response = router
        .clone()
        .oneshot(builder.body(body).expect("request builds"))
        .await
        .expect("router answers");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("response body reads");
    (status, serde_json::from_slice(&bytes).expect("JSON body"))
}

/// Mints a tenant, returning `(api_key, tenant_id)`.
async fn mint_tenant(kit: &TestHarness, name: &str) -> (String, String) {
    let (status, body) = send(
        &kit.router,
        Method::POST,
        ADMIN,
        ADMIN_TOKEN,
        Some(serde_json::json!({ "name": name }).to_string()),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    (
        body["api_key"].as_str().expect("api_key").to_owned(),
        body["tenant_id"].as_str().expect("tenant_id").to_owned(),
    )
}

async fn search(kit: &TestHarness, key: &str, query: &str) -> (StatusCode, Value) {
    send(
        &kit.router,
        Method::GET,
        &format!("{SEARCH}?{query}"),
        key,
        None,
    )
    .await
}

/// What passed through the port since the last reset.
#[derive(Default)]
struct Stats {
    queries: u64,
    rows_returned: u64,
    /// Lower-cased SQL of every `query`; ingest batches are not kept.
    sql: Vec<String>,
}

/// One measured window, from one `take_window` to the next:
/// `(queries, rows, sql)`.
fn take_window(stats: &Mutex<Stats>) -> (u64, u64, Vec<String>) {
    let mut stats = stats.lock().expect("stats lock");
    let window = (stats.queries, stats.rows_returned, stats.sql.clone());
    *stats = Stats::default();
    window
}

/// No `COUNT`/`AVG`/`SUM` anywhere in a measured window: the query path
/// must never derive statistics from the rows it reads.
fn assert_no_aggregates(sql: &[String]) {
    assert!(
        sql.iter().all(|sql| {
            !sql.contains("count(") && !sql.contains("avg(") && !sql.contains("sum(")
        }),
        "the query path must not aggregate over the index: {sql:?}"
    );
}

/// The `Database` port, wrapped: everything the module reads or writes
/// passes through here, so a measured window's counts are the query's
/// whole database cost. The hand-written impl is the `async_trait`
/// expansion, the way `routes.rs` writes its test model.
struct CountingDb {
    inner: Arc<dyn Database>,
    stats: Arc<Mutex<Stats>>,
}

impl Database for CountingDb {
    fn execute<'life0, 'life1, 'async_trait>(
        &'life0 self,
        stmt: &'life1 Statement,
    ) -> Pin<Box<dyn Future<Output = Result<u64, DbError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.inner.execute(stmt).await })
    }

    fn query<'life0, 'life1, 'async_trait>(
        &'life0 self,
        stmt: &'life1 Statement,
    ) -> Pin<Box<dyn Future<Output = Result<Rows, DbError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let rows = self.inner.query(stmt).await?;
            let mut stats = self.stats.lock().expect("stats lock");
            stats.queries += 1;
            stats.rows_returned += n(rows.len());
            stats.sql.push(stmt.sql.to_ascii_lowercase());
            Ok(rows)
        })
    }

    fn batch_atomic<'life0, 'life1, 'async_trait>(
        &'life0 self,
        stmts: &'life1 [Statement],
    ) -> Pin<Box<dyn Future<Output = Result<(), DbError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.inner.batch_atomic(stmts).await })
    }
}

/// A kit whose database port counts what passes through it. `kit.db`
/// stays the raw migrated database: the fixture seeds through it, so a
/// measured window covers only the queries the routes run.
fn counted_kit(dialect: Dialect) -> (TestHarness, Arc<Mutex<Stats>>) {
    let stats = Arc::new(Mutex::new(Stats::default()));
    let counted = Arc::clone(&stats);
    let modules: Vec<Box<dyn Module>> = vec![Box::new(Support::new())];
    let kit = TestHarness::with_database_and_ports(modules, dialect, move |ports| {
        ports.config = Arc::new(MapConfig::from_pairs([("ADMIN_TOKEN", ADMIN_TOKEN)]));
        let inner = ports.db.take().expect("the harness mounts the database");
        ports.db = Some(Arc::new(CountingDb {
            inner,
            stats: counted,
        }));
    });
    (kit, stats)
}

/// Total tf — so the exact BM25 document length — of every fixture
/// chunk. The equality is what the scoring-parity claim stands on:
/// within a tf group every score ties and `rank`'s `chunk_id` tie-break
/// orders the group exactly as the `(tf DESC, chunk_id)` fetch reads it,
/// so the full rank's top-k is always a prefix of the bounded fetch's
/// rows and the test may demand the two rankings be identical. Without
/// it, a shorter (and so higher-scoring) chunk can rank high while
/// sitting outside a term's fetched rows — the fetch's documented trade,
/// not a bug this fixture may paper over.
const CHUNK_TERMS: u32 = 8;

/// One synthetic chunk: the controlled `(term, tf)` pairs plus one
/// unique filler term, padded to [`CHUNK_TERMS`]. The id and text are
/// arbitrary — only the term counts reach the ranking.
fn chunk(index: usize, controlled: &[(String, u32)]) -> Chunk {
    let mut terms = controlled.to_owned();
    let padded = CHUNK_TERMS - terms.iter().map(|(_, tf)| tf).sum::<u32>();
    terms.push((format!("f{index:05}"), padded));
    Chunk {
        id: format!("c{index:05}"),
        ordinal: u32::try_from(index % 100).expect("ordinal fits"),
        text: "synthetic".to_owned(),
        terms,
        length: CHUNK_TERMS,
    }
}

/// Inserts the specs — one chunk's controlled terms each — as 100-chunk
/// sources through the store primitive, the same `batch_atomic` the
/// HTTP ingest uses, and returns the chunks: the fixture's ground truth
/// for the reference below.
async fn seed(db: &dyn Database, tenant_id: &str, specs: &[Vec<(String, u32)>]) -> Vec<Chunk> {
    let chunks: Vec<Chunk> = specs
        .iter()
        .enumerate()
        .map(|(index, controlled)| chunk(index, controlled))
        .collect();
    for (number, group) in chunks.chunks(100).enumerate() {
        let source = SourceRow {
            id: format!("bulk-src-{number:04}"),
            tenant_id: tenant_id.to_owned(),
            title: "Bulk".to_owned(),
            url: None,
            external_id: None,
            byte_len: 0,
            created_at: "2026-01-01T00:00:00Z".to_owned(),
            updated_at: "2026-01-01T00:00:00Z".to_owned(),
        };
        store::insert_source_with_chunks(db, &source, group)
            .await
            .expect("bulk insert runs");
    }
    chunks
}

/// The unbounded side of every comparison: df recounted from the chunks,
/// N and the mean length recomputed, and every posting row of every kept
/// term, in the query path's accumulation order — the same reads without
/// the `LIMIT`.
fn reference(
    chunks: &[Chunk],
    kept: &[String],
) -> (HashMap<String, u64>, bm25::Corpus, Vec<bm25::Posting>) {
    let mut df: HashMap<String, u64> = HashMap::new();
    for chunk in chunks {
        for (term, _) in &chunk.terms {
            *df.entry(term.clone()).or_insert(0) += 1;
        }
    }
    let total: u64 = chunks.iter().map(|chunk| u64::from(chunk.length)).sum();
    #[expect(clippy::cast_precision_loss)]
    let avg_length = total as f64 / n(chunks.len()) as f64;
    let corpus = bm25::Corpus {
        chunk_count: n(chunks.len()),
        avg_length,
    };
    let mut postings: Vec<bm25::Posting> = Vec::new();
    for term in kept {
        let mut rows: Vec<bm25::Posting> = chunks
            .iter()
            .filter_map(|chunk| {
                let tf = chunk
                    .terms
                    .iter()
                    .find(|(candidate, _)| candidate == term)
                    .map(|(_, tf)| *tf)?;
                Some(bm25::Posting {
                    chunk_id: chunk.id.clone(),
                    term: term.clone(),
                    tf,
                    length: chunk.length,
                })
            })
            .collect();
        rows.sort_by(|x, y| y.tf.cmp(&x.tf).then_with(|| x.chunk_id.cmp(&y.chunk_id)));
        postings.extend(rows);
    }
    (df, corpus, postings)
}

/// Scoring parity where the truncation bites: 1000 chunks, "the" in 300
/// of them — 2.3x the per-term bound, all at equal tf — and "quokka" in
/// 10. The bounded query reads 128 of the 300 rows and must still return
/// exactly the full rank's top 50, bit for bit: the truncation hides
/// chunks, it never moves a score.
#[pollster::test]
async fn a_truncated_fetch_scores_exactly_what_the_full_index_scores() {
    for dialect in Dialect::available() {
        let (kit, stats) = counted_kit(dialect);
        let (api_key, tenant_id) = mint_tenant(&kit, "Parity").await;

        let mut specs = vec![vec![("quokka".to_owned(), 5), ("the".to_owned(), 2)]; 10];
        specs.extend(vec![vec![("the".to_owned(), 2)]; 290]);
        specs.extend(vec![vec![]; 700]);
        let chunks = seed(kit.db.as_ref(), &tenant_id, &specs).await;
        assert_eq!(chunks.len(), 1000);

        let kept = vec!["quokka".to_owned(), "the".to_owned()];
        let (df, corpus, postings) = reference(&chunks, &kept);
        assert_eq!(
            (df["the"], df["quokka"]),
            (300, 10),
            "the fixture built what it claims"
        );
        assert!(
            n(MAX_POSTINGS_PER_TERM) < df["the"],
            "the fixture must force the per-term truncation"
        );
        let expected = bm25::rank(&kept, &postings, &df, &corpus, &bm25::Params::default());
        assert_eq!(expected.len(), 300, "the full rank sees every candidate");

        take_window(&stats);
        let (code, body) = search(&kit, &api_key, "q=quokka+the&limit=50").await;
        assert_eq!(code, StatusCode::OK, "{body}");
        let results = body["results"].as_array().expect("results");
        assert_eq!(
            results.len(),
            50,
            "the clamp, not the corpus, bounds it: {body}"
        );
        for (hit, want) in results.iter().zip(&expected) {
            assert_eq!(
                hit["chunk_id"].as_str().expect("chunk id"),
                want.chunk_id,
                "order: {body}"
            );
            // Bit equality, not "close": the bounded fetch reads every row
            // the reference does for these chunks, so the sums are the
            // same additions in the same order.
            assert_eq!(
                hit["score"].as_f64().expect("score").to_bits(),
                want.score.to_bits(),
                "score parity for {}",
                want.chunk_id
            );
        }

        let (queries, rows, sql) = take_window(&stats);
        assert_no_aggregates(&sql);
        let terms = n(kept.len());
        let budget = AUTH_ROWS + 1 + terms + terms * n(MAX_POSTINGS_PER_TERM) + n(results.len());
        assert!(
            rows <= budget,
            "{rows} rows read must fit the {budget} budget"
        );
        eprintln!(
            "[parity] q=quokka+the over 1000 chunks (df(the)=300 > {MAX_POSTINGS_PER_TERM}): \
             {rows} rows read over {queries} queries, budget {budget}"
        );
    }
}

/// The flatness claim, measured: 10 000 chunks, then the two queries
/// whose old shape read the whole index — a stopword term (95% of the
/// corpus) and the worst case, 32 kept mid-frequency terms each past the
/// per-term bound. Every query reads the invariant's budget of rows and
/// aggregates nothing; on SQLite the per-term fetch is an index search
/// in the ORDER BY's own order, no sort, no scan.
#[pollster::test]
async fn a_query_reads_a_fixed_budget_of_rows_whatever_the_corpus_size() {
    for dialect in Dialect::available() {
        let (kit, stats) = counted_kit(dialect);
        let (api_key, tenant_id) = mint_tenant(&kit, "Bench").await;

        // 10 000 chunks: "the" in 9500 of them, one mid-frequency term
        // (`m00`..`m31`, 312 or 313 chunks each) in every one.
        let specs: Vec<Vec<(String, u32)>> = (0..10_000)
            .map(|i| {
                let mut terms: Vec<(String, u32)> = Vec::new();
                if i < 9_500 {
                    terms.push(("the".to_owned(), 2));
                }
                terms.push((format!("m{:02}", i % 32), 4));
                terms
            })
            .collect();
        let chunks = seed(kit.db.as_ref(), &tenant_id, &specs).await;
        assert_eq!(chunks.len(), 10_000);

        // What the pre-statistics shape read for these two queries,
        // counted from the fixture's known df: one COUNT/AVG over every
        // chunk, every posting row of every kept term, the result rows.
        let old_stopword = chunks.len() + 9_500 + 10;
        // Every chunk carries exactly one mid term, so the worst case's
        // 32 unbounded fetches sum to the whole index.
        let old_worst = chunks.len() * 2 + 10;
        let all_mid: Vec<String> = (0..MAX_QUERY_TERMS).map(|m| format!("m{m:02}")).collect();
        let worst = format!("q={}", all_mid.join("+"));
        let worst_budget =
            AUTH_ROWS + 1 + n(MAX_QUERY_TERMS) + n(MAX_QUERY_TERMS * MAX_POSTINGS_PER_TERM) + 10;

        for (query, want, budget, old, label) in [
            ("q=the", 0, AUTH_ROWS + 2, old_stopword, "q=the"),
            (worst.as_str(), 10, worst_budget, old_worst, "worst case"),
        ] {
            take_window(&stats);
            let (code, body) = search(&kit, &api_key, query).await;
            assert_eq!(code, StatusCode::OK, "{body}");
            assert_eq!(
                body["results"].as_array().expect("results").len(),
                want,
                "{label}: {body}"
            );
            let (queries, rows, sql) = take_window(&stats);
            assert_no_aggregates(&sql);
            assert!(
                rows <= budget,
                "{label}: {rows} rows read must fit the {budget} budget"
            );
            eprintln!(
                "[bench] {label} over 10000 chunks: {rows} rows over {queries} queries, \
                 budget {budget} (old shape read ~{old})"
            );
        }

        if kit.dialect == "sqlite" {
            assert_sqlite_fetch_is_an_index_search(&kit, &tenant_id);
        }
    }
}

/// On SQLite, `EXPLAIN QUERY PLAN` for the per-term fetch the query path
/// runs: the tf index serves the equality, the order and the limit — a
/// search, never a scan, never a sort.
fn assert_sqlite_fetch_is_an_index_search(kit: &TestHarness, tenant_id: &str) {
    let plan = pollster::block_on(kit.db.query(&Statement::new(format!(
        "EXPLAIN QUERY PLAN SELECT sg_postings.term, sg_postings.chunk_id, sg_postings.tf, \
         sg_chunks.term_count AS length \
         FROM sg_postings JOIN sg_chunks ON sg_chunks.id = sg_postings.chunk_id \
         AND sg_chunks.tenant_id = '{tenant_id}' \
         WHERE sg_postings.tenant_id = '{tenant_id}' AND sg_postings.term = 'm00' \
         ORDER BY sg_postings.tf DESC, sg_postings.chunk_id ASC LIMIT {MAX_POSTINGS_PER_TERM}"
    ))))
    .expect("explain runs");
    let details: Vec<String> = plan
        .rows
        .iter()
        .map(|row| row.get("detail").expect("plan detail"))
        .collect();
    assert!(
        details.iter().any(|detail| {
            detail.contains("SEARCH")
                && detail.contains("sg_postings")
                && detail.contains("idx_sg_postings_tenant_term_tf")
        }),
        "the fetch must search the tf index: {details:?}"
    );
    assert!(
        details.iter().all(|detail| !detail.contains("TEMP B-TREE")),
        "the ORDER BY must come free from the index: {details:?}"
    );
}
