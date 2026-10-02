//! Shared by the integration tests: the persisted search statistics
//! (`sg_terms`, `sg_tenant_stats`, issue #31) must equal a full recount
//! of the index after any write, through any path. The recount is the
//! expensive shape the query path no longer runs: df per term from the
//! postings whose chunk exists, N and total length from the chunks.

use cratefield_core::Statement;
use cratefield_testing::TestHarness;

/// `(tenant, term, df)` rows of `sql`, as the query returns them.
fn term_rows(kit: &TestHarness, sql: &str) -> Vec<(String, String, i64)> {
    let rows = pollster::block_on(kit.db.query(&Statement::new(sql))).expect("term query runs");
    rows.rows
        .iter()
        .map(|row| {
            (
                row.get("tenant_id").expect("tenant_id"),
                row.get("term").expect("term"),
                row.get("df").expect("df"),
            )
        })
        .collect()
}

/// `(tenant, n_chunks, total_len)` rows of `sql`.
fn tenant_rows(kit: &TestHarness, sql: &str) -> Vec<(String, i64, i64)> {
    let rows = pollster::block_on(kit.db.query(&Statement::new(sql))).expect("tenant query runs");
    rows.rows
        .iter()
        .map(|row| {
            (
                row.get("tenant_id").expect("tenant_id"),
                row.get("n_chunks").expect("n_chunks"),
                row.get("total_len").expect("total_len"),
            )
        })
        .collect()
}

/// Both statistics tables equal the recount for every tenant, no
/// statistics row outlives what it counts (no zero df, no tenant row
/// without chunks), and no posting outlives its chunk. Returns the
/// number of `sg_terms` rows, so a caller can also assert the check was
/// not vacuous.
pub(crate) fn assert_stats_exact(kit: &TestHarness, when: &str) -> usize {
    let persisted_terms = term_rows(
        kit,
        "SELECT tenant_id, term, df FROM sg_terms ORDER BY tenant_id, term",
    );
    let recounted_terms = term_rows(
        kit,
        "SELECT p.tenant_id AS tenant_id, p.term AS term, COUNT(*) AS df \
         FROM sg_postings p JOIN sg_chunks c \
           ON c.id = p.chunk_id AND c.tenant_id = p.tenant_id \
         GROUP BY p.tenant_id, p.term ORDER BY p.tenant_id, p.term",
    );
    assert_eq!(
        persisted_terms, recounted_terms,
        "sg_terms equals the recount {when}"
    );

    let persisted_tenants = tenant_rows(
        kit,
        "SELECT tenant_id, n_chunks, total_len FROM sg_tenant_stats ORDER BY tenant_id",
    );
    let recounted_tenants = tenant_rows(
        kit,
        "SELECT tenant_id, COUNT(*) AS n_chunks, SUM(term_count) AS total_len \
         FROM sg_chunks GROUP BY tenant_id ORDER BY tenant_id",
    );
    assert_eq!(
        persisted_tenants, recounted_tenants,
        "sg_tenant_stats equals the recount {when}"
    );

    let orphans = pollster::block_on(kit.db.query(&Statement::new(
        "SELECT COUNT(*) AS n FROM sg_postings p WHERE NOT EXISTS \
         (SELECT 1 FROM sg_chunks c WHERE c.id = p.chunk_id AND c.tenant_id = p.tenant_id)",
    )))
    .expect("orphan query runs");
    let orphans: i64 = orphans
        .rows
        .first()
        .and_then(|row| row.get("n"))
        .expect("aggregate row");
    assert_eq!(orphans, 0, "no posting outlives its chunk {when}");

    persisted_terms.len()
}

/// One tenant's df for `term`, `None` when the term has no row.
#[allow(dead_code)]
pub(crate) fn df_of(kit: &TestHarness, tenant_id: &str, term: &str) -> Option<i64> {
    let rows = pollster::block_on(kit.db.query(&Statement::new(format!(
        "SELECT df FROM sg_terms WHERE tenant_id = '{tenant_id}' AND term = '{term}'"
    ))))
    .expect("df query runs");
    rows.rows.first().and_then(|row| row.get("df"))
}
