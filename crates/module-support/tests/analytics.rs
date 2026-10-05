//! Issue #36 acceptance: the analytics rollup and the three
//! `/v1/support/analytics` routes, over every available dialect.
//!
//! The fixture is a scripted two-tenant day, seeded by direct SQL rather
//! than through `/sources` and `/messages`: the rollup's whole contract
//! is *which stored rows count*, so the tests write exactly the rows they
//! mean — a handoff on the next day, a clarify that retrieved nothing, an
//! answered turn whose two cited chunks belong to one source — instead of
//! steering those states through the answer pipeline. The roots (`sg_tenants`,
//! `sg_api_keys`) still come from the real admin route, because the
//! routes authenticate against them.

use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode, header};
use cratefield_core::{BoxFuture, Database, DbError, MapConfig, Module, Statement};
use cratefield_testing::{Dialect, FakeTextModel, TestHarness, TextModelMode};
use module_support::Support;
use module_support::analytics::{
    ROLLUP_DAYS, TicketCounts, TicketStats, rollup_day, rollup_recent,
};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use tower::ServiceExt;

const ADMIN_TOKEN: &str = "test-admin-token-0123456789abcdef";
const ADMIN: &str = "/v1/support/admin/tenants";
const SOURCES: &str = "/v1/support/sources";
const MESSAGES: &str = "/v1/support/messages";
const ANALYTICS: &str = "/v1/support/analytics";

/// The fixture's days: the "yesterday" the rollup is asked for, and its
/// "today". Both are in the past relative to any real clock, so a test
/// never accidentally overlaps a run's own day.
const DAY: &str = "2026-05-10";
const DAY2: &str = "2026-05-11";

// ---------------------------------------------------------------------------
// Harness plumbing (the `routes.rs` helpers, trimmed to what this file
// needs).
// ---------------------------------------------------------------------------

struct Reply {
    status: StatusCode,
    body: Value,
}

impl Reply {
    async fn of(response: axum::response::Response) -> Self {
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("response body reads");
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).expect("response body is JSON")
        };
        Self { status, body }
    }

    fn problem_type(&self) -> &str {
        self.body["type"].as_str().unwrap_or_default()
    }
}

async fn send(
    router: &axum::Router,
    method: Method,
    path: &str,
    bearer: Option<&str>,
    json_body: Option<&str>,
) -> Reply {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(key) = bearer {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {key}"));
    }
    let body = match json_body {
        Some(payload) => {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
            Body::from(payload.to_owned())
        }
        None => Body::empty(),
    };
    let response = router
        .clone()
        .oneshot(builder.body(body).expect("request builds"))
        .await
        .expect("router answers");
    Reply::of(response).await
}

/// One kit per available dialect, configured with the admin token.
fn kits() -> Vec<TestHarness> {
    Dialect::available()
        .into_iter()
        .map(|dialect| {
            TestHarness::with_database_and_ports(
                vec![Box::new(Support::new()) as Box<dyn Module>],
                dialect,
                |ports| {
                    ports.config = Arc::new(MapConfig::from_pairs([("ADMIN_TOKEN", ADMIN_TOKEN)]));
                },
            )
        })
        .collect()
}

async fn mint_tenant(kit: &TestHarness, name: &str) -> (String, String) {
    let body = json!({ "name": name }).to_string();
    let reply = send(
        &kit.router,
        Method::POST,
        ADMIN,
        Some(ADMIN_TOKEN),
        Some(&body),
    )
    .await;
    assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
    (
        reply.body["tenant_id"]
            .as_str()
            .expect("tenant_id")
            .to_owned(),
        reply.body["api_key"].as_str().expect("api_key").to_owned(),
    )
}

fn count_of(kit: &TestHarness, table: &str) -> usize {
    let rows = pollster::block_on(kit.db.query(&Statement::new(format!(
        "SELECT COUNT(*) AS n FROM {table}"
    ))))
    .expect("count query runs");
    let n = rows
        .rows
        .first()
        .and_then(|row| row.get::<i64>("n"))
        .expect("aggregate row");
    usize::try_from(n).expect("count is non-negative")
}

/// Runs one parameterless statement (fixture seeding, or a mid-test
/// mutation), whose SQL the test wrote — no request data reaches it.
fn exec(kit: &TestHarness, sql: &str) {
    pollster::block_on(kit.db.execute(&Statement::new(sql))).expect("statement runs");
}

/// One table dumped as text: rows of nullable string cells.
type Rows = Vec<Vec<Option<String>>>;

/// Every row of `sql`, each cell read as text, for whole-table equality
/// checks: the SQL must `CAST` its columns to text.
fn dump(kit: &TestHarness, sql: &str, columns: &[&str]) -> Rows {
    let rows = pollster::block_on(kit.db.query(&Statement::new(sql))).expect("dump query runs");
    rows.rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .map(|column| row.get::<Option<String>>(column).flatten())
                .collect()
        })
        .collect()
}

/// `SELECT` of every named column cast to text and aliased back to its
/// own name, so [`dump`] can read it — `CAST(x AS TEXT)` alone would name
/// the result column `CAST(x AS TEXT)`.
fn cast_all(table: &str, columns: &[&str], order: &str) -> String {
    let projected: Vec<String> = columns
        .iter()
        .map(|column| format!("CAST({column} AS TEXT) AS {column}"))
        .collect();
    format!(
        "SELECT {} FROM {table} ORDER BY {order}",
        projected.join(", ")
    )
}

/// The three rollup tables as text, the whole state the rollup owns.
fn rollup_state(kit: &TestHarness) -> (Rows, Rows, Rows) {
    const STATS: &[&str] = &[
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
    ];
    const GAPS: &[&str] = &["tenant_id", "day", "term", "hits"];
    const CITATIONS: &[&str] = &["tenant_id", "day", "source_id", "cites"];
    (
        dump(
            kit,
            &cast_all("sg_daily_stats", STATS, "tenant_id, day"),
            STATS,
        ),
        dump(
            kit,
            &cast_all("sg_daily_gaps", GAPS, "tenant_id, day, term"),
            GAPS,
        ),
        dump(
            kit,
            &cast_all("sg_daily_citations", CITATIONS, "tenant_id, day, source_id"),
            CITATIONS,
        ),
    )
}

async fn get(kit: &TestHarness, path: &str, key: &str) -> Reply {
    send(&kit.router, Method::GET, path, Some(key), None).await
}

/// `GET /analytics` for one tenant over an inclusive day range.
async fn analytics(kit: &TestHarness, key: &str, from: &str, to: &str) -> Reply {
    get(kit, &format!("{ANALYTICS}?from={from}&to={to}"), key).await
}

/// `GET /analytics/gaps` over an inclusive day range.
async fn gaps(kit: &TestHarness, key: &str, from: &str, to: &str) -> Reply {
    get(kit, &format!("{ANALYTICS}/gaps?from={from}&to={to}"), key).await
}

/// `GET /analytics/citations` over an inclusive day range.
async fn citations(kit: &TestHarness, key: &str, from: &str, to: &str) -> Reply {
    get(
        kit,
        &format!("{ANALYTICS}/citations?from={from}&to={to}"),
        key,
    )
    .await
}

/// The `conversations` the rollup stored for one tenant-day, 0 if absent.
fn stat_conversations(kit: &TestHarness, tenant_id: &str, day: &str) -> i64 {
    dump(
        kit,
        &format!(
            "SELECT CAST(conversations AS TEXT) AS conversations FROM sg_daily_stats \
             WHERE tenant_id = {} AND day = {}",
            lit(tenant_id),
            lit(day),
        ),
        &["conversations"],
    )
    .first()
    .and_then(|row| row.first())
    .and_then(|cell| cell.as_ref())
    .and_then(|text| text.parse().ok())
    .unwrap_or(0)
}

fn day_row<'a>(body: &'a Value, day: &str) -> &'a Value {
    body["days"]
        .as_array()
        .expect("days")
        .iter()
        .find(|row| row["day"] == day)
        .unwrap_or_else(|| panic!("{day} in {body}"))
}

// ---------------------------------------------------------------------------
// The ticket-stats port, faked: escalation's own tests cover the SQL
// behind it (`ticket_counts_for_day`), so these tests only need the rollup
// to receive the counts the port hands over.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct FakeTicketStats {
    counts: HashMap<(String, String), TicketCounts>,
}

impl FakeTicketStats {
    fn with(mut self, counts: TicketCounts, day: &str) -> Self {
        self.counts
            .insert((counts.tenant_id.clone(), day.to_owned()), counts);
        self
    }
}

impl TicketStats for FakeTicketStats {
    fn day_counts<'a>(
        &'a self,
        _db: &'a dyn Database,
        day: &'a str,
    ) -> BoxFuture<'a, Result<Vec<TicketCounts>, DbError>> {
        Box::pin(async move {
            Ok(self
                .counts
                .iter()
                .filter(|((_, held), _)| held == day)
                .map(|(_, counts)| counts.clone())
                .collect())
        })
    }
}

fn tickets(
    tenant_id: &str,
    filed: i64,
    rejected: i64,
    needs_info: i64,
    duplicates: i64,
    dead_lettered: i64,
) -> TicketCounts {
    TicketCounts {
        tenant_id: tenant_id.to_owned(),
        filed,
        rejected,
        needs_info,
        duplicates,
        dead_lettered,
    }
}

// ---------------------------------------------------------------------------
// The fixture.
// ---------------------------------------------------------------------------

/// Tenant ids and API keys, plus the source ids the citations route reads
/// back by title.
struct Fixture {
    tenant_a: String,
    key_a: String,
    key_b: String,
    /// Tenant A's cited source (`Reset guide`).
    source_a1: String,
    /// Tenant A's never-cited source (`Billing FAQ`).
    source_a2: String,
    /// Tenant B's cited source (`B docs`).
    source_b1: String,
    stats: FakeTicketStats,
}

/// Escapes a value for a single-quoted SQL literal.
fn lit(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Seeds the scripted fixture:
///
/// - Tenant A: conversation `conv-a1` opened on [`DAY`], with an
///   `answered` turn that day (confidence 90, citing `c-a1`) and a
///   `handoff` turn the next day (confidence 20, nothing retrieved, and
///   the question `billing owner` behind it); conversation `conv-a2`
///   opened on [`DAY2`] with a `clarify` turn (confidence 40, nothing
///   retrieved, question `clarify this`).
/// - Tenant B: conversation `conv-b1` opened on [`DAY`] with an
///   `answered` turn (confidence 60, citing `c-b1`).
/// - Tenant A's second source is never cited; the ticket counts are the
///   fakes above.
async fn seed(kit: &TestHarness) -> Fixture {
    let (tenant_a, key_a) = mint_tenant(kit, "Tenant A").await;
    let (tenant_b, key_b) = mint_tenant(kit, "Tenant B").await;

    seed_corpus(kit, &tenant_a, &tenant_b);

    // Tenant A's ticket counts on each day; tenant B has none.
    let stats = FakeTicketStats::default()
        .with(tickets(&tenant_a, 2, 1, 0, 1, 0), DAY)
        .with(tickets(&tenant_a, 1, 0, 0, 0, 0), DAY2);

    Fixture {
        tenant_a,
        key_a,
        key_b,
        source_a1: "src-a1".to_owned(),
        source_a2: "src-a2".to_owned(),
        source_b1: "src-b1".to_owned(),
        stats,
    }
}

/// Seeds the three sources, the three conversations and [`TURNS`] — the
/// corpus the rollup is meant to read.
fn seed_corpus(kit: &TestHarness, tenant_a: &str, tenant_b: &str) {
    for (source_id, tenant_id, title) in [
        ("src-a1", tenant_a, "Reset guide"),
        ("src-a2", tenant_a, "Billing FAQ"),
        ("src-b1", tenant_b, "B docs"),
    ] {
        exec(
            kit,
            &format!(
                "INSERT INTO sg_sources (id, tenant_id, title, url, byte_len, created_at, external_id, updated_at) \
                 VALUES ({}, {}, {}, NULL, 10, {}, NULL, {})",
                lit(source_id),
                lit(tenant_id),
                lit(title),
                lit(&format!("{DAY}T00:00:00+00:00")),
                lit(&format!("{DAY}T00:00:00+00:00")),
            ),
        );
    }
    // `src-a1` carries two chunks, so a turn citing both proves a citation
    // is counted per turn, not per chunk.
    for (chunk_id, source_id, tenant_id, ordinal, body) in [
        ("c-a1", "src-a1", tenant_a, 0, "Reset guide"),
        (
            "c-a1b",
            "src-a1",
            tenant_a,
            1,
            "the reset password link expires",
        ),
        ("c-a2", "src-a2", tenant_a, 0, "Billing FAQ"),
        ("c-b1", "src-b1", tenant_b, 0, "B docs"),
    ] {
        exec(
            kit,
            &format!(
                "INSERT INTO sg_chunks (id, tenant_id, source_id, ordinal, body, term_count, created_at, tokenizer_version) \
                 VALUES ({}, {}, {}, {}, {}, 1, {}, 2)",
                lit(chunk_id),
                lit(tenant_id),
                lit(source_id),
                ordinal,
                lit(body),
                lit(&format!("{DAY}T00:00:00+00:00")),
            ),
        );
    }

    for (conversation_id, tenant_id, created_at) in [
        ("conv-a1", tenant_a, DAY),
        ("conv-b1", tenant_b, DAY),
        ("conv-a2", tenant_a, DAY2),
    ] {
        exec(
            kit,
            &format!(
                "INSERT INTO sg_conversations (id, tenant_id, status, needs_escalation, created_at, updated_at) \
                 VALUES ({}, {}, 'open', 0, {}, {})",
                lit(conversation_id),
                lit(tenant_id),
                lit(&format!("{created_at}T09:00:00+00:00")),
                lit(&format!("{created_at}T09:00:00+00:00")),
            ),
        );
    }

    seed_messages(kit, tenant_a, tenant_b);
}

/// One scripted `sg_messages` row: (conversation, seq, role, body, outcome,
/// confidence, citations, retrieved chunks, day).
type MessageRow = (
    &'static str,
    i64,
    &'static str,
    &'static str,
    Option<&'static str>,
    Option<i64>,
    &'static str,
    Option<i64>,
    &'static str,
);

/// The scripted turns, written by hand rather than through `/messages` so
/// each row's outcome, confidence, citations and retrieval count are
/// exactly what the rollup is meant to read: an `answered` turn that cites
/// two chunks of one source, a `handoff`/`clarify` that retrieved nothing,
/// a pre-migration turn whose `retrieved_chunks` is NULL, and a duplicate
/// question across the two tenants.
const TURNS: &[MessageRow] = &[
    (
        "conv-a1",
        0,
        "user",
        "reset password",
        None,
        None,
        "[]",
        None,
        DAY,
    ),
    (
        "conv-a1",
        1,
        "assistant",
        "Use the reset link.",
        Some("answered"),
        Some(90),
        r#"[{"chunk_id":"c-a1","quote":"reset"},{"chunk_id":"c-a1b","quote":"expires"}]"#,
        Some(2),
        DAY,
    ),
    (
        "conv-a1",
        2,
        "user",
        "billing owner",
        None,
        None,
        "[]",
        None,
        DAY2,
    ),
    (
        "conv-a1",
        3,
        "assistant",
        "A person will help.",
        Some("handoff"),
        Some(20),
        "[]",
        Some(0),
        DAY2,
    ),
    (
        "conv-a1",
        4,
        "user",
        "legacy question",
        None,
        None,
        "[]",
        None,
        DAY2,
    ),
    (
        "conv-a1",
        5,
        "assistant",
        "A person will help.",
        Some("handoff"),
        Some(10),
        "[]",
        // Written before 0009 added the column: NULL, not 0. The gap
        // rollup must not read this question's terms — nothing here says
        // retrieval returned *nothing*, only that the count is unknown.
        None,
        DAY2,
    ),
    (
        "conv-a2",
        0,
        "user",
        "clarify this",
        None,
        None,
        "[]",
        None,
        DAY2,
    ),
    (
        "conv-a2",
        1,
        "assistant",
        "Which product?",
        Some("clarify"),
        Some(40),
        "[]",
        Some(0),
        DAY2,
    ),
    (
        "conv-b1",
        0,
        "user",
        "reset password",
        None,
        None,
        "[]",
        None,
        DAY,
    ),
    (
        "conv-b1",
        1,
        "assistant",
        "Use the reset link.",
        Some("answered"),
        Some(60),
        r#"[{"chunk_id":"c-b1","quote":"reset"}]"#,
        Some(1),
        DAY,
    ),
];

/// Writes [`TURNS`], deriving the tenant from the conversation id and
/// the timestamp's minute from the seq (so ordering is deterministic).
fn seed_messages(kit: &TestHarness, tenant_a: &str, tenant_b: &str) {
    for (conversation_id, seq, role, body, outcome, confidence, citations, retrieved, at) in TURNS {
        let tenant_id = if conversation_id.starts_with("conv-b") {
            tenant_b
        } else {
            tenant_a
        };
        let text = |value: Option<&str>| value.map_or_else(|| "NULL".to_owned(), lit);
        let number =
            |value: Option<i64>| value.map_or_else(|| "NULL".to_owned(), |n| n.to_string());
        exec(
            kit,
            &format!(
                "INSERT INTO sg_messages (id, conversation_id, tenant_id, role, seq, body, model_answer, outcome, confidence_pct, citations, lang, created_at, retrieved_chunks) \
                 VALUES ({}, {}, {}, {}, {}, {}, NULL, {}, {}, {}, NULL, {}, {})",
                lit(&format!("{conversation_id}-{seq}")),
                lit(conversation_id),
                lit(tenant_id),
                lit(role),
                seq,
                lit(body),
                text(*outcome),
                number(*confidence),
                lit(citations),
                lit(&format!("{at}T09:{:02}:00+00:00", seq + 1)),
                number(*retrieved),
            ),
        );
    }
}

// ---------------------------------------------------------------------------
// The rollup.
// ---------------------------------------------------------------------------

/// The setup every rollup test starts with: seed the fixture, then
/// recompute each named day. Returns the fixture.
async fn rolled_up(kit: &TestHarness, days: &[&str]) -> Fixture {
    let fixture = seed(kit).await;
    for day in days {
        rollup_day(kit.db.as_ref(), Some(&fixture.stats), day)
            .await
            .expect("rollup of the day");
    }
    fixture
}

#[pollster::test]
async fn rollup_writes_every_column_for_every_tenant() {
    for kit in kits() {
        let fixture = rolled_up(&kit, &[DAY, DAY2]).await;

        // Three stats rows: A on both days, B on DAY. B has nothing on
        // DAY2, so no row for it.
        assert_eq!(
            count_of(&kit, "sg_daily_stats"),
            3,
            "one row per tenant-day with activity"
        );

        let body = analytics(&kit, &fixture.key_a, DAY, DAY).await;
        assert_eq!(body.status, StatusCode::OK, "{}", body.body);
        let row = day_row(&body.body, DAY);
        assert_eq!(row["conversations"], 1, "conv-a1 opened on DAY");
        assert_eq!(row["answered"], 1, "the DAY turn");
        assert_eq!(row["clarify"], 0);
        assert_eq!(row["handoff"], 0, "the handoff turn is on DAY2");
        assert_eq!(row["handed_off"], 1, "conv-a1 escalates, whenever it did");
        assert_eq!(row["median_confidence"], 90);
        assert_eq!(row["filed"], 2);
        assert_eq!(row["rejected"], 1);
        assert_eq!(row["needs_info"], 0);
        assert_eq!(row["duplicates"], 1, "linked counts as a duplicate");
        assert_eq!(row["dead_lettered"], 0);

        let body = analytics(&kit, &fixture.key_a, DAY2, DAY2).await;
        let row = day_row(&body.body, DAY2);
        assert_eq!(row["conversations"], 1, "conv-a2 opened on DAY2");
        assert_eq!(row["answered"], 0);
        assert_eq!(row["clarify"], 1);
        // conv-a1's two handoff turns (seq 3 and the NULL-retrieval seq 5)
        // ran on DAY2; conv-a1 itself opened on DAY, so neither lands in
        // DAY2's `handed_off` cohort.
        assert_eq!(row["handoff"], 2);
        assert_eq!(row["handed_off"], 0, "conv-a2 never escalated");
        // The odd turn count's median is the middle value: 20 of [10, 20,
        // 40].
        assert_eq!(row["median_confidence"], 20);
        assert_eq!(row["filed"], 1);
        assert_eq!(row["rejected"], 0);

        // Tenant B's day: its own numbers, never A's.
        let body = analytics(&kit, &fixture.key_b, DAY, DAY).await;
        let row = day_row(&body.body, DAY);
        assert_eq!(row["conversations"], 1);
        assert_eq!(row["answered"], 1);
        assert_eq!(row["handed_off"], 0);
        assert_eq!(row["median_confidence"], 60);
        assert_eq!(row["filed"], 0, "no ticket stats were faked for B");

        // DAY2 for B is a day with no rows at all: an empty `days` list,
        // not a zero row.
        let body = analytics(&kit, &fixture.key_b, DAY2, DAY2).await;
        assert_eq!(body.body["days"].as_array().expect("days").len(), 0);
        assert_eq!(body.body["totals"]["conversations"], 0);
        assert_eq!(body.body["deflection_rate"], Value::Null);
    }
}

#[pollster::test]
async fn rollup_records_normalized_gap_terms_and_citations() {
    for kit in kits() {
        let fixture = rolled_up(&kit, &[DAY, DAY2]).await;

        // Two unanswered turns retrieved nothing (`Some(0)`), so both
        // questions are gaps — normalized to their tokens, each once.
        // conv-a1 seq 5 also handed off but its `retrieved_chunks` is NULL
        // (a pre-migration row), so its question `legacy question` is
        // *not* a gap: unknown is not zero.
        let body = gaps(&kit, &fixture.key_a, DAY, DAY2).await;
        assert_eq!(body.status, StatusCode::OK, "{}", body.body);
        let terms: Vec<(String, i64)> = body.body["terms"]
            .as_array()
            .expect("terms")
            .iter()
            .map(|term| {
                (
                    term["term"].as_str().expect("term").to_owned(),
                    term["hits"].as_i64().expect("hits"),
                )
            })
            .collect();
        assert_eq!(
            terms,
            [
                ("billing".to_owned(), 1),
                ("clarify".to_owned(), 1),
                ("owner".to_owned(), 1),
                ("this".to_owned(), 1),
            ],
            "sorted by hits desc, then term asc; NULL retrieval adds nothing"
        );

        // The DAY answered turn cites two chunks of `src-a1`: one turn, so
        // one citation of the source, not two. A source never cited is
        // exactly that.
        let body = citations(&kit, &fixture.key_a, DAY, DAY2).await;
        assert_eq!(body.status, StatusCode::OK, "{}", body.body);
        let most = body.body["most_cited"].as_array().expect("most_cited");
        assert_eq!(most.len(), 1);
        assert_eq!(most[0]["source_id"], fixture.source_a1.as_str());
        assert_eq!(most[0]["title"], "Reset guide");
        assert_eq!(most[0]["cites"], 1, "two chunks of one source, one turn");
        let never = body.body["never_cited"].as_array().expect("never_cited");
        assert_eq!(never.len(), 1);
        assert_eq!(never[0]["source_id"], fixture.source_a2.as_str());
        assert_eq!(never[0]["title"], "Billing FAQ");

        // Tenant B's sources are its own: its cited one is not "never
        // cited", and A's never-cited source never appears.
        let body = citations(&kit, &fixture.key_b, DAY, DAY2).await;
        let most = body.body["most_cited"].as_array().expect("most_cited");
        assert_eq!(most.len(), 1);
        assert_eq!(most[0]["source_id"], fixture.source_b1.as_str());
        assert_eq!(
            body.body["never_cited"]
                .as_array()
                .expect("never_cited")
                .len(),
            0
        );
    }
}

#[pollster::test]
async fn rollup_is_idempotent() {
    for kit in kits() {
        let fixture = rolled_up(&kit, &[DAY, DAY2]).await;
        let before = rollup_state(&kit);

        for day in [DAY, DAY2] {
            rollup_day(kit.db.as_ref(), Some(&fixture.stats), day)
                .await
                .expect("a second rollup of the day");
        }

        assert_eq!(rollup_state(&kit), before, "a recompute converges");
        assert_eq!(count_of(&kit, "sg_daily_stats"), 3);
        assert_eq!(count_of(&kit, "sg_daily_gaps"), 4);
        assert_eq!(count_of(&kit, "sg_daily_citations"), 2);
    }
}

#[pollster::test]
async fn rollup_recent_rewrites_only_the_trailing_window() {
    for kit in kits() {
        let fixture = seed(&kit).await;

        // `rollup_recent(today, 7)` recomputes `today` and the six days
        // before it, so `today - 7` is outside the window. A stale row
        // there and a stale row inside it: the sweep must leave the one
        // and rewrite the other.
        let outside = "2026-05-04"; // DAY2 − 7
        for day in [outside, DAY] {
            exec(
                &kit,
                &format!(
                    "INSERT INTO sg_daily_stats (tenant_id, day, conversations) VALUES ({}, {}, 999)",
                    lit(&fixture.tenant_a),
                    lit(day),
                ),
            );
        }

        rollup_recent(kit.db.as_ref(), Some(&fixture.stats), DAY2, ROLLUP_DAYS)
            .await
            .expect("the trailing-week sweep");

        assert_eq!(
            stat_conversations(&kit, &fixture.tenant_a, outside),
            999,
            "a day a week back is outside the window and untouched"
        );
        assert_eq!(stat_conversations(&kit, &fixture.tenant_a, DAY), 1);
        assert_eq!(stat_conversations(&kit, &fixture.tenant_a, DAY2), 1);
    }
}

#[pollster::test]
async fn routes_read_rollups_until_the_day_is_recomputed() {
    for kit in kits() {
        let fixture = seed(&kit).await;
        rollup_day(kit.db.as_ref(), Some(&fixture.stats), DAY)
            .await
            .expect("rollup of DAY");

        // A turn written after the rollup: the routes must not see it,
        // because they read only the rollup tables.
        let written_at = format!("{DAY}T23:00:00+00:00");
        exec(
            &kit,
            &format!(
                "INSERT INTO sg_messages (id, conversation_id, tenant_id, role, seq, body, model_answer, outcome, confidence_pct, citations, lang, created_at, retrieved_chunks) \
                 VALUES ('conv-a1-6', 'conv-a1', {}, 'assistant', 6, 'later', NULL, 'handoff', 10, '[]', NULL, {}, 0)",
                lit(&fixture.tenant_a),
                lit(&written_at),
            ),
        );
        let body = analytics(&kit, &fixture.key_a, DAY, DAY).await;
        assert_eq!(
            day_row(&body.body, DAY)["handoff"],
            0,
            "the routes read rollups"
        );

        // Recomputing the day picks it up — the rollup is a recompute,
        // not an append-only counter.
        rollup_day(kit.db.as_ref(), Some(&fixture.stats), DAY)
            .await
            .expect("recompute of DAY");
        let body = analytics(&kit, &fixture.key_a, DAY, DAY).await;
        assert_eq!(day_row(&body.body, DAY)["handoff"], 1);
        assert_eq!(day_row(&body.body, DAY)["handed_off"], 1);
    }
}

#[pollster::test]
async fn deflection_rate_is_the_share_that_never_reached_a_person() {
    for kit in kits() {
        let fixture = rolled_up(&kit, &[DAY, DAY2]).await;

        // DAY: one conversation, and it escalated → 0.0.
        let body = analytics(&kit, &fixture.key_a, DAY, DAY).await;
        assert_eq!(body.body["deflection_rate"], 0.0);
        // DAY2: one conversation, none escalated → 1.0.
        let body = analytics(&kit, &fixture.key_a, DAY2, DAY2).await;
        assert_eq!(body.body["deflection_rate"], 1.0);
        // The range: two conversations, one escalated → 0.5.
        let body = analytics(&kit, &fixture.key_a, DAY, DAY2).await;
        assert_eq!(body.body["deflection_rate"], 0.5);
        assert_eq!(body.body["totals"]["conversations"], 2);
        assert_eq!(body.body["totals"]["handed_off"], 1);
        assert_eq!(body.body["totals"]["filed"], 3);
        assert_eq!(body.body["from"], DAY);
        assert_eq!(body.body["to"], DAY2);
    }
}

#[pollster::test]
async fn analytics_rejects_malformed_and_unbounded_ranges() {
    for kit in kits() {
        let fixture = seed(&kit).await;

        for path in [
            // Not a day, an impossible month, and a range that runs
            // backwards.
            format!("{ANALYTICS}?from=notaday&to={DAY}"),
            format!("{ANALYTICS}?from=2026-13-01&to={DAY}"),
            format!("{ANALYTICS}?from={DAY2}&to={DAY}"),
            // Wider than a year (366 inclusive days), two days too wide.
            format!("{ANALYTICS}?from=2024-01-01&to=2025-01-03"),
            // A `limit` that is not an integer, on a list route.
            format!("{ANALYTICS}/gaps?from={DAY}&to={DAY}&limit=lots"),
        ] {
            let reply = get(&kit, &path, &fixture.key_a).await;
            assert_eq!(
                reply.status,
                StatusCode::BAD_REQUEST,
                "{path}: {}",
                reply.body
            );
            assert!(
                reply.problem_type().ends_with("/validation-failed"),
                "{path}: {}",
                reply.problem_type()
            );
        }

        // An unauthenticated bad range is still a 401: the query is
        // parsed only after the guards.
        let reply = get(&kit, &format!("{ANALYTICS}?from=notaday"), "sg_not-a-key").await;
        assert_eq!(reply.status, StatusCode::UNAUTHORIZED);

        // A non-positive limit clamps into range rather than failing.
        let reply = get(
            &kit,
            &format!("{ANALYTICS}/gaps?from={DAY}&to={DAY}&limit=0"),
            &fixture.key_a,
        )
        .await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    }
}

// ---------------------------------------------------------------------------
// `retrieved_chunks` from the real turn path.
// ---------------------------------------------------------------------------

#[pollster::test]
async fn a_turn_persists_how_much_retrieval_returned() {
    for dialect in Dialect::available() {
        let model = FakeTextModel::new(TextModelMode::NotConfigured);
        let kit = TestHarness::with_database_and_ports(
            vec![Box::new(Support::new()) as Box<dyn Module>],
            dialect,
            |ports| {
                ports.config = Arc::new(MapConfig::from_pairs([("ADMIN_TOKEN", ADMIN_TOKEN)]));
                ports.text_model = Some(Arc::new(model.clone()));
            },
        );
        let (tenant_id, api_key) = mint_tenant(&kit, "Turns").await;

        // A grounded reply the fake answers every turn with: the outcome
        // does not matter here, only the chunk count the turn records.
        let answer = json!({ "answer": "Use the reset link.", "citations": [], "confidence": 0.9 });
        model.set_mode(TextModelMode::Complete(
            cratefield_core::Completion::new(answer.to_string(), "fake-fast").json(answer),
        ));

        // Nothing indexed: the turn retrieves nothing, and the assistant
        // row says so. The model still answers (the fake is scripted), so
        // this is the "retrieved nothing anyway" case the gap rollup
        // deliberately excludes at answer time.
        let reply = send(
            &kit.router,
            Method::POST,
            MESSAGES,
            Some(&api_key),
            Some(&json!({ "message": "hello there" }).to_string()),
        )
        .await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);

        let chunks = |kit: &TestHarness| {
            dump(
                kit,
                &format!(
                    "SELECT CAST(seq AS TEXT) AS seq, CAST(retrieved_chunks AS TEXT) AS retrieved_chunks \
                     FROM sg_messages WHERE tenant_id = {} AND role = 'assistant' ORDER BY seq",
                    lit(&tenant_id),
                ),
                &["seq", "retrieved_chunks"],
            )
        };
        assert_eq!(
            chunks(&kit),
            vec![vec![Some("1".to_owned()), Some("0".to_owned())]],
            "a turn with nothing indexed retrieved zero chunks"
        );

        // Ingest a matching document, ask again in a new conversation:
        // retrieval returns the chunk and the count follows.
        let ingest = send(
            &kit.router,
            Method::POST,
            SOURCES,
            Some(&api_key),
            Some(
                &json!({ "title": "Reset", "text": "the reset password link expires" }).to_string(),
            ),
        )
        .await;
        assert_eq!(ingest.status, StatusCode::CREATED, "{}", ingest.body);
        let reply = send(
            &kit.router,
            Method::POST,
            MESSAGES,
            Some(&api_key),
            Some(&json!({ "message": "reset password" }).to_string()),
        )
        .await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        assert_eq!(
            chunks(&kit),
            vec![
                vec![Some("1".to_owned()), Some("0".to_owned())],
                vec![Some("1".to_owned()), Some("1".to_owned())],
            ],
            "the matching turn retrieved one chunk"
        );
    }
}
