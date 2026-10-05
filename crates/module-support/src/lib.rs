//! The SupportGenius support module's retrieval core: tokenisation,
//! chunking and BM25 ranking, as pure Rust with no I/O of any kind, so
//! the same code runs in a Cloudflare Worker isolate and in `cargo test`.
//!
//! At ingest, a document — up to 48 KiB of text inline (see
//! [`handlers::MAX_TEXT_BYTES`] for why), or up to 4 MiB in chunks
//! through the upload routes (`uploads`) — is split into overlapping
//! word windows by [`chunk::Chunker`] and inverted into `sg_postings`
//! rows, with the corpus statistics BM25 needs (`sg_terms` document
//! frequencies, `sg_tenant_stats` chunk counts) maintained in the same
//! atomic batch by every write path. At query time the caller reads
//! those statistics, then fetches at most
//! [`store::MAX_POSTINGS_PER_TERM`] postings per query term, and
//! [`bm25::rank`] scores them in-process with Okapi BM25 — so a query's
//! cost is bounded by the query, not by the corpus.
//!
//! [`tokenize`] is shared by both halves, which is the point: a query
//! tokenised differently from the index it searches finds nothing, so
//! there is exactly one tokenizer and both sides call it.
//!
//! **Tenancy is app-level.** One venture, many customer companies; every
//! table carries `tenant_id` and every query in [`store`] filters on it.
//! The self-hosted binary is this same module with exactly one tenant
//! row.
//!
//! **Why retrieval is boring.** Portable SQL forbids FTS5 and pgvector
//! (ADR 0004, linted by `fz doctor`), Cloudflare D1 has no vector type,
//! and a Worker isolate has roughly 128 MB of memory. So: an inverted
//! table, an in-process ranker, and no magic.
//!
//! **Answers are grounded or they are not answers.** `POST /messages`
//! retrieves the top chunks for the user's message, asks the
//! [`TextModel`] (fast tier) for `{answer, citations, confidence}`, and
//! a pure decision turns that into `answered`, `clarify` or
//! `handoff`: a citation naming any chunk not retrieved for *this* request
//! can never be `answered`, and neither can a confidence below the
//! tenant's threshold ([`DEFAULT_ANSWER_THRESHOLD`] unless the tenant set
//! one).//!
//! **Connectors keep the index fresh on a schedule** (issue #29).
//! `POST /v1/support/connectors` registers a crawl root — a sitemap, a
//! URL prefix, or a GitHub repository — and from then on the module's
//! `Module::scheduled` hook (cron) re-syncs it: every fetch job takes one
//! URL, conditional-GETs it (`If-None-Match`/`If-Modified-Since`; a 304
//! writes nothing), replaces the source it indexed if the body changed,
//! deletes the source if the page answers 404/410, and discovers the
//! URLs the page legitimately leads to. Every effect is an upsert or
//! delete keyed by the URL, so the outbox's at-least-once delivery needs
//! no inbox. The caps, clamped at connector creation: `max_pages` rows in
//! `sg_ingest_pages` per connector — sources *and* navigation rows
//! (sitemaps, GitHub trees) alike, which is what bounds a sitemap index's
//! breadth — default 200, at most 2000; `max_bytes` per response
//! (default 1 MiB, at most the `HttpClient` port's 4 MiB ceiling);
//! `max_depth` link hops from the seed (default 3, at most 5). Each URL
//! holds at most one un-retired outbox row at a time (the row's `subject`
//! is the dedup key), so a re-sync tick never stacks retries.
//!
//! **The allowlist is the fetch policy.** A sitemap connector fetches
//! only URLs on the sitemap's own scheme and authority; a URL-prefix
//! connector, only URLs under its prefix; a GitHub connector, only
//! `api.github.com` under the configured owner and repo. Anything else a
//! page links to — or a sitemap names — is skipped before any network
//! call. On Cloudflare Workers, platform `fetch` additionally cannot
//! reach private networks at all, which together with the allowlist is
//! the SSRF story; the self-hosted binary runs the same fetches through
//! the hardened `ReqwestClient` vetting (loopback and link-local
//! destinations refused). One accepted residual there: the `HttpClient`
//! port exposes neither a no-redirect policy nor a response's final URL,
//! so a 3xx onto a *public* host outside the allowlist would be followed
//! and its body indexed under the URL the connector did name. GitHub
//! fetches authenticate with
//! `Authorization: Bearer …` resolved from the Config port by the
//! connector's stored `credential_ref` — a config key *name*, never the
//! token itself.
//!
//! The scheduled hook runs wherever the venture is deployed: the
//! Cloudflare Worker wires `[triggers] crons` in `wrangler.toml`, and the
//! native binary takes a comma-separated `CRONS` environment variable of
//! standard five-field cron expressions.

pub mod analytics;
mod answer;
pub mod chunk;
mod connectors;
mod extract;
mod handlers;
mod handoff;
mod human;
/// MCP over Streamable HTTP (issue #34). Public so an MCP client test can
/// name the tool argument types and compare their derived schemas with
/// what `tools/list` serves.
pub mod mcp;
mod messages;
mod openapi;
pub mod store;
mod uploads;
mod widget;

pub use analytics::TicketStats;
pub use answer::DEFAULT_ANSWER_THRESHOLD;
pub use chunk::tokenize;
pub use handlers::{MAX_BOOST_CANDIDATES, REVIEWED_BOOST};
pub use handoff::{HandoffSink, TicketView};
pub use messages::MessageBody;
// The tokenizer and BM25 ranker now live in the shared `lexical` crate
// (escalation's duplicate scoring tokenizes with the same rules); this
// re-export keeps `module_support::bm25` — and `crate::bm25` inside the
// module — exactly where callers already expect it.
pub use lexical::bm25;
pub use store::reindex_stale_chunks;

/// Whether `tenant_id` names a tenant that exists and is `active` — the
/// same test every tenant-key route here applies after verifying the key
/// (`handlers::authenticate`). Public so a composition can give another
/// module (escalation's destination routes) the identical answer without
/// that module reading this one's tables.
///
/// # Errors
///
/// The database's error when the lookup itself fails.
pub async fn tenant_is_active(
    db: &dyn cratefield_core::Database,
    tenant_id: &str,
) -> Result<bool, cratefield_core::DbError> {
    Ok(store::find_tenant(db, tenant_id)
        .await?
        .is_some_and(|tenant| tenant.status == store::STATUS_ACTIVE))
}

/// Whether the tenant's API key `key_id` is still on record in
/// `sg_api_keys` — the row that is the key's revocation state. A deleted
/// row means a revoked key. Exposed for the composition, so a module that
/// authenticates the same `sg_` keys (escalation's destination routes)
/// refuses a revoked one exactly as this module's own routes do.
///
/// # Errors
///
/// The database's error when the lookup itself fails.
pub async fn api_key_is_live(
    db: &dyn cratefield_core::Database,
    tenant_id: &str,
    key_id: &str,
) -> Result<bool, cratefield_core::DbError> {
    Ok(store::find_api_key(db, tenant_id, key_id).await?.is_some())
}

use std::sync::Arc;

use cratefield_core::{
    AnyError, BoxFuture, Config, ConfigError, DataKind, Disposition, Migrations, Module,
    ModuleConfig, ModuleContext, PersonalDataSet, Port, SqlMigration, SystemClock, UlidIdGen,
    assert_migration_set,
};

pub(crate) const MODULE_NAME: &str = "support";

/// How many stale-version chunks one sweep of the scheduled re-index
/// re-tokenizes. Bounded on purpose: the chunker counts words, so
/// spaceless CJK text can be one very long chunk, and that chunk's
/// postings rewrite is bounded by its character count — a 48 KiB Han run
/// is thousands of bigram rows. Each chunk is one `batch_atomic`, so
/// batching per chunk keeps every rewrite atomic.
const REINDEX_BATCH: usize = 200;

/// How many sweeps one tick spends before leaving the rest to the next
/// cron tick — the escalation module's bounded-drain shape, so a large
/// backlog drains across days rather than in one isolate.
const REINDEX_MAX_SWEEPS: usize = 10;

/// The v0 schema: five tables of portable SQL (ADR 0004), every one of
/// them carrying `tenant_id`.
const MIGRATION_INIT: SqlMigration = SqlMigration::new(
    "0001",
    "init",
    include_str!("../migrations/sqlite/0001_init.sql"),
);

/// Conversations, their messages and the per-tenant answer threshold.
const MIGRATION_CONVERSATIONS: SqlMigration = SqlMigration::new(
    "0002",
    "conversations",
    include_str!("../migrations/sqlite/0002_conversations.sql"),
);

/// Source management: `external_id` (caller-side identity, one source per
/// id per tenant) and `updated_at`, with the per-tenant unique index the
/// replace-in-place ingest stands on.
const MIGRATION_SOURCE_MANAGEMENT: SqlMigration = SqlMigration::new(
    "0003",
    "source_management",
    include_str!("../migrations/sqlite/0003_source_management.sql"),
);

/// Unspaced-script retrieval and the per-turn language: `tokenizer_version`
/// on every chunk — the watermark the scheduled re-index drains — and
/// `lang` on every message.
const MIGRATION_INTERNATIONALIZATION: SqlMigration = SqlMigration::new(
    "0004",
    "internationalization",
    include_str!("../migrations/sqlite/0004_internationalization.sql"),
);

/// Chunked uploads: upload and part rows, plus the module's outbox (the
/// `extract` job lives in), created from `Outbox::new(…).create_table_sql()`.
const MIGRATION_UPLOADS: SqlMigration = SqlMigration::new(
    "0005",
    "uploads",
    include_str!("../migrations/sqlite/0005_uploads.sql"),
);

/// Connectors (issue #29): the crawl roots, their per-URL fetch state and
/// the ingest outbox. The outbox block inside is generated from core's
/// `Outbox::create_table_sql` — see the migration file's header before
/// touching it. Connector pages index into `sg_sources` under the
/// `external_id` column 0003 added, so this migration leaves that table
/// alone.
const MIGRATION_CONNECTORS: SqlMigration = SqlMigration::new(
    "0006",
    "connectors",
    include_str!("../migrations/sqlite/0006_connectors.sql"),
);

/// Persisted corpus statistics (`sg_terms`, `sg_tenant_stats`, backfilled
/// from the existing index) and the per-term postings index the bounded
/// query path reads (issue #31).
const MIGRATION_SEARCH_STATS: SqlMigration = SqlMigration::new(
    "0007",
    "search_stats",
    include_str!("../migrations/sqlite/0007_search_stats.sql"),
);

/// The web widget's per-tenant origin allowlist (`widget_origins`, JSON
/// TEXT, null = widget refused).
const MIGRATION_WIDGET_SETTINGS: SqlMigration = SqlMigration::new(
    "0008",
    "widget_settings",
    include_str!("../migrations/sqlite/0008_widget_settings.sql"),
);

/// Human-in-the-loop (issue #35, support side): per-staff API keys, the
/// conversation `state`/`assignee` columns and their index, the `author`
/// of a staff message, and the `reviewed` provenance on a source an agent
/// wrote.
const MIGRATION_HUMAN_HANDOFF: SqlMigration = SqlMigration::new(
    "0009",
    "human_handoff",
    include_str!("../migrations/sqlite/0009_human_handoff.sql"),
);

/// Analytics (issue #36): the daily rollups `/v1/support/analytics` reads
/// (`analytics`), plus `sg_messages.retrieved_chunks` — how many chunks a
/// turn's retrieval returned, which is what tells a gap ("nothing was
/// retrieved") from a turn that retrieved and still would not answer.
const MIGRATION_ANALYTICS: SqlMigration = SqlMigration::new(
    "0010",
    "analytics",
    include_str!("../migrations/sqlite/0010_analytics.sql"),
);

/// The support module: tenant provisioning behind the harness admin
/// token, API-key-authenticated source ingest, BM25 search and grounded
/// answers, the cron-driven connectors that keep a workspace's index
/// synced from a sitemap, a URL prefix or a GitHub repository
/// (`POST /connectors`, issue #29), and the web widget's publishable-key
/// routes (`crate::widget`).
///
/// Nearly knob-free. Everything tunable — the revoked-kid list, the admin
/// token, the widget's captcha threshold and site key — is deployment
/// configuration read through the `Config` port, and the answer threshold
/// and the widget origin allowlist are per-tenant data
/// (`PUT /admin/tenants/{tenant_id}/settings`), so a builder setter for
/// either would be a second place the same setting lived. The text model
/// is likewise not passed here: it arrives through the runtime's
/// [`Ports`](cratefield_core::Ports) at route-build time, so one instance
/// serves a venture with a model and one without.
///
/// Two pieces of builder state, both optional:
///
/// - The [`HandoffSink`]: a handoff turn appends the sink's statements to
///   its own atomic batch and kicks it to run. `Support::new()` carries no
///   sink, so the module composes exactly as it did before one existed
///   (see [`handoff`](self) for why the seam is a trait and not a
///   dependency on `module-escalation`).
/// - [`Support::visitor_rate_limiter`]: the widget's per-visitor and
///   per-IP buckets want a limiter *separate* from the port a runtime
///   fills for the whole process (on Workers, a second Rate Limiting
///   binding), so one anonymous browser's ceiling is not the tenant's own
///   budget. `None` — the default — falls back to the shared
///   `RateLimiter` port, and to no visitor limiting where that is absent
///   too.
#[derive(Default)]
pub struct Support {
    /// `None` is the unwired module: a handoff still marks
    /// `needs_escalation` and nothing files a ticket.
    handoff: Option<Arc<dyn HandoffSink>>,
    /// `None` is the unwired module: the daily rollup still runs, but the
    /// five ticket columns report zero — support rolls up its own numbers
    /// without reaching into another module's tables.
    ticket_stats: Option<Arc<dyn TicketStats>>,
    visitor_rate_limiter: Option<Arc<dyn cratefield_core::RateLimiter>>,
    /// Other modules' surfaces the `OpenAPI` document (`GET /openapi.json`,
    /// issue #34) covers, paired with the module name their paths mount
    /// under. The composition injects `("escalation", …)` because
    /// module-support cannot depend on module-escalation; the document
    /// then spans `/v1/support` and `/v1/escalation`.
    api_surfaces: Vec<(String, cratefield_core::Surface)>,
}

impl Support {
    /// A `Support` module with defaults and no handoff sink. Whether
    /// `POST /messages` can answer depends on what the runtime provides:
    /// with no `TextModel` port it answers `503 text-model-not-configured`
    /// and every other route still works.
    #[must_use]
    pub fn new() -> Self {
        Self {
            handoff: None,
            ticket_stats: None,
            visitor_rate_limiter: None,
            api_surfaces: Vec::new(),
        }
    }

    /// Adds another module's `surface` to the `OpenAPI` document this module
    /// serves at `GET /openapi.json`, under `module`'s mount (`/v1/<module>`).
    /// The composition calls this with escalation's surface, so one
    /// document describes both `/v1/support/*` and `/v1/escalation/*`
    /// without this module depending on that one.
    #[must_use]
    pub fn with_api_surface(
        mut self,
        module: impl Into<String>,
        surface: cratefield_core::Surface,
    ) -> Self {
        self.api_surfaces.push((module.into(), surface));
        self
    }

    /// A `Support` that hands an escalating turn to `handoff`: the sink's
    /// statements join the turn's atomic write, and it is kicked once that
    /// write commits. This is the one thing a builder needs to set by
    /// hand, because the sink is the module's own seam and not a core
    /// port.
    #[must_use]
    pub fn with_handoff(mut self, handoff: Arc<dyn HandoffSink>) -> Self {
        self.handoff = Some(handoff);
        self
    }

    /// A `Support` that reads its ticket columns from `stats`: the daily
    /// rollup asks the port for each day's filed/rejected/needs-info/
    /// duplicate/dead-lettered counts. The other seam the composition
    /// wires, and unwired it is simply absent — the ticket columns roll
    /// up as zero rather than the module refusing to compute its own
    /// numbers.
    #[must_use]
    pub fn with_ticket_stats(mut self, stats: Arc<dyn TicketStats>) -> Self {
        self.ticket_stats = Some(stats);
        self
    }
    /// Gives the widget routes their own limiter, for the per-visitor and
    /// per-IP buckets (`support-widget:{tenant}:v:{vid}` and
    /// `…:ip:{ip}`). On Workers this is a second Rate Limiting binding
    /// (`VISITOR_RATE_LIMITER`); where it is not wired, the widget falls
    /// back to the shared `RateLimiter` port.
    #[must_use]
    pub fn visitor_rate_limiter(
        mut self,
        limiter: Option<Arc<dyn cratefield_core::RateLimiter>>,
    ) -> Self {
        self.visitor_rate_limiter = limiter;
        self
    }
}

impl Module for Support {
    fn name(&self) -> &'static str {
        MODULE_NAME
    }

    fn version(&self) -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    fn requires(&self) -> &'static [Port] {
        &[Port::Db, Port::Signer, Port::Clock, Port::IdGen]
    }

    /// `HttpClient` for the `{"url"}` ingest form, for `POST /connectors`
    /// and for every fetch job the connectors enqueue; `RateLimiter` for
    /// the per-tenant budget on ingest, search and messages; `TextModel`
    /// for `POST /messages`' grounded answers; and `Captcha` for the
    /// widget's abuse gate (`crate::widget`). All four degrade honestly
    /// when absent: URL ingest and connector creation answer `503
    /// not-ready` (and the connector re-sync is a silent no-op), the
    /// limiter is skipped, messages answer `503
    /// text-model-not-configured`, and the widget captcha gate follows the
    /// harness `verify_human_form` posture — demanded with no port in
    /// production, stood down below it.
    ///
    /// `Blob` and `Defer` serve the chunked-upload routes. `Blob` is
    /// where upload parts land (`503 not-ready` on the upload routes when
    /// absent — ingest, search and answers never touch it); `Defer`
    /// drains the `extract` job inline after `complete`, and cron is the
    /// backstop that runs it regardless. `Defer` likewise pulls a new
    /// connector's first crawl sweep (and each fetch's discoveries) into
    /// the request that caused them. Neither is required because a
    /// deployment without object storage still gets the whole retrieval
    /// core through the inline `POST /sources` form.
    ///
    /// `Tracker` and `Mailer` are here for the handoff sink, not for a
    /// route: they are the ports the escalation pipeline reads when a
    /// handoff kicks it, and support's [`ModuleContext`] is a filtered
    /// view, so a port it does not declare is `None` in the sink's
    /// pipeline. `Tracker` is what files the ticket; `Mailer` is what
    /// sends the notify stage's message — dropping either would make a
    /// kicked run behave differently from the scheduled one. The
    /// escalation module's own `requires()` is where they are load-bearing;
    /// a deployment with no handoff sink never touches them.
    ///
    /// Optional, not required, on purpose: retrieval and ingest — the
    /// parts that make a workspace useful — work without a model, and a
    /// venture that never wires one should still boot (the escalation
    /// module is the one that cannot run without it, and it declares the
    /// port required).
    fn optional(&self) -> &'static [Port] {
        &[
            Port::HttpClient,
            Port::RateLimiter,
            Port::TextModel,
            Port::Tracker,
            Port::Mailer,
            Port::Captcha,
            Port::Blob,
            Port::Defer,
        ]
    }

    fn tables(&self) -> &'static [&'static str] {
        &[
            "sg_tenants",
            "sg_api_keys",
            "sg_sources",
            "sg_chunks",
            "sg_postings",
            "sg_terms",
            "sg_tenant_stats",
            "sg_conversations",
            "sg_messages",
            "sg_tenant_settings",
            "sg_uploads",
            "sg_upload_parts",
            "sg_support_outbox",
            "sg_connectors",
            "sg_ingest_pages",
            "sg_ingest_outbox",
            "sg_daily_stats",
            "sg_daily_gaps",
            "sg_daily_citations",
        ]
    }

    /// Every write here is authenticated — admin token for
    /// `POST /admin/tenants` and its settings route, a tenant API key for
    /// `/sources`, `/search` and `/messages` — so the harness's
    /// guarded-write boot gate has nothing to hold back.
    fn public_writes(&self) -> bool {
        false
    }

    /// One declaration per table, or conformance refuses the module.
    ///
    /// **`sg_tenants` is `Retain`, not `Erase`.** The row *is* the
    /// account: its subject is the id every other table points at, so a
    /// data-subject erasure against it has nothing to mean. Removing it
    /// is account closure — a tenant-lifecycle act (upstream ADR 0020),
    /// which takes the tenant's whole index with it in the order the
    /// lifecycle chooses, not the order an erasure request runs.
    ///
    /// **`sg_api_keys` is `none`.** It holds a key id, a label, an
    /// optional staff id (issue #35 — a caller-chosen handle for the
    /// support agent a key belongs to, not a real-world identity this
    /// module can resolve) and timestamps. The secret itself and its MAC
    /// live only in the minted response, shown once; the row names a
    /// company's credential, not a person.
    ///
    /// **The index tables are `unreachable`, and that is the honest
    /// answer.** `sg_sources`, `sg_chunks` and `sg_postings` hold the
    /// tenant's own documents, which may mention people; there is no
    /// column that identifies one, so no `… = ?` predicate can match a
    /// subject into them, and saying `none` would tell a manifest a
    /// table full of quoted prose holds nothing about anybody. A tenant
    /// can delete (or replace) a source it indexed, which removes that
    /// document's rows wholesale — but a deletion names a *document*, not
    /// a person, so erasure of a subject from the corpus is still not
    /// expressible and content leaves for good only with account closure
    /// — the only subject this data has is the tenant itself.
    ///
    /// **`sg_messages` is `unreachable` for the same reason.** The end
    /// users who write in are anonymous to this module — no email, no
    /// account id — so their words are content without a subject column.
    /// `sg_conversations` and `sg_tenant_settings` hold flags, a
    /// threshold and timestamps, and are `none`.
    ///
    /// **The search statistics split the same way.** `sg_terms` is a
    /// projection of the index — the workspace's words with their counts —
    /// so it is `unreachable` like `sg_postings`; `sg_tenant_stats` is two
    /// numbers per workspace and is `none`.
    ///
    /// **The connector tables follow their elders.** `sg_connectors` is
    /// `none` — configuration about the workspace, and a credential
    /// *reference* rather than a credential. `sg_ingest_pages` and
    /// `sg_ingest_outbox` hold crawled addresses, which are content the
    /// workspace pointed a connector at: no column identifies a person,
    /// so both are `unreachable`, same as the index tables they feed.
    fn personal_data(&self) -> &'static [PersonalDataSet] {
        PERSONAL_DATA
    }

    fn migrations(&self) -> Migrations {
        const MIGRATIONS: [SqlMigration; 10] = [
            MIGRATION_INIT,
            MIGRATION_CONVERSATIONS,
            MIGRATION_SOURCE_MANAGEMENT,
            MIGRATION_INTERNATIONALIZATION,
            MIGRATION_UPLOADS,
            MIGRATION_CONNECTORS,
            MIGRATION_SEARCH_STATS,
            MIGRATION_WIDGET_SETTINGS,
            MIGRATION_HUMAN_HANDOFF,
            MIGRATION_ANALYTICS,
        ];
        // Refuses a gap, a duplicate or an out-of-order id at compile
        // time.
        const _: () = assert_migration_set(&MIGRATIONS);
        // The SQL is written once, portably (ADR 0004), so Postgres
        // needs no override.
        Migrations {
            sqlite: &MIGRATIONS,
            postgres: &[],
        }
    }

    /// Two module-owned settings. `REVOKED_KIDS` (config key
    /// `SUPPORT_REVOKED_KIDS`) is a comma-separated list of key ids whose
    /// credentials must stop working immediately; it is parsed the same
    /// way at verification time (`tenancy::parse_revoked_kids` trims,
    /// drops empties, lowercases), so validation rejects exactly what
    /// verification would silently ignore: an entry that could never
    /// equal a real kid — empty after trimming is already dropped, so
    /// what is left to reject is an entry with the wrong characters or
    /// the wrong length. `UPLOAD_QUOTA_BYTES`
    /// (`SUPPORT_UPLOAD_QUOTA_BYTES`) is the per-tenant retained-upload
    /// budget; a value that does not parse would silently read as the
    /// default, which is the one failure mode a budget must not have.
    fn validate_config(&self, cfg: &dyn Config) -> Result<(), ConfigError> {
        let module = ModuleConfig::new(MODULE_NAME, cfg);
        let mut errors = ConfigError::default();

        if let Some(raw) = module.get_opt(tenancy::REVOKED_KIDS_KEY) {
            for kid in tenancy::parse_revoked_kids(&raw) {
                // Mirrors tenancy's `is_kid_name`: kid names are
                // lowercase alphanumeric plus `_` and `-`, bounded by
                // MAX_KID_NAME. A list entry outside that shape can
                // never match the kid a key presents, so carrying it
                // would be a revocation that does nothing.
                let well_formed = !kid.is_empty()
                    && kid.len() <= cratefield_core::MAX_KID_NAME
                    && kid
                        .chars()
                        .all(|c| matches!(c, 'a'..='z' | '0'..='9' | '_' | '-'));
                if !well_formed {
                    errors.push(format!(
                        "{}: entry {kid:?} is not a key id (lowercase letters, digits, \
                         '_' and '-', at most {} characters)",
                        module.key(tenancy::REVOKED_KIDS_KEY),
                        cratefield_core::MAX_KID_NAME
                    ));
                }
            }
        }

        if let Some(raw) = module.get_opt(uploads::QUOTA_KEY)
            && raw.parse::<u32>().is_err()
        {
            errors.push(format!(
                "{}: {raw:?} is not a byte count (a whole number of bytes; the default is {})",
                module.key(uploads::QUOTA_KEY),
                uploads::DEFAULT_QUOTA_BYTES
            ));
        }

        errors.into_result()
    }

    fn router(&self, ctx: ModuleContext) -> axum::Router {
        // The model `POST /messages` asks: whatever the runtime resolved,
        // `None` — and the degraded 503 — where it resolved nothing.
        let text_model = ctx.ports.text_model.clone();
        // The widget's own limiter, as given to the builder (`None`
        // falls back to the shared port inside the widget routes).
        let visitor_rate_limiter = self.visitor_rate_limiter.clone();
        handlers::router(
            Arc::new(ctx),
            text_model,
            self.handoff.clone(),
            visitor_rate_limiter,
            self.api_surfaces.clone(),
        )
    }

    /// The routes this module mounts (ADR 0010), plus whatever
    /// [`with_api_surface`](Support::with_api_surface) added is **not**
    /// declared here: it belongs to its own module, whose `surface()` the
    /// harness composes. This is support's own declaration alone.
    fn surface(&self) -> cratefield_core::Surface {
        handlers::surface()
    }

    /// The re-index drain: every chunk whose `tokenizer_version` stamp
    /// predates [`chunk::TOKENIZER_VERSION`] is re-tokenized from its
    /// stored text and its postings, `term_count` and stamp rewritten,
    /// [`REINDEX_BATCH`] at a time, until a sweep comes back short or
    /// [`REINDEX_MAX_SWEEPS`] are spent — so a tokenizer change re-claims
    /// the existing index over the cron ticks that follow it, and a chunk
    /// never sits half-rewritten. The venture's daily Worker cron (and the
    /// native binary's scheduler, where one runs) fans out here; the
    /// sweep's body is [`reindex_stale_chunks`], which tests and operators
    /// can drive directly.
    ///
    /// The same tick is the cron half of `uploads`: it drains leftover
    /// `extract` jobs (a deployment without `Defer`, or one whose deferred
    /// drain crashed) and collects the uploads nobody finished.
    ///
    /// It is also the connectors' re-sync (issue #29): every known page
    /// of every connector, plus its seed, is re-enqueued and the ingest
    /// outbox drained in bounded sweeps. Each re-enqueued fetch runs its
    /// conditional GET, so an unchanged page costs a 304 and writes
    /// nothing, a vanished page deletes its source and a changed one
    /// re-indexes.
    ///
    /// The three sweeps are independent, so all of them always run — a
    /// failing re-index must not strand an upload at `complete` or skip a
    /// connector re-sync, nor any other way round — and the first error,
    /// in that order, is what the tick reports.
    ///
    /// A fourth step rides the same daily tick: the analytics rollup
    /// recomputes the trailing [`analytics::ROLLUP_DAYS`] days
    /// ([`analytics::rollup_recent`]), so `/v1/support/analytics` reads
    /// precomputed rows. It is the last of the four, so the numbers a
    /// dashboard sees are computed from a corpus the same tick has
    /// already brought up to date.
    fn scheduled<'a>(
        &'a self,
        ctx: &'a ModuleContext,
        cron: &'a str,
    ) -> BoxFuture<'a, Result<(), AnyError>> {
        Box::pin(async move {
            let reindexed = reindex_sweep(ctx).await;
            let uploads = uploads::scheduled(ctx, cron).await;
            let connectors = connector_resync(ctx).await;
            let analytics = analytics::scheduled(ctx, self.ticket_stats.as_deref()).await;
            reindexed.and(uploads).and(connectors).and(analytics)
        })
    }
}

/// The connector third of [`Support::scheduled`]. With no database or no
/// `HttpClient` there is nothing this tick can fetch; both are silent
/// no-ops rather than cron noise, the same reading the other sweeps give
/// their ports.
async fn connector_resync(ctx: &ModuleContext) -> Result<(), AnyError> {
    let (Some(db), Some(http)) = (ctx.ports.db.clone(), ctx.ports.http.clone()) else {
        return Ok(());
    };
    let clock = ctx
        .ports
        .clock
        .clone()
        .unwrap_or_else(|| Arc::new(SystemClock));
    let id_gen = ctx
        .ports
        .id_gen
        .clone()
        .unwrap_or_else(|| Arc::new(UlidIdGen));
    let runner = connectors::Runner::new(
        db,
        http,
        ctx.ports.config.clone(),
        clock,
        id_gen,
        ctx.ports.defer.clone(),
    );
    runner
        .resync()
        .await
        .map(|_| ())
        .map_err(|err| Box::new(err) as AnyError)
}

/// [`Support::personal_data`]'s declarations, one per table — kept out
/// of the method body so the list can grow with the tables.
const PERSONAL_DATA: &[PersonalDataSet] = &[
    PersonalDataSet {
        table: "sg_tenants",
        subject: "id",
        kind: DataKind::Identifier,
        disposition: Disposition::Retain(
            "The row is the customer account itself. It is removed by closing the \
                 account, which deletes the tenant's whole index with it — not by a \
                 data-subject erasure request.",
        ),
        description: "The support workspace itself: its name, its status and when it \
                          was created.",
        redacted: &[],
        subject_via: None,
    },
    PersonalDataSet::none(
        "sg_api_keys",
        "A list of the workspace's API key ids: a random key id, a label and a \
             timestamp per key. The key material is never stored and nothing here names \
             a person.",
    ),
    PersonalDataSet::unreachable(
        "sg_sources",
        DataKind::Content,
        "Documents the workspace added — a title, an optional source URL, the \
             caller's optional external id for it and the size of what was indexed. \
             The document text itself may mention people.",
        "The rows belong to the workspace, not to any person the rows can name, so \
             no erasure predicate can match them. `DELETE /v1/support/sources/{id}` \
             removes a source the workspace indexed — but a deletion names a document, \
             not a person; content leaves for good when the workspace's account is \
             closed.",
    ),
    PersonalDataSet::unreachable(
        "sg_chunks",
        DataKind::Content,
        "Passages of the workspace's documents, kept verbatim so an answer can \
             quote them. They may quote people.",
        "Same as sg_sources: the text is reachable only through the workspace. \
             Chunks leave when their source is deleted or replaced, and when the \
             workspace's account is closed.",
    ),
    PersonalDataSet::unreachable(
        "sg_postings",
        DataKind::Content,
        "The search index over the workspace's documents: which word occurs in \
             which passage and how often.",
        "The index is a projection of sg_chunks and leaves with them — on source \
             deletion or replacement, or when the workspace's account is closed; no \
             person is identifiable from a word-count row.",
    ),
    PersonalDataSet::unreachable(
        "sg_terms",
        DataKind::Content,
        "One row per indexed word per workspace: how many passages carry it — a \
             count derived from sg_postings, kept so ranking never has to count the \
             index. The words are the workspace's own vocabulary and may include a name.",
        "Same as sg_postings: a projection of the workspace's documents with no column \
             that identifies a person. A word's row leaves when no passage carries it any \
             more — on source deletion or replacement — and when the workspace's account \
             is closed.",
    ),
    PersonalDataSet::none(
        "sg_tenant_stats",
        "One row per workspace: how many passages it holds and their total word count \
             — the two aggregates BM25 normalises with. Numbers only; nothing here names \
             anyone.",
    ),
    PersonalDataSet::none(
        "sg_conversations",
        "One row per support conversation: its status, whether it needs a person, \
             and timestamps. What was said lives in sg_messages; nothing here names \
             anyone.",
    ),
    PersonalDataSet::unreachable(
        "sg_messages",
        DataKind::Content,
        "The messages of each support conversation: what the end user wrote, what \
             they were shown, what the model answered and the language the turn was \
             detected to be in. The text may mention people.",
        "The workspace's end users are anonymous to this module: a message carries \
             no email, account or other column that identifies its author, so no \
             erasure predicate can match a person into it. Messages leave when the \
             workspace's account is closed.",
    ),
    PersonalDataSet::none(
        "sg_tenant_settings",
        "The workspace's answer threshold, the origins its web widget may be embedded \
             on, and when either was last set.",
    ),
    PersonalDataSet::unreachable(
        "sg_uploads",
        DataKind::Content,
        "The workspace's uploaded documents as bookkeeping: filename, content type, \
             sizes, status and the ids of the source each was indexed into. The document \
             bytes live in the Blob port and the indexed text in sg_sources; a filename \
             or a PDF's words may mention people.",
        "The rows belong to the workspace, not to any person the rows can name. An \
             upload past its status change keeps no bytes anywhere (part blobs are deleted \
             the moment the upload turns extracted, failed or collected), so there is no \
             content here to erase beyond what account closure removes.",
    ),
    PersonalDataSet::none(
        "sg_upload_parts",
        "One size row per uploaded part: the part's ordinal and its byte count. The \
             bytes themselves live in the Blob port and are deleted with the upload; \
             nothing here names a person.",
    ),
    PersonalDataSet::none(
        "sg_support_outbox",
        "The module's durable work queue: an extract job per completed upload, \
             carrying two ids (upload and tenant) and timestamps. No content, no person.",
    ),
    PersonalDataSet::none(
        "sg_connectors",
        "The workspace's content connectors: which kind of source they sync (a \
             sitemap, a URL prefix or a GitHub repository), the addresses they sync \
             from, per-sync caps, and a *reference* to the credential (a config key \
             name, never the secret). Configuration about the workspace, not about a \
             person.",
    ),
    PersonalDataSet::unreachable(
        "sg_ingest_pages",
        DataKind::Content,
        "The addresses a connector has synced, with the fetch validators and the \
             indexed document each maps to. A page address can name a person (an \
             about page, a profile path).",
        "The rows belong to the workspace's connectors, not to any person the rows \
             can name — the address is indexed content, the same status as a synced \
             document. Deleting a connector's content is connector deletion, not a \
             subject erasure; the content leaves when the workspace's account is \
             closed.",
    ),
    PersonalDataSet::unreachable(
        "sg_ingest_outbox",
        DataKind::Content,
        "The queue of fetches a connector's sync has pending: which connector, \
             which address, at what crawl depth. A queued address can name a person.",
        "The address in a pending fetch is content the connector was pointed at, \
             with no column identifying a person, so no erasure predicate can match \
             one into the queue. The queue drains to the same page rows (above) and \
             leaves when the workspace's account is closed.",
    ),
    PersonalDataSet::none(
        "sg_daily_stats",
        "One rolled-up day per workspace: how many conversations opened and turns \
             were answered, clarified or handed off, how many tickets were filed or \
             rejected, and the day's median model confidence. Counts and one \
             percentage; nothing here names anyone.",
    ),
    PersonalDataSet::unreachable(
        "sg_daily_gaps",
        DataKind::Content,
        "The normalized query terms behind a day's unanswered turns — the words a \
             workspace's end users asked about and the index could not serve, counted \
             per term. The words are the workspace's own vocabulary and may include a \
             name.",
        "Only normalized terms are stored — never the question they came from — so \
             there is no sentence and no column identifying a person for an erasure \
             predicate to match. The rows are a projection of sg_messages and leave \
             when the workspace's account is closed.",
    ),
    PersonalDataSet::none(
        "sg_daily_citations",
        "One rolled-up day per workspace and source: how many times that source's \
             passages were cited by the day's answers. Two ids and a count; nothing \
             here names a person.",
    ),
];

/// The re-index third of [`Support::scheduled`]: bounded sweeps of
/// [`reindex_stale_chunks`] until one comes back short.
async fn reindex_sweep(ctx: &ModuleContext) -> Result<(), AnyError> {
    let Some(db) = ctx.ports.db.clone() else {
        // `Port::Db` is required, so a composed venture always resolves
        // it; a hand-rolled context may not, and sweeping nothing beats
        // panicking a cron tick.
        return Ok(());
    };
    for _ in 0..REINDEX_MAX_SWEEPS {
        let rewritten = reindex_stale_chunks(db.as_ref(), REINDEX_BATCH)
            .await
            .map_err(|err| Box::new(err) as AnyError)?;
        if rewritten < REINDEX_BATCH {
            break;
        }
    }
    Ok(())
}
