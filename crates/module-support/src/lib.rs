//! The SupportGenius support module's retrieval core: tokenisation,
//! chunking and BM25 ranking, as pure Rust with no I/O of any kind, so
//! the same code runs in a Cloudflare Worker isolate and in `cargo test`.
//!
//! At ingest, a document (up to 48 KiB of text — see
//! [`handlers::MAX_TEXT_BYTES`] for why) is split into overlapping word
//! windows by [`chunk::Chunker`] and inverted into `sg_postings` rows.
//! At query time, the caller fetches the postings for the query's terms
//! and [`bm25::rank`] scores them in-process with Okapi BM25.
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
//! one).
//!
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

mod answer;
pub mod bm25;
pub mod chunk;
mod connectors;
mod handlers;
mod messages;
mod store;

pub use answer::DEFAULT_ANSWER_THRESHOLD;
pub use chunk::tokenize;

use std::sync::Arc;

use cratefield_core::{
    AnyError, BoxFuture, Config, ConfigError, DataKind, Disposition, Migrations, Module,
    ModuleConfig, ModuleContext, PersonalDataSet, Port, SqlMigration, SystemClock, UlidIdGen,
    assert_migration_set,
};

pub(crate) const MODULE_NAME: &str = "support";

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

/// Connectors (issue #29): the crawl roots, their per-URL fetch state and
/// the ingest outbox. The outbox block inside is generated from core's
/// `Outbox::create_table_sql` — see the migration file's header before
/// touching it.
const MIGRATION_CONNECTORS: SqlMigration = SqlMigration::new(
    "0003",
    "connectors",
    include_str!("../migrations/sqlite/0003_connectors.sql"),
);

/// The support module: tenant provisioning behind the harness admin
/// token, API-key-authenticated source ingest, BM25 search and grounded
/// answers, and the cron-driven connectors that keep a workspace's index
/// synced from a sitemap, a URL prefix or a GitHub repository
/// (`POST /connectors`, issue #29).
///
/// Knob-free. Everything tunable — the revoked-kid list, the admin token —
/// is deployment configuration read through the `Config` port, and the
/// answer threshold is per-tenant data
/// (`PUT /admin/tenants/{tenant_id}/settings`), so a builder setter for
/// either would be a second place the same setting lived. The text model
/// is likewise not passed here: it arrives through the runtime's
/// [`Ports`](cratefield_core::Ports) at route-build time, so one instance
/// serves a venture with a model and one without.
#[derive(Default)]
pub struct Support;

impl Support {
    /// A `Support` module with defaults. Whether `POST /messages` can
    /// answer depends on what the runtime provides: with no `TextModel`
    /// port it answers `503 text-model-not-configured` and every other
    /// route still works.
    pub fn new() -> Self {
        Self
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

    /// `HttpClient` for the `{"url"}` ingest form, for `POST /connectors`,
    /// and for every fetch job the connectors enqueue; `RateLimiter` for
    /// the per-tenant budget on ingest, search and messages; `TextModel`
    /// for `POST /messages`' grounded answers; `Defer` so a connector
    /// creation (and each fetch's discoveries) can run their first sweep
    /// inside the request that caused them. All four degrade honestly when
    /// absent: URL ingest and connector creation answer `503 not-ready`,
    /// the limiter is skipped, messages answer `503
    /// text-model-not-configured`, and without `Defer` the work waits for
    /// the scheduled tick that is its backstop anyway.
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
            "sg_conversations",
            "sg_messages",
            "sg_tenant_settings",
            "sg_connectors",
            "sg_ingest_pages",
            "sg_ingest_outbox",
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
    /// **`sg_api_keys` is `none`.** It holds a key id, a label and
    /// timestamps. The secret itself and its MAC live only in the minted
    /// response, shown once; the row names a company's credential, not a
    /// person.
    ///
    /// **The index tables are `unreachable`, and that is the honest
    /// answer.** `sg_sources`, `sg_chunks` and `sg_postings` hold the
    /// tenant's own documents, which may mention people; there is no
    /// column that identifies one, so no `… = ?` predicate can match a
    /// subject into them, and saying `none` would tell a manifest a
    /// table full of quoted prose holds nothing about anybody. This
    /// version ships no source-deletion route, so content leaves only
    /// with account closure — the only subject this data has is the
    /// tenant itself.
    ///
    /// **`sg_messages` is `unreachable` for the same reason.** The end
    /// users who write in are anonymous to this module — no email, no
    /// account id — so their words are content without a subject column.
    /// `sg_conversations` and `sg_tenant_settings` hold flags, a
    /// threshold and timestamps, and are `none`.
    ///
    /// **The connector tables follow their elders.** `sg_connectors` is
    /// `none` — configuration about the workspace, and a credential
    /// *reference* rather than a credential. `sg_ingest_pages` and
    /// `sg_ingest_outbox` hold crawled addresses, which are content the
    /// workspace pointed a connector at: no column identifies a person,
    /// so both are `unreachable`, same as the index tables they feed.
    fn personal_data(&self) -> &'static [PersonalDataSet] {
        const SETS: &[PersonalDataSet] = &[
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
                "Documents the workspace added — a title, an optional source URL and the \
                 size of what was indexed. The document text itself may mention people.",
                "The rows belong to the workspace, not to any person the rows can name, so \
                 no erasure predicate can match them. This version ships no source-deletion \
                 route; the content leaves when the workspace's account is closed.",
            ),
            PersonalDataSet::unreachable(
                "sg_chunks",
                DataKind::Content,
                "Passages of the workspace's documents, kept verbatim so an answer can \
                 quote them. They may quote people.",
                "Same as sg_sources: the text is reachable only through the workspace. \
                 This version ships no source-deletion route; chunks leave when the \
                 workspace's account is closed.",
            ),
            PersonalDataSet::unreachable(
                "sg_postings",
                DataKind::Content,
                "The search index over the workspace's documents: which word occurs in \
                 which passage and how often.",
                "The index is a projection of sg_chunks and leaves with them when the \
                 workspace's account is closed; no person is identifiable from a word-count \
                 row. This version ships no source-deletion route.",
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
                 they were shown and what the model answered. The text may mention people.",
                "The workspace's end users are anonymous to this module: a message carries \
                 no email, account or other column that identifies its author, so no \
                 erasure predicate can match a person into it. Messages leave when the \
                 workspace's account is closed.",
            ),
            PersonalDataSet::none(
                "sg_tenant_settings",
                "The workspace's answer threshold and when it was last set.",
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
        ];
        SETS
    }

    fn migrations(&self) -> Migrations {
        const MIGRATIONS: [SqlMigration; 3] = [
            MIGRATION_INIT,
            MIGRATION_CONVERSATIONS,
            MIGRATION_CONNECTORS,
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

    /// The only module-owned setting is `REVOKED_KIDS` (config key
    /// `SUPPORT_REVOKED_KIDS`): a comma-separated list of key ids whose
    /// credentials must stop working immediately. It is parsed the same
    /// way at verification time (`tenancy::parse_revoked_kids` trims,
    /// drops empties, lowercases), so validation rejects exactly what
    /// verification would silently ignore: an entry that could never
    /// equal a real kid — empty after trimming is already dropped, so
    /// what is left to reject is an entry with the wrong characters or
    /// the wrong length.
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

        errors.into_result()
    }

    fn router(&self, ctx: ModuleContext) -> axum::Router {
        // The model `POST /messages` asks: whatever the runtime resolved,
        // `None` — and the degraded 503 — where it resolved nothing.
        let text_model = ctx.ports.text_model.clone();
        handlers::router(Arc::new(ctx), text_model)
    }

    /// The connectors' cron tick (issue #29): re-enqueue every known page
    /// of every connector plus its seed, then drain the ingest outbox the
    /// way module-escalation's scheduled hook drains its pipeline —
    /// bounded sweeps, so one tick cannot spin forever.
    ///
    /// Each re-enqueued fetch runs its conditional GET: an unchanged page
    /// costs a 304 and writes nothing, a vanished page deletes its source,
    /// a changed one re-indexes. With no database or no `HttpClient` there
    /// is nothing this tick can do; both are silent no-ops rather than
    /// cron noise, the same reading escalation gives its required ports.
    fn scheduled<'a>(
        &'a self,
        ctx: &'a ModuleContext,
        _cron: &'a str,
    ) -> BoxFuture<'a, Result<(), AnyError>> {
        Box::pin(async move {
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
        })
    }
}
