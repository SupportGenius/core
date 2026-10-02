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

mod answer;
pub mod chunk;
mod handlers;
mod messages;
mod store;

pub use answer::DEFAULT_ANSWER_THRESHOLD;
pub use chunk::tokenize;
// The tokenizer and BM25 ranker now live in the shared `lexical` crate
// (escalation's duplicate scoring tokenizes with the same rules); this
// re-export keeps `module_support::bm25` — and `crate::bm25` inside the
// module — exactly where callers already expect it.
pub use lexical::bm25;
pub use store::reindex_stale_chunks;

use std::sync::Arc;

use cratefield_core::{
    AnyError, BoxFuture, Config, ConfigError, DataKind, Disposition, Migrations, Module,
    ModuleConfig, ModuleContext, PersonalDataSet, Port, SqlMigration, assert_migration_set,
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

/// The support module: tenant provisioning behind the harness admin
/// token, API-key-authenticated source ingest, BM25 search and grounded
/// answers for everything else.
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

    /// `HttpClient` for the `{"url"}` ingest form, `RateLimiter` for the
    /// per-tenant budget on ingest, search and messages, and `TextModel`
    /// for `POST /messages`' grounded answers. All three degrade honestly
    /// when absent: URL ingest answers `503 not-ready`, the limiter is
    /// skipped, and messages answer `503 text-model-not-configured`.
    ///
    /// Optional, not required, on purpose: retrieval and ingest — the
    /// parts that make a workspace useful — work without a model, and a
    /// venture that never wires one should still boot (the escalation
    /// module is the one that cannot run without it, and it declares the
    /// port required).
    fn optional(&self) -> &'static [Port] {
        &[Port::HttpClient, Port::RateLimiter, Port::TextModel]
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
                "The workspace's answer threshold and when it was last set.",
            ),
        ];
        SETS
    }

    fn migrations(&self) -> Migrations {
        const MIGRATIONS: [SqlMigration; 4] = [
            MIGRATION_INIT,
            MIGRATION_CONVERSATIONS,
            MIGRATION_SOURCE_MANAGEMENT,
            MIGRATION_INTERNATIONALIZATION,
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
    fn scheduled<'a>(
        &'a self,
        ctx: &'a ModuleContext,
        _cron: &'a str,
    ) -> BoxFuture<'a, Result<(), AnyError>> {
        Box::pin(async move {
            let Some(db) = ctx.ports.db.clone() else {
                // `Port::Db` is required, so a composed venture always
                // resolves it; a hand-rolled context may not, and
                // sweeping nothing beats panicking a cron tick.
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
        })
    }
}
