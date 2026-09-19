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

pub mod bm25;
pub mod chunk;
mod handlers;
mod store;

pub use chunk::tokenize;

use cratefield_core::{
    Config, ConfigError, DataKind, Disposition, Migrations, Module, ModuleConfig, ModuleContext,
    PersonalDataSet, Port, SqlMigration, assert_migration_set,
};

pub(crate) const MODULE_NAME: &str = "support";

/// The schema, in one migration: five tables of portable SQL (ADR 0004),
/// every one of them carrying `tenant_id`.
const MIGRATION_INIT: SqlMigration = SqlMigration::new(
    "0001",
    "init",
    include_str!("../migrations/sqlite/0001_init.sql"),
);

/// The support module: tenant provisioning behind the harness admin
/// token, API-key-authenticated source ingest and BM25 search for
/// everything else.
///
/// Deliberately knob-free. Everything tunable — the revoked-kid list,
/// the admin token — is deployment configuration read through the
/// `Config` port, so a builder setter here would be a second place the
/// same setting lived.
#[derive(Debug, Default)]
pub struct Support;

impl Support {
    /// A `Support` module with defaults.
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
    /// per-tenant budget on ingest and search. Both degrade honestly
    /// when absent: URL ingest answers `503 not-ready`, the limiter is
    /// skipped.
    fn optional(&self) -> &'static [Port] {
        &[Port::HttpClient, Port::RateLimiter]
    }

    fn tables(&self) -> &'static [&'static str] {
        &[
            "sg_tenants",
            "sg_api_keys",
            "sg_sources",
            "sg_chunks",
            "sg_postings",
        ]
    }

    /// Every write here is authenticated — admin token for
    /// `POST /admin/tenants`, a tenant API key for `/sources` and
    /// `/search` — so the harness's guarded-write boot gate has nothing
    /// to hold back.
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
        ];
        SETS
    }

    fn migrations(&self) -> Migrations {
        const MIGRATIONS: [SqlMigration; 1] = [MIGRATION_INIT];
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
        handlers::router(std::sync::Arc::new(ctx))
    }
}
