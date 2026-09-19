//! `module-support`: the SupportGenius support module (issue #3). A
//! customer asks at `POST /v1/support/messages`; the module retrieves
//! chunks for the tenant, asks a [`TextModel`] for an answer grounded in
//! them, and publishes the answer only when it can stand behind it.
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! let module = module_support::Support::new();
//! // A deployment that has a model hands it to the builder:
//! // Support::new().text_model(Arc::new(my_model));
//! ```
//!
//! **A citation must name a chunk that was retrieved** (module `answer`):
//! one fabricated id downgrades the turn to a clarify, whatever the
//! model's confidence — a citation that can name anything is decoration.
//! `answered` needs valid citations, at least one of them, and a
//! confidence at or above the answer threshold; a tenant's stored
//! threshold wins over the deployment default (precedence in the README).
//!
//! **Failures do not consume the conversation.** Retrieval and the model
//! call both happen before the first write, and the turn's three writes
//! run in one `batch_atomic`, so a `503 text-model-unavailable` leaves
//! no row behind and the same request can simply be retried.

#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

mod answer;
mod fakes;
mod handlers;
mod store;
mod text_model;

pub use fakes::{FakeTextModel, ScriptedReply};
pub use text_model::{ModelTier, TextCompletion, TextModel, TextModelError, TextRequest};

use cratefield_core::{
    Config, ConfigError, DataKind, Migrations, Module, ModuleConfig, ModuleContext,
    PersonalDataSet, Port, SqlMigration, Surface,
};
use std::sync::Arc;

/// The module's migrations: the retrieval stub the v0/v1 seam expects to
/// be replaced (see the migration's header comment), and support v1's own
/// conversation schema.
const MIGRATION_RETRIEVAL_STUB: SqlMigration = SqlMigration::new(
    "0001",
    "retrieval_stub",
    include_str!("../migrations/sqlite/0001_retrieval_stub.sql"),
);

const MIGRATION_CONVERSATIONS: SqlMigration = SqlMigration::new(
    "0002",
    "conversations",
    include_str!("../migrations/sqlite/0002_conversations.sql"),
);

/// The support module.
///
/// `text_model` starts as `None`: the route then answers exactly as
/// [`TextModelError::NotConfigured`] — the same clean 503
/// `text-model-not-configured`, no panic, nothing written. That mirrors
/// the venture's "no silent no-op" rule: an unconfigured model reports
/// itself, it does not pretend to answer.
pub struct Support {
    settings: handlers::Settings,
}

impl Default for Support {
    fn default() -> Self {
        Self::new()
    }
}

impl Support {
    /// No model, deployment-default threshold and top-k.
    pub fn new() -> Self {
        Self {
            settings: handlers::Settings {
                text_model: None,
                answer_threshold: None,
                top_k: None,
            },
        }
    }

    /// Hands the module its model. `None` (the default) makes every turn
    /// answer `503 text-model-not-configured`.
    #[must_use]
    pub fn text_model(mut self, model: Arc<dyn TextModel>) -> Self {
        self.settings.text_model = Some(model);
        self
    }

    /// The builder-level answer threshold, below `SUPPORT_ANSWER_THRESHOLD`
    /// and the tenant's stored value in the precedence order (README:
    /// "Threshold precedence").
    ///
    /// Clamped on the way in: `NaN` becomes the documented default
    /// ([`handlers::DEFAULT_ANSWER_THRESHOLD`]) and anything else is
    /// clamped to `0.0..=1.0`. A raw `NaN` would poison every turn — it
    /// compares `false` even against itself, so no confidence could ever
    /// clear it and every conversation would clarify then hand off — and
    /// the admin route and `validate_config` both reject out-of-range
    /// values, so the builder holds itself to the same standard.
    #[must_use]
    pub fn answer_threshold(mut self, threshold: f32) -> Self {
        self.settings.answer_threshold = Some(if threshold.is_nan() {
            handlers::DEFAULT_ANSWER_THRESHOLD
        } else {
            threshold.clamp(0.0, 1.0)
        });
        self
    }

    /// The builder-level retrieval top-k, below `SUPPORT_TOP_K` in the
    /// precedence order.
    ///
    /// Floored at 1: `top_k(0)` would retrieve nothing, so no citation
    /// could ever be valid and every turn would hand off — the same
    /// "must be a positive integer" rule `validate_config` enforces for
    /// `SUPPORT_TOP_K`.
    #[must_use]
    pub fn top_k(mut self, top_k: u32) -> Self {
        self.settings.top_k = Some(top_k.max(1));
        self
    }
}

impl Module for Support {
    fn name(&self) -> &'static str {
        "support"
    }

    fn version(&self) -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    /// The database, and only the database: the model arrives through the
    /// builder (it is this crate's local port, not a harness `Port` yet),
    /// ids and timestamps come from `SystemClock`/`UlidIdGen` directly,
    /// and the tenant-scoped handle comes from the `TenantConn`
    /// extractor, which the resolution layer fills in — it is not a port
    /// a module declares.
    fn requires(&self) -> &'static [Port] {
        &[Port::Db]
    }

    fn tables(&self) -> &'static [&'static str] {
        &[
            "sg_chunks",
            "sg_conversations",
            "sg_messages",
            "sg_tenant_settings",
        ]
    }

    /// The honest shape of "who is named here" while support conversations
    /// have **no account attached** — tenancy and API keys are issue #2,
    /// and these declarations are revisited when a person becomes
    /// matchable.
    ///
    /// `sg_messages` is the hard one, and [`unreachable`]'s case exactly:
    /// the rows hold whatever the end user typed, which is personal
    /// content and often names its author — but no column identifies the
    /// person who wrote it, so no `… = ?` predicate can find their rows.
    /// Declaring `none` would tell the subject a table holding their words
    /// holds nothing about anybody; declaring a subject column that does
    /// not exist would promise an erasure that matches nothing. The
    /// reachable alternative is a future change, not a present lie.
    ///
    /// The other three genuinely name nobody, and each says so rather than
    /// staying silent: conversations are an opaque id plus status and
    /// timestamps, chunks are the tenant's own reference content, and the
    /// settings row is one number.
    fn personal_data(&self) -> &'static [PersonalDataSet] {
        const SETS: &[PersonalDataSet] = &[
            PersonalDataSet::unreachable(
                "sg_messages",
                DataKind::Content,
                "What you wrote to support and what was answered, on the conversation you sent \
                 it in.",
                "Your messages are free text and no account is attached to support \
                 conversations yet, so we have no way to find your rows with an erasure \
                 request alone. This declaration changes once support accounts land.",
            ),
            PersonalDataSet::none(
                "sg_conversations",
                "One row per support conversation: an opaque id, whether it is open or \
                 escalated, and two timestamps. It names nobody — the words live in \
                 sg_messages.",
            ),
            PersonalDataSet::none(
                "sg_chunks",
                "Snippets of the tenant's own help content, indexed so questions can be \
                 matched against them. Reference material about the product; it names no \
                 customer.",
            ),
            PersonalDataSet::none(
                "sg_tenant_settings",
                "One row per tenant holding the confidence threshold below which the \
                 assistant will not claim an answer. A number and a timestamp; nobody is \
                 named in it.",
            ),
        ];
        SETS
    }

    fn migrations(&self) -> Migrations {
        const MIGRATIONS: [SqlMigration; 2] = [MIGRATION_RETRIEVAL_STUB, MIGRATION_CONVERSATIONS];
        // The array is the apply order; this refuses a gap, a duplicate
        // or an entry out of order at build time.
        const _: () = cratefield_core::assert_migration_set(&MIGRATIONS);
        Migrations {
            sqlite: &MIGRATIONS,
            postgres: &[],
        }
    }

    /// `SUPPORT_ANSWER_THRESHOLD` must parse as a fraction in `0.0..=1.0`
    /// and `SUPPORT_TOP_K` as a positive integer. Both problems
    /// accumulate — an operator fixing two typos should not discover them
    /// one boot apart.
    fn validate_config(&self, cfg: &dyn Config) -> Result<(), ConfigError> {
        let module = ModuleConfig::new("support", cfg);
        let mut errors = ConfigError::default();

        if let Some(raw) = cfg.get(&module.key("ANSWER_THRESHOLD")) {
            let valid = raw
                .parse::<f32>()
                .is_ok_and(|threshold| (0.0..=1.0).contains(&threshold));
            if !valid {
                errors.push(format!(
                    "support: {} must be a number in 0.0..=1.0, got {raw:?}",
                    module.key("ANSWER_THRESHOLD")
                ));
            }
        }
        if let Some(raw) = cfg.get(&module.key("TOP_K")) {
            let valid = raw.parse::<u32>().is_ok_and(|top_k| top_k > 0);
            if !valid {
                errors.push(format!(
                    "support: {} must be a positive integer, got {raw:?}",
                    module.key("TOP_K")
                ));
            }
        }

        errors.into_result()
    }

    fn router(&self, ctx: ModuleContext) -> axum::Router {
        handlers::router(Arc::new(ctx), self.settings.clone())
    }

    fn surface(&self) -> Surface {
        handlers::surface()
    }

    /// True, and said plainly: `POST /v1/support/messages` writes with no
    /// caller check at all today. The declared actions carry the default
    /// `Open` policy —
    /// [`cratefield_core::RoutePolicy::HumanForm`](cratefield_core::RoutePolicy::HumanForm)
    /// would be a lie the handler does not earn (no token is requested or
    /// verified), and the signature/signed-link variants describe proofs
    /// no support turn presents. The harness's conservative fallback
    /// therefore still counts this module as a CAPTCHA writer in
    /// production, which is the truthful reading of an unauthenticated
    /// public write. When issue #2 lands tenant API keys, the handler
    /// authenticates its caller and both this flag and the fallback
    /// resolve to genuinely `Open`.
    fn public_writes(&self) -> bool {
        true
    }

    fn public_write_policy(&self) -> cratefield_core::RoutePolicy {
        // Consulted only because the surface declares no *guarded* action;
        // kept at the default rather than weakened: an open write to a
        // human-facing widget is exactly what the conservative default
        // describes, until tenancy replaces it.
        cratefield_core::RoutePolicy::HumanForm
    }
}

#[cfg(test)]
mod tests {
    use super::Support;
    use cratefield_core::{MapConfig, Module, Port};

    #[test]
    fn defaults_match_the_issue() {
        let module = Support::new();
        assert_eq!(module.name(), "support");
        assert_eq!(
            module.tables(),
            [
                "sg_chunks",
                "sg_conversations",
                "sg_messages",
                "sg_tenant_settings"
            ]
        );
        assert_eq!(module.requires(), [Port::Db]);
        assert!(module.public_writes());
        assert!(module.settings.text_model.is_none());
        assert_eq!(module.settings.top_k, None);
        assert_eq!(module.settings.answer_threshold, None);
    }

    #[test]
    fn builder_sets_the_overrides() {
        let model: std::sync::Arc<dyn crate::TextModel> =
            std::sync::Arc::new(crate::FakeTextModel::replying("{}"));
        let module = Support::new()
            .text_model(std::sync::Arc::clone(&model))
            .answer_threshold(0.75)
            .top_k(3);
        assert!(module.settings.text_model.is_some());
        assert_eq!(module.settings.answer_threshold, Some(0.75));
        assert_eq!(module.settings.top_k, Some(3));
    }

    #[test]
    fn builder_clamps_its_inputs_to_usable_ranges() {
        use super::handlers::DEFAULT_ANSWER_THRESHOLD;
        // A NaN threshold compares false against every confidence, so
        // unclamped it would clarify then hand off every turn; it falls
        // back to the documented default instead.
        assert_eq!(
            Support::new()
                .answer_threshold(f32::NAN)
                .settings
                .answer_threshold,
            Some(DEFAULT_ANSWER_THRESHOLD)
        );
        // Out of range clamps to the nearest bound; in range passes through.
        assert_eq!(
            Support::new()
                .answer_threshold(1.5)
                .settings
                .answer_threshold,
            Some(1.0)
        );
        assert_eq!(
            Support::new()
                .answer_threshold(-0.1)
                .settings
                .answer_threshold,
            Some(0.0)
        );
        assert_eq!(
            Support::new()
                .answer_threshold(0.75)
                .settings
                .answer_threshold,
            Some(0.75)
        );
        // top_k(0) would retrieve nothing, so every turn would hand off.
        assert_eq!(Support::new().top_k(0).settings.top_k, Some(1));
        assert_eq!(Support::new().top_k(6).settings.top_k, Some(6));
    }

    #[test]
    fn config_validation_accumulates_both_problems() {
        let module = Support::new();
        let cfg =
            MapConfig::from_pairs([("SUPPORT_ANSWER_THRESHOLD", "1.5"), ("SUPPORT_TOP_K", "0")]);
        let err = module
            .validate_config(&cfg)
            .expect_err("both keys are invalid");
        assert_eq!(err.problems.len(), 2, "both problems reported together");
        assert!(err.problems[0].contains("SUPPORT_ANSWER_THRESHOLD"));
        assert!(err.problems[1].contains("SUPPORT_TOP_K"));
    }

    #[test]
    fn config_validation_accepts_the_valid_range() {
        let module = Support::new();
        let cfg = MapConfig::from_pairs([
            ("SUPPORT_ANSWER_THRESHOLD", "0.55"),
            ("SUPPORT_TOP_K", "12"),
        ]);
        assert!(module.validate_config(&cfg).is_ok());
        // Absent keys are fine too: the compile-time defaults apply.
        assert!(module.validate_config(&MapConfig::default()).is_ok());
    }

    #[test]
    fn migrations_pass_the_portable_sql_lint() {
        // The same predicate `fz doctor` and the Postgres migration runner
        // apply, run here so a non-portable statement cannot ship.
        let module = Support::new();
        for migration in module.migrations().sqlite {
            let violations = cratefield_core::lint_portable_sql(migration.sql);
            assert!(
                violations.is_empty(),
                "{} is not portable: {violations:?}",
                migration.id
            );
            assert!(
                cratefield_core::lint_card_data(migration.sql).is_empty(),
                "{} must not carry card-data-shaped columns",
                migration.id
            );
        }
    }
}
