//! `module-escalation`: a conversation becomes a ticket — drafted by one
//! model, checked by an independent one, filed by a router, followed up
//! until it closes (issue #4, `/workspace/README.md`).
//!
//! The crate is four layers:
//!
//! - [`ports`] mirrors the `TextModel` and `Tracker` traits the pipeline
//!   is written against (with the `testing` fakes behind the feature
//!   gate) — core 0.4.3 publishes no such ports, so until it does they
//!   live here and move when it does.
//! - [`model`] is the stage/status vocabulary and the two JSON schemas;
//!   [`store`] is the sea-query data access; [`intake`] is the
//!   cross-module handoff that turns a conversation into statements the
//!   caller commits, which enqueue the first outbox row.
//! - [`Pipeline`] is the durable stage runner: it claims due outbox rows
//!   and drives draft → judge → file → notify, one port call and one
//!   all-or-nothing commit per stage, with an
//!   [`Inbox`](cratefield_core::Inbox) claim per `ticket:stage` so
//!   draining twice files once, and a [`RetryPolicy`] that bounds how
//!   long a transient failure retries before dead-lettering.
//! - [`Escalation`] wires it into the [`Module`] contract: migrations,
//!   tables, the personal-data declarations, config validation and the
//!   scheduled drain.
//!
//! There is deliberately **no HTTP surface**: see [`Escalation::router`].

#![forbid(unsafe_code)]

mod pipeline;

pub mod error;
pub mod intake;
pub mod model;
pub mod ports;
pub mod store;

pub use error::Error;
pub use intake::{Handoff, Intake};
pub use pipeline::{Pipeline, RetryPolicy};

/// Fake [`crate::ports::text_model::TextModel`] and
/// [`crate::ports::tracker::Tracker`] doubles for tests. Behind the
/// `testing` feature so the Worker build never carries them; `tests/`
/// enable it through the crate's self dev-dependency.
#[cfg(feature = "testing")]
pub mod testing;

/// The module's migrations: the five `sg_*` tables in the portable SQL
/// subset (issue #4). The outbox/inbox blocks inside are generated from
/// core's `create_table_sql` helpers — see the migration file's header
/// before touching them.
pub const MIGRATION_ESCALATION: SqlMigration = SqlMigration::new(
    "0001",
    "escalation",
    include_str!("../migrations/sqlite/0001_escalation.sql"),
);

use std::sync::Arc;
use std::time::Duration;

use cratefield_core::{
    AnyError, BoxFuture, Config, ConfigError, DataKind, Disposition, Migrations, Module,
    ModuleConfig, ModuleContext, PersonalDataSet, Port, SqlMigration, SystemClock, UlidIdGen,
};

use crate::intake::OUTBOX_TABLE;

/// The SupportGenius escalation module (issue #4).
///
/// Construct it with the two ports the published harness does not have
/// yet, register it with the harness, and let [`Module::scheduled`] drain
/// the outbox on cron. The conversation enters through
/// [`Escalation::intake`], whose statements the caller commits with its
/// own write; everything after that is the [`Pipeline`], driven by cron:
///
/// ```no_run
/// use std::sync::Arc;
///
/// use cratefield_core::Module as _;
/// use module_escalation::Escalation;
///
/// # struct MyModel;
/// #[async_trait::async_trait]
/// impl module_escalation::ports::text_model::TextModel for MyModel {
///     async fn complete(
///         &self,
///         _prompt: &module_escalation::ports::text_model::Prompt,
///     ) -> Result<
///         module_escalation::ports::text_model::Completion,
///         module_escalation::ports::text_model::TextModelError,
///     > {
///         unimplemented!("your model adapter")
///     }
/// }
/// # struct MyTracker;
/// #[async_trait::async_trait]
/// impl module_escalation::ports::tracker::Tracker for MyTracker {
///     async fn file(
///         &self,
///         _dest: &module_escalation::ports::tracker::Destination,
///         _cred: &module_escalation::ports::tracker::Credential,
///         _draft: &module_escalation::ports::tracker::TicketDraft,
///     ) -> Result<
///         module_escalation::ports::tracker::Filed,
///         module_escalation::ports::tracker::TrackerError,
///     > {
///         unimplemented!("your tracker adapter")
///     }
///     async fn status(
///         &self,
///         _dest: &module_escalation::ports::tracker::Destination,
///         _cred: &module_escalation::ports::tracker::Credential,
///         _external_id: &str,
///     ) -> Result<
///         module_escalation::ports::tracker::TicketStatus,
///         module_escalation::ports::tracker::TrackerError,
///     > {
///         unimplemented!("your tracker adapter")
///     }
/// }
///
/// let module = Escalation::new(Arc::new(MyModel), Arc::new(MyTracker));
/// assert_eq!(module.name(), "escalation");
/// ```
#[derive(Clone)]
pub struct Escalation {
    model: Arc<dyn TextModel>,
    tracker: Arc<dyn Tracker>,
}

use crate::ports::text_model::TextModel;
use crate::ports::tracker::Tracker;

impl Escalation {
    /// The module's name as the harness mounts it (`/v1/escalation`).
    pub const NAME: &'static str = "escalation";

    /// Builds the module over the two ports the pipeline cannot run
    /// without.
    ///
    /// **These arrive through the module's own constructor, not through
    /// [`ModuleContext::ports`](cratefield_core::Ports)**, because
    /// `cratefield-core` 0.4.3's [`Port`] enum has no `TextModel` or
    /// `Tracker` variant to require or resolve — they are mirrored in
    /// [`crate::ports`] for exactly that reason. When core publishes the
    /// two ports, both move: the mirror is deleted, the fields become
    /// `ctx.ports` resolutions, and this constructor loses its arguments.
    #[must_use]
    pub fn new(model: Arc<dyn TextModel>, tracker: Arc<dyn Tracker>) -> Self {
        Self { model, tracker }
    }

    /// The conversation → first-outbox-row handoff.
    ///
    /// It stamps rows with core's [`SystemClock`] and [`UlidIdGen`]
    /// defaults; tests that need a frozen clock call [`Intake::new`]
    /// directly with a fake — the constructor is public for exactly that.
    #[must_use]
    pub fn intake(&self) -> Intake {
        Intake::new(OUTBOX_TABLE, Arc::new(SystemClock), Arc::new(UlidIdGen))
    }
}

impl Module for Escalation {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn version(&self) -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    /// The one port that exists to require. The pipeline's other inputs
    /// cannot be declared here yet: `TextModel` and `Tracker` have no
    /// [`Port`] variant in published core (see [`Escalation::new`]), so
    /// they arrive through the module constructor instead — a real gap in
    /// the harness's port list, tracked by the issue, not a choice to
    /// hide a dependency.
    fn requires(&self) -> &'static [Port] {
        &[Port::Db]
    }

    /// `Mailer` delivers the notify stage's message; `Clock` and `IdGen`
    /// stamp the outbox rows (core's `SystemClock`/`UlidIdGen` otherwise);
    /// `Defer` lets a finished stage run the one it just enqueued without
    /// waiting for the next cron tick.
    fn optional(&self) -> &'static [Port] {
        &[Port::Mailer, Port::Clock, Port::IdGen, Port::Defer]
    }

    /// Every table the migration creates. Duplicates across modules are a
    /// build error, so this list is the ownership claim.
    fn tables(&self) -> &'static [&'static str] {
        &[
            "sg_tickets",
            "sg_ticket_events",
            "sg_destinations",
            "sg_escalation_outbox",
            "sg_escalation_inbox",
        ]
    }

    /// What the module holds about a person, per table.
    ///
    /// `sg_tickets` and `sg_ticket_events` hold the conversation itself —
    /// the transcript and the audit trail that quotes it — so both are
    /// `Erase`: a person's escalation should be able to disappear with
    /// them. (Erasure that only removed the ticket would leave a trail of
    /// events quoting them, so the events go too; they are matched
    /// through the same ticket id, which is their `ticket_id` column's
    /// value.)
    ///
    /// `sg_escalation_outbox` carries only ids (a stage payload is two
    /// ids, never the transcript), and its `subject` column *is* the
    /// ticket id, so a subject erasure reaches it directly.
    ///
    /// The other two declare honestly rather than stay silent:
    /// `sg_destinations` is tenant configuration that names nobody; the
    /// inbox's identifying value is inside the composite
    /// `<ticket_id>:<stage>` key, which no `column = ?` predicate can
    /// reach — core's `unreachable` shape exists for exactly that.
    fn personal_data(&self) -> &'static [PersonalDataSet] {
        const SETS: &[PersonalDataSet] = &[
            PersonalDataSet {
                table: "sg_tickets",
                subject: "id",
                kind: DataKind::Content,
                disposition: Disposition::Erase,
                description: "Your support conversation: the transcript you sent, the draft \
                              and judgement made from it, and where it was filed or why it \
                              was not.",
                redacted: &[],
                subject_via: None,
            },
            PersonalDataSet {
                table: "sg_ticket_events",
                subject: "ticket_id",
                kind: DataKind::Content,
                disposition: Disposition::Erase,
                description: "The audit trail of your escalation: what each step did, \
                              including the model's draft and judgement in full and the \
                              reason for every retry or refusal.",
                redacted: &[],
                subject_via: None,
            },
            PersonalDataSet {
                table: "sg_escalation_outbox",
                subject: "subject",
                kind: DataKind::Identifier,
                disposition: Disposition::Erase,
                description: "The queue of pending work for your escalation, holding only \
                              the ticket's id and which step is next — never the \
                              conversation itself.",
                redacted: &[],
                subject_via: None,
            },
            PersonalDataSet::none(
                "sg_destinations",
                "Per-tenant tracker configuration: which tracker a tenant files into and a \
                 *reference* to the credential (a config key name, never the secret), keyed \
                 to the tenant, not to any person.",
            ),
            // Not `none`: the key embeds the ticket id, which is the
            // subject value — but as `<ticket_id>:<stage>`, so a plain
            // column equality cannot match it and erasure cannot reach it.
            // Same shape (and same honest answer) as the waitlist module's
            // send-cooldown table. The rows are transient idempotency
            // markers — claim time and stage, nothing else — released on
            // every retry and irrelevant once a stage has ended.
            PersonalDataSet::unreachable(
                "sg_escalation_inbox",
                DataKind::Identifier,
                "A marker that a step of your escalation has been started, so it cannot run \
                 twice; the marker names the ticket's id inside a compound key.",
                "The identifying value is inside the compound `<ticket id>:<stage>` key, so \
                 no equality predicate can match it. Nothing else about you is in the row, \
                 and it stops mattering once the step has run.",
            ),
        ];
        SETS
    }

    fn migrations(&self) -> Migrations {
        const MIGRATIONS: [SqlMigration; 1] = [MIGRATION_ESCALATION];
        // The array is the apply order; this refuses a gap, a duplicate
        // or an entry out of order at build time.
        const _: () = cratefield_core::assert_migration_set(&MIGRATIONS);
        Migrations {
            sqlite: &MIGRATIONS,
            postgres: &[],
        }
    }

    /// Validates the retry knobs the scheduled drain reads. All three are
    /// optional with the [`RetryPolicy`] defaults; an explicitly-set value
    /// must parse and be sane, and every problem is reported together.
    fn validate_config(&self, cfg: &dyn Config) -> Result<(), ConfigError> {
        let module = ModuleConfig::new(Self::NAME, cfg);
        let mut errors = ConfigError::default();

        for (suffix, description) in [
            ("RETRY_BASE_SECS", "a positive integer of seconds"),
            ("RETRY_CAP_SECS", "a positive integer of seconds"),
            ("RETRY_MAX_ATTEMPTS", "a positive integer"),
        ] {
            if let Some(parsed) = cfg
                .get(&module.key(suffix))
                .map(|raw| raw.trim().parse::<u32>())
            {
                let invalid = match parsed {
                    Ok(value) => value < 1,
                    Err(_) => true,
                };
                if invalid {
                    errors.push(format!(
                        "escalation: {} must be {description}",
                        module.key(suffix)
                    ));
                }
            }
        }

        errors.into_result()
    }

    /// **Empty, on purpose.** Issue #4's escalation is machinery with no
    /// public surface: the conversation arrives through the intake
    /// handoff, and every customer-facing read of a ticket ("your ticket
    /// was filed, here is the link") belongs to `module-support`
    /// (issue #2) — the module that owns tenancy and API-key auth, which
    /// does not exist yet. A ticket-read route here would be mounted
    /// unauthenticated and un-tenant-scoped, and the first thing it leaks
    /// is other tenants' support transcripts, verbatim. Returning no
    /// routes is the honest v0; the routes arrive with the auth module
    /// they need.
    fn router(&self, _ctx: ModuleContext) -> axum::Router {
        axum::Router::new()
    }

    /// The drain: build a [`Pipeline`] from whatever ports the runtime
    /// resolved and sweep the outbox until a sweep comes back empty or
    /// [`Pipeline::MAX_SWEEPS`] is spent — bounded, so a row that keeps
    /// re-enqueueing work cannot spin one cron tick forever; the rest
    /// waits for the next tick, which is what cron is for.
    ///
    /// A stage that just finished re-drains immediately through the
    /// `Defer` port (see [`Pipeline`]), so this sweep is the backstop that
    /// makes the work durable even where there is no defer port and
    /// nothing else is running.
    fn scheduled<'a>(
        &'a self,
        ctx: &'a ModuleContext,
        _cron: &'a str,
    ) -> BoxFuture<'a, Result<(), AnyError>> {
        Box::pin(async move {
            let Some(db) = ctx.ports.db.clone() else {
                // No database, no outbox, nothing to drain. The module
                // declares `Port::Db` as required, so a composed venture
                // always resolves it; a test harness may not.
                return Ok(());
            };
            let clock = ctx
                .ports
                .clock
                .clone()
                .unwrap_or_else(|| Arc::new(SystemClock));
            let idgen = ctx
                .ports
                .id_gen
                .clone()
                .unwrap_or_else(|| Arc::new(UlidIdGen));
            let cfg = ModuleConfig::new(Self::NAME, &*ctx.ports.config);
            let policy = RetryPolicy::new()
                .base(Duration::from_secs(u64::from(
                    cfg.get_u32("RETRY_BASE_SECS", RetryPolicy::DEFAULT_BASE_SECS),
                )))
                .cap(Duration::from_secs(u64::from(
                    cfg.get_u32("RETRY_CAP_SECS", RetryPolicy::DEFAULT_CAP_SECS),
                )))
                .max_attempts(cfg.get_u32("RETRY_MAX_ATTEMPTS", RetryPolicy::DEFAULT_MAX_ATTEMPTS));

            let pipeline = Pipeline::new(
                db,
                self.model.clone(),
                self.tracker.clone(),
                ctx.ports.mailer.clone(),
                ctx.ports.config.clone(),
                clock,
                idgen,
                ctx.ports.defer.clone(),
            )
            .with_retry_policy(policy);

            for _ in 0..Pipeline::MAX_SWEEPS {
                let processed = pipeline
                    .drain(Pipeline::SWEEP_LIMIT)
                    .await
                    .map_err(|err| Box::new(err) as AnyError)?;
                if processed == 0 {
                    break;
                }
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ports::text_model::ModelTier;
    use crate::ports::tracker::Filed;
    use crate::testing::{FakeTextModel, FakeTracker};
    use cratefield_core::{EmptyConfig, MapConfig};

    fn module() -> Escalation {
        Escalation::new(
            Arc::new(FakeTextModel::json(
                ModelTier::Fast,
                serde_json::json!({
                    "title": "t", "repro_steps": [], "expected": "e", "actual": "a",
                    "severity": "info",
                }),
            )),
            Arc::new(FakeTracker::accepting(Filed {
                external_id: "ext-1".to_owned(),
                url: "https://tracker.test/ext-1".to_owned(),
            })),
        )
    }

    #[test]
    fn the_module_declares_what_it_is() {
        let module = module();
        assert_eq!(module.name(), "escalation");
        assert_eq!(module.version(), env!("CARGO_PKG_VERSION"));
        assert_eq!(module.requires(), &[Port::Db]);
        assert!(module.optional().contains(&Port::Mailer));
        let tables = module.tables();
        assert_eq!(tables.len(), 5);
        for table in tables {
            assert!(table.starts_with("sg_"), "{table} is not a sg_ table");
        }
    }

    /// Every declared table is owned, and every owned table is declared —
    /// the check the conformance kit runs (a module that owns tables and
    /// declares nothing is refused there).
    #[test]
    fn every_table_has_a_personal_data_answer() {
        let module = module();
        let owned = module.tables();
        let declared: Vec<&str> = module.personal_data().iter().map(|set| set.table).collect();
        assert_eq!(owned.len(), declared.len(), "every owned table declares");
        for table in owned {
            assert!(
                declared.contains(table),
                "{table} is owned but not declared"
            );
        }
    }

    #[test]
    fn the_migration_set_is_exactly_the_one_migration() {
        let migrations = module().migrations();
        assert_eq!(migrations.sqlite.len(), 1);
        assert_eq!(migrations.sqlite[0].id, "0001");
        assert!(migrations.postgres.is_empty());
        assert!(migrations.sqlite[0].sql.contains("CREATE TABLE"));
    }

    #[test]
    fn valid_retry_config_passes_validation() {
        let module = module();
        let ok = MapConfig::from_pairs([
            ("ESCALATION_RETRY_BASE_SECS", "5"),
            ("ESCALATION_RETRY_CAP_SECS", "60"),
            ("ESCALATION_RETRY_MAX_ATTEMPTS", "2"),
        ]);
        assert!(module.validate_config(&ok).is_ok());
        // Everything unset is the defaults: valid.
        assert!(module.validate_config(&EmptyConfig).is_ok());
    }

    #[test]
    fn invalid_retry_config_reports_every_problem() {
        let module = module();
        let bad = MapConfig::from_pairs([
            ("ESCALATION_RETRY_BASE_SECS", "zero"),
            ("ESCALATION_RETRY_CAP_SECS", "0"),
            ("ESCALATION_RETRY_MAX_ATTEMPTS", "-1"),
        ]);
        let err = module
            .validate_config(&bad)
            .expect_err("all three knobs are bad");
        assert_eq!(err.problems.len(), 3, "{err}");
        assert!(err.to_string().contains("ESCALATION_RETRY_BASE_SECS"));
    }
}
