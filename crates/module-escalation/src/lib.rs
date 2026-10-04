//! `module-escalation`: a conversation becomes a ticket — drafted by one
//! model, checked by an independent one, filed by a router, followed up
//! until it closes (issue #4, `/workspace/README.md`).
//!
//! [`model`]/[`store`]/[`intake`] are the vocabulary and data access;
//! [`Pipeline`] is the durable stage runner (one port call and one
//! all-or-nothing commit per stage, an inbox claim per `ticket:stage`, a
//! bounded [`RetryPolicy`]); [`Escalation`] wires it into the [`Module`]
//! contract. The HTTP surface is two route groups under
//! `/v1/escalation`: the `destinations` module (issue #23), where a tenant
//! — or an operator acting for one — names the tracker its escalations
//! file into and the credential to file with (validated once and stored
//! envelope-encrypted through `cratefield-secrets` under a KMS resolved
//! from config, `secrets::kms_from_config`); and the `tickets` module
//! (issue #24, part 2), where a tenant lists, reads and closes the
//! built-in tickets the file stage kept in the module itself.

#![forbid(unsafe_code)]

mod destinations;
mod pipeline;
mod secrets;
mod tenants;
mod tickets;

pub mod connectors;
pub mod error;
pub mod intake;
pub mod model;
pub mod store;

pub use error::Error;
pub use intake::{Handoff, Intake};
pub use pipeline::{Pipeline, RetryPolicy};
pub use secrets::local_kms_refusal;
pub use tenants::TenantDirectory;

/// Test doubles the published fakes do not cover: a seeded in-memory
/// [`cratefield_core::Config`] and a clock a test can move forward. The
/// `TextModel`/`Tracker` doubles themselves are
/// `cratefield_testing::{FakeTextModel, FakeTracker}` — the port and its
/// fake publish together, so nothing is mirrored here. Behind the
/// `testing` feature so the Worker build never carries test doubles;
/// `tests/` enable it through the crate's self dev-dependency.
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

/// Duplicate detection (issue #27): the `match_count` column and the
/// `sg_ticket_links` table the judge stage's duplicate branch writes.
pub const MIGRATION_DUPLICATES: SqlMigration = SqlMigration::new(
    "0002",
    "duplicates",
    include_str!("../migrations/sqlite/0002_duplicates.sql"),
);

/// Ticket kind and per-kind routing (issue #24): the `sg_tickets.kind`
/// column and the `sg_routes` table that sends each kind to its own
/// tracker destination.
pub const MIGRATION_ROUTING: SqlMigration = SqlMigration::new(
    "0007",
    "routing",
    include_str!("../migrations/sqlite/0007_routing.sql"),
);

/// One `cratefield-secrets` migration under this module's own id,
/// preserving its `transactional` flag. The bytes are the source crate's,
/// never a copy, so the embedded schema cannot drift from the one the
/// store is written against (the `control-plane-dashboard` idiom).
const fn sub_migration(
    set: &'static [SqlMigration],
    index: usize,
    id: &'static str,
    name: &'static str,
) -> SqlMigration {
    let source = &set[index];
    if source.transactional {
        SqlMigration::new(id, name, source.sql)
    } else {
        SqlMigration::new(id, name, source.sql).non_transactional()
    }
}

use std::sync::Arc;
use std::time::Duration;

use cratefield_core::{
    AnyError, BoxFuture, Config, ConfigError, DataKind, Defer, Disposition, Migrations, Module,
    ModuleConfig, ModuleContext, PersonalDataSet, Port, SqlMigration, SystemClock, UlidIdGen,
};

use crate::intake::OUTBOX_TABLE;
use cratefield_module_webhooks::Webhooks;

/// The SupportGenius escalation module (issue #4).
///
/// Register it with the harness and let [`Module::scheduled`] drain the
/// outbox on cron. The conversation enters through [`Escalation::intake`],
/// whose statements the caller commits with its own write; everything
/// after that is the [`Pipeline`], driven by cron. The two ports the
/// pipeline cannot run without — `TextModel` and `Tracker` — are not
/// passed here at all: the module requires them, and the harness hands
/// them to every stage through the runtime's
/// [`Ports`](cratefield_core::Ports):
///
/// ```no_run
/// use cratefield_core::Module as _;
/// use module_escalation::Escalation;
///
/// let module = Escalation::new();
/// assert_eq!(module.name(), "escalation");
/// // requires() names what the runtime must provide before build():
/// assert_eq!(
///     module.requires(),
///     &[
///         cratefield_core::Port::Db,
///         cratefield_core::Port::TextModel,
///         cratefield_core::Port::Tracker,
///     ]
/// );
/// ```
///
/// The destination routes additionally need a [`TenantDirectory`]
/// ([`Escalation::with_tenant_directory`]) to refuse suspended tenants;
/// without one they refuse every tenant (fail closed).
#[derive(Clone, Default)]
pub struct Escalation {
    /// Who may use the destination routes; `None` refuses everyone.
    tenants: Option<Arc<dyn TenantDirectory>>,
}

impl std::fmt::Debug for Escalation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Escalation")
            .field("tenant_directory", &self.tenants.is_some())
            .finish()
    }
}

impl Escalation {
    /// The module's name as the harness mounts it (`/v1/escalation`).
    pub const NAME: &'static str = "escalation";

    /// Builds the module. It carries no state: the pipeline's ports are
    /// resolved per drain from the runtime's
    /// [`Ports`](cratefield_core::Ports), so one instance can serve any
    /// number of harnesses.
    #[must_use]
    pub fn new() -> Self {
        Self { tenants: None }
    }

    /// The directory the destination routes check tenant status against
    /// (see [`TenantDirectory`]): a suspended or closed tenant is refused
    /// there the way the rest of the API refuses it.
    #[must_use]
    pub fn with_tenant_directory(mut self, tenants: Arc<dyn TenantDirectory>) -> Self {
        self.tenants = Some(tenants);
        self
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

    /// Everything the pipeline cannot run without: the database the
    /// outbox lives in, the model that drafts and judges, and the tracker
    /// the draft is filed into. Declaring the last two means
    /// [`cratefield_core::HarnessBuilder::build`] refuses a composition
    /// that forgets them with "module `escalation` requires port … which
    /// the runtime does not provide" at startup, instead of the first
    /// cron tick finding a `None` and silently draining nothing.
    fn requires(&self) -> &'static [Port] {
        &[Port::Db, Port::TextModel, Port::Tracker]
    }

    /// `Mailer` delivers the notify stage's message; `Clock` and `IdGen`
    /// stamp the outbox rows (core's `SystemClock`/`UlidIdGen` otherwise);
    /// `Defer` lets a finished stage run the one it just enqueued without
    /// waiting for the next cron tick; `Signer` verifies the tenant API
    /// key the destination routes are guarded by, and is optional because
    /// a composition that never exposes them (a pure cron drainer) needs
    /// no signer.
    fn optional(&self) -> &'static [Port] {
        &[
            Port::Mailer,
            Port::Clock,
            Port::IdGen,
            Port::Defer,
            Port::Signer,
        ]
    }

    /// Every table the migration creates. Duplicates across modules are a
    /// build error, so this list is the ownership claim — the module's
    /// `sg_*` tables plus the embedded `cratefield-secrets` schema (the
    /// module applies it to store tenant credentials; see
    /// [`Escalation::migrations`]).
    fn tables(&self) -> &'static [&'static str] {
        &[
            "sg_tickets",
            "sg_ticket_events",
            "sg_ticket_links",
            "sg_destinations",
            "sg_routes",
            "sg_escalation_outbox",
            "sg_escalation_inbox",
            "harness_secret_keys",
            "harness_secrets",
            "harness_secret_audit",
        ]
    }

    /// What the module holds about a person, per table.
    ///
    /// `sg_tickets` and `sg_ticket_events` hold the conversation itself —
    /// the transcript and the audit trail that quotes it — so both are
    /// `Erase`, matched through the ticket id. `sg_escalation_outbox`
    /// carries only ids and its `subject` column *is* the ticket id, so a
    /// subject erasure reaches it directly. `sg_destinations` is tenant
    /// configuration that names nobody, and the inbox's identifying value
    /// sits inside a composite `<ticket_id>:<stage>` key no `column = ?`
    /// predicate can reach — core's `unreachable` shape exists for that.
    /// The embedded secrets tables declare `none` for the same reason the
    /// harness's own dashboard does: they are keyed to a store and a name,
    /// never to a person.
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
            // Links a later escalation to the ticket it duplicates. The
            // identifying value is the *source* ticket's id (the one
            // being erased), so erasing that escalation removes its link;
            // the existing `ticket_id` is a tracker reference, not this
            // person's data.
            PersonalDataSet {
                table: "sg_ticket_links",
                subject: "source_ticket_id",
                kind: DataKind::Identifier,
                disposition: Disposition::Erase,
                description: "A link from your escalation to an existing filed ticket it \
                              duplicates: the two ids and the conversation id.",
                redacted: &[],
                subject_via: None,
            },
            PersonalDataSet::none(
                "sg_destinations",
                "Per-tenant tracker configuration: which tracker a tenant files into and a \
                 *reference* to the credential (a `secret:` name in the encrypted store, never \
                 the secret itself), keyed to the tenant, not to any person.",
            ),
            // Same answer as `sg_destinations`: per-tenant routing config,
            // keyed to a tenant and a ticket kind. The `kind` column names
            // a category of request, never a person.
            PersonalDataSet::none(
                "sg_routes",
                "Per-tenant, per-kind routing configuration: where each kind of escalation \
                 (defect, support case, lead) is filed and a *reference* (never the value) to \
                 the credential, keyed to the tenant and the kind, not to any person.",
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
            // The embedded secrets schema (issue #23), declared rather
            // than left silent — the same call the harness's own dashboard
            // makes for these tables.
            PersonalDataSet::none(
                "harness_secrets",
                "A tenant's tracker credential, envelope-encrypted (name, version, \
                 ciphertext, nonce, timestamp) — never in the clear, keyed to a store and a \
                 name, not to a person.",
            ),
            PersonalDataSet::none(
                "harness_secret_keys",
                "The wrapped data keys that protect each store's secrets, with their ids, \
                 states and the KMS reference that wrapped them. No key material in the \
                 clear; nothing names a person.",
            ),
            PersonalDataSet::none(
                "harness_secret_audit",
                "The append-only audit chain of accesses to a store's secrets — action, \
                 store, name, version, actor (a tenant id or `admin`). It holds no secret \
                 value by construction.",
            ),
        ];
        SETS
    }

    /// The module's own `sg_*` migrations, then the `cratefield-secrets`
    /// schema embedded under this module's ids (issue #23), taken from the
    /// crate's own `SQLITE_MIGRATIONS`/`POSTGRES_MIGRATIONS` and re-id-ed
    /// (the `control-plane-dashboard` idiom) so the embedded copy cannot
    /// drift. The module files are portable SQL, so the postgres set carries
    /// the same bytes for ids `0001`, `0002` and `0007`, but the secrets
    /// schema is not (BLOB vs
    /// BYTEA, per-engine triggers), so each set embeds its own dialect's.
    /// The postgres set must be complete: `select_set` applies it wholesale.
    fn migrations(&self) -> Migrations {
        const SQLITE: [SqlMigration; 7] = [
            MIGRATION_ESCALATION,
            MIGRATION_DUPLICATES,
            sub_migration(
                &cratefield_secrets::SQLITE_MIGRATIONS,
                0,
                "0003",
                "secrets-init",
            ),
            sub_migration(
                &cratefield_secrets::SQLITE_MIGRATIONS,
                1,
                "0004",
                "secrets-audit",
            ),
            sub_migration(
                &cratefield_secrets::SQLITE_MIGRATIONS,
                2,
                "0005",
                "secrets-audit-store",
            ),
            sub_migration(
                &cratefield_secrets::SQLITE_MIGRATIONS,
                3,
                "0006",
                "secrets-store-attribution",
            ),
            MIGRATION_ROUTING,
        ];
        // The array is the apply order; this refuses a gap, a duplicate
        // or an entry out of order at build time.
        const _: () = cratefield_core::assert_migration_set(&SQLITE);
        const POSTGRES: [SqlMigration; 7] = [
            MIGRATION_ESCALATION,
            MIGRATION_DUPLICATES,
            sub_migration(
                &cratefield_secrets::POSTGRES_MIGRATIONS,
                0,
                "0003",
                "secrets-init",
            ),
            sub_migration(
                &cratefield_secrets::POSTGRES_MIGRATIONS,
                1,
                "0004",
                "secrets-audit",
            ),
            sub_migration(
                &cratefield_secrets::POSTGRES_MIGRATIONS,
                2,
                "0005",
                "secrets-audit-store",
            ),
            sub_migration(
                &cratefield_secrets::POSTGRES_MIGRATIONS,
                3,
                "0006",
                "secrets-store-attribution",
            ),
            MIGRATION_ROUTING,
        ];
        const _: () = cratefield_core::assert_migration_set(&POSTGRES);
        Migrations {
            sqlite: &SQLITE,
            postgres: &POSTGRES,
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

        // The development file KMS outside an explicit ENV=development:
        // the routes already fail closed (no KMS, `503`); this makes the
        // misconfiguration loud wherever config is validated, and the
        // native binary refuses to boot on it.
        if let Some(refusal) = secrets::local_kms_refusal(cfg) {
            errors.push(format!("escalation: {refusal}"));
        }

        errors.into_result()
    }

    /// The module's HTTP surface: the destination routes (issue #23) and
    /// the built-in ticketing routes (issue #24, part 2), mounted under
    /// the same `/v1/escalation` prefix. Auth runs before the body is
    /// parsed: the tenant mounts check the `sg_…` API key, the destination
    /// admin mount the harness admin token, and only then does a handler
    /// read its payload — a `Json<T>` extractor would answer a malformed
    /// body to an unauthenticated caller. The KMS the credential store is
    /// built over is resolved from config once, here. Both surfaces share
    /// the tenant directory, so a suspended tenant is refused identically
    /// on either.
    fn router(&self, ctx: ModuleContext) -> axum::Router {
        let ctx = Arc::new(ctx);
        let kms = secrets::kms_from_config(&*ctx.config);
        destinations::router(Arc::clone(&ctx), kms, self.tenants.clone())
            .merge(tickets::router(ctx, self.tenants.clone()))
    }

    /// The destination routes, declared (ADR 0010). The composition also
    /// injects this surface into `module-support`'s `OpenAPI` document
    /// (issue #34), so one document describes both `/v1/support/*` and
    /// `/v1/escalation/*`.
    fn surface(&self) -> cratefield_core::Surface {
        destinations::surface()
    }

    /// The drain: build a [`Pipeline`] from whatever ports the runtime
    /// resolved and sweep the outbox until a sweep comes back empty or
    /// [`Pipeline::MAX_SWEEPS`] is spent — bounded, so a row that keeps
    /// re-enqueueing work cannot spin one cron tick forever; the rest
    /// waits for the next tick, which is what cron is for.
    ///
    /// **Every** cron drains, deliberately: the venture schedules this on a
    /// five-minute tick (the recovery path for whatever a handoff's
    /// best-effort [`kick`](Escalation::kick) left staged) and a daily one
    /// (the backstop), but the drain is idempotent — each stage is claimed
    /// through an [`Inbox`](cratefield_core::Inbox) — so draining on a
    /// trigger the module did not specifically anticipate is harmless,
    /// while a module that drained on only one of its crons would silently
    /// stall the moment the schedule changed. `cron` is therefore unused.
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
            let Some(pipeline) = Self::pipeline(ctx, ctx.ports.defer.clone()) else {
                // Whatever is missing, there is nothing to drain: no
                // database is no outbox, and the two ports `build()`
                // refuses to compose without are ones a hand-rolled
                // context skipped. Draining nothing beats panicking a cron
                // tick.
                return Ok(());
            };
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

impl Escalation {
    /// Runs the escalation pipeline now, on the given [`Defer`], instead of
    /// waiting for the next scheduled drain.
    ///
    /// This is the second half of a handoff: `module-support` commits the
    /// statements [`Escalation::intake`] built in the same batch as the
    /// turn (see [`Intake::enqueue`]), then calls this so the staged
    /// outbox row is driven draft → judge → file → notify immediately.
    /// A failure here is only a delay — the row is already durable and
    /// [`Module::scheduled`] is the backstop — so nothing is returned.
    ///
    /// `defer` is the caller's own port (the request's, when support calls
    /// it), so the work rides the runtime's background execution rather
    /// than the caller's response.
    pub fn kick(ctx: &ModuleContext, defer: Arc<dyn Defer>) {
        // The pipeline's finished stages re-drain through `defer` (see
        // [`Pipeline::defer_next`]); `wake` is the same port for this first
        // kick, so the staged run and everything it enqueues behind it all
        // land on the caller's background execution.
        let wake = Arc::clone(&defer);
        let Some(pipeline) = Self::pipeline(ctx, Some(defer)) else {
            // Missing ports mean the row cannot be driven now; the
            // scheduled drain will find it if the composition ever gains
            // them. Nothing to report and nothing to do.
            return;
        };
        wake.wait_until(Box::pin(async move {
            let _ = pipeline.drain(1).await;
        }));
    }

    /// Builds the durable stage runner from whatever ports a context
    /// resolved, with `defer` overriding the context's own for the
    /// self-re-drain a finished stage hands back (see
    /// [`Pipeline::defer_next`]). `None` means the context is missing a
    /// port the pipeline cannot run without — the caller decides whether
    /// that is an empty scheduled tick or a kick with nothing to drive.
    ///
    /// Shared by [`Module::scheduled`] and [`Escalation::kick`] so the two
    /// entry points cannot drift: a change to how the pipeline is
    /// configured lands in both.
    fn pipeline(ctx: &ModuleContext, defer: Option<Arc<dyn Defer>>) -> Option<Pipeline> {
        let db = ctx.ports.db.clone()?;
        let model = ctx.ports.text_model.clone()?;
        let tracker = ctx.ports.tracker.clone()?;
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

        Some(
            Pipeline::new(
                db,
                model,
                tracker,
                ctx.ports.mailer.clone(),
                ctx.ports.config.clone(),
                clock,
                idgen,
                defer,
            )
            .with_retry_policy(policy)
            // Every stage that changes a ticket's life — filed,
            // dead-lettered, parked for a human — also publishes the
            // matching `escalation.*` event, in the stage's own atomic
            // batch (see [`Pipeline`]). The publish is fail-safe: a
            // venture that mounts this module without `Webhooks` has no
            // webhook tables, and the fan-out is skipped rather than
            // allowed to break every filing (see [`Pipeline::webhook_stmts`]).
            // Shared by `scheduled` and `kick`, so both publish alike.
            .with_webhooks(Webhooks::new()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cratefield_core::{EmptyConfig, MapConfig};

    fn module() -> Escalation {
        Escalation::new()
    }

    #[test]
    fn the_module_declares_what_it_is() {
        let module = module();
        assert_eq!(module.name(), "escalation");
        assert_eq!(module.version(), env!("CARGO_PKG_VERSION"));
        assert_eq!(
            module.requires(),
            &[Port::Db, Port::TextModel, Port::Tracker]
        );
        assert!(module.optional().contains(&Port::Mailer));
        // `Signer` is optional: the destination routes need it, a pure
        // cron drainer does not.
        assert!(module.optional().contains(&Port::Signer));
        let tables = module.tables();
        assert_eq!(tables.len(), 10);
        let sg: Vec<&str> = tables
            .iter()
            .copied()
            .filter(|table| table.starts_with("sg_"))
            .collect();
        assert_eq!(sg.len(), 7, "the module owns seven sg_ tables");
        let harness: Vec<&str> = tables
            .iter()
            .copied()
            .filter(|table| table.starts_with("harness_secret"))
            .collect();
        assert_eq!(harness.len(), 3, "the embedded secrets schema is declared");
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
    fn the_migration_set_is_the_module_files_then_the_embedded_secrets_schema() {
        let migrations = module().migrations();
        assert_eq!(migrations.sqlite.len(), 7);
        assert_eq!(
            migrations
                .sqlite
                .iter()
                .map(|migration| migration.id)
                .collect::<Vec<_>>(),
            ["0001", "0002", "0003", "0004", "0005", "0006", "0007"]
        );
        assert!(migrations.sqlite[0].sql.contains("CREATE TABLE"));
        assert!(migrations.sqlite[1].sql.contains("sg_ticket_links"));
        assert!(migrations.sqlite[6].sql.contains("sg_routes"));
        // The embedded bytes are the secrets crate's, re-id-ed: the first
        // secret table appears in the module's third migration.
        assert_eq!(
            migrations.sqlite[2].sql,
            cratefield_secrets::SQLITE_MIGRATIONS[0].sql
        );
        // The postgres set is complete (select_set applies it wholesale),
        // carrying the same portable module files for 0001-0002 and 0007
        // and the secrets crate's own postgres bytes thereafter.
        assert_eq!(migrations.postgres.len(), 7);
        assert_eq!(migrations.postgres[0].sql, migrations.sqlite[0].sql);
        assert_eq!(migrations.postgres[1].sql, migrations.sqlite[1].sql);
        assert_eq!(migrations.postgres[6].sql, migrations.sqlite[6].sql);
        assert_eq!(
            migrations.postgres[2].sql,
            cratefield_secrets::POSTGRES_MIGRATIONS[0].sql
        );
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
