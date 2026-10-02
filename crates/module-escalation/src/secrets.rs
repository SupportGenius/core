//! The escalation module's slice of `cratefield-secrets`: the KMS is
//! resolved **from configuration** (no new port), and the secret names the
//! destination routes store under are declared here.
//!
//! The deployment names a KMS in config, exactly as the harness's own
//! control-plane dashboard does: `HARNESS_KEK_CURRENT` set → the
//! production Worker-secret provider (checked first, so a Worker never
//! falls back to a file); else `ESCALATION_KMS_KEY_FILE` → the development
//! `LocalFileKms`, used **only** when `ENV` is explicitly `development`
//! (see [`local_kms_refusal`]: unset, `staging`, `production` or anything
//! unrecognised fails closed, and the native binary refuses to boot);
//! else `None`, and a route that needs it answers `503` rather than
//! storing a plaintext secret. Config here is the **raw** harness config
//! (unprefixed), read the same way the harness runtime reads its secrets.
//!
//! Every store this module opens is [`audited_secrets`]: each access is
//! appended to the `harness_secret_audit` chain in the same database (and
//! still logged through `tracing`), and an access that cannot be recorded
//! is refused rather than served.

use std::sync::Arc;

use cratefield_core::{Config, Database, VentureEnv};
use cratefield_kms::{Kms, LocalFileKms, WorkerSecretKms};
use cratefield_secrets::{Audit, AuditEvent, Secrets, SecretsError, TracingAudit, chain_sink};

/// The config key naming the Worker-secret KEK version new wraps use
/// (`WorkerSecretKms` reads the whole ring under it).
const WORKER_KEK_CURRENT: &str = "HARNESS_KEK_CURRENT";

/// The config key naming a development master-key file (`LocalFileKms`).
const LOCAL_KEY_FILE: &str = "ESCALATION_KMS_KEY_FILE";

/// The harness environment key (`"development"`, `"staging"`,
/// `"production"`, parsed by [`VentureEnv::parse`]). [`LocalFileKms`] is
/// allowed only when it is explicitly `development`.
const ENV_KEY: &str = "ENV";

/// The marker prefix that turns a stored reference into a
/// `cratefield-secrets` name: `credential_ref` (and, for a webhook, the URL
/// inside the stored `destination`) starts with this when the value lives
/// in the encrypted store rather than in config.
pub(crate) const SECRET_REF_PREFIX: &str = "secret:";

/// The secret name a tenant's tracker credential is stored under, per
/// tenant store.
pub(crate) const CREDENTIAL_SECRET: &str = "escalation.tracker.credential";

/// The secret name a webhook destination's real URL is stored under (a
/// webhook URL is credential material; see `cratefield_core::Destination`).
pub(crate) const WEBHOOK_SECRET: &str = "escalation.tracker.destination";

/// The `sg_destinations.credential_ref` value for a credential held in the
/// encrypted store.
pub(crate) const CREDENTIAL_REF: &str = concat!("secret:", "escalation.tracker.credential");

/// What a webhook destination's `url` is replaced with in the stored
/// `destination` column: the real URL is in the encrypted store and the
/// column keeps only this marker.
pub(crate) const WEBHOOK_URL_MARKER: &str = concat!("secret:", "escalation.tracker.destination");

/// Resolves the KMS a deployment configured, or `None` when none is
/// configured (credential storage unavailable — routes answer `503`).
/// A configured-but-broken provider (a malformed KEK ring, an unreadable
/// key file) logs the reason and returns `None` too: the honest answer is
/// "no usable KMS", and panicking a Worker isolate at boot costs more than
/// the 503.
pub(crate) fn kms_from_config(cfg: &dyn Config) -> Option<Arc<dyn Kms>> {
    if cfg.get(WORKER_KEK_CURRENT).is_some() {
        return match WorkerSecretKms::from_lookup(|name| cfg.get(name)) {
            Ok(kms) => Some(Arc::new(kms)),
            Err(err) => {
                tracing::error!(error = %err, "the Worker-secret KEK ring could not be read");
                None
            }
        };
    }
    let path = cfg.get(LOCAL_KEY_FILE)?;
    if let Some(refusal) = local_kms_refusal(cfg) {
        tracing::error!("{refusal}");
        return None;
    }
    let env = cfg.get(ENV_KEY).unwrap_or_default();
    match LocalFileKms::open(&path, &env) {
        Ok(kms) => Some(Arc::new(kms)),
        Err(err) => {
            tracing::error!(error = %err, path = %path, "the local KMS key file could not be opened");
            None
        }
    }
}

/// Why the configured `LocalFileKms` must not be used, or `None` when it
/// may (or none is configured). The development file KMS keeps its master
/// key in a plain file, so it is allowed **only** under an explicit
/// `ENV=development`: an unset `ENV`, `staging`, `production` or an
/// unrecognised value all refuse. A Worker-secret KEK ring takes
/// precedence and is never refused here.
///
/// The message names both settings and never a key or a path's contents.
#[must_use]
pub fn local_kms_refusal(cfg: &dyn Config) -> Option<String> {
    if cfg.get(WORKER_KEK_CURRENT).is_some() || cfg.get(LOCAL_KEY_FILE).is_none() {
        return None;
    }
    let raw = cfg.get(ENV_KEY);
    if raw.as_deref().and_then(VentureEnv::parse) == Some(VentureEnv::Development) {
        return None;
    }
    let shown = raw.map_or_else(|| "unset".to_owned(), |env| format!("`{}`", env.trim()));
    Some(format!(
        "{LOCAL_KEY_FILE} is set but {ENV_KEY} is {shown}: the local file KMS is a development \
         provider and is used only with {ENV_KEY}=development. Unset {LOCAL_KEY_FILE}, set \
         {WORKER_KEK_CURRENT} (the Worker-secret KEK ring), or set {ENV_KEY}=development for a \
         development deployment."
    ))
}

/// A `Secrets` service whose every access is written to the
/// `harness_secret_audit` chain in `db` and, as before, logged through
/// `tracing`. The chain row carries the store (the tenant id), the secret
/// name, the action, the actor, whether it was allowed and the time —
/// never a value or a ciphertext ([`AuditEvent`] has no field for one).
pub(crate) fn audited_secrets(kms: Arc<dyn Kms>, db: Arc<dyn Database>) -> Secrets {
    Secrets::new(kms).with_audit(Arc::new(ChainAndTracing {
        chain: chain_sink(db),
    }))
}

/// The durable chain first — if it cannot record, the store refuses the
/// access — then the `tracing` line operators already watch.
struct ChainAndTracing {
    chain: Arc<dyn Audit>,
}

#[async_trait::async_trait]
impl Audit for ChainAndTracing {
    async fn record(&self, event: &AuditEvent<'_>) -> Result<(), SecretsError> {
        self.chain.record(event).await?;
        TracingAudit.record(event).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cratefield_core::{EmptyConfig, MapConfig};

    /// A 32-byte key, hex, in a fresh temp file — what an operator writes.
    fn key_file(tag: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "escalation-kek-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after the epoch")
                .as_nanos()
        ));
        std::fs::write(&path, "00".repeat(32)).expect("write a test key file");
        path
    }

    #[test]
    fn no_kms_config_means_no_kms() {
        assert!(kms_from_config(&EmptyConfig).is_none());
    }

    #[test]
    fn a_local_key_file_is_a_development_provider_only() {
        let path = key_file("local");
        let path = path.to_str().expect("temp path is UTF-8");
        let with_env = |env: &'static str| {
            MapConfig::from_pairs([("ESCALATION_KMS_KEY_FILE", path), ("ENV", env)])
        };
        let kms = kms_from_config(&with_env("development")).expect("the key file builds a KMS");
        assert_eq!(kms.provider(), "local-file");
        assert!(
            kms_from_config(&with_env("production")).is_none(),
            "a local key file is refused in production"
        );
        assert!(
            kms_from_config(&with_env("staging")).is_none(),
            "a local key file is refused in staging"
        );
        assert!(
            kms_from_config(&with_env("dev")).is_none(),
            "an unrecognised ENV is not development"
        );
        let unset = MapConfig::from_pairs([("ESCALATION_KMS_KEY_FILE", path)]);
        assert!(
            kms_from_config(&unset).is_none(),
            "a local key file is refused with ENV unset"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn the_local_kms_refusal_names_the_settings() {
        let unset = MapConfig::from_pairs([("ESCALATION_KMS_KEY_FILE", "/tmp/kek")]);
        let message = local_kms_refusal(&unset).expect("ENV unset refuses");
        assert!(message.contains("ESCALATION_KMS_KEY_FILE"), "{message}");
        assert!(message.contains("ENV is unset"), "{message}");
        assert!(message.contains("ENV=development"), "{message}");

        let production = MapConfig::from_pairs([
            ("ESCALATION_KMS_KEY_FILE", "/tmp/kek"),
            ("ENV", "production"),
        ]);
        let message = local_kms_refusal(&production).expect("production refuses");
        assert!(message.contains("`production`"), "{message}");

        let development = MapConfig::from_pairs([
            ("ESCALATION_KMS_KEY_FILE", "/tmp/kek"),
            ("ENV", "development"),
        ]);
        assert!(local_kms_refusal(&development).is_none());
        // Nothing to refuse without a key file, or when the Worker-secret
        // ring (which takes precedence) is configured.
        assert!(local_kms_refusal(&EmptyConfig).is_none());
        let ring = MapConfig::from_pairs([
            ("ESCALATION_KMS_KEY_FILE", "/tmp/kek"),
            ("HARNESS_KEK_CURRENT", "v1"),
        ]);
        assert!(local_kms_refusal(&ring).is_none());
    }
}
