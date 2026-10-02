//! The escalation module's slice of `cratefield-secrets`: the KMS is
//! resolved **from configuration** (no new port), and the secret names the
//! destination routes store under are declared here.
//!
//! The deployment names a KMS in config, exactly as the harness's own
//! control-plane dashboard does: `HARNESS_KEK_CURRENT` set → the
//! production Worker-secret provider (checked first, so a Worker never
//! falls back to a file); else `ESCALATION_KMS_KEY_FILE` → the development
//! `LocalFileKms` (which refuses to construct under `ENV=production`);
//! else `None`, and a route that needs it answers `503` rather than
//! storing a plaintext secret. Config here is the **raw** harness config
//! (unprefixed), read the same way the harness runtime reads its secrets.

use std::sync::Arc;

use cratefield_core::Config;
use cratefield_kms::{Kms, LocalFileKms, WorkerSecretKms};

/// The config key naming the Worker-secret KEK version new wraps use
/// (`WorkerSecretKms` reads the whole ring under it).
const WORKER_KEK_CURRENT: &str = "HARNESS_KEK_CURRENT";

/// The config key naming a development master-key file (`LocalFileKms`).
const LOCAL_KEY_FILE: &str = "ESCALATION_KMS_KEY_FILE";

/// The harness environment key (`"development"`, `"production"`), which
/// [`LocalFileKms`] refuses to wrap under in production.
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
    let env = cfg.get(ENV_KEY).unwrap_or_default();
    match LocalFileKms::open(&path, &env) {
        Ok(kms) => Some(Arc::new(kms)),
        Err(err) => {
            tracing::error!(error = %err, path = %path, "the local KMS key file could not be opened");
            None
        }
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
        let _ = std::fs::remove_file(path);
    }
}
