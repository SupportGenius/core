//! Shared, non-module tenancy for SupportGenius ventures: signed tenant
//! API keys, issued here rather than inside any one module so every
//! venture and sidecar verifies the same credential format.
//!
//! Tenancy here is app-level: this is one venture serving many customer
//! companies, so every table carries a `tenant_id` and the credential
//! names the tenant a request acts for. The self-hosted binary is the
//! same code with exactly one tenant row — nothing below is cloud-only.
//!
//! **Wire format.** A key is `sg_<kid>.<payload>.<mac>`, built over
//! `cratefield_core`'s `Signer` port (the same HMAC-backed port the
//! runtime assembles from the `HARNESS_SECRET` secret). The
//! `<payload>.<mac>` part is the signer's own token form; `sg_<kid>` is
//! an *unauthenticated display prefix* — it lets an operator read the key
//! id off a leaked key and route or revoke it without importing the
//! secret anywhere. Verification never trusts it: the kid is re-derived
//! from the MAC'd payload and a mismatching prefix is refused, not
//! corrected. Keys live out of the module boundary so modules consume
//! tenancy through ports and never issue keys themselves.
//!
//! **Revocation.** This crate stays stateless — a pure function of
//! (signer, presented key, revoked kids) on the `verify` side: no config
//! plumbing, no clock, no I/O — so the revoked-kid list a caller supplies
//! per verification (sourced from config under [`REVOKED_KIDS_KEY`]) is
//! the emergency override on the *signing* kid. An individual key is
//! revoked by the consuming module: it keeps a row per **key id** and
//! refuses to authenticate one whose row is gone, so deleting that row
//! revokes the key with no config edit.
//!
//! **No cryptography here.** The MAC, the constant-time comparison, the
//! key ring, expiry and the venture/environment binding all belong to the
//! `Signer` port. This crate adds only the credential shape and its
//! rules.

use std::fmt;

use cratefield_core::{IdGen, Kid, MAX_KID_NAME, Payload, Signer, UlidIdGen};

/// The purpose every tenant API key is signed with: `"tenancy.api-key"`.
///
/// Purposes elsewhere are module-qualified because the module part *is*
/// the module binding — a token minted by one module can never verify for
/// another. This constant deliberately breaks that convention: it names
/// the tenancy layer, not a module, because the credential it scopes must
/// work on *both* consumer modules (support and escalation) with no
/// re-issue between them. The scoping the convention usually buys still
/// holds: a token with this purpose verifies as a tenant API key and as
/// nothing else, and every other purpose fails here.
pub const API_KEY_PURPOSE: &str = "tenancy.api-key";

/// The display prefix on every key: `sg_`.
///
/// Deliberately unauthenticated. The prefix plus the kid segment is
/// routing and triage metadata — an operator can read the key id (and so
/// the rotation generation) off a pasted, leaked key before deciding
/// anything — and it is what makes a tenancy key visually
/// distinct from the signer's bare tokens. It carries no authority:
/// [`verify`] demands the MAC'd payload name the same kid the prefix
/// does, so a lying prefix is refused rather than believed.
pub const KEY_PREFIX: &str = "sg_";

/// The config key under which operators list revoked key ids,
/// comma-separated: `REVOKED_KIDS = "cur, k-2026-08"`. Both consumer
/// modules read this same name so one edit revokes everywhere at once;
/// [`parse_revoked_kids`] turns the raw value into the list [`verify`]
/// takes. It is the emergency override, checked alongside the per-key row
/// a module now keeps: the row is deleted to revoke one key, this list to
/// revoke a kid everywhere at once whatever rows exist.
pub const REVOKED_KIDS_KEY: &str = "REVOKED_KIDS";

/// Separates the tenant id from the per-key id in a key's signed subject:
/// `"{tenant_id}.{key_id}"`. Tenant ids are ULIDs (Crockford base32, no
/// punctuation), so a `.` cannot occur inside one and the split is
/// unambiguous whatever the tenant id is.
const SUBJECT_SEPARATOR: char = '.';

/// A minted key: the secret string (`key`, shown to the holder exactly
/// once) plus the metadata to remember. `kid` is the signing-key
/// generation the token was signed with (what the revoked-kid config list
/// matches); `key_id` is the per-key id this crate generates, bound into
/// the token, and the identity the consuming module stores per row.
#[derive(Debug, Clone)]
pub struct MintedKey {
    /// The full `sg_<kid>.<payload>.<mac>` string.
    pub key: String,
    /// The signing-key generation the token was signed with.
    pub kid: String,
    /// This key's own id, bound into the signed subject.
    pub key_id: String,
    /// The tenant this key authenticates.
    pub tenant_id: String,
}

/// What a verified key authenticates: the tenant it names, the per-key id
/// it was minted with — so a caller can look the key's own row up and
/// revoke it individually — and the signing-key generation, so a request
/// log can say which key generation a call came in on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantKey {
    /// The tenant the key authenticates.
    pub tenant_id: String,
    /// The signing-key generation the token was signed with.
    pub kid: String,
    /// The key's own id, from the signed subject.
    pub key_id: String,
}

/// Why a key failed to mint or verify. Deliberately coarse: `Revoked` is
/// worth distinguishing (tell the holder to re-mint) from `Invalid`
/// (someone presented junk), but nothing here says *why* the signer
/// refused a token — that granularity belongs to the signer's logs, not
/// to an API response an attacker can read.
#[derive(Debug, PartialEq, Eq)]
pub enum KeyError {
    /// The string does not have the `sg_<kid>.<payload>.<mac>` shape:
    /// missing prefix, no kid segment, or a kid segment outside
    /// `[a-z0-9_-]{1,32}`. Also returned when the signed subject is not
    /// `{tenant_id}.{key_id}` — a token minted before per-key ids existed
    /// carries no key id and is refused here rather than treated as a
    /// tenant named by the whole subject.
    Malformed,
    /// The signer named a kid this crate cannot render into a prefix.
    /// Only reachable with a hand-built ring whose names ignore the
    /// charset every wire form here assumes.
    UnknownKid,
    /// The authenticated kid is on the caller's revoked list.
    Revoked,
    /// The signer refused the credential — tampered payload or MAC, wrong
    /// purpose, expired, foreign venture/environment scope — or the
    /// display prefix named a different kid than the payload does.
    Invalid,
}

impl fmt::Display for KeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed => f.write_str("api key is not sg_<kid>.<payload>.<mac>"),
            Self::UnknownKid => f.write_str("api key names a key id outside [a-z0-9_-]{1,32}"),
            Self::Revoked => f.write_str("api key was signed with a revoked key id"),
            Self::Invalid => f.write_str("api key failed verification"),
        }
    }
}

impl std::error::Error for KeyError {}

/// Mints a tenant API key over the `Signer` port.
///
/// A fresh per-key id (a ULID) is generated and bound into the signed
/// payload as `subject = "{tenant_id}.{key_id}"`, so the credential names
/// both its tenant and its own identity — that is what lets a caller
/// revoke this key and no other. `tenant_id` itself is opaque to this
/// crate: no charset or shape rules.
///
/// Signing always uses the ring's current key and cannot be steered, so
/// the signing kid is recovered by verifying the token just produced —
/// one cheap MAC that also proves the key round-trips before a holder
/// ever sees it. `exp: None` leaves lifetime to the signer's own
/// `TokenPolicy` (the 30-day default ceiling with the reference signer).
///
/// # Errors
///
/// [`KeyError::Invalid`] when the freshly signed token fails the
/// round-trip verification (a ring with no signing key emits an invalid
/// token), and [`KeyError::UnknownKid`] when it signed with a kid this
/// crate cannot render into the prefix.
pub fn mint(signer: &dyn Signer, tenant_id: &str) -> Result<MintedKey, KeyError> {
    let key_id = UlidIdGen.ulid();
    let token = signer.sign(&Payload {
        purpose: API_KEY_PURPOSE.to_owned(),
        subject: format!("{tenant_id}{SUBJECT_SEPARATOR}{key_id}"),
        // Expiry is the signer's policy call, not ours.
        exp: None,
        // The ring overrides this on sign; the real kid is recovered
        // from the round-trip below.
        kid: Kid::Cur,
    });
    let verified = signer
        .verify(&token, API_KEY_PURPOSE)
        .ok_or(KeyError::Invalid)?;
    let kid = kid_name(&verified.kid).ok_or(KeyError::UnknownKid)?;
    Ok(MintedKey {
        key: format!("{KEY_PREFIX}{kid}.{token}"),
        kid,
        key_id,
        tenant_id: tenant_id.to_owned(),
    })
}

/// Verifies a presented key and returns the tenant and key it
/// authenticates.
///
/// The `sg_<kid>` prefix is display metadata and is not trusted: the kid
/// segment must be a well-formed name, but authority comes only from the
/// signer's verdict on `<payload>.<mac>` — after which the payload's own
/// kid must equal the prefix, so a key whose two halves name different
/// keys is refused rather than routed by the prefix. Revocation is
/// checked against the *authenticated* kid, so relabelling the prefix
/// cannot dodge a revoked entry.
///
/// The signed subject is split into `{tenant_id}.{key_id}`; a token whose
/// subject carries no key id is [`KeyError::Malformed`].
///
/// `revoked_kids` is the caller's list, usually parsed out of config
/// under [`REVOKED_KIDS_KEY`] with [`parse_revoked_kids`] as the
/// emergency override on the signing kid; a consuming module revokes an
/// individual key by dropping the row for its `key_id` instead.
///
/// # Errors
///
/// [`KeyError::Malformed`] when the string lacks the
/// `sg_<kid>.<payload>.<mac>` shape or its subject carries no key id,
/// [`KeyError::Invalid`] when the signer rejects the credential or the
/// prefix kid does not match the payload kid, [`KeyError::Revoked`] when
/// the authenticated kid is on `revoked_kids`, and [`KeyError::UnknownKid`]
/// when the payload names a kid this crate cannot render.
pub fn verify(
    signer: &dyn Signer,
    presented: &str,
    revoked_kids: &[String],
) -> Result<TenantKey, KeyError> {
    let body = presented
        .strip_prefix(KEY_PREFIX)
        .ok_or(KeyError::Malformed)?;
    let (kid_segment, token) = body.split_once('.').ok_or(KeyError::Malformed)?;
    // Kid names can never contain a `.` — that is what makes the split
    // above unambiguous. Enforce the charset before anything else.
    if !is_kid_name(kid_segment) {
        return Err(KeyError::Malformed);
    }
    // Authority is the signer's verdict alone: MAC, purpose, expiry and
    // the venture/environment binding were stamped on the token there.
    let payload = signer
        .verify(token, API_KEY_PURPOSE)
        .ok_or(KeyError::Invalid)?;
    let kid = kid_name(&payload.kid).ok_or(KeyError::UnknownKid)?;
    // The payload must name the kid the prefix claimed.
    if kid != kid_segment {
        return Err(KeyError::Invalid);
    }
    if revoked_kids
        .iter()
        .any(|revoked| kid.eq_ignore_ascii_case(revoked))
    {
        return Err(KeyError::Revoked);
    }
    // `rsplit_once`: the key id is appended last and is a ULID (no `.`),
    // so this is the tenant/key split even if a tenant id ever held one.
    let (tenant_id, key_id) = payload
        .subject
        .rsplit_once(SUBJECT_SEPARATOR)
        .filter(|(tenant_id, key_id)| !tenant_id.is_empty() && !key_id.is_empty())
        .ok_or(KeyError::Malformed)?;
    Ok(TenantKey {
        tenant_id: tenant_id.to_owned(),
        kid,
        key_id: key_id.to_owned(),
    })
}

/// Pulls the bearer credential out of an `Authorization` header value:
/// case-insensitive on the scheme, tolerant of extra whitespace around
/// it, strict about the rest — exactly one credential token, no trailing
/// junk, nothing on a header that carries another scheme or none. The
/// credential is returned as-is, `sg_` prefix and all; hand it straight
/// to [`verify`], which owns shape checking.
#[must_use]
pub fn bearer(header_value: &str) -> Option<&str> {
    let mut parts = header_value.split_whitespace();
    if !parts.next()?.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let credential = parts.next()?;
    // "Bearer a b" is a malformed header, not a credential.
    parts.next().is_none().then_some(credential)
}

/// Parses a comma-separated revoked-kid config value into the list
/// [`verify`] expects: each entry trimmed, empties dropped, lowercased —
/// so `"cur, prev"` and `"CUR,, prev "` mean the same list, and an empty
/// or all-comma value means nothing is revoked.
#[must_use]
pub fn parse_revoked_kids(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|kid| !kid.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

/// Renders a `Kid` as the name used in a key's prefix: `Kid::Cur` is
/// `"cur"`, `Kid::Prev` is `"prev"` (the reference signer's own wire
/// names), `Kid::Named` passes through — but only when the name is
/// `[a-z0-9_-]{1,32}`. `None` means "no renderable name": such a kid can
/// never appear in a prefix, so [`mint`] refuses to issue and [`verify`]
/// refuses to trust anything claiming it. The charset bound is
/// load-bearing — a kid that could contain a `.` would make
/// `sg_<kid>.<payload>.<mac>` ambiguous to split.
#[must_use]
pub fn kid_name(kid: &Kid) -> Option<String> {
    match kid {
        Kid::Cur => Some("cur".to_owned()),
        Kid::Prev => Some("prev".to_owned()),
        Kid::Named(name) if is_kid_name(name) => Some(name.clone()),
        // Invalid names, and any variant the port adds later: nothing a
        // prefix could safely carry.
        _ => None,
    }
}

/// The kid charset every prefix assumes: `[a-z0-9_-]{1,32}`, capped at
/// the signer's own [`MAX_KID_NAME`].
fn is_kid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_KID_NAME
        && name
            .chars()
            .all(|c| matches!(c, 'a'..='z' | '0'..='9' | '_' | '-'))
}

#[cfg(test)]
mod tests {
    use cratefield_core::{HmacSigner, KeyRing, Kid, MAX_KID_NAME, Payload, Signer};

    use crate::{KEY_PREFIX, KeyError, bearer, kid_name, mint, parse_revoked_kids, verify};

    /// Exactly [`cratefield_core::MIN_SECRET_BYTES`] long.
    const SECRET: &str = "0123456789abcdef0123456789abcdef";
    const PREVIOUS_SECRET: &str = "fedcba9876543210fedcba9876543210";

    const _: () = assert!(SECRET.len() >= cratefield_core::MIN_SECRET_BYTES);

    fn signer() -> HmacSigner {
        HmacSigner::new(SECRET, None).expect("secret meets the minimum length")
    }

    /// Flips the first character of a segment to a different base64url
    /// character, keeping the tampered key shape-valid.
    fn flip(segment: &str) -> String {
        let mut chars = segment.chars();
        let first = chars.next().expect("segments are never empty");
        let rest = chars.as_str();
        if first == 'A' {
            format!("B{rest}")
        } else {
            format!("A{rest}")
        }
    }

    fn segments(key: &str) -> (&str, &str, &str) {
        let body = key.strip_prefix(KEY_PREFIX).expect("prefix present");
        let parts: Vec<&str> = body.split('.').collect();
        assert_eq!(parts.len(), 3, "sg_<kid>.<payload>.<mac>");
        (parts[0], parts[1], parts[2])
    }

    #[test]
    fn minted_key_round_trips_to_its_tenant() {
        let signer = signer();
        let minted = mint(&signer, "tenant-alpha").expect("mint");
        assert!(minted.key.starts_with(KEY_PREFIX));
        assert_eq!(minted.tenant_id, "tenant-alpha");
        assert!(!minted.key_id.is_empty());

        let key = verify(&signer, &minted.key, &[]).expect("verify");
        assert_eq!(key.tenant_id, "tenant-alpha");
        assert_eq!(key.kid, minted.kid);
        assert_eq!(key.key_id, minted.key_id, "the key id round-trips");
    }

    #[test]
    fn two_mints_for_one_tenant_are_distinct_keys() {
        let signer = signer();
        // Same tenant, same second: only the generated per-key id can
        // tell these apart, and it must.
        let first = mint(&signer, "tenant-alpha").expect("mint");
        let second = mint(&signer, "tenant-alpha").expect("mint");
        assert_ne!(first.key_id, second.key_id);
        assert_ne!(first.key, second.key);
        let verified = verify(&signer, &second.key, &[]).expect("verify");
        assert_eq!(verified.tenant_id, "tenant-alpha");
        assert_eq!(verified.key_id, second.key_id);
    }

    #[test]
    fn a_subject_without_a_key_id_is_malformed() {
        let signer = signer();
        // A token minted before per-key ids existed: the subject is the
        // bare tenant id, no separator. It must be rejected outright, not
        // read as a tenant.
        let legacy = signer.sign(&Payload {
            purpose: crate::API_KEY_PURPOSE.to_owned(),
            subject: "tenant-alpha".to_owned(),
            exp: None,
            kid: Kid::Cur,
        });
        let disguised = format!("{KEY_PREFIX}cur.{legacy}");
        assert_eq!(verify(&signer, &disguised, &[]), Err(KeyError::Malformed));

        // An empty key id after the separator is equally not a key id.
        let trailing = signer.sign(&Payload {
            purpose: crate::API_KEY_PURPOSE.to_owned(),
            subject: "tenant-alpha.".to_owned(),
            exp: None,
            kid: Kid::Cur,
        });
        assert_eq!(
            verify(&signer, &format!("{KEY_PREFIX}cur.{trailing}"), &[]),
            Err(KeyError::Malformed)
        );
    }

    #[test]
    fn key_id_never_carries_the_separator() {
        // The split in `verify` is unambiguous only because a key id is a
        // ULID (Crockford base32). Guard the generator choice, not just
        // the current output.
        let signer = signer();
        let minted = mint(&signer, "tenant-alpha").expect("mint");
        assert!(!minted.key_id.contains(crate::SUBJECT_SEPARATOR));
    }

    #[test]
    fn key_has_the_documented_shape() {
        let signer = signer();
        let minted = mint(&signer, "t1").expect("mint");
        let (kid, payload, mac) = segments(&minted.key);
        assert_eq!(kid, minted.kid);
        assert!(!payload.is_empty());
        assert!(!mac.is_empty());
    }

    #[test]
    fn a_key_authenticates_its_own_tenant_only() {
        let signer = signer();
        let alpha = mint(&signer, "tenant-alpha").expect("mint");
        let beta = mint(&signer, "tenant-beta").expect("mint");
        assert_ne!(alpha.key, beta.key);
        assert_eq!(
            verify(&signer, &alpha.key, &[]).expect("verify").tenant_id,
            "tenant-alpha"
        );
        assert_eq!(
            verify(&signer, &beta.key, &[]).expect("verify").tenant_id,
            "tenant-beta"
        );
    }

    #[test]
    fn tampering_with_any_segment_fails() {
        let signer = signer();
        let minted = mint(&signer, "t1").expect("mint");
        let (kid, payload, mac) = segments(&minted.key);

        // The kid segment is display metadata, but relabelling it still
        // cannot smuggle the key through: it must match the MAC'd payload.
        let relabelled = format!("{KEY_PREFIX}zzz.{payload}.{mac}");
        assert_eq!(verify(&signer, &relabelled, &[]), Err(KeyError::Invalid));

        let payload_tampered = format!("{KEY_PREFIX}{kid}.{}.{mac}", flip(payload));
        assert_eq!(
            verify(&signer, &payload_tampered, &[]),
            Err(KeyError::Invalid)
        );

        let mac_tampered = format!("{KEY_PREFIX}{kid}.{payload}.{}", flip(mac));
        assert_eq!(verify(&signer, &mac_tampered, &[]), Err(KeyError::Invalid));
    }

    #[test]
    fn a_revoked_kid_is_refused() {
        let signer = signer();
        let minted = mint(&signer, "t1").expect("mint");
        let revoked = vec![minted.kid.clone()];
        assert_eq!(
            verify(&signer, &minted.key, &revoked),
            Err(KeyError::Revoked)
        );
        // Revocation is per kid: other kids on the list revoke nothing
        // here, and the unrevoked key still verifies.
        assert!(verify(&signer, &minted.key, &["other".to_owned()]).is_ok());
        assert!(verify(&signer, &minted.key, &[]).is_ok());
    }

    #[test]
    fn prefix_cannot_relabel_a_cur_signed_key_as_prev() {
        let mut ring = KeyRing::new();
        ring.rotate_signing(Kid::Cur, SECRET)
            .expect("current key installs");
        ring.add_verifying_only(Kid::Prev, PREVIOUS_SECRET)
            .expect("previous key installs");
        let signer = HmacSigner::from_ring(ring);

        let minted = mint(&signer, "t1").expect("mint");
        assert_eq!(minted.kid, "cur");

        // `prev` is a live key in this ring, so `sg_prev.` is a real
        // label — but the MAC still names `cur`, and the prefix alone
        // must never move verification onto another key.
        let (_, payload, mac) = segments(&minted.key);
        let relabelled = format!("{KEY_PREFIX}prev.{payload}.{mac}");
        assert_eq!(verify(&signer, &relabelled, &[]), Err(KeyError::Invalid));

        // The untouched key still verifies: the failure above is the
        // mismatch, not the ring.
        assert!(verify(&signer, &minted.key, &[]).is_ok());
    }

    #[test]
    fn garbage_is_malformed() {
        let signer = signer();
        let minted = mint(&signer, "t1").expect("mint");
        // The bare signer token — prefix and kid segment stripped — is
        // not an API key.
        let bare = segments(&minted.key);
        let bare_token = format!("{}.{}", bare.1, bare.2);
        for garbage in ["", "sg_", "sg_cur", "nope", bare_token.as_str()] {
            assert_eq!(
                verify(&signer, garbage, &[]),
                Err(KeyError::Malformed),
                "input {garbage:?}"
            );
        }
    }

    #[test]
    fn a_token_with_another_purpose_is_not_an_api_key() {
        let signer = signer();
        let foreign = signer.sign(&Payload {
            purpose: "support.thread.read".to_owned(),
            subject: "t1".to_owned(),
            exp: None,
            kid: Kid::Cur,
        });
        let disguised = format!("{KEY_PREFIX}cur.{foreign}");
        assert_eq!(verify(&signer, &disguised, &[]), Err(KeyError::Invalid));
    }

    #[test]
    fn a_named_signing_key_round_trips_and_revokes() {
        let mut ring = KeyRing::new();
        ring.rotate_signing(Kid::named("k-2026-09"), SECRET)
            .expect("named key installs");
        let signer = HmacSigner::from_ring(ring);

        let minted = mint(&signer, "t1").expect("mint");
        assert_eq!(minted.kid, "k-2026-09");
        let key = verify(&signer, &minted.key, &[]).expect("verify");
        assert_eq!(key.kid, "k-2026-09");

        // Config lists kids in any case; matching is not.
        assert_eq!(
            verify(&signer, &minted.key, &["K-2026-09".to_owned()]),
            Err(KeyError::Revoked)
        );
    }

    #[test]
    fn kid_names_render_only_safe_names() {
        assert_eq!(kid_name(&Kid::Cur).as_deref(), Some("cur"));
        assert_eq!(kid_name(&Kid::Prev).as_deref(), Some("prev"));
        assert_eq!(kid_name(&Kid::named("team-2")).as_deref(), Some("team-2"));
        assert_eq!(kid_name(&Kid::named("UPPER")), None);
        assert_eq!(kid_name(&Kid::named("dot.dot")), None);
        assert_eq!(kid_name(&Kid::named("")), None);
        let too_long = "x".repeat(MAX_KID_NAME + 1);
        let max_len = "x".repeat(MAX_KID_NAME);
        assert_eq!(kid_name(&Kid::named(too_long)), None);
        assert!(kid_name(&Kid::named(max_len)).is_some());
    }

    #[test]
    fn bearer_extracts_the_credential() {
        assert_eq!(bearer("Bearer sg_cur.a.b"), Some("sg_cur.a.b"));
        assert_eq!(bearer("bearer sg_cur.a.b"), Some("sg_cur.a.b"));
        assert_eq!(bearer("BEARER   sg_x"), Some("sg_x"));
        assert_eq!(bearer("sg_cur.a.b"), None, "missing scheme");
        assert_eq!(bearer("Basic sg_x"), None, "another scheme");
        assert_eq!(bearer("Bearer"), None, "scheme with no credential");
        assert_eq!(bearer("Bearer   "), None, "scheme with blank credential");
        assert_eq!(bearer("Bearer two tokens"), None, "trailing junk");
        assert_eq!(bearer(""), None);
    }

    #[test]
    fn revoked_kid_parsing_normalizes() {
        assert_eq!(parse_revoked_kids("a, b ,,C "), vec!["a", "b", "c"]);
        assert_eq!(parse_revoked_kids("cur"), vec!["cur"]);
        assert!(parse_revoked_kids("").is_empty());
        assert!(parse_revoked_kids(" , , ").is_empty());
    }
}
