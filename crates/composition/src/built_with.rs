//! The venture's "built with" stack: what SupportGenius is made of, who
//! it is made with, and which of those claims are live today.
//!
//! The list is **vendored, not fetched**. `built-with.json` beside this
//! file is the Factory Zero registry entry for FZ-008 exactly as
//! `https://factory0.ventures/stack.json` served it, committed so the
//! About/Settings section renders offline and in a Worker slice with no
//! network of its own. A vendored copy is only useful if it is also
//! *honest*, so `tests/built_with.rs` pins it against the registry in
//! two ways: an offline shape test that runs on every `cargo test`, and
//! a `#[ignore]`d drift test that fetches the live registry. Run the
//! drift test by hand:
//!
//! ```sh
//! cargo test -p supportgenius-composition --test built_with -- --ignored
//! ```
//!
//! It shells out to `curl`, so it is not part of the suite: this
//! repository has no network-touching tests by deliberate convention
//! (see `bin/supportgenius/tests/ssrf.rs`), and a suite that can fail
//! because a host is unreachable is a suite people learn to ignore.
//!
//! # Live first, then planned
//!
//! [`stack`] sorts by [`Status`], live before planned, keeping registry
//! order inside each group. The order is the promise: a reader scanning
//! the About section should see what the site actually does *today*
//! (Cloudflare) above what it intends to (Cratefield, Polar,
//! promptdecode, Keep Shipping). Nothing planned is ever presented as
//! live, which is the whole reason `status` is a per-entry field rather
//! than a section heading.
//!
//! # Mail attribution follows the registry
//!
//! Owlpost is not in FZ-008's stack today, so [`owlpost_sends_mail`] is
//! `false` and [`mail_attribution`] is `None`: no footer claiming mail
//! went out through a service that does not send it. Both are read from
//! the registry rather than hardcoded, so when the registry gains a live
//! `owlpost` entry they flip by themselves and the footer may then be
//! added — a reviewer reading this module will not have to remember to
//! look for a second place to change.

use std::sync::OnceLock;

/// The registry this stack is copied from: the published
/// `stack.json`, and the address the drift test re-reads to check the
/// copy has not gone stale.
pub const SOURCE: &str = "https://factory0.ventures/stack.json";

/// The venture's page on the Factory Zero registry — the "Listed by /
/// Factory Zero" chip the site appends after the stack, and the
/// subprocessors list the privacy copy links to.
pub const SUBPROCESSORS_URL: &str = "https://factory0.ventures/ventures/supportgenius/";

/// The venture name as the registry spells it (`SupportGenius`, not the
/// kebab-case [`crate::NAME`]): this string is printed in the About
/// section and shipped in [`summary_json`], so it is pinned rather than
/// formatted.
pub const VENTURE_NAME: &str = "SupportGenius";

/// Who owns an entry: a sibling venture on the Factory Zero registry,
/// or somebody else's product.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Another Factory Zero venture — a sibling we build on.
    FactoryZero,
    /// A third-party product, and so a potential subprocessor.
    ThirdParty,
}

/// Whether the entry is in production today, or still the plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// Running: the site, or a service the site actually depends on,
    /// right now.
    Live,
    /// Committed to, not yet deployed. Rendered as planned, and never
    /// counted as a subprocessor in use.
    Planned,
}

impl Status {
    /// The registry's own spelling of the status, as [`summary_json`]
    /// emits it and as the widget's `data-status` attribute carries it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Planned => "planned",
        }
    }
}

/// One row of the About/Settings "built with" list: a single thing the
/// venture is built with or runs on, with the registry's own link,
/// role, one-line explanation and status.
///
/// Every field borrows from the `include_str!`'d `built-with.json`, so
/// an entry costs no allocation and the whole stack lives in the
/// binary's read-only data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StackEntry {
    /// The registry's identifier for the entry (`FZ-004`, `cloudflare`).
    pub id: &'static str,
    /// The display name, as printed.
    pub name: &'static str,
    /// Factory Zero venture or third party.
    pub kind: Kind,
    /// Where the entry links to.
    pub url: &'static str,
    /// The registry's role slug (`framework`, `hosting`).
    pub role: &'static str,
    /// The words the site renders above the name ("Hosted on").
    pub phrase: &'static str,
    /// Live, or planned.
    pub status: Status,
    /// The registry's one-line explanation of what this entry does.
    pub note: &'static str,
}

/// The parsed `built-with.json`, parsed once.
///
/// A `serde_json::Value` over the `include_str!`'d text rather than a
/// derived struct: the value lives in a `static`, so every string read
/// out of it is `&'static str` for free, and no second copy of the
/// registry's wording can exist in the binary.
static REGISTRY: OnceLock<serde_json::Value> = OnceLock::new();

fn registry() -> &'static serde_json::Value {
    REGISTRY.get_or_init(|| {
        serde_json::from_str(include_str!("built-with.json"))
            .expect("built-with.json is valid JSON")
    })
}

/// A required string field of an entry, or a panic naming the field.
///
/// Committed source, so a missing field is a bug to catch at
/// `cargo test` rather than a chip that renders blank in production.
fn field<'a>(entry: &'a serde_json::Value, key: &str) -> &'a str {
    entry
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| panic!("built-with.json entry has a string `{key}`"))
}

fn kind(raw: &str) -> Kind {
    match raw {
        "factory-zero" => Kind::FactoryZero,
        "third-party" => Kind::ThirdParty,
        other => panic!("built-with.json has no `{other}` kind"),
    }
}

fn status(raw: &str) -> Status {
    match raw {
        "live" => Status::Live,
        "planned" => Status::Planned,
        other => panic!("built-with.json has no `{other}` status"),
    }
}

/// The stack as the site renders it: live entries first, then planned,
/// registry order preserved inside each group.
///
/// A `sort_by_key` over the registry's own order is what keeps the two
/// properties together — the grouping the About section promises, and
/// the registry's ordering, which is authoritative and must not be
/// tidied. `sort_by_key` is stable, so the second key never moves a row
/// inside its group.
///
/// # Panics
///
/// Panics if `built-with.json` is malformed or carries a `kind` or
/// `status` this module does not know. It is committed source, so a
/// malformed one is a bug to catch at `cargo test`, not a deployment to
/// degrade: a section that silently drops a row is worse than one that
/// will not build.
#[must_use]
pub fn stack() -> &'static [StackEntry] {
    static STACK: OnceLock<Box<[StackEntry]>> = OnceLock::new();
    STACK
        .get_or_init(|| {
            let uses = registry()
                .get("uses")
                .and_then(serde_json::Value::as_array)
                .unwrap_or_else(|| panic!("built-with.json has a `uses` array"));
            let mut entries: Vec<StackEntry> = uses
                .iter()
                .map(|entry| StackEntry {
                    id: field(entry, "id"),
                    name: field(entry, "name"),
                    kind: kind(field(entry, "kind")),
                    url: field(entry, "url"),
                    role: field(entry, "role"),
                    phrase: field(entry, "phrase"),
                    status: status(field(entry, "status")),
                    note: field(entry, "note"),
                })
                .collect();
            entries.sort_by_key(|entry| entry.status != Status::Live);
            entries.into_boxed_slice()
        })
        .as_ref()
}

/// The entries that are somebody else's product: the subprocessor
/// list the privacy copy links to, live first then planned.
///
/// Third-party only. A Factory Zero sibling is not a subprocessor — it
/// is us, on our own infrastructure — and listing Cratefield or
/// Keep Shipping beside Cloudflare would tell a reader that a venture
/// we control processes their data. Planned entries stay in the list
/// because the registry records them as intended, and the section
/// labels each one as planned rather than dropping it.
#[must_use]
pub fn subprocessors() -> Vec<&'static StackEntry> {
    stack()
        .iter()
        .filter(|entry| entry.kind == Kind::ThirdParty)
        .collect()
}

/// Whether Owlpost actually sends this venture's mail.
///
/// Read from the registry rather than a constant, and `false` whenever
/// the entry is missing or merely planned — so an attribution footer
/// cannot be shipped ahead of the service that earns it, and cannot
/// outlive one that stops sending.
#[must_use]
pub fn owlpost_sends_mail() -> bool {
    stack()
        .iter()
        .any(|entry| entry.id == "owlpost" && entry.status == Status::Live)
}

/// The mail footer's Owlpost line, if Owlpost sends the mail.
///
/// `Some("Sent with Owlpost")` exactly when [`owlpost_sends_mail`], and
/// `None` otherwise — a footer is a claim about where the mail went,
/// and an unsent claim is a small lie in every email the venture sends.
#[must_use]
pub fn mail_attribution() -> Option<&'static str> {
    owlpost_sends_mail().then_some("Sent with Owlpost")
}

/// A string as a JSON string literal, quotes and all.
///
/// [`summary_json`] assembles its document by hand rather than through
/// [`serde_json::json!`], because key order in that macro follows
/// whichever map `serde_json` was compiled with (`preserve_order` or
/// sorted) — and this payload's key order is part of its contract, the
/// widget and the dashboard both parse it. Hand-assembling against
/// `to_string` for each value makes the order the code's own and the
/// escaping still `serde_json`'s.
fn quoted(value: &str) -> String {
    serde_json::to_string(value).expect("a string is always serialisable")
}

/// One entry of the summary payload's `uses` array.
fn use_json(entry: &StackEntry) -> String {
    format!(
        "{{\"name\":{},\"phrase\":{},\"url\":{},\"status\":{},\"note\":{}}}",
        quoted(entry.name),
        quoted(entry.phrase),
        quoted(entry.url),
        quoted(entry.status.as_str()),
        quoted(entry.note),
    )
}

/// The "Listed by / Factory Zero" chip the site appends after the
/// stack, carried here too so a consumer building the same section
/// from the payload does not have to know the registry's own shape.
///
/// Not a [`stack`] entry: it is not something the venture is *built
/// with*, and counting it as one would put a row in the subprocessor
/// list that is not a subprocessor.
fn listed_by_json() -> String {
    format!(
        "{{\"name\":{},\"phrase\":{},\"url\":{},\"status\":{},\"note\":{}}}",
        quoted("Factory Zero"),
        quoted("Listed by"),
        quoted(SUBPROCESSORS_URL),
        quoted(Status::Live.as_str()),
        quoted("The venture page this stack is copied from."),
    )
}

/// The whole stack as one compact single-line JSON object, for the
/// About/Settings widget and the operations dashboard.
///
/// Built by hand in a fixed key order (`venture`, `source`,
/// `registry`, `subprocessors`, `uses`) with the [`listed_by_json`] chip
/// appended after the [`stack`] rows, mirroring the site's render order.
/// One line, because it is embedded in a widget and served to a browser:
/// no pretty-printing to pay for on every page load.
#[must_use]
pub fn summary_json() -> String {
    let mut uses: Vec<String> = stack().iter().map(use_json).collect();
    uses.push(listed_by_json());
    format!(
        "{{\"venture\":{},\"source\":{},\"registry\":{},\"subprocessors\":{},\"uses\":[{}]}}",
        quoted(VENTURE_NAME),
        quoted(SOURCE),
        quoted(SUBPROCESSORS_URL),
        quoted(SUBPROCESSORS_URL),
        uses.join(","),
    )
}
