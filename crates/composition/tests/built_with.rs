//! Issue #66 acceptance for the venture's "built with" stack: the
//! About/Settings section lists exactly the registry entries for FZ-008
//! (live before planned, each labelled and linked), the subprocessor
//! list is the third-party entries only, the mail footer claims Owlpost
//! only when Owlpost actually sends, and the vendored copy of the
//! registry entry is pinned — offline for shape, and against the live
//! registry for drift by a test that is deliberately `#[ignore]`d so
//! this repository keeps its no-network-in-the-suite convention.

use std::process::Command;

use supportgenius_composition::built_with::{
    Kind, SOURCE, SUBPROCESSORS_URL, Status, VENTURE_NAME, mail_attribution, owlpost_sends_mail,
    stack, subprocessors, summary_json,
};

/// Every entry the registry names for FZ-008, in the registry's order —
/// which is *not* the rendered order (see [`stack`]).
const REGISTRY_IDS: [&str; 5] = ["FZ-004", "polar", "FZ-009", "FZ-012", "cloudflare"];

#[test]
fn the_stack_lists_exactly_the_registry_entries_live_first() {
    let ids: Vec<&str> = stack().iter().map(|entry| entry.id).collect();
    assert_eq!(
        ids,
        vec![
            "cloudflare", // the only live one
            "FZ-004",     // the rest keep registry order
            "polar",
            "FZ-009",
            "FZ-012",
        ]
    );
    assert_eq!(
        stack().len(),
        REGISTRY_IDS.len(),
        "the stack is exactly the registry's five entries, no more and no fewer"
    );
    for id in REGISTRY_IDS {
        assert!(stack().iter().any(|entry| entry.id == id), "{id} is listed");
    }
}

#[test]
fn every_entry_is_named_linked_and_labelled() {
    for entry in stack() {
        assert!(!entry.name.is_empty(), "{}: a name", entry.id);
        assert!(!entry.phrase.is_empty(), "{}: a phrase", entry.id);
        assert!(!entry.note.is_empty(), "{}: a note", entry.id);
        assert!(
            entry.url.starts_with("https://"),
            "{}: an absolute https link, not {}",
            entry.id,
            entry.url
        );
        // The label is the registry's own word, and the status is what
        // tells a reader which claims are running.
        assert!(
            matches!(entry.status.as_str(), "live" | "planned"),
            "{}: status is live or planned, never anything else",
            entry.id
        );
    }
}

#[test]
fn nothing_planned_is_shown_as_live() {
    let live: Vec<&str> = stack()
        .iter()
        .filter(|entry| entry.status == Status::Live)
        .map(|entry| entry.id)
        .collect();
    assert_eq!(
        live,
        vec!["cloudflare"],
        "exactly one entry is live, and it is the hosting one"
    );
    // And the rest are all planned rather than unlabelled: a reader
    // cannot mistake Cratefield or Polar for something already running.
    for entry in stack().iter().filter(|e| e.status != Status::Live) {
        assert_eq!(entry.status, Status::Planned, "{}: planned", entry.id);
    }
}

#[test]
fn subprocessors_are_the_third_party_entries_only() {
    let ids: Vec<&str> = subprocessors().iter().map(|entry| entry.id).collect();
    assert_eq!(
        ids,
        vec!["cloudflare", "polar"],
        "third-party entries, live first then planned"
    );
    for entry in subprocessors() {
        assert_eq!(entry.kind, Kind::ThirdParty, "{}: third party", entry.id);
    }
    // A Factory Zero sibling is us, on our own infrastructure: putting
    // one in a subprocessor list would tell a reader a venture we
    // control processes their data.
    for entry in stack()
        .iter()
        .filter(|entry| entry.kind == Kind::FactoryZero)
    {
        assert!(
            !subprocessors().iter().any(|sub| sub.id == entry.id),
            "{}: a Factory Zero sibling is not a subprocessor",
            entry.id
        );
    }
}

/// Registry first: Owlpost is **not** in FZ-008's stack today, so no
/// mail this venture sends went through it, and the footer must not say
/// otherwise. Both of these flip by themselves — no second edit — the
/// moment the registry gains a live `owlpost` entry; at that point the
/// mail footer may be added, and this test is what should then fail,
/// saying so.
#[test]
fn owlpost_attribution_is_off_while_owlpost_is_not_in_the_stack() {
    assert!(
        !stack().iter().any(|entry| entry.id == "owlpost"),
        "the registry still does not list Owlpost"
    );
    assert!(!owlpost_sends_mail());
    assert_eq!(mail_attribution(), None);
}

/// The section links the subprocessors list, and the payload carries
/// the address in both the places a consumer looks for it.
#[test]
fn the_section_links_the_subprocessors_list() {
    assert_eq!(
        SUBPROCESSORS_URL,
        "https://factory0.ventures/ventures/supportgenius/"
    );
    let summary: serde_json::Value =
        serde_json::from_str(&summary_json()).expect("the summary is valid JSON");
    assert_eq!(summary["subprocessors"], SUBPROCESSORS_URL);
    assert_eq!(summary["registry"], SUBPROCESSORS_URL);
}

#[test]
fn the_summary_payload_has_the_documented_shape() {
    let payload = summary_json();
    assert!(
        !payload.contains('\n'),
        "one line: it is embedded in a widget and served to a browser"
    );
    let summary: serde_json::Value =
        serde_json::from_str(&payload).expect("the summary is valid JSON");
    let keys: Vec<&str> = summary
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        vec!["venture", "source", "registry", "subprocessors", "uses"],
        "the payload's key set and order, which the widget parses"
    );
    assert_eq!(summary["venture"], VENTURE_NAME);
    assert_eq!(summary["source"], SOURCE);

    let uses = summary["uses"].as_array().expect("a uses array");
    // The stack's five rows, live first, then the "Listed by / Factory
    // Zero" chip the site appends — which is deliberately not a stack
    // entry, so it is absent from `stack()`.
    assert_eq!(uses.len(), stack().len() + 1);
    for (row, entry) in uses.iter().zip(stack()) {
        assert_eq!(row["name"], entry.name);
        assert_eq!(row["phrase"], entry.phrase);
        assert_eq!(row["url"], entry.url);
        assert_eq!(row["status"], entry.status.as_str());
        assert_eq!(row["note"], entry.note);
    }
    let chip = uses.last().expect("the chip");
    assert_eq!(chip["name"], "Factory Zero");
    assert_eq!(chip["phrase"], "Listed by");
    assert_eq!(chip["url"], SUBPROCESSORS_URL);
    assert!(
        !stack().iter().any(|entry| entry.phrase == "Listed by"),
        "the chip is the payload's, not the stack's"
    );

    // Live before planned, in the same order `stack()` renders.
    let statuses: Vec<&str> = stack().iter().map(|entry| entry.status.as_str()).collect();
    assert_eq!(
        statuses,
        vec!["live", "planned", "planned", "planned", "planned"]
    );
    for (row, expected) in uses.iter().take(stack().len()).zip(&statuses) {
        assert_eq!(row["status"], *expected);
    }
}

#[test]
fn the_stack_is_a_stable_static_slice() {
    // The `OnceLock` must be idempotent: two calls hand back the same
    // slice, pointer and all, so a caller that holds one across a
    // request is looking at the same rows another caller renders.
    let first = stack();
    let second = stack();
    assert_eq!(first, second);
    assert_eq!(first.as_ptr(), second.as_ptr());
    assert_eq!(
        first.as_ptr() as usize,
        stack().as_ptr() as usize,
        "a `&'static`, leaked once — not rebuilt per call"
    );
}

/// These strings are baked into shipped UI and into the mail footer:
/// the About section prints the venture name, every rendered payload
/// carries the registry and the subprocessors link, and the drift test
/// fetches `SOURCE`. Changing any of them is a user-visible edit to
/// what the venture claims about itself, so it is pinned here rather
/// than left to the vendored JSON to agree by luck.
#[test]
fn the_public_strings_are_pinned() {
    assert_eq!(VENTURE_NAME, "SupportGenius");
    assert_eq!(SOURCE, "https://factory0.ventures/stack.json");
    assert_eq!(
        SUBPROCESSORS_URL,
        "https://factory0.ventures/ventures/supportgenius/"
    );
}

/// The cheap half of the drift check, and the half that runs every
/// time. Catches the realistic failures: a hand-edited copy, a stale
/// one whose ids no longer match, an entry dropped or invented, a URL
/// downgraded from `https://`, an unrecognised status.
#[test]
fn the_vendored_entry_is_well_formed() {
    let raw: serde_json::Value = serde_json::from_str(include_str!("../src/built-with.json"))
        .expect("the vendored entry is valid JSON");
    assert_eq!(raw["id"], "FZ-008");
    assert_eq!(raw["source"], SOURCE, "the copy names where it came from");

    let uses = raw["uses"].as_array().expect("a uses array");
    assert_eq!(uses.len(), REGISTRY_IDS.len());
    for (entry, expected_id) in uses.iter().zip(REGISTRY_IDS) {
        assert_eq!(entry["id"], expected_id, "the registry's own order");
        assert!(
            entry["url"]
                .as_str()
                .is_some_and(|url| url.starts_with("https://")),
            "{expected_id}: an absolute https url"
        );
        assert!(
            matches!(entry["status"].as_str(), Some("live" | "planned")),
            "{expected_id}: status is live or planned only"
        );
        for key in ["phrase", "note"] {
            assert!(
                entry[key].as_str().is_some_and(|s| !s.trim().is_empty()),
                "{expected_id}: a non-empty {key}"
            );
        }
    }
    // The copy and the module agree: `stack()` reads this file, so a
    // disagreement here is a broken test somewhere else.
    assert_eq!(
        stack().len(),
        uses.len(),
        "the module renders every entry the file holds"
    );
}

/// The expensive half: fetch the registry and compare.
///
/// Not in the suite on purpose. This repository has no network-touching
/// tests by deliberate convention — `bin/supportgenius/tests/ssrf.rs`
/// is the note, and `FakeHttpClient` is how every outbound call is
/// exercised elsewhere — because a test that fails because a host is
/// unreachable, or a machine is offline, is a test people learn to skip.
/// So it is `#[ignore]`d and run by hand:
///
/// ```sh
/// cargo test -p supportgenius-composition --test built_with -- --ignored
/// ```
///
/// Compare as `serde_json::Value`, not as strings: the registry's
/// whitespace and key order are not ours to enforce, and a
/// re-serialised copy that only differs in formatting is not drift. If
/// the entry ever must be byte-exact — the registry publishing a
/// signature over it, say — compare the raw strings instead and drop
/// this normalisation.
#[test]
#[ignore = "hits the network: run with cargo test -p supportgenius-composition -- --ignored"]
fn the_vendored_entry_matches_the_published_registry() {
    let fetched = Command::new("curl")
        .args(["-sSfL", SOURCE])
        .output()
        .unwrap_or_else(|err| {
            panic!(
                "curl could not be run to fetch {SOURCE}; install curl or fetch the \
                 registry by hand and compare it with src/built-with.json: {err}"
            )
        });
    assert!(
        fetched.status.success(),
        "curl -sSfL {SOURCE} failed ({}): {}",
        fetched.status,
        String::from_utf8_lossy(&fetched.stderr)
    );
    let registry: serde_json::Value =
        serde_json::from_slice(&fetched.stdout).unwrap_or_else(|err| {
            panic!(
                "{SOURCE} did not return JSON ({err}); the registry's shape may have \
                 changed — check the venture page before re-vendoring"
            )
        });

    // The registry file holds every venture; the copy holds ours. Take
    // ours and add the one key the registry file does not have per
    // venture: where the vendored copy came from.
    let venture = registry["ventures"]
        .as_array()
        .unwrap_or_else(|| panic!("{SOURCE} has a `ventures` array"))
        .iter()
        .find(|venture| venture["id"] == "FZ-008")
        .unwrap_or_else(|| {
            panic!("{SOURCE} has no FZ-008 entry; the venture may have been renamed")
        })
        .clone();
    let mut expected = venture;
    expected["source"] = SOURCE.into();

    let vendored: serde_json::Value = serde_json::from_str(include_str!("../src/built-with.json"))
        .expect("the vendored entry parses");
    assert_eq!(
        vendored, expected,
        "src/built-with.json has drifted from {SOURCE}; re-vendor it (the registry's \
         order inside `uses` is authoritative — do not tidy it)"
    );
}
