//! Issue #12 acceptance for the composition's own mail theme: the committed
//! `mail-theme.json` parses into a `MailTheme`, both waitlist mails render
//! in supportgeni.us's tokens, and a real join through the harness sends the
//! themed confirmation (logo, button colour, contact — not the module's
//! neutral default).

use axum::http::{Method, StatusCode};
use cratefield_core::{Rendered, Template};
use cratefield_testing::{TestHarness, request};
use serde_json::json;
use supportgenius_composition::{mail_theme, waitlist};

/// The themed template with `id`, exactly as [`supportgenius_composition`]
/// registers it on the harness builder.
fn themed(id: &str) -> Box<dyn Template> {
    cratefield_module_waitlist::themed_templates(&mail_theme())
        .into_iter()
        .find(|(template_id, _)| template_id == id)
        .unwrap_or_else(|| panic!("{id} registered"))
        .1
}

/// The list is a single product whose slug is the venture's own name, so the
/// mail reads "the SupportGenius waitlist" — asserted here so a wording
/// change in the module is caught next to the theme, not only upstream.
fn confirm_data() -> serde_json::Value {
    json!({
        "venture": "supportgenius",
        "product": "supportgenius",
        "email": "ada@example.com",
        "confirm_url": "https://supportgeni.us/v1/waitlist/confirm?token=abc123",
    })
}

fn confirmed_data() -> serde_json::Value {
    json!({
        "venture": "supportgenius",
        "product": "supportgenius",
        "email": "ada@example.com",
        "position": 42,
        "status_url": "https://supportgeni.us/v1/waitlist/status?token=def456",
    })
}

#[test]
fn mail_theme_reads_the_committed_json() {
    let theme = mail_theme();
    assert_eq!(theme.brand_name, "SupportGenius");
    assert_eq!(theme.site_url, "https://supportgeni.us");
    assert_eq!(theme.contact.as_deref(), Some("hello@supportgeni.us"));
    assert_eq!(
        theme.logo_url.as_deref(),
        Some("https://supportgeni.us/assets/email/logo-64.png")
    );
    // The light scheme's accent/button are the site's, not the neutral
    // defaults: a missing key would have left `MailTheme::default`'s greys.
    assert_eq!(theme.light.button, "#5b9dff");
    assert_eq!(theme.dark.button, "#5b9dff");
}

#[test]
fn confirm_mail_snapshots() {
    let rendered = themed("waitlist/confirm")
        .render(&confirm_data(), "en")
        .expect("renders");
    insta::assert_snapshot!("confirm_subject", rendered.subject);
    insta::assert_snapshot!("confirm_html", rendered.html);
    insta::assert_snapshot!("confirm_text", rendered.text);
}

#[test]
fn confirmed_mail_snapshots() {
    let rendered = themed("waitlist/confirmed")
        .render(&confirmed_data(), "en")
        .expect("renders");
    insta::assert_snapshot!("confirmed_subject", rendered.subject);
    insta::assert_snapshot!("confirmed_html", rendered.html);
    insta::assert_snapshot!("confirmed_text", rendered.text);
}

/// The theme actually reaches the rendered mail: the logo it names, the
/// button colour the site uses, and the contact address in the footer. The
/// snapshots pin the whole document; these pin the few facts a reader of
/// this diff cares about.
#[test]
fn themed_mail_carries_the_logo_button_and_contact() {
    for (id, data) in [
        ("waitlist/confirm", confirm_data()),
        ("waitlist/confirmed", confirmed_data()),
    ] {
        let Rendered { html, text, .. } = themed(id).render(&data, "en").expect("renders");
        assert!(
            html.contains("https://supportgeni.us/assets/email/logo-64.png"),
            "{id}: logo url in html"
        );
        assert!(html.contains("#5b9dff"), "{id}: button colour in html");
        assert!(
            html.contains("hello@supportgeni.us"),
            "{id}: contact in html"
        );
        assert!(!text.trim().is_empty(), "{id}: text part is non-empty");
    }
}

/// The theme survives the whole path the venture uses: a join posted to a
/// harness whose builder registered the composition's themed templates
/// (the call `modules_with` makes) sends a confirmation whose html is
/// supportgeni.us's, logo and all.
#[pollster::test]
async fn a_join_sends_the_themed_confirmation() {
    let kit = TestHarness::with_builder(
        vec![Box::new(waitlist())],
        |builder| builder.templates(cratefield_module_waitlist::themed_templates(&mail_theme())),
        |_| {},
    );
    let response = request(
        &kit.router,
        Method::POST,
        "/v1/waitlist",
        Some(
            &json!({
                "email": "ada@example.com",
                "product": "supportgenius",
                "captchaToken": "x",
            })
            .to_string(),
        ),
    )
    .await;
    assert_eq!(
        response.status,
        StatusCode::ACCEPTED,
        "{:?}",
        response.body()
    );
    // The join's confirmation is sent off the request path, through the
    // `Defer` port; drain it before looking at what the mailer recorded.
    kit.defer.drain().await;
    let sent = kit.mailer.sent();
    assert_eq!(sent.len(), 1, "one confirmation mail sent");
    assert!(
        sent[0]
            .html
            .contains("https://supportgeni.us/assets/email/logo-64.png"),
        "the join mail wears the composition's theme: {}",
        sent[0].html
    );
}
