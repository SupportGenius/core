//! Hand-written stand-ins for the outbound ports, compiled in only when
//! the `dev-fakes` feature is selected and activated only when
//! `SUPPORTGENIUS_DEV_FAKES` is set at boot (see `main.rs`, which logs a
//! loud warning when either happens, so a fake-backed process can never
//! be mistaken for a production boot).
//!
//! Written by hand rather than lifted from `cratefield-testing`: a
//! shipping binary must not depend on the testing crate, and these stubs
//! are a tenth of what it carries. They live **in the binary** — never in
//! the modules and never in the composition crate — so the module
//! contracts stay honest in every configuration.
//!
//! No `StubTextModel` here yet: published cratefield-core has no
//! `TextModel` port (see the landing-site comment in `main.rs`).

use async_trait::async_trait;
use cratefield_core::{Captcha, CaptchaError, MailError, Mailer, Message, SendOutcome, Verdict};

/// Accepts every mail and records it, so a developer can exercise the
/// join flow without a Resend key and read what *would* have gone out in
/// the log. Reports `Sent`, not `NotConfigured`: the flow is being
/// exercised, and the warning in `main.rs` is the honest marker that the
/// send was not real.
pub(crate) struct StubMailer;

#[async_trait]
impl Mailer for StubMailer {
    async fn send(&self, message: Message) -> Result<SendOutcome, MailError> {
        tracing::warn!(
            to = %message.to,
            subject = %message.subject,
            "StubMailer: recorded mail, did not send (dev fakes active)"
        );
        Ok(SendOutcome::Sent {
            id: "dev-fake".to_owned(),
        })
    }
}

/// Accepts the fixed token any Turnstile widget produces — including the
/// dummy token `1x00000000000000000000AA`, which is what a site
/// integration is developed against.
pub(crate) struct StubCaptcha;

#[async_trait]
impl Captcha for StubCaptcha {
    async fn verify(
        &self,
        _token: &str,
        _remote_ip: Option<&str>,
    ) -> Result<Verdict, CaptchaError> {
        tracing::warn!("StubCaptcha: accepted token without verification (dev fakes active)");
        Ok(Verdict {
            ok: true,
            reason: Some("dev-fakes: accepted without verification".to_owned()),
        })
    }
}
