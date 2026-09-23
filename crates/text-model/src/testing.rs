//! A fake [`TextModel`] double for module tests, in the style of
//! `cratefield_testing::fakes`: a scriptable response queue that answers in
//! order and then fails loudly, recorded calls cloned out from behind a
//! fixture mutex, a cheap [`Clone`] handle over a shared interior.
//!
//! It mirrors the port in this crate, so it is deleted in the same pass
//! when core publishes the real port and its fake.
//!
//! Behind the `testing` feature: the default build (the one the venture
//! links into the Worker) never carries test doubles. Module crates turn
//! the feature on from their dev-dependencies.

// Recording fixtures, not request state: every accessor locks an
// unpoisoned fixture mutex; per-method `# Panics` sections would add
// noise without information.
#![allow(clippy::missing_panics_doc)]

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

use crate::{Completion, ModelTier, Prompt, TextModel, TextModelError};

/// An in-memory [`TextModel`] that answers from a scripted queue: each
/// [`TextModel::complete`] pops the next response, and records the
/// [`Prompt`] it received so a test can assert the tier, the system prompt
/// and the JSON Schema a stage actually asked for.
///
/// Responses are plain `Result`s so every error arm is reachable from a
/// test — a `Transient` with its `retry_after`, a `Rejected` with exactly
/// the provider text the caller must survive, not just a generic failure
/// (the reason `cratefield_testing`'s `MailerMode::Error` exists).
///
/// When the script runs dry the fake answers
/// [`TextModelError::Transport`] with a fixed marker string, the way
/// `cratefield_testing::FakeHttpClient` fails loudly on an exhausted
/// script: a test that walks off the end of its own script is a bug in the
/// test, not a `NotConfigured` the pipeline should degrade on.
#[derive(Clone)]
pub struct FakeTextModel {
    inner: Arc<FakeTextModelInner>,
}

struct FakeTextModelInner {
    scripted: Mutex<VecDeque<Result<Completion, TextModelError>>>,
    prompts: Mutex<Vec<Prompt>>,
}

impl FakeTextModel {
    /// Answers with `responses` in order, then with the exhausted-script
    /// transport error.
    #[must_use]
    pub fn scripted(responses: Vec<Result<Completion, TextModelError>>) -> Self {
        Self {
            inner: Arc::new(FakeTextModelInner {
                scripted: Mutex::new(responses.into_iter().collect()),
                prompts: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Answers once with a structured completion: the parsed `json` in
    /// [`Completion::json`], the same value serialised as the text, and
    /// the model named `"fake-<tier>"`. Everything after that is the
    /// exhausted-script error. The one-response case is the common one — a
    /// stage asks for a draft, or a judgement, and that is the whole test.
    #[must_use]
    pub fn json(tier: ModelTier, json: serde_json::Value) -> Self {
        let completion =
            Completion::new(json.to_string(), format!("fake-{}", tier.name())).json(json);
        Self::scripted(vec![Ok(completion)])
    }

    /// Appends `response` to the script. For a test whose reply depends
    /// on state the kit creates after the model is wired in — a chunk id
    /// minted by ingest, say.
    pub fn push(&self, response: Result<Completion, TextModelError>) {
        self.inner
            .scripted
            .lock()
            .expect("text model lock")
            .push_back(response);
    }

    /// Every prompt recorded so far, in completion order.
    #[must_use]
    pub fn prompts(&self) -> Vec<Prompt> {
        self.inner.prompts.lock().expect("text model lock").clone()
    }
}

#[async_trait::async_trait]
impl TextModel for FakeTextModel {
    async fn complete(&self, prompt: &Prompt) -> Result<Completion, TextModelError> {
        self.inner
            .prompts
            .lock()
            .expect("text model lock")
            .push(prompt.clone());
        let next = self
            .inner
            .scripted
            .lock()
            .expect("text model lock")
            .pop_front();
        next.unwrap_or_else(|| {
            Err(TextModelError::Transport(
                "fake text model script exhausted".to_owned(),
            ))
        })
    }
}
