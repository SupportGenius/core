//! [`FakeTextModel`]: the test double for the [`TextModel`] port, in the
//! house style of `cratefield_testing`'s `FakeMailer` — an internal
//! `Mutex`, a mode or script, accessors. Public and **unconditionally**
//! compiled, not feature-gated: `cargo test --workspace` drives it here,
//! and a venture composing this module drives the same fake instead of
//! writing its own.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::text_model::{TextCompletion, TextModel, TextModelError, TextRequest};

/// One step of a [`FakeTextModel::scripted`] run, consumed per call.
#[derive(Debug, Clone)]
pub enum ScriptedReply {
    /// Hand this raw text back as the completion.
    Reply(String),
    /// Answer as if no model were configured.
    NotConfigured,
    /// Answer as if the model could not answer right now.
    Transient(String),
    /// Fail with [`TextModelError::Invalid`] — a model that answered but
    /// whose answer the port itself judges unusable. The route-level
    /// `text-model-invalid-answer` 502 is otherwise reachable only through
    /// the module's own parser (script raw text with [`ScriptedReply::Reply`]
    /// for that); this step covers the port-reported arm.
    Invalid(String),
}

#[derive(Debug)]
enum Step {
    Reply(String),
    NotConfigured,
    Transient(String),
    Invalid(String),
}

impl From<ScriptedReply> for Step {
    fn from(scripted: ScriptedReply) -> Self {
        match scripted {
            ScriptedReply::Reply(text) => Self::Reply(text),
            ScriptedReply::NotConfigured => Self::NotConfigured,
            ScriptedReply::Transient(detail) => Self::Transient(detail),
            ScriptedReply::Invalid(detail) => Self::Invalid(detail),
        }
    }
}

/// What the fake answers with, next call onwards.
#[derive(Debug)]
enum Mode {
    /// The same raw text for every call — the common case, one turn per
    /// test.
    Standing(String),
    /// One step per call. A script that runs dry is a test bug and panics
    /// rather than answering with something the test did not ask for.
    Script(VecDeque<Step>),
    NotConfigured,
    Transient(String),
}

struct Inner {
    mode: Mutex<Mode>,
    requests: Mutex<Vec<TextRequest>>,
}

/// A `TextModel` that answers from a script and records every request, so
/// a test can assert what the module asked for.
#[derive(Clone)]
pub struct FakeTextModel {
    inner: Arc<Inner>,
}

impl FakeTextModel {
    /// Always answers with `raw` (typically the JSON answer the module
    /// should parse).
    #[must_use]
    pub fn replying(raw: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(Inner {
                mode: Mutex::new(Mode::Standing(raw.into())),
                requests: Mutex::new(Vec::new()),
            }),
        }
    }

    /// The convenience constructor for the common case: a reply shaped
    /// exactly like the module's schema — `answer`, `citations` as
    /// `(chunk id, quote)` pairs, and a `confidence` in 0.0..=1.0.
    #[must_use]
    pub fn answering(
        answer: impl Into<String>,
        citations: &[(&str, &str)],
        confidence: f32,
    ) -> Self {
        // `chunk_id`, snake_case: the fake replies in exactly the shape the
        // issue's model schema specifies, which is what the parser accepts.
        let citations: Vec<serde_json::Value> = citations
            .iter()
            .map(|(chunk_id, quote)| serde_json::json!({ "chunk_id": chunk_id, "quote": quote }))
            .collect();
        Self::replying(
            serde_json::json!({
                "answer": answer.into(),
                "citations": citations,
                "confidence": confidence,
            })
            .to_string(),
        )
    }

    /// Answers `NotConfigured` for every call.
    #[must_use]
    pub fn not_configured() -> Self {
        Self {
            inner: Arc::new(Inner {
                mode: Mutex::new(Mode::NotConfigured),
                requests: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Answers `Transient` for every call.
    #[must_use]
    pub fn transient(detail: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(Inner {
                mode: Mutex::new(Mode::Transient(detail.into())),
                requests: Mutex::new(Vec::new()),
            }),
        }
    }

    /// One step per call, in order.
    #[must_use]
    pub fn scripted(replies: Vec<ScriptedReply>) -> Self {
        Self {
            inner: Arc::new(Inner {
                mode: Mutex::new(Mode::Script(replies.into_iter().map(Step::from).collect())),
                requests: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Switches the mode (e.g. degrade to `Transient` mid-test, or come
    /// back up after one).
    ///
    /// # Panics
    ///
    /// Only if the fake's internal lock is poisoned, which cannot happen
    /// short of a panic while holding it.
    pub fn set_replying(&self, raw: impl Into<String>) {
        *self.inner.mode.lock().expect("fake model lock") = Mode::Standing(raw.into());
    }

    /// How many times the module asked.
    ///
    /// # Panics
    ///
    /// Only if the fake's internal lock is poisoned, which cannot happen
    /// short of a panic while holding it.
    #[must_use]
    pub fn calls(&self) -> usize {
        self.inner.requests.lock().expect("fake model lock").len()
    }

    /// The most recent request, so a test can assert the tier, the prompt
    /// and the budget the module chose.
    ///
    /// # Panics
    ///
    /// Only if the fake's internal lock is poisoned, which cannot happen
    /// short of a panic while holding it.
    #[must_use]
    pub fn last_request(&self) -> Option<TextRequest> {
        self.inner
            .requests
            .lock()
            .expect("fake model lock")
            .last()
            .cloned()
    }
}

#[async_trait]
impl TextModel for FakeTextModel {
    async fn complete(&self, request: TextRequest) -> Result<TextCompletion, TextModelError> {
        self.inner
            .requests
            .lock()
            .expect("fake model lock")
            .push(request);
        let step = match &mut *self.inner.mode.lock().expect("fake model lock") {
            Mode::Standing(raw) => Some(Step::Reply(raw.clone())),
            Mode::Script(script) => script.pop_front(),
            Mode::NotConfigured => return Err(TextModelError::NotConfigured),
            Mode::Transient(detail) => {
                return Err(TextModelError::Transient(detail.clone()));
            }
        };
        match step {
            Some(Step::Reply(text)) => Ok(TextCompletion { text }),
            Some(Step::NotConfigured) => Err(TextModelError::NotConfigured),
            Some(Step::Transient(detail)) => Err(TextModelError::Transient(detail)),
            Some(Step::Invalid(detail)) => Err(TextModelError::Invalid(detail)),
            None => panic!(
                "FakeTextModel::scripted ran dry after {} calls; give the script one more step",
                self.calls()
            ),
        }
    }
}
