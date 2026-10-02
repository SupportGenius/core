//! The eval runner (issue #38): a golden set of questions over a fixture
//! corpus in a SQLite harness, scored three ways — retrieval on its own,
//! the answers the module actually returns, and a post-hoc sweep of the
//! answer threshold.
//!
//!     cargo run -p evals -- --model <fake|anthropic[:MODEL_ID]>
//!                         [--fixtures DIR] [--out FILE] [--write-baseline]
//!
//! `fake` is a deterministic retrieval-as-answer model: it cites the first
//! retrieved chunk, so its scores describe retrieval-as-answer rather than
//! model quality. Committed real-model runs live under
//! `evals/results/<date>-<model>.json`.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode, header};
use cratefield_core::{Completion, MapConfig, Prompt, Role, Statement, TextModel, TextModelError};
use cratefield_testing::{Dialect, TestHarness};
use module_support::{DEFAULT_ANSWER_THRESHOLD, Support};
use serde::Serialize;
use serde_json::{Value, json};
use time::OffsetDateTime;
use tower::ServiceExt;

mod metrics;
use metrics::{Rank, encode_query, mrr, normalize, ratio, recall_at};

const ADMIN: &str = "/v1/support/admin/tenants";
const SOURCES: &str = "/v1/support/sources";
const SEARCH: &str = "/v1/support/search";
const MESSAGES: &str = "/v1/support/messages";
const ADMIN_TOKEN: &str = "evals-admin-token-0123456789abcdef";
const ANSWERABLE: &str = "answerable";
const NEEDS_PERSON: &str = "needs_person";
const RETRIEVAL_K: usize = 10;
/// The search route's `MAX_LIMIT`. Rank lookup uses it so a gold reference
/// beyond the reported top-k still contributes its true rank to MRR
/// instead of scoring zero; recall@k is still counted at rank <= k.
const SEARCH_LIMIT: usize = 50;
/// The settings route accepts 0.0..=1.0; storing 0.0 leaves every
/// threshold to the sweep, which re-derives them from the one run.
const MIN_THRESHOLD: f64 = 0.0;
const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures");
const BASELINE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/baseline.json");

/// source id -> corpus file name, and file name -> that source's chunks.
type Sources = HashMap<String, String>;
type Corpus = HashMap<String, Vec<(String, String)>>;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("evals: {err}");
            ExitCode::FAILURE
        }
    }
}

struct Args {
    model: String,
    fixtures: PathBuf,
    out: Option<PathBuf>,
    write_baseline: bool,
}

fn parse_args() -> Result<Args, String> {
    let (mut model, mut fixtures, mut out, mut write) = (None, None, None, false);
    let mut argv = std::env::args().skip(1);
    while let Some(flag) = argv.next() {
        let mut value = || argv.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--model" => model = Some(value()?),
            "--fixtures" => fixtures = Some(PathBuf::from(value()?)),
            "--out" => out = Some(PathBuf::from(value()?)),
            "--write-baseline" => write = true,
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    Ok(Args {
        model: model.ok_or("--model is required: fake | anthropic[:ID]")?,
        fixtures: fixtures.unwrap_or_else(|| PathBuf::from(FIXTURES)),
        out,
        write_baseline: write,
    })
}

fn today_utc() -> String {
    let d = OffsetDateTime::now_utc().date();
    format!("{}-{:02}-{:02}", d.year(), u8::from(d.month()), d.day())
}

async fn run() -> Result<(), String> {
    let args = parse_args()?;
    let (model, label, model_id) = build_model(&args.model)?;
    eprintln!("evals: {label} {model_id} over {}", args.fixtures.display());

    let kit = TestHarness::with_database_and_ports(
        vec![Box::new(Support::new())],
        Dialect::Sqlite,
        move |ports| {
            ports.config = Arc::new(MapConfig::from_pairs([("ADMIN_TOKEN", ADMIN_TOKEN)]));
            ports.text_model = Some(model);
        },
    );
    let (tenant, key) = mint(&kit.router).await?;
    let sources = ingest(&kit.router, &key, &args.fixtures.join("corpus")).await?;
    let corpus = chunks(&kit, &tenant, &sources).await?;
    let prepared = prepare(&args.fixtures.join("questions.jsonl"), &corpus)?;
    eprintln!("{} sources, {} questions", sources.len(), prepared.len());

    // Retrieval: no model involved.
    let ranks = retrieval_ranks(&kit.router, &key, &prepared).await?;
    let retrieval = json!({
        "recall_at_1": recall_at(&ranks, 1),
        "recall_at_3": recall_at(&ranks, 3),
        "recall_at_6": recall_at(&ranks, 6),
        "recall_at_10": recall_at(&ranks, RETRIEVAL_K),
        "mrr": mrr(&ranks),
    });
    eprintln!("evals: retrieval {retrieval}");
    gate(retrieval["recall_at_6"].as_f64(), args.write_baseline)?;

    // Answers: one fresh conversation per question, then re-scored at every
    // threshold — no model call is repeated.
    let turns = answer_phase(&kit.router, &key, &tenant, &prepared).await?;
    let at_default = answer_metrics(&turns, f64::from(DEFAULT_ANSWER_THRESHOLD));
    let sweep: Vec<Metrics> = (0..=20)
        .map(|step| answer_metrics(&turns, f64::from(step) * 0.05))
        .collect();

    let report = json!({
        "date": today_utc(),
        "model": label,
        "model_id": model_id,
        "k": RETRIEVAL_K,
        "corpus_sources": sources.len(),
        "questions": prepared.len(),
        "retrieval": retrieval,
        "answer": { "default_threshold": at_default, "sweep": sweep },
    });
    let text = serde_json::to_string_pretty(&report).map_err(|err| err.to_string())?;
    if let Some(path) = &args.out {
        std::fs::write(path, &text).map_err(|err| format!("writing {}: {err}", path.display()))?;
    }
    println!("{text}");
    Ok(())
}

/// A question with its gold references resolved to chunk ids.
struct Prepared {
    question: String,
    label: String,
    /// Per reference, the ids of the source's chunks carrying the phrase.
    refs: Vec<Vec<String>>,
}

/// Reads `questions.jsonl` and resolves every gold phrase to the chunk ids
/// of its source. A phrase that matches nothing is a fixture error, not a
/// zero: the eval would otherwise quietly score an unanswerable question.
fn prepare(path: &Path, corpus: &Corpus) -> Result<Vec<Prepared>, String> {
    let read = |err: std::io::Error| format!("reading {}: {err}", path.display());
    let text = std::fs::read_to_string(path).map_err(read)?;
    let lines = text
        .lines()
        .enumerate()
        .map(|(n, l)| (n + 1, l.trim()))
        .filter(|(_, l)| !l.is_empty());
    lines
        .map(|(line, raw)| {
            let q: Value = serde_json::from_str(raw)
                .map_err(|err| format!("{}:{line}: {err}", path.display()))?;
            let id = q["id"].as_str().unwrap_or("?");
            let gold = q["gold"].as_array().cloned().unwrap_or_default();
            let mut refs = Vec::with_capacity(gold.len());
            for g in &gold {
                let source = g["source"].as_str().unwrap_or_default();
                let phrase = normalize(g["phrase"].as_str().unwrap_or_default());
                let bodies = corpus
                    .get(source)
                    .ok_or_else(|| format!("question {id}: {source:?} not in corpus"))?;
                let ids: Vec<String> = bodies
                    .iter()
                    .filter(|(_, body)| !phrase.is_empty() && normalize(body).contains(&phrase))
                    .map(|(id, _)| id.clone())
                    .collect();
                if ids.is_empty() {
                    return Err(format!(
                        "question {id}: {:?} from {source:?} matches no chunk",
                        g["phrase"]
                    ));
                }
                refs.push(ids);
            }
            Ok(Prepared {
                question: q["question"].as_str().unwrap_or_default().to_owned(),
                label: q["label"].as_str().unwrap_or_default().to_owned(),
                refs,
            })
        })
        .collect()
}

async fn mint(r: &Router) -> Result<(String, String), String> {
    let body = json!({ "name": "Evals" }).to_string();
    let (status, reply) = call(r, Method::POST, ADMIN, Some(ADMIN_TOKEN), Some(&body)).await?;
    if status != StatusCode::CREATED {
        return Err(format!("minting a tenant answered {status}: {reply}"));
    }
    match (reply["tenant_id"].as_str(), reply["api_key"].as_str()) {
        (Some(tenant), Some(key)) => Ok((tenant.to_owned(), key.to_owned())),
        _ => Err(format!("mint response has no tenant_id/api_key: {reply}")),
    }
}

/// Ingests every `corpus/*.md` in sorted order — title and `external_id`
/// both the file name — returning source id -> file name.
async fn ingest(r: &Router, key: &str, dir: &Path) -> Result<Sources, String> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)
        .map_err(|err| format!("reading {}: {err}", dir.display()))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "md"))
        .collect();
    paths.sort();
    let mut sources = Sources::new();
    for path in paths {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| format!("bad corpus file {}", path.display()))?;
        let text = std::fs::read_to_string(&path)
            .map_err(|err| format!("reading {}: {err}", path.display()))?;
        let body = json!({ "title": name, "text": text, "external_id": name }).to_string();
        let (status, reply) = call(r, Method::POST, SOURCES, Some(key), Some(&body)).await?;
        if status != StatusCode::CREATED && status != StatusCode::OK {
            return Err(format!("ingesting {name} answered {status}: {reply}"));
        }
        let id = reply["source_id"]
            .as_str()
            .ok_or("ingest response has no source_id")?;
        sources.insert(id.to_owned(), name.to_owned());
    }
    Ok(sources)
}

/// Reads this tenant's chunks back, grouped by corpus file name. Selecting
/// all rows (scoped to the tenant) keeps file bodies out of any SQL string.
async fn chunks(kit: &TestHarness, tenant: &str, sources: &Sources) -> Result<Corpus, String> {
    let sql =
        "SELECT id, source_id, body FROM sg_chunks WHERE tenant_id = ? ORDER BY source_id, ordinal";
    let rows = kit
        .db
        .query(&Statement::with_values(sql, vec![tenant.to_owned().into()]))
        .await
        .map_err(|err| format!("reading sg_chunks: {err}"))?;
    let mut corpus = Corpus::new();
    for row in &rows.rows {
        let id = row.get::<String>("id").ok_or("chunk row has no id")?;
        let source = row
            .get::<String>("source_id")
            .ok_or("chunk row has no source_id")?;
        let body = row.get::<String>("body").ok_or("chunk row has no body")?;
        if let Some(name) = sources.get(source.as_str()) {
            corpus.entry(name.clone()).or_default().push((id, body));
        }
    }
    Ok(corpus)
}

/// One rank per gold reference of every answerable question, in question
/// order: `Some(k)` when the reference's first chunk lands at rank `k`.
async fn retrieval_ranks(r: &Router, key: &str, items: &[Prepared]) -> Result<Vec<Rank>, String> {
    let mut ranks = Vec::new();
    for item in items.iter().filter(|item| item.label == ANSWERABLE) {
        let uri = format!(
            "{SEARCH}?q={}&limit={SEARCH_LIMIT}",
            encode_query(&item.question)
        );
        let (status, body) = call(r, Method::GET, &uri, Some(key), None).await?;
        if status != StatusCode::OK {
            return Err(format!(
                "search {} answered {status}: {body}",
                item.question
            ));
        }
        let missing = || format!("search {} has no results array", item.question);
        let hits = body["results"].as_array().ok_or_else(missing)?;
        let ids: Vec<&str> = hits
            .iter()
            .filter_map(|hit| hit["chunk_id"].as_str())
            .collect();
        for gold in &item.refs {
            ranks.push(
                ids.iter()
                    .position(|id| gold.iter().any(|g| g == id))
                    .map(|at| at + 1),
            );
        }
    }
    Ok(ranks)
}

/// The regression gate: `recall@6` may not fall below the committed
/// baseline, and `--write-baseline` commits the measured value instead.
fn gate(measured: Option<f64>, write_baseline: bool) -> Result<(), String> {
    let measured = measured.ok_or("no references to score: recall@6 is undefined")?;
    if write_baseline {
        let body = serde_json::to_string_pretty(&json!({ "recall_at_6": measured }))
            .map_err(|err| err.to_string())?;
        std::fs::write(BASELINE, format!("{body}\n"))
            .map_err(|err| format!("writing baseline: {err}"))?;
        eprintln!("evals: wrote baseline recall@6 = {measured:.6}");
        return Ok(());
    }
    let text =
        std::fs::read_to_string(BASELINE).map_err(|err| format!("reading baseline: {err}"))?;
    let body =
        serde_json::from_str::<Value>(&text).map_err(|err| format!("parsing baseline: {err}"))?;
    let baseline = body["recall_at_6"]
        .as_f64()
        .ok_or("baseline.json: recall_at_6 is not a number")?;
    if measured < baseline - 1e-9 {
        return Err(format!(
            "recall@6 regressed: {measured:.6} < baseline {baseline:.6}"
        ));
    }
    eprintln!("evals: recall@6 {measured:.6} >= baseline {baseline:.6}");
    Ok(())
}

/// One turn, with the citation counts already reduced: they depend only on
/// the answer, so every threshold reuses the one run.
struct Turn {
    label: String,
    answered: bool,
    handoff: bool,
    conf_pct: i64,
    cited: usize,
    hit: usize,
    refs: usize,
    covered: usize,
}

#[derive(Serialize)]
struct Metrics {
    threshold: f64,
    answer_rate: Option<f64>,
    wrong_answer_rate: Option<f64>,
    citation_precision: Option<f64>,
    citation_recall: Option<f64>,
    handoff_precision: Option<f64>,
}

async fn answer_phase(
    r: &Router,
    key: &str,
    tenant: &str,
    items: &[Prepared],
) -> Result<Vec<Turn>, String> {
    // The threshold only ever guards the confidence comparison, so the run
    // stores the minimum and the sweep re-applies every other value.
    let settings = format!("{ADMIN}/{tenant}/settings");
    let body = json!({ "answer_threshold": MIN_THRESHOLD }).to_string();
    let (status, reply) = call(r, Method::PUT, &settings, Some(ADMIN_TOKEN), Some(&body)).await?;
    if status != StatusCode::OK {
        return Err(format!("setting the threshold answered {status}: {reply}"));
    }

    let mut turns = Vec::with_capacity(items.len());
    for item in items {
        let body = json!({ "message": item.question }).to_string();
        let (status, reply) = call(r, Method::POST, MESSAGES, Some(key), Some(&body)).await?;
        if status != StatusCode::OK {
            return Err(format!(
                "asking {} answered {status}: {reply}",
                item.question
            ));
        }
        let cited: Vec<&str> = reply["citations"].as_array().map_or_else(Vec::new, |list| {
            list.iter().filter_map(|c| c["chunk_id"].as_str()).collect()
        });
        let gold: HashSet<&str> = item.refs.iter().flatten().map(String::as_str).collect();
        turns.push(Turn {
            label: item.label.clone(),
            answered: reply["outcome"] == "answered",
            handoff: reply["outcome"] == "handoff",
            conf_pct: pct_of(reply["confidence"].as_f64().unwrap_or(0.0)),
            cited: cited.len(),
            hit: cited.iter().filter(|id| gold.contains(*id)).count(),
            refs: item.refs.len(),
            covered: item
                .refs
                .iter()
                .filter(|ids| ids.iter().any(|id| cited.contains(&id.as_str())))
                .count(),
        });
    }
    Ok(turns)
}

/// The module's whole-percent grain (`answer::confidence_pct`), mirrored so
/// the sweep's comparison agrees with the decision the module made.
#[allow(
    clippy::cast_possible_truncation,
    reason = "clamped to 0..=100 before the cast"
)]
fn pct_of(fraction: f64) -> i64 {
    (fraction * 100.0).round().clamp(0.0, 100.0) as i64
}

/// The answer metrics at one threshold: a turn counts as answered only when
/// the module said so *and* its whole-percent confidence reaches it.
fn answer_metrics(turns: &[Turn], threshold: f64) -> Metrics {
    let at = pct_of(threshold);
    let answered = |turn: &Turn| turn.answered && turn.conf_pct >= at;
    let (mut cited, mut hit, mut refs, mut covered) = (0, 0, 0, 0);
    let (mut answers, mut wrong, mut wrong_total) = (0, 0, 0);
    let (mut handoffs, mut person) = (0, 0);
    for turn in turns {
        let yes = answered(turn);
        answers += usize::from(yes);
        if turn.label == ANSWERABLE {
            if yes {
                cited += turn.cited;
                hit += turn.hit;
                refs += turn.refs;
                covered += turn.covered;
            }
        } else {
            wrong_total += 1;
            wrong += usize::from(yes);
        }
        handoffs += usize::from(turn.handoff);
        person += usize::from(turn.handoff && turn.label == NEEDS_PERSON);
    }
    Metrics {
        threshold: f64::from(u32::try_from(at).unwrap_or(0)) / 100.0,
        answer_rate: ratio(answers, turns.len()),
        wrong_answer_rate: ratio(wrong, wrong_total),
        citation_precision: ratio(hit, cited),
        citation_recall: ratio(covered, refs),
        handoff_precision: ratio(person, handoffs),
    }
}

/// The retrieval-as-answer baseline: it cites the first `[chunk_id] body`
/// line the prompt embeds, with no notion of whether it answers the
/// question — exactly what the retrieval numbers alone would say.
struct RetrievalAsAnswer;

const FAKE_ID: &str = "retrieval-as-answer";

#[async_trait]
impl TextModel for RetrievalAsAnswer {
    async fn complete(&self, prompt: &Prompt) -> Result<Completion, TextModelError> {
        let ask = prompt
            .messages
            .iter()
            .rev()
            .find(|turn| turn.role == Role::User);
        // Everything after the context header is one `[id] body` per line.
        let context = ask.map_or("", |turn| turn.content.as_str());
        let context = context
            .split_once("chunk ids:\n")
            .map_or("", |(_, rest)| rest);
        for line in context.lines() {
            let Some((id, body)) = line
                .strip_prefix('[')
                .and_then(|rest| rest.split_once("] "))
            else {
                continue;
            };
            if id.is_empty() {
                continue;
            }
            let sentence = first_sentence(body);
            let value = json!({ "answer": sentence, "citations": [{ "chunk_id": id, "quote": sentence }], "confidence": 0.9 });
            return Ok(Completion::new(value.to_string(), FAKE_ID).json(value));
        }
        // Nothing retrieved: an honest non-answer, which the module turns
        // into a handoff — exactly what should happen.
        let value =
            json!({ "answer": "No context retrieved.", "citations": [], "confidence": 0.0 });
        Ok(Completion::new(value.to_string(), FAKE_ID).json(value))
    }
}

/// The first sentence of a body, capped so a chunk without punctuation
/// does not quote itself whole.
fn first_sentence(body: &str) -> String {
    let head = body
        .trim()
        .split_once(". ")
        .map_or(body.trim(), |(head, _)| head);
    let head: String = head.chars().take(200).collect();
    if head.trim().is_empty() {
        return "See the cited source.".to_owned();
    }
    head
}

/// The model for `--model`, with the report's label and concrete model id.
fn build_model(spec: &str) -> Result<(Arc<dyn TextModel>, String, String), String> {
    if spec == "fake" {
        return Ok((
            Arc::new(RetrievalAsAnswer),
            "fake".to_owned(),
            FAKE_ID.to_owned(),
        ));
    }
    // Exactly `anthropic` or `anthropic:<non-empty ID>`; anything else
    // (e.g. `anthropicgpt`) is a bad spec, not an id.
    let bad = || format!("unknown --model {spec:?}: expected fake or anthropic[:MODEL_ID]");
    let id = match spec.strip_prefix("anthropic") {
        Some("") => None,
        Some(rest) => Some(
            rest.strip_prefix(':')
                .filter(|id| !id.is_empty())
                .ok_or_else(bad)?,
        ),
        None => return Err(bad()),
    };
    // The adapter reaches the network over the native ports; the spec id,
    // or `ANTHROPIC_MODEL`, or the adapter default.
    let http = Arc::new(cratefield_runtime_native::ReqwestClient::new());
    let clock = Arc::new(cratefield_runtime_native::TokioClock);
    let (model, id) = if let Some(id) = id {
        let key = std::env::var("ANTHROPIC_API_KEY").ok();
        let model = cratefield_adapter_anthropic::Anthropic::new(http, clock, key, id);
        (model, id.to_owned())
    } else {
        let default = cratefield_adapter_anthropic::DEFAULT_MODEL;
        let id = std::env::var("ANTHROPIC_MODEL").unwrap_or_else(|_| default.to_owned());
        let model = cratefield_adapter_anthropic::Anthropic::from_env(http, clock);
        (model, id)
    };
    Ok((Arc::new(model), "anthropic".to_owned(), id))
}

/// One buffered request against the in-process router. Every route here
/// answers JSON, success or problem+json.
async fn call(
    r: &Router,
    method: Method,
    uri: &str,
    bearer: Option<&str>,
    body: Option<&str>,
) -> Result<(StatusCode, Value), String> {
    let mut request = Request::builder().method(method).uri(uri);
    if let Some(key) = bearer {
        request = request.header(header::AUTHORIZATION, format!("Bearer {key}"));
    }
    let sent = match body {
        Some(payload) => {
            request = request.header(header::CONTENT_TYPE, "application/json");
            request
                .body(Body::from(payload.to_owned()))
                .map_err(|err| err.to_string())?
        }
        None => request.body(Body::empty()).map_err(|err| err.to_string())?,
    };
    let response = r
        .clone()
        .oneshot(sent)
        .await
        // `Router`'s error type is `Infallible`, so oneshot cannot fail here.
        .expect("the in-process router answers");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1 << 20)
        .await
        .map_err(|err| err.to_string())?;
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).map_err(|err| err.to_string())?
    };
    Ok((status, value))
}
