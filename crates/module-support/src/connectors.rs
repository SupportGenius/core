//! Connectors (issue #29): cron-driven source syncing from a sitemap, a
//! URL prefix or a GitHub repository. A connector is a crawl root; the
//! fetches it produces are outbox jobs — one URL per job — drained by
//! [`crate::Support`]'s `Module::scheduled` hook and, opportunistically,
//! right after the connector is created (through the `Defer` port).
//!
//! The invariants, mirroring `module-escalation`'s pipeline:
//!
//! - **One URL per job, one batch per outcome.** Whatever a fetch does —
//!   re-index a page, record validators, delete a page that vanished,
//!   enqueue the URLs it discovered — commits together with the
//!   completion of its own outbox row, so a crash can re-run a job but
//!   never strand a URL between states.
//! - **The allowlist is absolute.** A job fetches only a URL its
//!   connector's kind allows: the sitemap's own scheme and authority, the
//!   URL prefix, or `api.github.com` under the configured owner/repo.
//!   Anything else in a payload — however it got there — is skipped
//!   before any network call. This is also the Workers SSRF story:
//!   Cloudflare `fetch` cannot reach private networks at all, and the
//!   allowlist additionally confines the worker to the tenant's own
//!   domain (see [`ConnectorConfig::allows`]).
//! - **Effects are idempotent, not exactly-once.** There is no inbox:
//!   every effect is an upsert or a delete keyed by the URL (or by the
//!   `external_id` it maps to), so a job that runs twice writes the same
//!   rows twice and nothing else. The outbox's at-least-once delivery is
//!   therefore safe without a second table.
//!
//! Caps, with their hard maxima (clamped at connector creation):
//! `max_pages` — rows the connector may hold in `sg_ingest_pages`,
//! sources and navigation rows (sitemaps, GitHub trees) alike, default
//! 200, at most 2000; `max_bytes` — per-response body bound, sent as the
//! request's [`HttpPolicy`] and re-checked before extraction, default
//! 1 MiB, at most core's 4 MiB port ceiling; `max_depth` — link hops
//! from the seed, default 3, at most 5.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;

use cratefield_core::{
    Clock, Config, Database, DbError, Defer, HttpClient, HttpError, HttpPolicy, IdGen, Outbox,
    OutboxRecord,
};

use crate::chunk::{Chunk, Chunker};
use crate::handlers::{MAX_TEXT_BYTES, decode_entities, html_to_text};
use crate::store::{self, ConnectorRow, PageRow, SourceRow, StoredChunk};

/// The outbox table this module owns (the migration carries the generated
/// DDL, pasted verbatim from core's `Outbox::create_table_sql`).
pub(crate) const OUTBOX_TABLE: &str = "sg_ingest_outbox";

/// The only topic on this outbox: fetch exactly one URL.
pub(crate) const TOPIC_FETCH: &str = "fetch";

/// How many claim/drain sweeps `scheduled` and a deferred sweep run at
/// most before leaving the rest to the next tick (the same bound as
/// `module-escalation`'s pipeline).
pub(crate) const MAX_SWEEPS: u32 = 8;

/// How many rows one sweep claims at once.
pub(crate) const SWEEP_LIMIT: u64 = 25;

// Caps, in the (default, hard maximum) pairs the route documents.
/// Rows per connector: the page cap's default and maximum.
pub(crate) const DEFAULT_MAX_PAGES: i64 = 200;
pub(crate) const MAX_MAX_PAGES: i64 = 2000;
/// Per-response byte bound: the default, and core's own port ceiling —
/// a connector may lower it, never raise it.
pub(crate) const DEFAULT_MAX_BYTES: i64 = 1024 * 1024;
#[allow(clippy::cast_possible_wrap)] // 4 MiB, nowhere near i64::MAX
pub(crate) const MAX_MAX_BYTES: i64 = cratefield_core::MAX_RESPONSE_BYTES as i64;
/// Link hops from the seed: the depth cap's default and maximum.
pub(crate) const DEFAULT_MAX_DEPTH: i64 = 3;
pub(crate) const MAX_MAX_DEPTH: i64 = 5;

/// How long one drainer's lease on an outbox row lasts.
const LEASE_SECS: u64 = 300;

/// Transient-failure backoff: 30 s base, doubling, 15 min ceiling, and a
/// five-attempt budget — after which the job is dropped and the next
/// scheduled re-sync picks the URL up again from a clean slate.
/// Deliberately constants, not config knobs: a connector that needs
/// different numbers is a connector the operator should reshape, not
/// retune.
const RETRY_BASE_SECS: u64 = 30;
const RETRY_CAP_SECS: u64 = 900;
const RETRY_MAX_ATTEMPTS: i64 = 5;

const GITHUB_API: &str = "https://api.github.com";
/// GitHub's API refuses requests without a User-Agent, and identifies
/// integrations by one: a static product token, never a credential.
const GITHUB_USER_AGENT: &str = concat!("supportgenius/", env!("CARGO_PKG_VERSION"));

/// The three crawl roots a connector can be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// A sitemap (or sitemap index) at a fixed URL.
    Sitemap,
    /// One seed page plus every same-origin link under its prefix.
    UrlPrefix,
    /// Files of one GitHub repository, filtered by a path glob.
    Github,
}

impl Kind {
    pub(crate) fn parse(raw: &str) -> Option<Self> {
        match raw {
            "sitemap" => Some(Self::Sitemap),
            "url_prefix" => Some(Self::UrlPrefix),
            "github" => Some(Self::Github),
            _ => None,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Sitemap => "sitemap",
            Self::UrlPrefix => "url_prefix",
            Self::Github => "github",
        }
    }
}

/// The connector kind's own configuration, stored as JSON in
/// `sg_connectors.config`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum ConnectorConfig {
    /// `{"url": …}` — the sitemap URL, or the prefix's seed page.
    Web { url: String },
    /// `{"owner": …, "repo": …, "path_glob": …, "ref": …}` — `path_glob`
    /// defaults to every file, `ref` to the repository's default branch.
    Github {
        owner: String,
        repo: String,
        path_glob: Option<String>,
        r#ref: Option<String>,
    },
}

impl ConnectorConfig {
    /// The URL the connector's first fetch job fetches.
    pub(crate) fn seed_url(&self) -> String {
        match self {
            Self::Web { url } => url.clone(),
            Self::Github {
                owner, repo, r#ref, ..
            } => format!(
                "{GITHUB_API}/repos/{owner}/{repo}/git/trees/{}?recursive=1",
                encode_uri_path(r#ref.as_deref().unwrap_or("HEAD"))
            ),
        }
    }

    /// Whether this connector may fetch `candidate` — the allowlist, and
    /// a function of the kind as much as the config. A sitemap connector
    /// fetches only URLs on the sitemap's own scheme and authority (every
    /// `<loc>` is fair game on the host that published the sitemap); a
    /// URL-prefix connector only the seed itself and URLs under it (a
    /// missing trailing slash on the prefix counts as one, so
    /// `https://example.com/docs` covers `…/docs/` and its children but
    /// not `…/docs-other`); a GitHub connector only the repository's own
    /// `api.github.com` paths, over https.
    ///
    /// Off-allowlist URLs are skipped before any network call. On Workers
    /// that, plus the platform's own refusal to fetch private networks,
    /// is the whole SSRF story. On the native binary the hardened client's
    /// vetting remains, but a *public* redirect target the connector did
    /// not name would still be followed, and the final body indexed under
    /// the URL the connector named: the [`HttpClient`] port at the pinned
    /// core revision exposes neither a no-follow policy nor the response's
    /// final URL, so the connector cannot see that a hop happened. That
    /// residual is accepted here — the operator's own seed names every
    /// host whose content can be indexed, and a same-host redirect (the
    /// common case) changes nothing.
    pub(crate) fn allows(&self, kind: Kind, candidate: &str) -> bool {
        match (kind, self) {
            (Kind::Sitemap, Self::Web { url }) => same_scheme_and_authority(url, candidate),
            (Kind::UrlPrefix, Self::Web { url }) => {
                same_scheme_and_authority(url, candidate)
                    && (candidate == url
                        || candidate.starts_with(&format!("{}/", url.trim_end_matches('/'))))
            }
            (Kind::Github, Self::Github { owner, repo, .. }) => {
                candidate.starts_with(&format!("{GITHUB_API}/repos/{owner}/{repo}/"))
                    && candidate
                        .parse::<http::Uri>()
                        .is_ok_and(|uri| uri.scheme_str() == Some("https"))
            }
            // A config that does not match the row's kind is a corrupted
            // row; nothing is fetchable.
            _ => false,
        }
    }
}

fn same_scheme_and_authority(seed: &str, candidate: &str) -> bool {
    match (seed.parse::<http::Uri>(), candidate.parse::<http::Uri>()) {
        (Ok(seed), Ok(candidate)) => {
            seed.scheme_str().map(str::to_ascii_lowercase)
                == candidate.scheme_str().map(str::to_ascii_lowercase)
                && seed.authority().map(|a| a.as_str().to_ascii_lowercase())
                    == candidate
                        .authority()
                        .map(|a| a.as_str().to_ascii_lowercase())
        }
        _ => false,
    }
}

/// What a fetch job does with the body it gets back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Role {
    /// Sitemap XML: `<loc>` entries are discovered, nothing is indexed.
    Sitemap,
    /// An HTML or text page: indexed, same-origin links discovered.
    Page,
    /// A GitHub tree listing: matching blobs are discovered, nothing is
    /// indexed.
    GithubTree,
    /// A GitHub file's raw contents: indexed, nothing discovered.
    GithubFile,
}

impl Role {
    pub(crate) fn parse(raw: &str) -> Option<Self> {
        match raw {
            "sitemap" => Some(Self::Sitemap),
            "page" => Some(Self::Page),
            "github_tree" => Some(Self::GithubTree),
            "github_file" => Some(Self::GithubFile),
            _ => None,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Sitemap => "sitemap",
            Self::Page => "page",
            Self::GithubTree => "github_tree",
            Self::GithubFile => "github_file",
        }
    }
}

/// One fetch job's payload: the JSON an `sg_ingest_outbox` row carries.
/// The connector and tenant travel along so a job is self-contained and
/// every downstream query is tenant-scoped before the connector row is
/// even loaded.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct FetchJob {
    pub connector_id: String,
    pub tenant_id: String,
    pub url: String,
    pub depth: i64,
    pub role: String,
}

impl FetchJob {
    fn new(connector: &ConnectorRow, url: String, depth: i64, role: Role) -> Self {
        Self {
            connector_id: connector.id.clone(),
            tenant_id: connector.tenant_id.clone(),
            url,
            depth,
            role: role.as_str().to_owned(),
        }
    }

    pub(crate) fn role(&self) -> Option<Role> {
        Role::parse(&self.role)
    }

    /// The outbox row's `subject`: connector and URL. One un-retired row
    /// per pair — the dedup key every enqueue path checks
    /// ([`store::pending_subjects`]), so a re-sync tick or two pages
    /// linking the same URL cannot stack rows (each fresh row would carry
    /// a fresh retry budget). Connector ids are ULIDs, so the space is an
    /// unambiguous separator.
    pub(crate) fn subject(&self) -> String {
        format!("{} {}", self.connector_id, self.url)
    }
}

/// Builds the fetch job for a connector's seed.
#[must_use]
pub(crate) fn seed_job(connector: &ConnectorRow, config: &ConnectorConfig) -> FetchJob {
    FetchJob::new(
        connector,
        config.seed_url(),
        0,
        match Kind::parse(&connector.kind) {
            // A url_prefix seed is an ordinary page; a sitemap seed is
            // sitemap XML; a GitHub seed is the tree listing.
            Some(Kind::Github) => Role::GithubTree,
            Some(Kind::Sitemap) => Role::Sitemap,
            Some(Kind::UrlPrefix) | None => Role::Page,
        },
    )
}

/// The fetch-job runner, over the ports the fetch path uses. `Clone`
/// (every field is an `Arc` or smaller) so a sweep can hand a clone into
/// a [`Defer`] future, which must be `'static`.
#[derive(Clone)]
pub(crate) struct Runner {
    db: Arc<dyn Database>,
    http: Arc<dyn HttpClient>,
    config: Arc<dyn Config>,
    clock: Arc<dyn Clock>,
    id_gen: Arc<dyn IdGen>,
    defer: Option<Arc<dyn Defer>>,
    outbox: Outbox,
}

impl Runner {
    pub(crate) fn new(
        db: Arc<dyn Database>,
        http: Arc<dyn HttpClient>,
        config: Arc<dyn Config>,
        clock: Arc<dyn Clock>,
        id_gen: Arc<dyn IdGen>,
        defer: Option<Arc<dyn Defer>>,
    ) -> Self {
        Self {
            db,
            http,
            config,
            clock,
            id_gen,
            defer,
            outbox: Outbox::new(OUTBOX_TABLE),
        }
    }

    /// The outbox the connector-creation handler enqueues the seed job
    /// through — the same one this runner drains, so the two cannot
    /// disagree about where the work lives.
    pub(crate) fn outbox(&self) -> &Outbox {
        &self.outbox
    }

    /// Runs one deferred sweep: an execution *opportunity* for work the
    /// connector creation or a fetch just enqueued. The rows are already
    /// durable; the scheduled re-sync is the backstop that guarantees
    /// they run.
    pub(crate) fn defer_sweep(&self) {
        let Some(defer) = &self.defer else {
            return;
        };
        let runner = self.clone();
        defer.wait_until(Box::pin(async move {
            let _ = runner.sweep().await;
        }));
    }

    /// Drains until a sweep claims nothing, bounded by [`MAX_SWEEPS`].
    /// Every job's discoveries are enqueued due immediately, so one
    /// deferred cascade can finish a whole small crawl.
    pub(crate) async fn sweep(&self) -> Result<usize, DbError> {
        let mut total = 0;
        for _ in 0..MAX_SWEEPS {
            let processed = self.drain(SWEEP_LIMIT).await?;
            total += processed;
            if processed == 0 {
                break;
            }
        }
        Ok(total)
    }

    /// Claims up to `limit` due fetch jobs and runs each. Returns how
    /// many rows it processed — including rows it retired as skipped,
    /// spent or dead.
    pub(crate) async fn drain(&self, limit: u64) -> Result<usize, DbError> {
        let now = rfc3339(self.clock.now());
        let lease_until = rfc3339_after(self.clock.now(), Duration::from_secs(LEASE_SECS));
        let due = self
            .outbox
            .claim_due(&*self.db, &now, &lease_until, limit)
            .await?;
        let mut processed = 0;
        for record in &due {
            self.process(record).await?;
            processed += 1;
        }
        Ok(processed)
    }

    /// The scheduled re-sync: for every connector, re-enqueue every URL
    /// it has ever fetched plus its seed (unless that already has a page
    /// row). Each fetch then runs its conditional GET — an unchanged page
    /// costs a 304 and nothing else, a vanished page deletes itself, a
    /// changed one re-indexes — and the sweep that follows runs whatever
    /// was just enqueued, same as `module-escalation`'s scheduled drain.
    ///
    /// Sitemap entries that have vanished from their sitemap are **not**
    /// proactively deleted; they keep re-fetching (cheaply, through their
    /// validators) until they answer 404/410, which is the delete path.
    /// Reconciling a parsed sitemap snapshot against the page table would
    /// mean holding a whole sitemap's URL set inside one job's batch.
    ///
    /// A URL with an un-retired outbox row — in flight, or waiting out a
    /// backoff — is **not** re-enqueued: one tick drains a bounded number
    /// of jobs, and duplicates would stack unboundedly across ticks, each
    /// with a fresh retry budget. Each connector's enqueues commit in
    /// their own batch: one tenant's crawl is one transaction, never a
    /// cross-tenant one.
    pub(crate) async fn resync(&self) -> Result<usize, DbError> {
        let now = rfc3339(self.clock.now());
        let pending = store::pending_subjects(&*self.db).await?;
        for connector in store::list_connectors(&*self.db).await? {
            let Some(config) = connector_config(&connector) else {
                continue;
            };
            let seed = config.seed_url();
            let mut seen_seed = false;
            let mut statements: Vec<cratefield_core::Statement> = Vec::new();
            for page in store::page_rows(&*self.db, &connector.id).await? {
                seen_seed = seen_seed || page.url == seed;
                let job = FetchJob::new(
                    &connector,
                    page.url,
                    page.depth,
                    Role::parse(&page.role).unwrap_or(Role::Page),
                );
                if pending.contains(&job.subject()) {
                    continue;
                }
                statements.push(self.enqueue_stmt(&job, &now));
            }
            if !seen_seed {
                let job = seed_job(&connector, &config);
                if !pending.contains(&job.subject()) {
                    statements.push(self.enqueue_stmt(&job, &now));
                }
            }
            if !statements.is_empty() {
                self.db.batch_atomic(&statements).await?;
            }
        }
        self.sweep().await
    }

    fn enqueue_stmt(&self, job: &FetchJob, at: &str) -> cratefield_core::Statement {
        // Infallible in practice — the payload is five strings and an
        // integer — and loud if that ever stops holding: an empty payload
        // would be a job that cannot even be parsed back.
        let payload = serde_json::to_string(job)
            .expect("FetchJob fields are strings and an integer; serialization cannot fail");
        store::enqueue_fetch_stmt(
            &self.outbox,
            &self.id_gen.ulid(),
            &payload,
            &job.subject(),
            at,
        )
    }

    /// Routes one claimed job. Every terminal outcome is recorded
    /// durably; the only errors that escape are database failures.
    async fn process(&self, record: &OutboxRecord) -> Result<(), DbError> {
        // An undecodable payload will not heal on a retry: retire it.
        let Ok(job) = serde_json::from_str::<FetchJob>(&record.payload) else {
            return self.complete(&record.id).await;
        };
        // An unknown connector (since deleted) or a payload whose tenant
        // does not match its connector: terminal, nothing to fetch.
        let Some(connector) = store::find_connector(&*self.db, &job.connector_id).await? else {
            return self.complete(&record.id).await;
        };
        if connector.tenant_id != job.tenant_id {
            return self.complete(&record.id).await;
        }
        // The allowlist: never fetch what the connector does not name.
        let Some(config) = connector_config(&connector) else {
            return self.complete(&record.id).await;
        };
        let Some(kind) = Kind::parse(&connector.kind) else {
            return self.complete(&record.id).await;
        };
        if !config.allows(kind, &job.url) {
            return self.complete(&record.id).await;
        }
        // Deeper than the connector's cap is terminal. Enqueues already
        // respect the cap; this guards rows written before it tightened.
        if job.depth > connector.max_depth {
            return self.complete(&record.id).await;
        }
        // Page cap: refetching a URL that already has a row is always
        // allowed (it replaces its own source); a *new* URL only fits
        // while the connector is under its cap. Every row spends the cap
        // — sources and navigation rows alike — which is what bounds a
        // sitemap index's breadth.
        let known = store::find_page(&*self.db, &connector.id, &job.url).await?;
        if known.is_none() {
            let used = store::count_page_rows(&*self.db, &connector.id).await?;
            if used >= connector.max_pages {
                return self.complete(&record.id).await;
            }
        }

        // All of NotModified (validators current), Skipped (nothing this
        // role does), Failed (deterministic — an oversized body, a blocked
        // destination — so a retry would fail identically) and Rejected
        // (terminal without a network diagnosis) retire the row; the next
        // scheduled re-sync starts the URL over from scratch.
        match self.fetch(&connector, &job, known.as_ref()).await? {
            FetchOutcome::Gone => self.delete_gone(&record.id, &connector, &job).await,
            FetchOutcome::Indexed(indexed) => {
                self.commit_indexed(&record.id, &connector, *indexed).await
            }
            FetchOutcome::Transient => self.retry_later(&record.id, record.attempts).await,
            FetchOutcome::NotModified
            | FetchOutcome::Skipped
            | FetchOutcome::Failed
            | FetchOutcome::Rejected => self.complete(&record.id).await,
        }
    }

    /// The one network call a job makes, and the local extraction after
    /// it.
    async fn fetch(
        &self,
        connector: &ConnectorRow,
        job: &FetchJob,
        known: Option<&PageRow>,
    ) -> Result<FetchOutcome, DbError> {
        let Some(role) = job.role() else {
            return Ok(FetchOutcome::Skipped);
        };
        let Ok(mut request) = http::Request::builder()
            .method(http::Method::GET)
            .uri(&job.url)
            .body(bytes::Bytes::new())
        else {
            return Ok(FetchOutcome::Rejected);
        };
        // Conditional GET: the validators the page row recorded last time.
        if let Some(page) = known {
            for (header, value) in [
                (http::header::IF_NONE_MATCH, &page.etag),
                (http::header::IF_MODIFIED_SINCE, &page.last_modified),
            ] {
                if let Some(text) = value
                    && let Ok(parsed) = http::HeaderValue::from_str(text)
                {
                    request.headers_mut().insert(header, parsed);
                }
            }
        }
        // The byte cap travels with the request: the port's
        // `BoundedHttpClient` enforces it, and the length check after the
        // call re-enforces it against clients that do not.
        let max_bytes = usize::try_from(connector.max_bytes).unwrap_or(usize::MAX);
        request.extensions_mut().insert(HttpPolicy {
            max_response_bytes: max_bytes,
            timeout: cratefield_core::DEFAULT_RESPONSE_TIMEOUT,
        });
        if matches!(role, Role::GithubTree | Role::GithubFile) {
            let headers = request.headers_mut();
            headers.insert(http::header::ACCEPT, github_accept(role));
            headers.insert(http::header::USER_AGENT, github_user_agent());
            if let Some(authorization) = self.github_authorization(connector) {
                headers.insert(http::header::AUTHORIZATION, authorization);
            }
        }

        let response = match self.http.send(request).await {
            Ok(response) => response,
            // Transport noise and a spent deadline are the retryable
            // failures; everything else the port can report is
            // deterministic.
            Err(HttpError::Transport(_) | HttpError::DeadlineExceeded { .. }) => {
                return Ok(FetchOutcome::Transient);
            }
            Err(HttpError::ResponseTooLarge { .. } | HttpError::BlockedDestination(_)) => {
                return Ok(FetchOutcome::Failed);
            }
        };

        let status = response.status();
        if status == http::StatusCode::NOT_MODIFIED {
            // Validators still current: the page is unchanged; write
            // nothing at all.
            return Ok(FetchOutcome::NotModified);
        }
        if status == http::StatusCode::NOT_FOUND || status == http::StatusCode::GONE {
            // The page is gone; its source and its page row go with it.
            return Ok(FetchOutcome::Gone);
        }
        if !status.is_success() {
            // A spent retry budget aside, an upstream 5xx is worth one
            // more try on a later tick; every other unusual status is
            // deterministic.
            return Ok(if status.is_server_error() {
                FetchOutcome::Transient
            } else {
                FetchOutcome::Rejected
            });
        }
        let body = response.body();
        if body.len() > max_bytes {
            return Ok(FetchOutcome::Failed);
        }
        let etag = header_value(response.headers(), http::header::ETAG);
        let last_modified = header_value(response.headers(), http::header::LAST_MODIFIED);
        // The same ceiling the inline `POST /sources` form enforces, so a
        // source's indexed size does not depend on how it arrived.
        let text = String::from_utf8_lossy(&body[..body.len().min(MAX_TEXT_BYTES)]).into_owned();

        Ok(match role {
            Role::Sitemap => Self::sitemap_outcome(connector, job, &text, etag, last_modified),
            Role::Page => {
                let is_html = response
                    .headers()
                    .get(http::header::CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    .is_some_and(|content_type| content_type.to_ascii_lowercase().contains("html"));
                let links = if is_html {
                    same_origin_links(&text, &job.url)
                } else {
                    Vec::new()
                };
                let final_text = if is_html { html_to_text(&text) } else { text };
                self.page_outcome(connector, job, &final_text, links, etag, last_modified)
                    .await?
            }
            Role::GithubTree => Self::tree_outcome(connector, job, &text, etag, last_modified),
            Role::GithubFile => {
                self.file_outcome(connector, job, &text, etag, last_modified)
                    .await?
            }
        })
    }

    /// A sitemap body: `<loc>` URLs to discover. A sitemap index's
    /// children are themselves sitemaps; a plain sitemap's are pages.
    /// Nothing is indexed from the sitemap itself, but its page row does
    /// spend the page cap like any other — that is what bounds a sitemap
    /// index's breadth (one index naming ten thousand children enqueues
    /// children only while the connector has room, not ten thousand
    /// fetches).
    fn sitemap_outcome(
        connector: &ConnectorRow,
        job: &FetchJob,
        text: &str,
        etag: Option<String>,
        last_modified: Option<String>,
    ) -> FetchOutcome {
        let child_role = if is_sitemap_index(text) {
            Role::Sitemap
        } else {
            Role::Page
        };
        let locs = sitemap_locs(text);
        FetchOutcome::Indexed(Box::new(Indexed {
            page: PageRow {
                connector_id: connector.id.clone(),
                url: job.url.clone(),
                tenant_id: connector.tenant_id.clone(),
                role: Role::Sitemap.as_str().to_owned(),
                depth: job.depth,
                etag,
                last_modified,
                source_id: None,
            },
            discovered: locs,
            child_role,
            replace: None,
        }))
    }

    /// An HTML or text page: index it (replacing whatever this URL
    /// indexed before) and discover its same-origin links.
    async fn page_outcome(
        &self,
        connector: &ConnectorRow,
        job: &FetchJob,
        text: &str,
        links: Vec<String>,
        etag: Option<String>,
        last_modified: Option<String>,
    ) -> Result<FetchOutcome, DbError> {
        let replace = self.replacement(job, text, &job.url, &job.url).await?;
        Ok(FetchOutcome::Indexed(Box::new(Indexed {
            page: PageRow {
                connector_id: connector.id.clone(),
                url: job.url.clone(),
                tenant_id: connector.tenant_id.clone(),
                role: Role::Page.as_str().to_owned(),
                depth: job.depth,
                etag,
                last_modified,
                source_id: Some(replace.source.id.clone()),
            },
            discovered: links,
            child_role: Role::Page,
            replace: Some(replace),
        })))
    }

    /// A GitHub tree listing: matching blobs become file jobs. Nothing is
    /// indexed from the listing itself.
    fn tree_outcome(
        connector: &ConnectorRow,
        job: &FetchJob,
        text: &str,
        etag: Option<String>,
        last_modified: Option<String>,
    ) -> FetchOutcome {
        let Some(ConnectorConfig::Github {
            owner,
            repo,
            path_glob,
            r#ref,
        }) = connector_config(connector)
        else {
            return FetchOutcome::Rejected;
        };
        let git_ref = r#ref.as_deref().unwrap_or("HEAD");
        let glob = path_glob.as_deref().unwrap_or("**");
        let files = tree_blobs(text)
            .into_iter()
            .filter(|path| glob_match(glob, path))
            .map(|path| {
                format!(
                    "{GITHUB_API}/repos/{owner}/{repo}/contents/{}?ref={}",
                    encode_uri_path(&path),
                    encode_uri_path(git_ref)
                )
            })
            .collect();
        FetchOutcome::Indexed(Box::new(Indexed {
            page: PageRow {
                connector_id: connector.id.clone(),
                url: job.url.clone(),
                tenant_id: connector.tenant_id.clone(),
                role: Role::GithubTree.as_str().to_owned(),
                depth: job.depth,
                etag,
                last_modified,
                source_id: None,
            },
            discovered: files,
            child_role: Role::GithubFile,
            replace: None,
        }))
    }

    /// A GitHub file's raw contents: indexed under
    /// `github:{owner}/{repo}:{path}`, titled with the path.
    async fn file_outcome(
        &self,
        connector: &ConnectorRow,
        job: &FetchJob,
        text: &str,
        etag: Option<String>,
        last_modified: Option<String>,
    ) -> Result<FetchOutcome, DbError> {
        let Some(ConnectorConfig::Github { owner, repo, .. }) = connector_config(connector) else {
            return Ok(FetchOutcome::Rejected);
        };
        let Some(path) = github_path(&job.url) else {
            return Ok(FetchOutcome::Rejected);
        };
        let external_id = format!("github:{owner}/{repo}:{path}");
        let replace = self.replacement(job, text, &external_id, &path).await?;
        Ok(FetchOutcome::Indexed(Box::new(Indexed {
            page: PageRow {
                connector_id: connector.id.clone(),
                url: job.url.clone(),
                tenant_id: connector.tenant_id.clone(),
                role: Role::GithubFile.as_str().to_owned(),
                depth: job.depth,
                etag,
                last_modified,
                source_id: Some(replace.source.id.clone()),
            },
            discovered: Vec::new(),
            child_role: Role::GithubFile,
            replace: Some(replace),
        })))
    }

    /// The source a page fetch would index: the existing row for this
    /// `external_id` if there is one — same id, so its stored chunk ids
    /// are comparable with the fresh ones — else a fresh one titled with
    /// `title` (the URL, or the repository path).
    ///
    /// The row fills the source-management columns the way `POST
    /// /sources` does: `external_id` is the connector's upsert key,
    /// `byte_len` the indexed text's size, `updated_at` this fetch's
    /// instant, and `created_at` the first index date — carried through
    /// a re-index untouched. A source the tenant indexed by hand under
    /// the same external id (the `{"url"}` form defaults it to the URL)
    /// is the same document, so the connector adopts and replaces it
    /// rather than colliding with it.
    ///
    /// `unchanged` is the cheap content-equality fast path: chunk ids are
    /// content addresses over (tenant, source, text), so an identical
    /// byte sequence under the same source id reproduces the identical
    /// id sequence, and the commit then writes nothing but the new
    /// validators the 200 proved changed.
    async fn replacement(
        &self,
        job: &FetchJob,
        text: &str,
        external_id: &str,
        title: &str,
    ) -> Result<Replacement, DbError> {
        let existing = store::find_source_by_external_id(&*self.db, &job.tenant_id, external_id)
            .await?
            .filter(|source| source.tenant_id == job.tenant_id);
        let now = rfc3339(self.clock.now());
        let byte_len = i64::try_from(text.len()).unwrap_or(i64::MAX);
        let source = match &existing {
            Some(row) => SourceRow {
                id: row.id.clone(),
                tenant_id: row.tenant_id.clone(),
                title: row.title.clone(),
                url: Some(job.url.clone()),
                external_id: Some(external_id.to_owned()),
                byte_len,
                // The first index date survives a re-index.
                created_at: row.created_at.clone(),
                updated_at: now,
            },
            None => SourceRow {
                id: self.id_gen.ulid(),
                tenant_id: job.tenant_id.clone(),
                title: title.to_owned(),
                url: Some(job.url.clone()),
                external_id: Some(external_id.to_owned()),
                byte_len,
                created_at: now.clone(),
                updated_at: now,
            },
        };
        let chunks = Chunker::default().split(&job.tenant_id, &source.id, text);
        let stored = match &existing {
            Some(row) => store::chunk_index(&*self.db, &job.tenant_id, &row.id).await?,
            None => Vec::new(),
        };
        let unchanged = existing.is_some() && stored.len() == chunks.len() && {
            let mut old: Vec<&str> = stored.iter().map(|chunk| chunk.id.as_str()).collect();
            let mut fresh: Vec<&str> = chunks.iter().map(|chunk| chunk.id.as_str()).collect();
            old.sort_unstable();
            fresh.sort_unstable();
            old == fresh
        };
        Ok(Replacement {
            source,
            stored: existing.is_some().then_some(stored),
            chunks,
            unchanged,
        })
    }

    /// Commits an `Indexed` outcome: the replacement (unless the content
    /// is unchanged), the page row's new validators, the discovered jobs
    /// and the completion of this job's own row — **one** batch. A
    /// discovery that survives the filters defers another sweep, which is
    /// what lets a small crawl finish inside the request that created the
    /// connector.
    async fn commit_indexed(
        &self,
        record_id: &str,
        connector: &ConnectorRow,
        indexed: Indexed,
    ) -> Result<(), DbError> {
        let Indexed {
            page,
            discovered,
            child_role,
            replace,
        } = indexed;
        let mut statements: Vec<cratefield_core::Statement> = Vec::new();
        // A source that existed is re-indexed as a diff (main's
        // replace-in-place: kept windows keep their rows); a first-time
        // URL is a plain insert.
        let mut replaced_existing: Option<(String, String)> = None;
        if let Some(replacement) = replace.filter(|replacement| !replacement.unchanged) {
            match &replacement.stored {
                Some(stored) => {
                    statements.extend(store::replace_source_statements(
                        &replacement.source,
                        stored,
                        &replacement.chunks,
                    ));
                    replaced_existing = Some((
                        replacement.source.tenant_id.clone(),
                        replacement.source.id.clone(),
                    ));
                }
                None => statements.extend(store::source_with_chunks_statements(
                    &replacement.source,
                    &replacement.chunks,
                )),
            }
        }
        statements.push(store::upsert_page_stmt(&page));
        let now = rfc3339(self.clock.now());
        let jobs = self
            .discoveries(connector, page.depth, discovered, child_role)
            .await?;
        for job in &jobs {
            statements.push(self.enqueue_stmt(job, &now));
        }
        statements.push(store::ingest_outbox_complete_stmt(record_id));
        self.db.batch_atomic(&statements).await?;
        // The same race guard `PUT /sources/{id}` has: a tenant DELETE
        // that committed between the lookup and this batch left the row
        // update matching nothing and the added windows orphaned.
        if let Some((tenant_id, source_id)) = replaced_existing {
            store::sweep_if_deleted(&*self.db, &tenant_id, &source_id).await?;
        }
        if !jobs.is_empty() {
            self.defer_sweep();
        }
        Ok(())
    }

    /// Filters and shapes discovered URLs into enqueueable jobs:
    /// on-allowlist, one hop deeper than the page they were found on but
    /// within the depth cap, only while the connector's page cap has room
    /// (every page row spends it — sources and navigation rows alike,
    /// which is what bounds a sitemap index's breadth), only for URLs
    /// without a page row — those belong to the scheduled re-sync — and
    /// only for URLs without an un-retired outbox row already, which is
    /// what keeps two pages linking the same URL from stacking jobs.
    async fn discoveries(
        &self,
        connector: &ConnectorRow,
        parent_depth: i64,
        urls: Vec<String>,
        child_role: Role,
    ) -> Result<Vec<FetchJob>, DbError> {
        let mut jobs = Vec::new();
        let child_depth = parent_depth + 1;
        if child_depth > connector.max_depth {
            return Ok(jobs);
        }
        let config = connector_config(connector);
        let kind = Kind::parse(&connector.kind);
        let mut room =
            connector.max_pages - store::count_page_rows(&*self.db, &connector.id).await?;
        let pending = store::pending_subjects(&*self.db).await?;
        for url in urls {
            if room <= 0 {
                break;
            }
            if !config
                .as_ref()
                .is_some_and(|config| kind.is_some_and(|kind| config.allows(kind, &url)))
            {
                continue;
            }
            if store::find_page(&*self.db, &connector.id, &url)
                .await?
                .is_some()
            {
                continue;
            }
            let job = FetchJob::new(connector, url, child_depth, child_role);
            if pending.contains(&job.subject()) || jobs.iter().any(|queued| queued.url == job.url) {
                continue;
            }
            jobs.push(job);
            room -= 1;
        }
        Ok(jobs)
    }

    /// A page that answered 404/410: its source (with chunks and
    /// postings) and its page row go, in one batch. The source is found
    /// by the external id the role indexed it under — the URL itself for
    /// pages, the `github:{owner}/{repo}:{path}` form for files.
    async fn delete_gone(
        &self,
        record_id: &str,
        connector: &ConnectorRow,
        job: &FetchJob,
    ) -> Result<(), DbError> {
        let external_id = match job.role() {
            Some(Role::GithubFile) => connector_config(connector).and_then(|config| match config {
                ConnectorConfig::Github { owner, repo, .. } => {
                    github_path(&job.url).map(|path| format!("github:{owner}/{repo}:{path}"))
                }
                ConnectorConfig::Web { .. } => None,
            }),
            _ => Some(job.url.clone()),
        };
        let source = match external_id {
            Some(external_id) => {
                store::find_source_by_external_id(&*self.db, &job.tenant_id, &external_id).await?
            }
            None => None,
        };
        let mut statements = match &source {
            Some(row) => store::delete_source_statements(&job.tenant_id, &row.id),
            None => Vec::new(),
        };
        statements.push(store::delete_page_stmt(&job.connector_id, &job.url));
        statements.push(store::ingest_outbox_complete_stmt(record_id));
        self.db.batch_atomic(&statements).await
    }

    /// `Authorization: Bearer …` for a GitHub connector, resolved from
    /// the Config port by the connector's stored `credential_ref` — the
    /// same pattern as `module-escalation`'s file stage: the database
    /// holds the reference, the secret is resolved at use and dropped. A
    /// ref that is set but names nothing returns `None`, so the request
    /// goes out unauthenticated and GitHub's 404/401 retires the job —
    /// a missing credential must not become a retry storm. A connector
    /// with no ref at all fetches anonymously, within GitHub's limits.
    fn github_authorization(&self, connector: &ConnectorRow) -> Option<http::HeaderValue> {
        let credential_ref = connector.credential_ref.as_deref()?.trim();
        if credential_ref.is_empty() {
            return None;
        }
        let secret = self.config.get(credential_ref)?;
        http::HeaderValue::from_str(&format!("Bearer {secret}")).ok()
    }

    async fn retry_later(&self, record_id: &str, attempts_so_far: i64) -> Result<(), DbError> {
        let attempts = attempts_so_far + 1;
        if attempts >= RETRY_MAX_ATTEMPTS {
            // Budget spent: drop the row. The next scheduled re-sync
            // enqueues the URL again from scratch — a bounded outage
            // delays a refresh, it does not lose one.
            return self.complete(record_id).await;
        }
        let step = u32::try_from(attempts_so_far.max(0))
            .unwrap_or(u32::MAX)
            .min(63);
        let factor = 1_u64.checked_shl(step).unwrap_or(u64::MAX);
        let wait = Duration::from_secs(RETRY_BASE_SECS.saturating_mul(factor))
            .min(Duration::from_secs(RETRY_CAP_SECS));
        let next_at = rfc3339_after(self.clock.now(), wait);
        self.db
            .batch_atomic(&[store::ingest_outbox_retry_later_stmt(record_id, &next_at)])
            .await
    }

    async fn complete(&self, record_id: &str) -> Result<(), DbError> {
        self.db
            .batch_atomic(&[store::ingest_outbox_complete_stmt(record_id)])
            .await
    }
}

/// What one fetch turned into. `discovered` carries raw URLs; the
/// filters run at commit time so the cap check reads post-write state.
/// The payload of a successful fetch: the page row to upsert (with the
/// new validators and source link), the URLs it discovered and the role
/// they fetch as, and the source replacement — `None` for roles that
/// index nothing.
struct Indexed {
    page: PageRow,
    discovered: Vec<String>,
    child_role: Role,
    replace: Option<Replacement>,
}

enum FetchOutcome {
    /// Validators still current: nothing to write.
    NotModified,
    /// 404/410: the page row — and the source it indexed — go.
    Gone,
    /// A 2xx body, extracted. `discovered` carries raw URLs; the filters
    /// run at commit time so the cap check reads post-write state.
    /// Boxed: the unit variants below are the common outcomes, and the
    /// payload is an order of magnitude larger than a word.
    Indexed(Box<Indexed>),
    /// Worth another tick: transport noise, a spent deadline, an
    /// upstream 5xx. Deliberately reason-free: the outbox row has no
    /// error column and this module keeps no audit table, so a reason
    /// would be a string nothing reads.
    Transient,
    /// Deterministic: never worth a retry (an oversized body, a blocked
    /// destination, a status the role cannot use).
    Failed,
    /// Terminal without a network diagnosis (a status like 401 on a
    /// public repo, a malformed URL): retire the row.
    Rejected,
    /// Nothing to do (an unknown role on a well-formed payload).
    Skipped,
}

/// A source re-index: the row to write (same id as the stored one when
/// there was one), the stored windows it replaces, the freshly split chunks,
/// and whether the content came back byte-identical.
struct Replacement {
    source: SourceRow,
    /// The stored windows of the source being replaced, `None` for a
    /// first-time URL.
    stored: Option<Vec<StoredChunk>>,
    chunks: Vec<Chunk>,
    unchanged: bool,
}

fn github_accept(role: Role) -> http::HeaderValue {
    match role {
        // Raw file contents, not the JSON metadata envelope.
        Role::GithubFile => http::HeaderValue::from_static("application/vnd.github.raw"),
        _ => http::HeaderValue::from_static("application/vnd.github+json"),
    }
}

fn github_user_agent() -> http::HeaderValue {
    http::HeaderValue::from_static(GITHUB_USER_AGENT)
}

fn header_value(headers: &http::HeaderMap, name: http::HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

/// Decodes a connector row's config JSON; `None` for bytes that do not
/// parse (a row written by a foreign version), which skips the connector
/// rather than guessing at its crawl root.
#[must_use]
pub(crate) fn connector_config(connector: &ConnectorRow) -> Option<ConnectorConfig> {
    serde_json::from_str(&connector.config).ok()
}

/// Percent-encodes every `/`-separated segment of a URI path or ref:
/// the unreserved set (`A-Z a-z 0-9 - . _ ~`) passes through, every
/// other byte becomes `%XX`. A tree path like `docs/a?b c.md` therefore
/// cannot splice a query or fragment into the contents-API URL this
/// module builds (and a literal `?` no longer truncates the path when
/// [`github_path`] inverts it). `/` stays a separator, so branch refs
/// like `feature/x` read the same encoded as raw.
fn encode_uri_path(path: &str) -> String {
    path.split('/')
        .map(encode_segment)
        .collect::<Vec<_>>()
        .join("/")
}

fn encode_segment(segment: &str) -> String {
    let mut encoded = String::with_capacity(segment.len());
    for byte in segment.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(*byte as char);
            }
            other => {
                let _ = write!(encoded, "%{other:02X}");
            }
        }
    }
    encoded
}

/// The inverse of [`encode_uri_path`]: `%XX` sequences become their
/// bytes. A `%` not followed by two hex digits stays literal — it is
/// not a sequence this module would have produced.
fn decode_uri_path(path: &str) -> String {
    let bytes = path.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let hex = bytes
            .get(index + 1..index + 3)
            .and_then(|hex| std::str::from_utf8(hex).ok())
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        if bytes[index] == b'%'
            && let Some(byte) = hex
        {
            decoded.push(byte);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

/// The repository path in a contents-API URL this module built:
/// `{owner}/{repo}/contents/{percent-encoded path}?ref=…`, decoded back
/// to the path GitHub reported ([`decode_uri_path`]).
fn github_path(url: &str) -> Option<String> {
    let rest = url.strip_prefix(GITHUB_API)?.strip_prefix("/repos/")?;
    // Owner and repo cannot contain `/`, so the third `/`-separated
    // component starts the contents path — even when the tree path
    // itself carries a `contents` directory.
    let contents = rest.splitn(3, '/').nth(2)?.strip_prefix("contents/")?;
    let path = contents.split_once('?').map_or(contents, |(path, _)| path);
    (!path.is_empty()).then(|| decode_uri_path(path))
}

/// Every `type == "blob"` path in a Git-trees API response. Only the two
/// fields this module uses are read; a tree entry without them is
/// skipped rather than guessed.
fn tree_blobs(json: &str) -> Vec<String> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(json) else {
        return Vec::new();
    };
    value
        .get("tree")
        .and_then(|tree| tree.as_array())
        .into_iter()
        .flatten()
        .filter(|entry| entry.get("type").and_then(|kind| kind.as_str()) == Some("blob"))
        .filter_map(|entry| {
            entry
                .get("path")
                .and_then(|path| path.as_str())
                .map(str::to_owned)
        })
        .collect()
}

/// Whether the sitemap document is an index (its `<loc>` children are
/// themselves sitemaps) rather than a URL set.
fn is_sitemap_index(xml: &str) -> bool {
    xml.to_ascii_lowercase().contains("<sitemapindex")
}

/// Every `<loc>` value in a sitemap document, trimmed and unescaped.
/// Namespaces do not interfere: `<loc>` appears literally in both
/// sitemap and sitemap-index documents.
fn sitemap_locs(xml: &str) -> Vec<String> {
    let lower = xml.to_ascii_lowercase();
    let mut locs = Vec::new();
    let mut cursor = 0;
    while let Some(open) = lower[cursor..].find("<loc>") {
        let start = cursor + open + "<loc>".len();
        let Some(close) = lower[start..].find("</loc>") else {
            break;
        };
        locs.push(decode_entities(xml[start..start + close].trim()));
        cursor = start + close + "</loc>".len();
    }
    locs
}

/// Same-origin `<a href>` targets of an HTML page, resolved against the
/// page URL, fragments stripped, in document order. Deliberately crude —
/// it feeds the frontier, not a reader: an `<a>` whose tag never closes
/// yields no link, and only the `href` attribute is read. The
/// same-origin cut here is a tidy-up for the frontier; the allowlist in
/// [`discoveries`] is what actually confines the crawl.
fn same_origin_links(html: &str, base: &str) -> Vec<String> {
    let lower = html.to_ascii_lowercase();
    let mut links = Vec::new();
    let mut cursor = 0;
    while let Some(tag_start) = lower[cursor..].find("<a") {
        let from = cursor + tag_start;
        let Some(tag_end) = lower[from..].find('>') else {
            break;
        };
        let tag = &lower[from..from + tag_end];
        cursor = from + tag_end;
        let Some(href_at) = tag.find("href") else {
            continue;
        };
        let rest = &tag[href_at + "href".len()..];
        let rest = rest.trim_start_matches(|c: char| c.is_ascii_whitespace());
        let Some(rest) = rest.strip_prefix('=') else {
            continue;
        };
        let rest = rest.trim_start_matches(|c: char| c.is_ascii_whitespace());
        let Some(quote) = rest.chars().next() else {
            continue;
        };
        if quote != '"' && quote != '\'' {
            continue;
        }
        let Some(value_end) = rest[1..].find(quote) else {
            continue;
        };
        let href = &rest[1..=value_end];
        let Some(resolved) = resolve_against(base, href) else {
            continue;
        };
        // Same origin as the page the link was found on.
        if !same_scheme_and_authority(base, &resolved) {
            continue;
        }
        if !links.contains(&resolved) {
            links.push(resolved);
        }
    }
    links
}

/// Resolves one `href` against a base URL: absolute http(s) URLs pass
/// through, scheme-relative and root-relative and bare-relative forms
/// are joined, everything else (other schemes, empty, junk) is dropped.
/// Fragments are stripped — they name a place in a page, not a page.
fn resolve_against(base: &str, href: &str) -> Option<String> {
    let href = href.trim();
    if href.is_empty() || href.starts_with('#') {
        return None;
    }
    let base = base.parse::<http::Uri>().ok()?;
    let scheme = base.scheme_str()?.to_ascii_lowercase();
    if scheme != "http" && scheme != "https" {
        return None;
    }
    let authority = base.authority()?.as_str();
    // An href with its own scheme is absolute: keep it only when that
    // scheme is http(s) — `mailto:`, `javascript:` and friends name no
    // page. Without this gate the scheme prefix would look like a
    // relative path.
    if href_scheme(href).is_some() {
        return (href_scheme(href).as_deref() == Some("http")
            || href_scheme(href).as_deref() == Some("https"))
        .then(|| href.split('#').next().unwrap_or_default().to_owned());
    }
    let joined = if let Some(rest) = href.strip_prefix("//") {
        format!("{scheme}://{rest}")
    } else if href.starts_with('/') {
        format!("{scheme}://{authority}{href}")
    } else {
        let directory = base.path().rsplit_once('/').map_or("/", |(dir, _)| dir);
        format!("{scheme}://{authority}{directory}/{href}")
    };
    let joined = joined.split('#').next().unwrap_or_default().to_owned();
    // Parseability is the last gate: a resolved link that is not a URI is
    // not a link.
    joined.parse::<http::Uri>().ok()?;
    (!joined.is_empty()).then_some(joined)
}

/// The RFC 3986 scheme of an href, if it opens with one:
/// `ALPHA *( ALPHA / DIGIT / "+" / "-" / "." ) ":"`.
fn href_scheme(href: &str) -> Option<String> {
    let mut end = href.len();
    for (index, ch) in href.char_indices() {
        let ok = if index == 0 {
            ch.is_ascii_alphabetic()
        } else {
            ch.is_ascii_alphanumeric() || matches!(ch, '+' | '-' | '.')
        };
        if !ok {
            end = index;
            break;
        }
    }
    let (head, rest) = href.split_at(end);
    if head.is_empty() || !rest.starts_with(':') {
        return None;
    }
    Some(head.to_ascii_lowercase())
}

/// The tiny path glob the GitHub connector filters with: `**` matches
/// any run of characters including `/`, `*` any run within one path
/// segment, `?` one character that is not `/`. Everything else is a
/// literal. Written in-crate on purpose — a glob crate for one call site
/// is a dependency, not a feature.
#[must_use]
pub(crate) fn glob_match(pattern: &str, path: &str) -> bool {
    glob_bytes(pattern.as_bytes(), path.as_bytes())
}

fn glob_bytes(pattern: &[u8], path: &[u8]) -> bool {
    let Some((&token, rest)) = pattern.split_first() else {
        return path.is_empty();
    };
    match token {
        b'*' if rest.first() == Some(&b'*') => {
            let after = &rest[1..];
            match after.first() {
                // `**/`: zero or more whole directories.
                Some(b'/') => {
                    let dirs = &after[1..];
                    if glob_bytes(dirs, path) {
                        return true;
                    }
                    match path.iter().position(|&byte| byte == b'/') {
                        Some(slash) => glob_bytes(pattern, &path[slash + 1..]),
                        None => false,
                    }
                }
                // A bare `**`: any run of characters, separators included.
                _ => (0..=path.len()).any(|skip| glob_bytes(after, &path[skip..])),
            }
        }
        b'*' => {
            // A single `*` stays within its path segment.
            let segment = path
                .iter()
                .position(|&byte| byte == b'/')
                .unwrap_or(path.len());
            (0..=segment).any(|skip| glob_bytes(rest, &path[skip..]))
        }
        b'?' => matches!(path.first(), Some(&byte) if byte != b'/') && glob_bytes(rest, &path[1..]),
        literal => path.first() == Some(&literal) && glob_bytes(rest, &path[1..]),
    }
}

/// RFC 3339, UTC, to the second — the spelling the outbox's due and
/// lease comparisons rely on (lexicographic order is chronological
/// order on this form).
fn rfc3339(at: time::OffsetDateTime) -> String {
    let at = at.to_offset(time::UtcOffset::UTC);
    at.replace_nanosecond(0)
        .unwrap_or(at)
        .format(&Rfc3339)
        .unwrap_or_default()
}

fn rfc3339_after(at: time::OffsetDateTime, after: Duration) -> String {
    let secs = i64::try_from(after.as_secs()).unwrap_or(i64::MAX);
    rfc3339(at.checked_add(time::Duration::seconds(secs)).unwrap_or(at))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_glob_matches_paths_not_wishes() {
        assert!(glob_match("**", "anything/at/all.rs"));
        assert!(glob_match("docs/**/*.md", "docs/a.md"));
        assert!(glob_match("docs/**/*.md", "docs/sub/b.md"));
        assert!(!glob_match("docs/**/*.md", "docs/sub/b.txt"));
        assert!(!glob_match("docs/**/*.md", "README.md"));
        assert!(!glob_match("docs/**/*.md", "src/docs/a.md"));
        assert!(glob_match("src/*.rs", "src/x.rs"));
        assert!(!glob_match("src/*.rs", "src/sub/x.rs"));
        assert!(glob_match("re?ease.md", "release.md"));
        assert!(!glob_match("re?ease.md", "releease.md"));
        assert!(glob_match("a*/b", "ax/b"));
        assert!(!glob_match("a*/b", "ax/c"));
    }

    #[test]
    fn sitemap_locs_survive_namespaces_and_entities() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
              <url><loc>https://docs.example.com/a?x=1&amp;y=2</loc></url>
              <url><loc> https://docs.example.com/b </loc></url>
            </urlset>"#;
        assert_eq!(
            sitemap_locs(xml),
            vec![
                "https://docs.example.com/a?x=1&y=2",
                "https://docs.example.com/b"
            ]
        );
        assert!(!is_sitemap_index(xml));
        let index = r#"<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
              <sitemap><loc>https://docs.example.com/sitemap-2.xml</loc></sitemap>
            </sitemapindex>"#;
        assert_eq!(
            sitemap_indexes_are_children(index),
            vec!["https://docs.example.com/sitemap-2.xml"]
        );
    }

    fn sitemap_indexes_are_children(xml: &str) -> Vec<String> {
        assert!(is_sitemap_index(xml));
        sitemap_locs(xml)
    }

    #[test]
    fn links_resolve_same_origin_and_drop_fragments() {
        let base = "https://docs.example.com/guide/setup.html";
        let html = r#"<a href="install.html">next</a>
            <a href="/api">api</a>
            <a href="https://other.example.com/x">off site</a>
            <a href="https://docs.example.com/guide/setup.html#step-2">self</a>
            <a href="mailto:hi@example.com">mail</a>
            <a href="//docs.example.com/mirror">mirror</a>"#;
        assert_eq!(
            same_origin_links(html, base),
            vec![
                "https://docs.example.com/guide/install.html",
                "https://docs.example.com/api",
                "https://docs.example.com/guide/setup.html",
                "https://docs.example.com/mirror",
            ]
        );
    }

    #[test]
    fn the_allowlist_confines_each_kind() {
        let sitemap = ConnectorConfig::Web {
            url: "https://docs.example.com/sitemap.xml".to_owned(),
        };
        // Same host as the sitemap: any path, any depth.
        assert!(sitemap.allows(Kind::Sitemap, "https://docs.example.com/anything"));
        assert!(sitemap.allows(Kind::Sitemap, "https://docs.example.com/deep/x/y"));
        assert!(!sitemap.allows(Kind::Sitemap, "http://docs.example.com/anything"));
        assert!(!sitemap.allows(Kind::Sitemap, "https://evil.example.com/anything"));
        assert!(!sitemap.allows(Kind::Sitemap, "https://docs.example.com.evil.com/"));
        // A sitemap config graded as a prefix (a corrupted row) allows
        // only its own URL — the kind is part of the answer.
        assert!(sitemap.allows(Kind::UrlPrefix, "https://docs.example.com/sitemap.xml"));
        assert!(!sitemap.allows(Kind::UrlPrefix, "https://docs.example.com/anything"));

        let prefix = ConnectorConfig::Web {
            url: "https://docs.example.com/docs".to_owned(),
        };
        assert!(prefix.allows(Kind::UrlPrefix, "https://docs.example.com/docs"));
        assert!(prefix.allows(Kind::UrlPrefix, "https://docs.example.com/docs/x"));
        assert!(!prefix.allows(Kind::UrlPrefix, "https://docs.example.com/docs-other"));
        assert!(!prefix.allows(Kind::UrlPrefix, "https://other.example.com/docs/x"));

        let github = ConnectorConfig::Github {
            owner: "acme".to_owned(),
            repo: "widgets".to_owned(),
            path_glob: Some("**".to_owned()),
            r#ref: Some("main".to_owned()),
        };
        assert!(github.allows(
            Kind::Github,
            "https://api.github.com/repos/acme/widgets/git/trees/main"
        ));
        assert!(github.allows(
            Kind::Github,
            "https://api.github.com/repos/acme/widgets/contents/x?ref=main"
        ));
        assert!(!github.allows(
            Kind::Github,
            "http://api.github.com/repos/acme/widgets/contents/x"
        ));
        assert!(!github.allows(
            Kind::Github,
            "https://api.github.com/repos/acme/other/contents/x"
        ));
        assert_eq!(
            github.seed_url(),
            "https://api.github.com/repos/acme/widgets/git/trees/main?recursive=1"
        );
    }

    #[test]
    fn github_paths_reverse_out_of_built_urls() {
        assert_eq!(
            github_path("https://api.github.com/repos/acme/widgets/contents/docs/a.md?ref=main"),
            Some("docs/a.md".to_owned())
        );
        // Repo-agnostic on purpose — the allowlist confines by repo;
        // github_path only has to invert the URL shape it built.
        assert_eq!(
            github_path("https://api.github.com/repos/acme/other/contents/x?ref=main"),
            Some("x".to_owned())
        );
        assert_eq!(
            github_path("https://api.github.com/repos/acme/widgets/git/trees/main?recursive=1"),
            None
        );
        assert_eq!(github_path("https://evil.example.com/contents/x"), None);
    }

    #[test]
    fn github_urls_round_trip_through_percent_encoding() {
        // `?`, `#`, and a space in a tree path must not splice a query or
        // fragment into the built URL — and `github_path` must hand back
        // the path GitHub reported, not its percent-escaped form.
        let spliced = format!(
            "{GITHUB_API}/repos/acme/widgets/contents/{}?ref={}",
            encode_uri_path("docs/a?b#c d.md"),
            encode_uri_path("feature/x")
        );
        assert_eq!(
            spliced,
            "https://api.github.com/repos/acme/widgets/contents/docs/a%3Fb%23c%20d.md?ref=feature/x"
        );
        assert_eq!(github_path(&spliced), Some("docs/a?b#c d.md".to_owned()));

        // Unreserved bytes pass through untouched, `/` stays a separator,
        // and a branch ref encodes to itself.
        assert_eq!(encode_uri_path("docs/a.md"), "docs/a.md");
        assert_eq!(encode_uri_path("feature/x"), "feature/x");
        assert_eq!(encode_uri_path("v1.0-rc_1~2"), "v1.0-rc_1~2");
        // Multi-byte UTF-8 survives encode then decode.
        assert_eq!(
            decode_uri_path(&encode_uri_path("résumé/über.md")),
            "résumé/über.md"
        );
        // A stray `%` is never guessed at.
        assert_eq!(decode_uri_path("100%_off.md"), "100%_off.md");
        assert_eq!(decode_uri_path("a%2Fb"), "a/b");
    }
}
