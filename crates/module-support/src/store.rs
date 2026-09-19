//! Data access for the `sg_*` tables, in the portable SQL subset:
//! parameterised statements through [`Statement::with_values`], reads
//! through [`Database::query`], and every multi-row write through
//! [`Database::batch_atomic`] so a turn lands whole or not at all.
//!
//! The retrieval seam is [`top_chunks`], and only its body is expected to
//! change: support v0 (issue #2) owns sources and real BM25 retrieval,
//! and replaces the term-overlap ranking below without touching a caller.

use cratefield_core::{Database, DbError, Statement};

/// A retrieved chunk: enough to build the prompt and to check citations
/// against.
#[derive(Debug, Clone)]
pub(crate) struct Chunk {
    pub id: String,
    pub body: String,
}

/// A conversation as loaded for a turn. `status` is deliberately not
/// loaded: in this schema `status = 'escalated'` iff
/// `needs_escalation = 1`, so the flag alone carries everything the turn
/// needs.
#[derive(Debug, Clone)]
pub(crate) struct Conversation {
    pub id: String,
    pub needs_escalation: bool,
}

/// The top-`k` chunks for `question`, best first.
///
/// **This is a placeholder for the BM25 retrieval that support v0 (issue
/// #2) owns.** It ranks with plain term overlap — tokenize the question
/// on non-alphanumerics, lowercase, drop tokens shorter than three
/// characters, score a chunk by how many *distinct* query terms its
/// lowercased body contains, order by score then id with zero scores
/// last, and keep the top `k` — over every chunk of the tenant. Zero
/// scores stay inside `k` on purpose: "nothing retrieved" must mean an
/// empty corpus, not a question with no lexical overlap, or every
/// oddly-worded follow-up would hand off before the clarify budget is
/// spent. It exists so the seam, the decision
/// and the route are all testable before real retrieval lands; only the
/// body of this function is expected to be replaced (a real engine ranks
/// in the database and never loads the tenant's whole corpus into the
/// isolate, which this must never do in production).
pub(crate) async fn top_chunks(
    db: &dyn Database,
    tenant_id: &str,
    question: &str,
    k: u32,
) -> Result<Vec<Chunk>, DbError> {
    let rows = db
        .query(&Statement::with_values(
            "SELECT id, body FROM sg_chunks WHERE tenant_id = ?",
            vec![tenant_id.into()],
        ))
        .await?;
    let chunks: Vec<Chunk> = rows
        .rows
        .iter()
        .map(|row| Chunk {
            id: row.get::<String>("id").unwrap_or_default(),
            body: row.get::<String>("body").unwrap_or_default(),
        })
        .collect();
    Ok(rank_chunks(&chunks, question, k))
}

/// The term-overlap ranking [`top_chunks`] applies. Pure, so the
/// `#[cfg(test)]` module below drives it without a database.
fn rank_chunks(chunks: &[Chunk], question: &str, k: u32) -> Vec<Chunk> {
    let terms = query_terms(question);
    let mut scored: Vec<(usize, &Chunk)> = chunks
        .iter()
        .map(|chunk| {
            let body = chunk.body.to_lowercase();
            let score = terms
                .iter()
                .filter(|term| body.contains(term.as_str()))
                .count();
            (score, chunk)
        })
        .collect();
    // Score first (best first), id second: the tie-break is stable and
    // tenant-visible, so two chunks that both match once always come back
    // in the same order. Zero-score chunks sort last but stay inside `k` —
    // the model still sees the tenant's context when nothing matches.
    scored.sort_by(|(a_score, a_chunk), (b_score, b_chunk)| {
        b_score
            .cmp(a_score)
            .then_with(|| a_chunk.id.cmp(&b_chunk.id))
    });
    let k = usize::try_from(k).unwrap_or(usize::MAX);
    scored
        .into_iter()
        .take(k)
        .map(|(_, chunk)| chunk.clone())
        .collect()
}

/// The question's search terms: lowercased, at least three characters.
/// Shorter tokens are noise at this precision — "how", "do", "I" carry
/// no retrieval signal a substring match can use.
fn query_terms(question: &str) -> Vec<String> {
    let mut terms: Vec<String> = question
        .split(|c: char| !c.is_alphanumeric())
        .filter(|token| token.chars().count() >= 3)
        .map(str::to_lowercase)
        .collect();
    terms.sort();
    terms.dedup();
    terms
}

/// The conversation for `conversation_id`, **scoped to the tenant**: a
/// row from another tenant is no row at all, which is what makes an
/// unknown and a foreign conversation the same `404`.
pub(crate) async fn find_conversation(
    db: &dyn Database,
    tenant_id: &str,
    conversation_id: &str,
) -> Result<Option<Conversation>, DbError> {
    let rows = db
        .query(&Statement::with_values(
            "SELECT id, needs_escalation FROM sg_conversations \
             WHERE id = ? AND tenant_id = ?",
            vec![conversation_id.into(), tenant_id.into()],
        ))
        .await?;
    Ok(rows.first().map(|row| Conversation {
        id: row.get::<String>("id").unwrap_or_default(),
        needs_escalation: row.get::<i64>("needs_escalation").unwrap_or(0) != 0,
    }))
}

/// How many assistant messages on this conversation were clarifies — the
/// budget [`crate::answer::MAX_CLARIFY_TURNS`] is spent against.
pub(crate) async fn count_clarifies(
    db: &dyn Database,
    tenant_id: &str,
    conversation_id: &str,
) -> Result<u32, DbError> {
    let rows = db
        .query(&Statement::with_values(
            "SELECT COUNT(*) AS clarifies FROM sg_messages \
             WHERE conversation_id = ? AND tenant_id = ? \
             AND role = 'assistant' AND outcome = 'clarify'",
            vec![conversation_id.into(), tenant_id.into()],
        ))
        .await?;
    let count = rows
        .first()
        .and_then(|row| row.get::<i64>("clarifies"))
        .unwrap_or(0);
    Ok(u32::try_from(count).unwrap_or(u32::MAX))
}

/// The tenant's stored answer threshold, in percent. `None` when the
/// tenant has never set one and the deployment default applies.
pub(crate) async fn tenant_threshold_pct(
    db: &dyn Database,
    tenant_id: &str,
) -> Result<Option<i64>, DbError> {
    let rows = db
        .query(&Statement::with_values(
            "SELECT answer_threshold_pct FROM sg_tenant_settings WHERE tenant_id = ?",
            vec![tenant_id.into()],
        ))
        .await?;
    Ok(rows
        .first()
        .and_then(|row| row.get::<i64>("answer_threshold_pct")))
}

/// Upserts the tenant's answer threshold. `ON CONFLICT … DO UPDATE SET …
/// = excluded.…` is the one upsert form SQLite and Postgres agree on.
pub(crate) async fn upsert_settings(
    db: &dyn Database,
    tenant_id: &str,
    answer_threshold_pct: i64,
    now: &str,
) -> Result<(), DbError> {
    db.execute(&Statement::with_values(
        "INSERT INTO sg_tenant_settings (tenant_id, answer_threshold_pct, updated_at) \
         VALUES (?, ?, ?) \
         ON CONFLICT (tenant_id) DO UPDATE \
         SET answer_threshold_pct = excluded.answer_threshold_pct, \
             updated_at = excluded.updated_at",
        vec![tenant_id.into(), answer_threshold_pct.into(), now.into()],
    ))
    .await?;
    Ok(())
}

/// One turn's write, built by the handler once the decision is made.
pub(crate) struct Turn {
    pub conversation_id: String,
    pub tenant_id: String,
    /// `None` when the conversation is created by this turn.
    pub conversation_existed: bool,
    pub conversation_status: String,
    pub conversation_needs_escalation: bool,
    pub created_at: String,
    pub updated_at: String,
    pub user_message_id: String,
    pub user_message: String,
    pub assistant_message_id: String,
    /// The assistant body that gets *published* — the model's answer when
    /// the outcome is `answered`, the canned message otherwise.
    pub assistant_body: String,
    /// What the model actually said, stored whatever the outcome. For an
    /// `answered` turn it equals `assistant_body`; for a downgraded one the
    /// two deliberately differ — `assistant_body` is the canned message the
    /// user was shown, this is the raw material an escalation audit reads.
    /// Keeping the published text in `body` is what stops a future
    /// transcript endpoint from leaking an unfounded answer by default.
    pub model_answer: String,
    pub outcome: String,
    pub confidence_pct: i64,
    /// The model's raw citations as a JSON string, persisted whatever the
    /// outcome — the response may hide them, the row does not.
    pub citations_json: String,
}

/// Writes one turn: conversation (create or update) plus the user message
/// and the assistant message, in **one** [`Database::batch_atomic`] —
/// all-or-nothing on every engine, so a half turn (a conversation with no
/// messages, an assistant reply with no user question behind it) cannot
/// survive a crash between statements.
pub(crate) async fn record_turn(db: &dyn Database, turn: &Turn) -> Result<(), DbError> {
    let escalation = i64::from(turn.conversation_needs_escalation);
    let conversation = if turn.conversation_existed {
        Statement::with_values(
            "UPDATE sg_conversations \
             SET status = ?, needs_escalation = ?, updated_at = ? \
             WHERE id = ? AND tenant_id = ?",
            vec![
                turn.conversation_status.clone().into(),
                escalation.into(),
                turn.updated_at.clone().into(),
                turn.conversation_id.clone().into(),
                turn.tenant_id.clone().into(),
            ],
        )
    } else {
        Statement::with_values(
            "INSERT INTO sg_conversations \
             (id, tenant_id, status, needs_escalation, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?)",
            vec![
                turn.conversation_id.clone().into(),
                turn.tenant_id.clone().into(),
                turn.conversation_status.clone().into(),
                escalation.into(),
                turn.created_at.clone().into(),
                turn.updated_at.clone().into(),
            ],
        )
    };
    let user_message = Statement::with_values(
        "INSERT INTO sg_messages \
         (id, conversation_id, tenant_id, role, body, outcome, confidence_pct, citations, \
          created_at) \
         VALUES (?, ?, ?, 'user', ?, NULL, NULL, NULL, ?)",
        vec![
            turn.user_message_id.clone().into(),
            turn.conversation_id.clone().into(),
            turn.tenant_id.clone().into(),
            turn.user_message.clone().into(),
            turn.created_at.clone().into(),
        ],
    );
    // `body` is what was shown to the user; `model_answer` is what the
    // model said. They are the same only when the outcome is `answered` —
    // for a downgraded turn the module published the canned message, and
    // the model's own words live here, out of every user-facing path.
    let assistant_message = Statement::with_values(
        "INSERT INTO sg_messages \
         (id, conversation_id, tenant_id, role, body, model_answer, outcome, confidence_pct, \
          citations, created_at) \
         VALUES (?, ?, ?, 'assistant', ?, ?, ?, ?, ?, ?)",
        vec![
            turn.assistant_message_id.clone().into(),
            turn.conversation_id.clone().into(),
            turn.tenant_id.clone().into(),
            turn.assistant_body.clone().into(),
            turn.model_answer.clone().into(),
            turn.outcome.clone().into(),
            turn.confidence_pct.into(),
            turn.citations_json.clone().into(),
            turn.updated_at.clone().into(),
        ],
    );
    db.batch_atomic(&[conversation, user_message, assistant_message])
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Chunk, query_terms, rank_chunks};

    fn chunk(id: &str, body: &str) -> Chunk {
        Chunk {
            id: id.to_owned(),
            body: body.to_owned(),
        }
    }

    #[test]
    fn terms_are_lowercase_and_at_least_three_characters() {
        assert_eq!(
            query_terms("How do I reset my Password?"),
            ["how", "password", "reset"]
        );
        // Tokens under three characters are dropped, and duplicates count
        // once.
        assert_eq!(query_terms("to be or do I"), Vec::<String>::new());
        assert_eq!(query_terms("refund refund refund"), ["refund"]);
    }

    #[test]
    fn ranking_scores_distinct_terms_and_orders_stably() {
        let chunks = [
            chunk("b", "Reset your password from the settings page."),
            chunk("a", "Password resets and password policies."),
            chunk("c", "Invoices are issued monthly."),
        ];
        // `a` matches twice (password, resets... "resets" does not match
        // the term "reset" — but "password" appears twice as a term only
        // once). Distinct-term scoring: `a` hits "password" and "reset"
        // (substring of "resets"), `b` hits "reset" and "password" — tie
        // broken by id. `c` scores zero and ranks last, but stays inside
        // k: "nothing retrieved" means an empty corpus, not a question
        // with no lexical overlap.
        let ranked = rank_chunks(&chunks, "password reset", 10);
        assert_eq!(ranked.len(), 3);
        assert_eq!(ranked[0].id, "a");
        assert_eq!(ranked[1].id, "b");
        assert_eq!(ranked[2].id, "c");
    }

    #[test]
    fn ranking_takes_k_and_ranks_zero_scores_last() {
        let chunks = [
            chunk("noise", "Nothing here."),
            chunk("hit", "The billing plan and the billing period."),
            chunk("also-noise", "Also nothing."),
        ];
        // k cuts after scoring; zero-score chunks come back inside it,
        // ordered by the id tie-break.
        let ranked = rank_chunks(&chunks, "billing plan", 1);
        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].id, "hit");

        let ranked = rank_chunks(&chunks, "billing plan", 2);
        assert_eq!(ranked.len(), 2);
        assert_eq!(ranked[0].id, "hit");
        assert_eq!(ranked[1].id, "also-noise");
    }

    #[test]
    fn ranking_beyond_the_end_returns_what_exists() {
        let chunks = [chunk("only", "The billing plan.")];
        let ranked = rank_chunks(&chunks, "billing", 6);
        assert_eq!(ranked.len(), 1);
    }
}
