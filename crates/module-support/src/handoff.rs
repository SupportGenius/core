//! The port the support module reaches a handoff through, and why it is a
//! port rather than a dependency.
//!
//! When a turn hands off, the escalation module must learn about it in
//! the *same* atomic write as the turn, and its pipeline must then run
//! now rather than at the next cron tick. Support cannot depend on
//! `module-escalation` — modules never depend on each other — and `Ports`
//! has no extension slot for a port core does not define, so support
//! declares the smallest port it needs and the *composition*, which
//! depends on both modules, adapts escalation to it.
//!
//! The split of work is deliberate. Escalation keeps sole ownership of
//! its schema and its statement builders: [`enqueue`](HandoffSink::enqueue)
//! hands back the [`Statement`]s escalation wrote, and support never
//! writes an `sg_tickets` row itself. Support knows only "something can
//! enqueue statements for a conversation, and can be kicked to run now".
//! Nothing new appears in [`Ports`](cratefield_core::Ports). A `Support`
//! built with [`Support::new`](crate::Support::new) and no sink keeps
//! today's behaviour exactly: a handoff still marks `needs_escalation` on
//! the turn and nothing files a ticket.

use std::sync::Arc;

use cratefield_core::{BoxFuture, Defer, ModuleContext, Statement};
use serde::Serialize;

/// One ticket as the MCP `get_ticket` tool (issue #34) shows it: the
/// support module's own small view of an escalation ticket, so it never
/// learns `module-escalation`'s `Ticket` type. A [`HandoffSink`] that can
/// look a ticket up maps its row into this.
#[derive(Debug, Clone, Serialize)]
pub struct TicketView {
    pub id: String,
    /// The module's own status wire form (`"filed"`, `"dead_letter"`, …).
    pub status: String,
    /// The stage whose work is queued for the ticket.
    pub stage: String,
    /// The tracker's ticket id, once the file stage has run.
    pub external_id: Option<String>,
    /// The tracker's ticket URL, once the file stage has run.
    pub external_url: Option<String>,
    pub conversation_id: String,
}

/// What the support module calls when a turn hands off.
///
/// Two phases, because they sit on opposite sides of the turn's one atomic
/// commit. [`enqueue`](Self::enqueue) returns statements the caller appends
/// to its own [`Database::batch_atomic`](cratefield_core::Database::batch_atomic),
/// so the turn and its ticket commit or fail together — there is never a
/// turn answered "escalated" with no ticket behind it, nor a ticket for a
/// turn that rolled back. [`kick`](Self::kick) is called only *after* that
/// batch committed, to run the escalation pipeline immediately instead of
/// waiting for the scheduled drain.
pub trait HandoffSink: Send + Sync {
    /// The statements that escalate one conversation, to be appended to the
    /// turn's own `batch_atomic`. Pure: nothing is written until the caller
    /// commits, and a caller that never commits writes nothing.
    fn enqueue(
        &self,
        ctx: &ModuleContext,
        tenant_id: &str,
        conversation_id: &str,
        transcript: &str,
    ) -> Vec<Statement>;

    /// Run the escalation pipeline now. Called only after the batch that
    /// carried [`enqueue`](Self::enqueue)'s statements committed, so a
    /// failure here costs a delay — the scheduled drain is the backstop —
    /// never the ticket. `defer` is the request's own port, so the work
    /// rides the runtime's background execution rather than blocking the
    /// response.
    fn kick(&self, ctx: &ModuleContext, defer: Arc<dyn Defer>);

    /// [`enqueue`](Self::enqueue), handing back the minted ticket id when
    /// the sink can name it. The MCP `escalate` tool (issue #34) answers
    /// with the id, which only a sink that reached escalation's
    /// `Intake::handoff` can supply; the default drops it and keeps the
    /// old one-value shape for every existing implementor.
    fn handoff(
        &self,
        ctx: &ModuleContext,
        tenant_id: &str,
        conversation_id: &str,
        transcript: &str,
    ) -> (Option<String>, Vec<Statement>) {
        (
            None,
            self.enqueue(ctx, tenant_id, conversation_id, transcript),
        )
    }

    /// Looks a ticket up for the MCP `get_ticket` tool, scoped to
    /// `tenant_id`: a sink that has no ticket store, or none by that id
    /// for this tenant, answers `None` — a ticket from another tenant
    /// looks exactly like a missing one. Defaults to `None`, so a sink
    /// that only files keeps compiling.
    fn ticket<'a>(
        &'a self,
        ctx: &'a ModuleContext,
        tenant_id: &'a str,
        id: &'a str,
    ) -> BoxFuture<'a, Option<TicketView>> {
        let _ = (ctx, tenant_id, id);
        Box::pin(async { None })
    }
}
