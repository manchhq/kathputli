use std::sync::Arc;

use anyhow::{Result, anyhow};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::stats::{ActorStats, ActorStatsSnapshot};
use crate::{Actor, ActorRef};

/// Cheap-clone handle to an actor's mailbox.
///
/// Cloning is O(1) — it just increments the Arc refcount on the channel sender.
/// When all handles are dropped the actor's receive loop exits naturally.
///
/// To also control the actor's lifecycle (explicit shutdown), use [`ActorRef`]
/// returned by [`spawn`]. Share `ActorHandle` clones with code that only needs
/// to communicate.
pub struct ActorHandle<Msg: Send + 'static> {
    pub(crate) sender: mpsc::Sender<Msg>,
    pub(crate) stats: Arc<ActorStats>,
}

impl<Msg: Send + 'static> Clone for ActorHandle<Msg> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            stats: self.stats.clone(),
        }
    }
}

impl<Msg: Send + 'static> ActorHandle<Msg> {
    /// Snapshot of the actor's activity counters and current mailbox depth.
    ///
    /// `mailbox_depth` is read live from the channel and is approximate — it
    /// excludes any message currently being handled and may lag in-flight sends.
    pub fn stats(&self) -> ActorStatsSnapshot {
        let depth = self.sender.max_capacity() - self.sender.capacity();
        self.stats.snapshot(depth)
    }

    /// Fire-and-forget: send a message without waiting for a reply.
    pub fn tell(&self, msg: Msg) -> Result<()> {
        self.sender
            .try_send(msg)
            .map_err(|e| anyhow!("actor mailbox error: {e}"))
    }

    /// Request-response: send a message and await the reply.
    ///
    /// `make_msg` receives the reply channel and must embed it in the message.
    ///
    /// # Errors
    /// Returns an error if the actor has shut down or the reply is never sent.
    pub async fn ask<Resp>(
        &self,
        make_msg: impl FnOnce(oneshot::Sender<Resp>) -> Msg,
    ) -> Result<Resp> {
        let (tx, rx) = oneshot::channel();
        let msg = make_msg(tx);
        self.sender
            .send(msg)
            .await
            .map_err(|_| anyhow!("actor is dead — cannot send message"))?;
        rx.await
            .map_err(|_| anyhow!("actor dropped reply sender without responding"))
    }
}

/// Spawn an actor and return a lifecycle-aware [`ActorRef`].
///
/// The actor runs in a dedicated `tokio::spawn` task and processes messages
/// sequentially. The task exits when:
/// - all `ActorHandle` clones derived from the ref are dropped, OR
/// - [`ActorRef::shutdown`] is called (token is cancelled).
///
/// An in-flight `handle()` call always completes before the loop exits —
/// cancellation is only checked between messages.
pub fn spawn<A: Actor>(mut actor: A, buffer: usize) -> ActorRef<A::Msg> {
    let (tx, mut rx) = mpsc::channel(buffer);
    let token = CancellationToken::new();
    let token_loop = token.clone();
    let stats = Arc::new(ActorStats::new());
    let stats_loop = stats.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = token_loop.cancelled() => break,
                msg = rx.recv() => match msg {
                    Some(m) => {
                        stats_loop.record_start();
                        handle_one(&mut actor, m).await;
                        stats_loop.record_finish();
                    }
                    None => break,
                },
            }
        }
    });
    ActorRef::new(ActorHandle { sender: tx, stats }, token)
}

/// Dispatch a single message to the actor.
///
/// With the `tracing` feature enabled, the handling is wrapped in a
/// `trace`-level span (via `Instrument`, so no span guard is held across the
/// `.await`). Without it, this is a zero-overhead direct call.
#[cfg(feature = "tracing")]
async fn handle_one<A: Actor>(actor: &mut A, msg: A::Msg) {
    use tracing::Instrument;
    actor
        .handle(msg)
        .instrument(tracing::trace_span!(
            "kathputli.handle",
            actor = std::any::type_name::<A>()
        ))
        .await;
}

#[cfg(not(feature = "tracing"))]
async fn handle_one<A: Actor>(actor: &mut A, msg: A::Msg) {
    actor.handle(msg).await;
}
