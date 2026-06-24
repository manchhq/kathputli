use anyhow::Result;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::ActorHandle;
use crate::stats::ActorStatsSnapshot;

/// Lifecycle-aware wrapper around [`ActorHandle`].
///
/// The owner of `ActorRef` controls the actor's lifetime via [`shutdown`].
/// Communicators that only need to send messages should receive a cloned
/// [`ActorHandle`] via [`handle`].
pub struct ActorRef<Msg: Send + 'static> {
    handle: ActorHandle<Msg>,
    token: CancellationToken,
    poison: CancellationToken,
}

impl<Msg: Send + 'static> Clone for ActorRef<Msg> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            token: self.token.clone(),
            poison: self.poison.clone(),
        }
    }
}

impl<Msg: Send + 'static> ActorRef<Msg> {
    pub(crate) fn new(handle: ActorHandle<Msg>, token: CancellationToken) -> Self {
        Self { handle, token, poison: CancellationToken::new() }
    }

    /// Like [`new`] but with an explicit poison token (used by the supervisor).
    pub(crate) fn new_with_poison(
        handle: ActorHandle<Msg>,
        token: CancellationToken,
        poison: CancellationToken,
    ) -> Self {
        Self { handle, token, poison }
    }

    /// Fire-and-forget: send a message without waiting for a reply.
    pub fn tell(&self, msg: Msg) -> Result<()> {
        self.handle.tell(msg)
    }

    /// Request-response: send a message and await the reply.
    ///
    /// `make_msg` receives the reply channel and must embed it in the message.
    pub async fn ask<R>(&self, make_msg: impl FnOnce(oneshot::Sender<R>) -> Msg) -> Result<R> {
        self.handle.ask(make_msg).await
    }

    /// Cancel the actor's token, causing the spawn loop to exit cleanly after
    /// any in-flight `handle()` call completes.
    pub fn shutdown(&self) {
        self.token.cancel();
    }

    /// Graceful stop: stop accepting new messages, drain what is already
    /// queued, then exit. Contrast with [`shutdown`], which stops as soon as
    /// the in-flight message completes.
    pub fn poison(&self) {
        self.poison.cancel();
    }

    /// Returns `true` if the actor has not yet been shut down.
    pub fn is_alive(&self) -> bool {
        !self.token.is_cancelled()
    }

    /// Snapshot of the actor's activity counters and current mailbox depth.
    /// See [`ActorHandle::stats`](crate::ActorHandle::stats).
    pub fn stats(&self) -> ActorStatsSnapshot {
        self.handle.stats()
    }

    /// Borrow the underlying [`ActorHandle`] for code that only needs to
    /// communicate with the actor, not own its lifecycle.
    pub fn handle(&self) -> &ActorHandle<Msg> {
        &self.handle
    }

    /// Create a child cancellation token that is cancelled automatically when
    /// this actor shuts down — use this for parent→child lifecycle propagation.
    pub fn child_token(&self) -> CancellationToken {
        self.token.child_token()
    }
}
