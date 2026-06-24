//! Actor context — passed to `update` each message, gives the actor access to
//! its own identity and the supervising system.

#[cfg(feature = "system")]
use std::sync::Arc;

#[cfg(feature = "system")]
use tokio_util::sync::CancellationToken;

#[cfg(feature = "system")]
use crate::actor_ref::ActorRef;
#[cfg(feature = "system")]
use crate::id::ActorId;
#[cfg(feature = "system")]
use crate::supervisor::ActorSystem;

/// Per-message context passed to the `update` function of every supervised actor.
///
/// Gives the actor read-only access to its own identity, its parent, its own
/// `ActorRef`, and the supervising system. Cheap to clone — all fields are
/// `Arc`/`Clone`.
///
/// Child-spawning is available via [`Context::spawn`] and [`Context::spawn_once`].
/// Status query comes in Task 7.
#[cfg(feature = "system")]
pub struct Context<M: Send + 'static> {
    pub(crate) id: ActorId,
    pub(crate) name: Arc<str>,
    pub(crate) parent: Option<ActorId>,
    pub(crate) token: CancellationToken,
    pub(crate) myself: ActorRef<M>,
    pub(crate) system: ActorSystem,
}

#[cfg(feature = "system")]
impl<M: Send + 'static> Clone for Context<M> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            name: self.name.clone(),
            parent: self.parent,
            token: self.token.clone(),
            myself: self.myself.clone(),
            system: self.system.clone(),
        }
    }
}

#[cfg(feature = "system")]
impl<M: Send + 'static> Context<M> {
    /// This actor's unique id.
    pub fn id(&self) -> ActorId {
        self.id
    }

    /// This actor's registered name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The parent actor's id, if any.
    pub fn parent_id(&self) -> Option<ActorId> {
        self.parent
    }

    /// A cloned `ActorRef` pointing back at this actor — useful for sending
    /// self-messages or passing the ref to other actors.
    pub fn myself(&self) -> ActorRef<M> {
        self.myself.clone()
    }

    /// A reference to the supervising `ActorSystem`.
    pub fn system(&self) -> &ActorSystem {
        &self.system
    }

    /// Spawn a supervised child of this actor (cascade-linked to its lifetime).
    pub fn spawn<M2, S, I, U, Fut>(
        &self,
        name: impl Into<String>,
        init: I,
        update: U,
    ) -> ActorRef<M2>
    where
        M2: Send + 'static,
        S: Send + 'static,
        I: Fn(crate::context::Context<M2>) -> S + Send + Sync + 'static,
        U: Fn(S, M2, crate::context::Context<M2>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = S> + Send,
    {
        let opts = crate::supervisor::SpawnOptions { name: name.into(), ..Default::default() };
        self.system
            .spawn_supervised(Some(self.id), self.token.child_token(), opts, init, update)
    }

    /// Spawn a kamikaze child of this actor.
    pub fn spawn_once<F, Fut>(&self, name: impl Into<String>, task: F) -> ActorRef<()>
    where
        F: Fn(crate::context::Context<()>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        let opts = crate::supervisor::SpawnOptions { name: name.into(), ..Default::default() };
        self.system
            .spawn_once_supervised(Some(self.id), self.token.child_token(), opts, task)
    }

    /// Park until this actor's token is cancelled (handy for long-lived children
    /// in `spawn_once`).
    pub async fn token_wait(&self) {
        self.token.cancelled().await;
    }

    /// Handle to the system's status actor.
    pub fn status(&self) -> crate::status::StatusRef {
        self.system.status()
    }
}
