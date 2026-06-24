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
/// Child-spawning and `status()` come in Tasks 6/7 — do NOT add them here yet.
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

    /// The actor's own cancellation token (cancelled when the actor stops).
    #[allow(dead_code)] // Task 6
    pub(crate) fn token(&self) -> &CancellationToken {
        &self.token
    }
}
