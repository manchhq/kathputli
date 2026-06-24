//! Status actor: query the live actor tree.

#[cfg(feature = "system")]
use tokio::sync::oneshot;

#[cfg(feature = "system")]
use crate::actor_ref::ActorRef;
#[cfg(feature = "system")]
use crate::id::ActorId;
#[cfg(feature = "system")]
use crate::supervisor::ActorSystem;

/// Messages the status actor answers.
#[cfg(feature = "system")]
pub enum StatusMsg {
    GetTree(oneshot::Sender<Vec<ActorNode>>),
    GetActor(ActorId, oneshot::Sender<Option<ActorStatus>>),
}

/// Point-in-time status of one actor.
#[cfg(feature = "system")]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ActorStatus {
    pub id: ActorId,
    pub name: String,
    pub parent: Option<ActorId>,
    pub alive: bool,
    pub busy: bool,
    pub mailbox_depth: usize,
    pub message_count: u64,
    pub restarts: u32,
    /// Milliseconds since this actor last started handling a message; `None` if
    /// it has never handled one.
    pub idle_ms: Option<u64>,
}

/// A node in the supervision tree snapshot.
#[cfg(feature = "system")]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ActorNode {
    pub status: ActorStatus,
    pub children: Vec<ActorNode>,
}

/// Cheap-clone handle to the status actor.
#[cfg(feature = "system")]
#[derive(Clone)]
pub struct StatusRef {
    inner: ActorRef<StatusMsg>,
}

#[cfg(feature = "system")]
impl StatusRef {
    pub(crate) fn new(inner: ActorRef<StatusMsg>) -> Self {
        Self { inner }
    }

    /// Whole-tree snapshot (forest rooted at actors with no parent).
    pub async fn tree(&self) -> Vec<ActorNode> {
        self.inner
            .ask(StatusMsg::GetTree)
            .await
            .unwrap_or_default()
    }

    /// Snapshot of a single actor by id.
    pub async fn actor(&self, id: ActorId) -> Option<ActorStatus> {
        self.inner
            .ask(|reply| StatusMsg::GetActor(id, reply))
            .await
            .ok()
            .flatten()
    }

    /// Raw ref (for advanced use / custom messages).
    pub fn raw(&self) -> &ActorRef<StatusMsg> {
        &self.inner
    }
}

/// Build the per-message status behavior. The actor reads the live system tree
/// on each query (pull model for structure + busy/idle).
#[cfg(feature = "system")]
pub(crate) fn spawn_status_actor(system: &ActorSystem, root: ActorId) -> StatusRef {
    let sys_for_init = system.clone();
    let parent_token = system
        .inner
        .tree
        .lock()
        .unwrap()
        .get(&root)
        .map(|e| e.token.clone())
        .unwrap_or_default();
    let opts = crate::supervisor::SpawnOptions {
        name: "status".to_string(),
        ..Default::default()
    };
    let actor = system.spawn_supervised(
        Some(root),
        parent_token,
        opts,
        move |_ctx| sys_for_init.clone(), // state = a system handle for reads
        |sys, msg: StatusMsg, _ctx| async move {
            match msg {
                StatusMsg::GetTree(reply) => {
                    let _ = reply.send(sys.snapshot_forest());
                }
                StatusMsg::GetActor(id, reply) => {
                    let _ = reply.send(sys.snapshot_actor(id));
                }
            }
            sys
        },
    );
    StatusRef::new(actor)
}

/// Placeholder for non-system builds (satisfies the type in Inner).
#[cfg(not(feature = "system"))]
#[derive(Clone)]
pub struct StatusRef;
