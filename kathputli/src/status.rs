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
    RecentEvents(oneshot::Sender<Vec<String>>),
    /// Internal: fed by the event forwarder.
    Event(crate::supervisor::SupervisionEvent),
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

    /// Recent lifecycle events as formatted strings (bounded log, up to 256 entries).
    pub async fn recent_events(&self) -> Vec<String> {
        self.inner
            .ask(StatusMsg::RecentEvents)
            .await
            .unwrap_or_default()
    }

    /// Raw ref (for advanced use / custom messages).
    pub fn raw(&self) -> &ActorRef<StatusMsg> {
        &self.inner
    }
}

/// Build the per-message status behavior. The actor reads the live system tree
/// on each query (pull model for structure + busy/idle). Also maintains a
/// bounded recent-events log fed by a forwarder task (push model).
#[cfg(feature = "system")]
pub(crate) fn spawn_status_actor(system: &ActorSystem, root: ActorId) -> StatusRef {
    use std::collections::VecDeque;

    struct StatusState {
        sys: ActorSystem,
        log: VecDeque<String>,
    }
    const LOG_CAP: usize = 256;

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
        move |_ctx| StatusState { sys: sys_for_init.clone(), log: VecDeque::new() },
        |mut st, msg: StatusMsg, _ctx| async move {
            match msg {
                StatusMsg::GetTree(reply) => {
                    let _ = reply.send(st.sys.snapshot_forest());
                }
                StatusMsg::GetActor(id, reply) => {
                    let _ = reply.send(st.sys.snapshot_actor(id));
                }
                StatusMsg::RecentEvents(reply) => {
                    let _ = reply.send(st.log.iter().cloned().collect());
                }
                StatusMsg::Event(ev) => {
                    if st.log.len() == LOG_CAP {
                        st.log.pop_front();
                    }
                    st.log.push_back(format!("{ev:?}"));
                }
            }
            st
        },
    );
    let status = StatusRef::new(actor);

    // Forwarder: stream lifecycle events into the status actor's log.
    // Use async send (backpressure) instead of try_send so a full mailbox does not
    // kill the forwarder.  Only a closed channel means the status actor is gone.
    let mut rx = system.events();
    let sink = status.raw().handle().clone();
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(ev) => {
                    // Backpressure: await delivery instead of dropping on full mailbox.
                    // Only a closed channel (status actor gone) returns Err here.
                    if sink.sender.send(StatusMsg::Event(ev)).await.is_err() {
                        break; // status actor gone
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    status
}
