//! Supervising actor system: lifecycle tree, restart, escalation.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use crate::handle::ActorHandle;
use crate::id::ActorId;
use crate::stats::ActorStats;

/// Lifecycle events broadcast by the system. Subscribe via [`ActorSystem::events`].
#[derive(Clone, Debug)]
pub enum SupervisionEvent {
    Spawned { id: ActorId, name: Arc<str>, parent: Option<ActorId> },
    Restarted { id: ActorId, restarts: u32 },
    Failed { id: ActorId, ancestry: Vec<ActorId>, error: Arc<str> },
    Stopped { id: ActorId },
}

/// Tuning for a supervised actor.
#[derive(Clone, Debug)]
pub struct SpawnOptions {
    pub name: String,
    pub max_restarts: u32,
    pub buffer: usize,
}

impl Default for SpawnOptions {
    fn default() -> Self {
        Self { name: String::new(), max_restarts: 3, buffer: 16 }
    }
}

/// Type-erased lifecycle record for one supervised actor.
// Fields are written here; `name`/`token`/`stats`/`restarts`/`depth_probe`
// are read by the status/child-spawn logic arriving in Tasks 6-8.
#[allow(dead_code)] // Task 6–8
pub(crate) struct SupervisedEntry {
    pub(crate) name: Arc<str>,
    pub(crate) parent: Option<ActorId>,
    pub(crate) children: Vec<ActorId>,
    pub(crate) token: CancellationToken,
    pub(crate) stats: Arc<ActorStats>,
    pub(crate) restarts: Arc<AtomicU32>,
    /// Reads the live mailbox depth (captures the typed sender).
    pub(crate) depth_probe: Arc<dyn Fn() -> usize + Send + Sync>,
}

pub(crate) struct Inner {
    pub(crate) tree: Mutex<HashMap<ActorId, SupervisedEntry>>,
    pub(crate) events: broadcast::Sender<SupervisionEvent>,
    /// Root cancellation token; child tokens cascade from here.
    pub(crate) token: CancellationToken,
    /// Set once the status actor is up (see Task 7). `None` in `bare()`.
    pub(crate) status: Mutex<Option<crate::status::StatusRef>>,
    /// Set once the root actor is up (see Task 7).
    pub(crate) root: Mutex<Option<ActorId>>,
    /// Keeps the root actor's mailbox alive so the root never exits on its own.
    /// `None` in `bare()`, set in `start()`.
    #[cfg(feature = "system")]
    pub(crate) root_ref: Mutex<Option<crate::actor_ref::ActorRef<()>>>,
}

/// The supervising actor system. Cheap to clone (`Arc` inside).
#[derive(Clone)]
pub struct ActorSystem {
    pub(crate) inner: Arc<Inner>,
}

impl ActorSystem {
    /// A system with no root/status actors — used by tree unit tests and as the
    /// base for [`start`](ActorSystem::start) (see Task 7).
    // Exercised by this task's tests (cfg(test)); the non-test spawn path that
    // calls bare/register/deregister/ancestry/emit lands in Task 5.
    pub(crate) fn bare() -> Self {
        let (events, _) = broadcast::channel(256);
        ActorSystem {
            inner: Arc::new(Inner {
                tree: Mutex::new(HashMap::new()),
                events,
                token: CancellationToken::new(),
                status: Mutex::new(None),
                root: Mutex::new(None),
                #[cfg(feature = "system")]
                root_ref: Mutex::new(None),
            }),
        }
    }

    /// Public constructor — creates a fresh system with a root cancellation token.
    pub fn new() -> Self {
        Self::bare()
    }

    /// Subscribe to lifecycle events.
    pub fn events(&self) -> broadcast::Receiver<SupervisionEvent> {
        self.inner.events.subscribe()
    }

    pub(crate) fn emit(&self, ev: SupervisionEvent) {
        let _ = self.inner.events.send(ev); // Err only means no subscribers.
    }

    pub(crate) fn register(&self, id: ActorId, entry: SupervisedEntry) {
        let mut tree = self.inner.tree.lock().unwrap();
        if let Some(parent) = entry.parent {
            if let Some(p) = tree.get_mut(&parent) {
                p.children.push(id);
            }
        }
        tree.insert(id, entry);
    }

    pub(crate) fn deregister(&self, id: ActorId) {
        let mut tree = self.inner.tree.lock().unwrap();
        if let Some(entry) = tree.remove(&id) {
            if let Some(parent) = entry.parent {
                if let Some(p) = tree.get_mut(&parent) {
                    p.children.retain(|c| *c != id);
                }
            }
        }
    }

    /// Parent chain from `id` (exclusive) up to the root, nearest first.
    pub(crate) fn ancestry(&self, id: ActorId) -> Vec<ActorId> {
        let tree = self.inner.tree.lock().unwrap();
        let mut out = Vec::new();
        let mut cur = tree.get(&id).and_then(|e| e.parent);
        while let Some(p) = cur {
            out.push(p);
            cur = tree.get(&p).and_then(|e| e.parent);
        }
        out
    }

    /// Snapshot helper for tests: clones the structural fields of the tree.
    #[cfg(test)]
    pub(crate) fn read_tree(&self) -> HashMap<ActorId, TreeView> {
        let tree = self.inner.tree.lock().unwrap();
        tree.iter()
            .map(|(id, e)| (*id, TreeView { children: e.children.clone() }))
            .collect()
    }

    pub(crate) fn root_token(&self) -> CancellationToken {
        self.inner.token.clone()
    }
}

impl Default for ActorSystem {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "system")]
impl ActorSystem {
    /// Spawn a supervised actor with `init`/`update` lifecycle.
    ///
    /// Returns an `ActorRef<M>` whose channel is backed by the supervised
    /// mailbox — messages survive restarts.
    pub fn spawn<M, S, I, U, Fut>(
        &self,
        name: impl Into<String>,
        init: I,
        update: U,
    ) -> crate::ActorRef<M>
    where
        M: Send + 'static,
        S: Send + 'static,
        I: Fn(crate::context::Context<M>) -> S + Send + Sync + 'static,
        U: Fn(S, M, crate::context::Context<M>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = S> + Send,
    {
        let opts = SpawnOptions {
            name: name.into(),
            ..Default::default()
        };
        // If a root actor exists (system was started via `start()`), attach to it
        // so the actor is cascade-stopped when the system shuts down.
        let (parent, parent_token) = {
            let root_guard = self.inner.root.lock().unwrap();
            if let Some(root_id) = *root_guard {
                let tree = self.inner.tree.lock().unwrap();
                let token = tree
                    .get(&root_id)
                    .map(|e| e.token.clone())
                    .unwrap_or_else(|| self.root_token());
                (Some(root_id), token)
            } else {
                (None, self.root_token())
            }
        };
        self.spawn_supervised(parent, parent_token, opts, init, update)
    }

    pub(crate) fn spawn_supervised<M, S, I, U, Fut>(
        &self,
        parent: Option<ActorId>,
        parent_token: CancellationToken,
        opts: SpawnOptions,
        init: I,
        update: U,
    ) -> crate::ActorRef<M>
    where
        M: Send + 'static,
        S: Send + 'static,
        I: Fn(crate::context::Context<M>) -> S + Send + Sync + 'static,
        U: Fn(S, M, crate::context::Context<M>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = S> + Send,
    {
        use futures_util::FutureExt;
        use std::panic::AssertUnwindSafe;

        let id = ActorId::next();
        let token = parent_token.child_token();
        let poison = CancellationToken::new();
        let (tx, mut rx) = mpsc::channel::<M>(opts.buffer);
        let stats = Arc::new(ActorStats::default());
        let name: Arc<str> = opts.name.as_str().into();
        let restarts = Arc::new(AtomicU32::new(0));

        let probe_tx = tx.clone();
        let depth_probe: Arc<dyn Fn() -> usize + Send + Sync> =
            Arc::new(move || probe_tx.max_capacity() - probe_tx.capacity());

        let entry = SupervisedEntry {
            name: name.clone(),
            parent,
            children: Vec::new(),
            token: token.clone(),
            stats: stats.clone(),
            restarts: restarts.clone(),
            depth_probe,
        };
        self.register(id, entry);
        self.emit(SupervisionEvent::Spawned { id, name: name.clone(), parent });

        let handle = ActorHandle { sender: tx, stats: stats.clone() };

        let ctx = crate::context::Context {
            id,
            name: name.clone(),
            parent,
            token: token.clone(),
            myself: crate::ActorRef::new_with_poison(handle.clone(), token.clone(), poison.clone()),
            system: self.clone(),
        };

        let system = self.clone();
        let loop_token = token.clone();
        let loop_poison = poison.clone();
        let max = opts.max_restarts;

        tokio::spawn(async move {
            loop {
                let incarnation = AssertUnwindSafe(run_incarnation(
                    &mut rx,
                    &loop_token,
                    &loop_poison,
                    &init,
                    &update,
                    &ctx,
                    &stats,
                ));
                match incarnation.catch_unwind().await {
                    Ok(()) => break, // clean exit
                    Err(panic) => {
                        // A panic skipped record_finish; clear the stale busy flag.
                        stats.record_finish();
                        let n = restarts.fetch_add(1, Ordering::Relaxed) + 1;
                        if n > max {
                            let ancestry = system.ancestry(id);
                            system.emit(SupervisionEvent::Failed {
                                id,
                                ancestry,
                                error: panic_message(panic),
                            });
                            break; // exhausted → die, no auto-restart
                        }
                        system.emit(SupervisionEvent::Restarted { id, restarts: n });
                        // loop: re-run incarnation, SAME rx (mailbox preserved).
                    }
                }
            }
            // Cleanup: cancel (cascade children), deregister (no zombie), notify.
            // Ensure busy flag is clear so a dead actor is never read as busy.
            stats.record_finish();
            loop_token.cancel();
            system.deregister(id);
            system.emit(SupervisionEvent::Stopped { id });
        });

        crate::ActorRef::new_with_poison(handle, token, poison)
    }

    /// Spawn a kamikaze (one-shot) actor: runs `task` to completion, then dies.
    /// Restart-on-panic still applies (up to `max_restarts`), then escalate+die.
    pub fn spawn_once<F, Fut>(&self, name: impl Into<String>, task: F) -> crate::ActorRef<()>
    where
        F: Fn(crate::context::Context<()>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send,
    {
        let (parent, parent_token) = {
            let root_guard = self.inner.root.lock().unwrap();
            if let Some(root_id) = *root_guard {
                let tree = self.inner.tree.lock().unwrap();
                let token = tree
                    .get(&root_id)
                    .map(|e| e.token.clone())
                    .unwrap_or_else(|| self.root_token());
                (Some(root_id), token)
            } else {
                (None, self.root_token())
            }
        };
        let opts = SpawnOptions { name: name.into(), ..Default::default() };
        self.spawn_once_supervised(parent, parent_token, opts, task)
    }

    pub(crate) fn spawn_once_supervised<F, Fut>(
        &self,
        parent: Option<ActorId>,
        parent_token: CancellationToken,
        opts: SpawnOptions,
        task: F,
    ) -> crate::ActorRef<()>
    where
        F: Fn(crate::context::Context<()>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send,
    {
        use futures_util::FutureExt;
        use std::panic::AssertUnwindSafe;

        let (tx, _rx) = mpsc::channel::<()>(opts.buffer);
        let token = parent_token.child_token();
        let poison = CancellationToken::new();
        let stats = Arc::new(ActorStats::new());
        let restarts = Arc::new(AtomicU32::new(0));
        let id = ActorId::next();
        let name: Arc<str> = Arc::from(opts.name.as_str());
        let handle = ActorHandle { sender: tx.clone(), stats: stats.clone() };

        let probe_tx = tx.clone();
        let depth_probe: Arc<dyn Fn() -> usize + Send + Sync> =
            Arc::new(move || probe_tx.max_capacity() - probe_tx.capacity());

        self.register(
            id,
            SupervisedEntry {
                name: name.clone(),
                parent,
                children: Vec::new(),
                token: token.clone(),
                stats: stats.clone(),
                restarts: restarts.clone(),
                depth_probe,
            },
        );
        self.emit(SupervisionEvent::Spawned { id, name: name.clone(), parent });

        let ctx = crate::context::Context {
            id,
            name: name.clone(),
            parent,
            token: token.clone(),
            myself: crate::ActorRef::new_with_poison(handle.clone(), token.clone(), poison.clone()),
            system: self.clone(),
        };

        let system = self.clone();
        let loop_token = token.clone();
        let max = opts.max_restarts;

        tokio::spawn(async move {
            loop {
                let run = AssertUnwindSafe(async {
                    stats.record_start();
                    // Race the task against cancellation so shutdown is honored.
                    tokio::select! {
                        biased;
                        _ = loop_token.cancelled() => {}
                        _ = task(ctx.clone()) => {}
                    }
                    stats.record_finish();
                });
                match run.catch_unwind().await {
                    Ok(()) => break,
                    Err(panic) => {
                        // A panic skipped record_finish; clear the stale busy flag.
                        stats.record_finish();
                        let n = restarts.fetch_add(1, Ordering::Relaxed) + 1;
                        if n > max {
                            let ancestry = system.ancestry(id);
                            system.emit(SupervisionEvent::Failed {
                                id,
                                ancestry,
                                error: panic_message(panic),
                            });
                            break;
                        }
                        system.emit(SupervisionEvent::Restarted { id, restarts: n });
                    }
                }
            }
            // Ensure busy flag is clear so a dead actor is never read as busy.
            stats.record_finish();
            loop_token.cancel();
            system.deregister(id);
            system.emit(SupervisionEvent::Stopped { id });
        });

        crate::ActorRef::new_with_poison(handle, token, poison)
    }

    // ── System bootstrap ────────────────────────────────────────────────────

    /// Start a system: brings up a root actor and a status actor (child of root).
    pub fn start() -> Self {
        let sys = ActorSystem::bare();

        // Root actor: a parked actor that anchors the tree and shutdown.
        let root_token = sys.inner.token.clone();
        let root_ref = sys.spawn_supervised(
            None,
            root_token,
            SpawnOptions { name: "root".to_string(), ..Default::default() },
            |_ctx| (),
            |s, _m: (), _ctx| async move { s }, // never receives (handle kept private)
        );

        // Find the root id we just registered (the only parentless entry).
        let root_id = {
            let tree = sys.inner.tree.lock().unwrap();
            tree.iter()
                .find(|(_, e)| e.parent.is_none())
                .map(|(id, _)| *id)
                .expect("root registered")
        };
        *sys.inner.root.lock().unwrap() = Some(root_id);

        // Keep the root handle alive so the root never exits on its own.
        *sys.inner.root_ref.lock().unwrap() = Some(root_ref);

        let status = crate::status::spawn_status_actor(&sys, root_id);
        *sys.inner.status.lock().unwrap() = Some(status);

        sys
    }

    /// Handle to the status actor (panics if called on a `bare()` system).
    pub fn status(&self) -> crate::status::StatusRef {
        self.inner
            .status
            .lock()
            .unwrap()
            .clone()
            .expect("status actor not started; use ActorSystem::start()")
    }

    /// Shut the whole system down by cancelling the root token (cascades).
    pub fn shutdown(&self) {
        if let Some(root) = *self.inner.root.lock().unwrap() {
            if let Some(e) = self.inner.tree.lock().unwrap().get(&root) {
                e.token.cancel();
            }
        }
    }

    // ── Snapshot readers ────────────────────────────────────────────────────

    pub(crate) fn snapshot_actor(&self, id: ActorId) -> Option<crate::status::ActorStatus> {
        let tree = self.inner.tree.lock().unwrap();
        tree.get(&id).map(|e| status_of(id, e))
    }

    pub(crate) fn snapshot_forest(&self) -> Vec<crate::status::ActorNode> {
        let tree = self.inner.tree.lock().unwrap();
        let roots: Vec<ActorId> = tree
            .iter()
            .filter(|(_, e)| e.parent.is_none())
            .map(|(id, _)| *id)
            .collect();
        roots.into_iter().map(|id| build_node(&tree, id)).collect()
    }
}

fn status_of(id: ActorId, e: &SupervisedEntry) -> crate::status::ActorStatus {
    let snap = e.stats.snapshot((e.depth_probe)());
    crate::status::ActorStatus {
        id,
        name: e.name.to_string(),
        parent: e.parent,
        alive: !e.token.is_cancelled(),
        busy: snap.is_busy,
        mailbox_depth: snap.mailbox_depth,
        message_count: snap.message_count,
        restarts: e.restarts.load(Ordering::Relaxed),
        idle_ms: snap.idle_for().map(|d| d.as_millis() as u64),
    }
}

fn build_node(
    tree: &HashMap<ActorId, SupervisedEntry>,
    id: ActorId,
) -> crate::status::ActorNode {
    let e = &tree[&id];
    let children = e
        .children
        .iter()
        .filter(|c| tree.contains_key(c))
        .map(|c| build_node(tree, *c))
        .collect();
    crate::status::ActorNode { status: status_of(id, e), children }
}

#[cfg(feature = "system")]
/// One actor incarnation: fold messages into state until a clean exit.
async fn run_incarnation<M, S, I, U, Fut>(
    rx: &mut mpsc::Receiver<M>,
    token: &CancellationToken,
    poison: &CancellationToken,
    init: &I,
    update: &U,
    ctx: &crate::context::Context<M>,
    stats: &Arc<ActorStats>,
) where
    M: Send + 'static,
    S: Send + 'static,
    I: Fn(crate::context::Context<M>) -> S,
    U: Fn(S, M, crate::context::Context<M>) -> Fut,
    Fut: Future<Output = S>,
{
    let mut state = init(ctx.clone());
    let mut draining = false;
    loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => break,
            _ = poison.cancelled(), if !draining => {
                rx.close();
                draining = true;
            }
            msg = rx.recv() => match msg {
                Some(m) => {
                    stats.record_start();
                    state = update(state, m, ctx.clone()).await;
                    stats.record_finish();
                }
                None => break,
            }
        }
    }
}

fn panic_message(panic: Box<dyn std::any::Any + Send>) -> Arc<str> {
    if let Some(s) = panic.downcast_ref::<&str>() {
        Arc::from(*s)
    } else if let Some(s) = panic.downcast_ref::<String>() {
        Arc::from(s.as_str())
    } else {
        Arc::from("panic")
    }
}

#[cfg(test)]
pub(crate) struct TreeView {
    pub(crate) children: Vec<ActorId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    fn entry(name: &str, parent: Option<ActorId>) -> SupervisedEntry {
        SupervisedEntry {
            name: Arc::from(name),
            parent,
            children: Vec::new(),
            token: CancellationToken::new(),
            stats: Arc::new(crate::stats::ActorStats::new()),
            restarts: Arc::new(AtomicU32::new(0)),
            depth_probe: Arc::new(|| 0),
        }
    }

    #[tokio::test]
    async fn register_links_parent_and_child() {
        let sys = ActorSystem::bare();
        let p = ActorId::next();
        let c = ActorId::next();
        sys.register(p, entry("parent", None));
        sys.register(c, entry("child", Some(p)));

        let tree = sys.read_tree();
        assert!(tree[&p].children.contains(&c));
        assert_eq!(sys.ancestry(c), vec![p]);
    }

    #[tokio::test]
    async fn deregister_removes_entry_and_unlinks_parent() {
        let sys = ActorSystem::bare();
        let p = ActorId::next();
        let c = ActorId::next();
        sys.register(p, entry("parent", None));
        sys.register(c, entry("child", Some(p)));
        sys.deregister(c);

        let tree = sys.read_tree();
        assert!(!tree.contains_key(&c));
        assert!(!tree[&p].children.contains(&c));
    }

    #[tokio::test]
    async fn emit_is_observable_by_subscribers() {
        let sys = ActorSystem::bare();
        let mut rx = sys.events();
        let id = ActorId::next();
        sys.emit(SupervisionEvent::Stopped { id });
        let ev = rx.recv().await.unwrap();
        assert!(matches!(ev, SupervisionEvent::Stopped { id: got } if got == id));
    }

    #[test]
    fn panic_message_str_variant() {
        // &str panic (most common)
        let panic: Box<dyn std::any::Any + Send> = Box::new("oops");
        assert_eq!(panic_message(panic).as_ref(), "oops");
    }

    #[test]
    fn panic_message_string_variant() {
        // String panic (less common but must be handled)
        let panic: Box<dyn std::any::Any + Send> = Box::new(String::from("string panic"));
        assert_eq!(panic_message(panic).as_ref(), "string panic");
    }

    #[test]
    fn panic_message_unknown_variant() {
        // Non-string panic payload falls back to "panic"
        let panic: Box<dyn std::any::Any + Send> = Box::new(42u64);
        assert_eq!(panic_message(panic).as_ref(), "panic");
    }
}
