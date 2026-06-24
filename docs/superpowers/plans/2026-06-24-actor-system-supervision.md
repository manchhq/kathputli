# Actor System: Supervision, Lifecycle & Status — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in (feature-gated) supervising actor system to `kathputli`: an FP `init`+`update` actor model, bounded restart with escalation, a parent→child supervision tree with cascade shutdown, kamikaze one-shot actors, poison-pill drain, and a queryable status actor.

**Architecture:** Existing core primitives stay untouched except for additive lifecycle/observability hooks (busy flag, poison drain). A new `system` feature gates `ActorSystem`, `Context<M>`, the supervision tree, the restart loop (panic caught via `futures_util::FutureExt::catch_unwind` so the mailbox survives restarts), and a status actor that reads the tree + lock-free stats. Snapshot types get optional `serde` derives.

**Tech Stack:** Rust (edition 2024, rust-version 1.85), Tokio (`sync`, `rt`, `macros`), `tokio-util` (`CancellationToken`), `futures-util` (optional, `catch_unwind`), `serde` (optional), `proptest` (dev).

## Global Constraints

- `edition = "2024"`, `rust-version = "1.85"` — do not use features beyond this floor.
- Core (default-feature) build must gain **no new required dependencies**. `futures-util` and `serde` are **optional**, pulled only by their features.
- New feature names: `system` (default off), `serde` (default off). `futures-util` is an optional dep enabled by `system`.
- Existing public API is **additive-only**: `Actor`, `spawn`, `ActorRef`, `ActorHandle`, `ActorPool`, `ActorRegistry`, `Envelope`, `ActorStatsSnapshot` keep working unchanged. New public fields/methods are allowed.
- FP style: supervised actors are `init`/`update` closures — no new trait required of users.
- Default restart limit: `max_restarts = 3`. After exhaustion: escalate (emit event) + die. **Never** auto-restart past the limit.
- No zombies: every actor exit path removes its tree entry and cancels its token (cascading to children).
- License headers / copy: none required (crate has none today).
- Commit after every task. Conventional-commit messages (`feat:`, `test:`, `docs:`, `chore:`).
- Work happens on branch `feat/actor-system-supervision` (already created).
- Test commands: core `cargo test -p kathputli`; system `cargo test -p kathputli --features system`; serde `cargo test -p kathputli --features system,serde`. Lint gate: `cargo clippy -p kathputli --all-features -- -D warnings`.

---

## File Structure

**Modified (core, additive):**
- `kathputli/Cargo.toml` — add `system`/`serde` features, optional `futures-util`/`serde` deps.
- `kathputli/src/stats.rs` — add `busy: AtomicBool`, `record_finish`, `is_busy` snapshot field.
- `kathputli/src/handle.rs` — add poison token + drain branch to the core spawn loop.
- `kathputli/src/actor_ref.rs` — add poison token field + `poison()` method.
- `kathputli/src/lib.rs` — register new feature-gated modules + re-exports.

**Created (all gated behind `#[cfg(feature = "system")]`):**
- `kathputli/src/id.rs` — `ActorId` (process-unique numeric id).
- `kathputli/src/context.rs` — `Context<M>` (Hewitt axioms as values).
- `kathputli/src/supervisor.rs` — `ActorSystem`, `SpawnOptions`, supervision tree, restart loop, `SupervisionEvent`, `spawn`/`spawn_once`.
- `kathputli/src/status.rs` — status actor, `StatusMsg`, `StatusRef`, `ActorStatus`, `ActorNode`.
- `kathputli/tests/test_system.rs` — integration tests for the whole feature.

**Created (docs/quality):**
- updates to `kathputli/README.md` and crate-level docs.

---

## Task 1: Stats `busy` flag (core, additive)

**Files:**
- Modify: `kathputli/src/stats.rs`
- Test: `kathputli/tests/test_stats.rs` (append)

**Interfaces:**
- Consumes: existing `ActorStats`, `ActorStatsSnapshot`.
- Produces: `ActorStats::record_finish(&self)`; `ActorStatsSnapshot.is_busy: bool` (set by `snapshot`).

- [ ] **Step 1: Write the failing test** — append to `kathputli/tests/test_stats.rs`:

```rust
#[tokio::test]
async fn stats_busy_is_false_when_idle() {
    let actor = spawn(Worker, 16);
    // No message in flight → not busy.
    assert!(!actor.stats().is_busy);
}

#[tokio::test]
async fn stats_busy_true_while_handling() {
    let actor = spawn(Worker, 16);
    actor.tell(WorkerMsg::Slow).expect("enqueued"); // 50ms handler
    // Give the loop a moment to dequeue and start handling.
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert!(actor.stats().is_busy, "actor should be busy mid-handle");
    // After the slow handler finishes it goes idle again.
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    assert!(!actor.stats().is_busy, "actor should be idle after handling");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p kathputli --test test_stats stats_busy -- --nocapture`
Expected: FAIL — `no field `is_busy` on type `ActorStatsSnapshot``.

- [ ] **Step 3: Implement** in `kathputli/src/stats.rs`:

Add `use std::sync::atomic::AtomicBool;` to the imports. Then:

```rust
pub(crate) struct ActorStats {
    message_count: AtomicU64,
    last_activity_ms: AtomicU64,
    busy: AtomicBool,
    base: Instant,
}

impl ActorStats {
    pub(crate) fn new() -> Self {
        Self {
            message_count: AtomicU64::new(0),
            last_activity_ms: AtomicU64::new(0),
            busy: AtomicBool::new(false),
            base: Instant::now(),
        }
    }

    pub(crate) fn record_start(&self) {
        self.message_count.fetch_add(1, Ordering::Relaxed);
        let elapsed = self.base.elapsed().as_millis() as u64;
        self.last_activity_ms.store(elapsed.max(1), Ordering::Relaxed);
        self.busy.store(true, Ordering::Relaxed);
    }

    /// Record that the actor finished handling the current message.
    pub(crate) fn record_finish(&self) {
        self.busy.store(false, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self, mailbox_depth: usize) -> ActorStatsSnapshot {
        let ms = self.last_activity_ms.load(Ordering::Relaxed);
        let last_activity = (ms != 0).then(|| self.base + Duration::from_millis(ms));
        ActorStatsSnapshot {
            message_count: self.message_count.load(Ordering::Relaxed),
            mailbox_depth,
            last_activity,
            is_busy: self.busy.load(Ordering::Relaxed),
        }
    }
}
```

Add the field to the snapshot struct:

```rust
#[derive(Debug, Clone)]
pub struct ActorStatsSnapshot {
    pub message_count: u64,
    pub mailbox_depth: usize,
    pub last_activity: Option<Instant>,
    /// `true` if the actor is currently inside `handle()` / `update()`.
    pub is_busy: bool,
}
```

- [ ] **Step 4: Wire `record_finish` into the core loop** — in `kathputli/src/handle.rs`, in `spawn`'s loop, after `handle_one(...).await;` add `stats_loop.record_finish();`:

```rust
                msg = rx.recv() => match msg {
                    Some(m) => {
                        stats_loop.record_start();
                        handle_one(&mut actor, m).await;
                        stats_loop.record_finish();
                    }
                    None => break,
                },
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p kathputli`
Expected: PASS (all existing + the two new busy tests).

- [ ] **Step 6: Commit**

```bash
git add kathputli/src/stats.rs kathputli/src/handle.rs kathputli/tests/test_stats.rs
git commit -m "feat: track actor busy/idle state in stats"
```

---

## Task 2: Poison-pill drain (core, additive)

**Files:**
- Modify: `kathputli/src/handle.rs` (spawn loop), `kathputli/src/actor_ref.rs`
- Test: `kathputli/tests/test_handle.rs` (append)

**Interfaces:**
- Consumes: `ActorHandle`, `CancellationToken`.
- Produces: `ActorRef::poison(&self)` — closes the mailbox to new sends, drains buffered messages, then exits cleanly. `ActorRef::new` signature unchanged (poison token created internally).

- [ ] **Step 1: Write the failing test** — append to `kathputli/tests/test_handle.rs`:

```rust
#[tokio::test]
async fn poison_drains_then_stops() {
    let actor_ref = spawn_counter(0);
    // Queue several increments, then poison. All queued messages must be handled
    // before the actor exits.
    for _ in 0..5 {
        actor_ref.tell(CounterMsg::Inc).unwrap();
    }
    actor_ref.poison();

    // Give the drain a moment.
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    // The actor has exited; the channel is closed so further tells fail.
    assert!(actor_ref.tell(CounterMsg::Inc).is_err());
    // is_alive() reflects that the loop exited (token auto-cancelled on exit).
    assert!(!actor_ref.is_alive());
}

#[tokio::test]
async fn poison_rejects_new_sends_during_drain() {
    let actor_ref = spawn_counter(0);
    actor_ref.tell(CounterMsg::Slow_or_inc_placeholder()).ok(); // see note below
}
```

> NOTE: delete the second test stub above — it references a non-existent helper.
> Keep only `poison_drains_then_stops`. (Listed to flag that the simpler single
> test is sufficient; do not invent `Slow_or_inc_placeholder`.)

Final test block to add (use exactly this):

```rust
#[tokio::test]
async fn poison_drains_then_stops() {
    let actor_ref = spawn_counter(0);
    for _ in 0..5 {
        actor_ref.tell(CounterMsg::Inc).unwrap();
    }
    actor_ref.poison();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert!(actor_ref.tell(CounterMsg::Inc).is_err());
    assert!(!actor_ref.is_alive());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p kathputli --test test_handle poison_drains_then_stops`
Expected: FAIL — `no method named `poison` found for ... ActorRef`.

- [ ] **Step 3: Add the poison token to `ActorRef`** — in `kathputli/src/actor_ref.rs`:

```rust
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

    /// Graceful stop: stop accepting new messages, drain what is already
    /// queued, then exit. Contrast with [`shutdown`], which stops as soon as
    /// the in-flight message completes.
    pub fn poison(&self) {
        self.poison.cancel();
    }

    // ... existing tell/ask/shutdown/is_alive/stats/handle/child_token unchanged ...
}
```

- [ ] **Step 4: Thread the poison token into `spawn`** — in `kathputli/src/handle.rs`:

```rust
pub fn spawn<A: Actor>(mut actor: A, buffer: usize) -> ActorRef<A::Msg> {
    let (tx, mut rx) = mpsc::channel(buffer);
    let token = CancellationToken::new();
    let token_loop = token.clone();
    let poison = CancellationToken::new();
    let poison_loop = poison.clone();
    let stats = Arc::new(ActorStats::new());
    let stats_loop = stats.clone();
    tokio::spawn(async move {
        let mut draining = false;
        loop {
            tokio::select! {
                biased;
                _ = token_loop.cancelled() => break,
                _ = poison_loop.cancelled(), if !draining => {
                    rx.close();      // reject new sends; buffered messages still drain
                    draining = true;
                }
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
    ActorRef::new_with_poison(ActorHandle { sender: tx, stats }, token, poison)
}
```

> The `if !draining` guard keeps the poison branch from re-firing once it has
> closed the channel; after that, `rx.recv()` drains the buffer and returns
> `None`, ending the loop.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p kathputli`
Expected: PASS (existing tests + `poison_drains_then_stops`).

- [ ] **Step 6: Commit**

```bash
git add kathputli/src/actor_ref.rs kathputli/src/handle.rs kathputli/tests/test_handle.rs
git commit -m "feat: add poison-pill graceful drain to actors"
```

---

## Task 3: Cargo features, `ActorId`, and module scaffolding

**Files:**
- Modify: `kathputli/Cargo.toml`, `kathputli/src/lib.rs`
- Create: `kathputli/src/id.rs`
- Test: inline `#[cfg(test)]` in `id.rs`

**Interfaces:**
- Produces: `ActorId` (`Copy, Clone, Debug, PartialEq, Eq, Hash`, `serde::Serialize` under `serde`); `ActorId::next() -> ActorId` (pub(crate)); `ActorId::value(&self) -> u64`.
- Produces: cargo features `system`, `serde`; optional deps `futures-util`, `serde`.

- [ ] **Step 1: Add features and deps** — in `kathputli/Cargo.toml`:

```toml
[dependencies]
tokio = { workspace = true, features = ["sync", "rt", "macros"] }
tokio-util = { workspace = true }
anyhow = { workspace = true }
async-trait = { workspace = true }
tracing = { workspace = true, optional = true }
futures-util = { version = "0.3", optional = true, default-features = false, features = ["std"] }
serde = { version = "1", optional = true, features = ["derive"] }

[features]
tracing = ["dep:tracing"]
# Opt-in supervising actor system: ActorSystem, supervision tree, restart,
# kamikaze one-shot actors, status actor. Pulls futures-util for catch_unwind.
system = ["dep:futures-util"]
# Derive serde::Serialize on status snapshot types (ActorStatus, ActorNode).
serde = ["dep:serde"]
```

- [ ] **Step 2: Write the failing test** — create `kathputli/src/id.rs`:

```rust
//! Process-unique actor identifiers.

use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// A process-unique, monotonically increasing actor id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ActorId(u64);

impl ActorId {
    /// Allocate the next unique id.
    pub(crate) fn next() -> Self {
        ActorId(NEXT_ID.fetch_add(1, Ordering::Relaxed))
    }

    /// The underlying numeric value.
    pub fn value(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for ActorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_unique_and_increasing() {
        let a = ActorId::next();
        let b = ActorId::next();
        assert_ne!(a, b);
        assert!(b.value() > a.value());
    }
}
```

- [ ] **Step 3: Register the module** — in `kathputli/src/lib.rs`, add the feature-gated modules and re-exports:

```rust
pub mod actor;
pub mod actor_ref;
pub mod envelope;
pub mod handle;
pub mod pool;
pub mod registry;
pub mod stats;

#[cfg(feature = "system")]
pub mod id;
#[cfg(feature = "system")]
pub mod context;
#[cfg(feature = "system")]
pub mod supervisor;
#[cfg(feature = "system")]
pub mod status;

pub use actor::Actor;
pub use actor_ref::ActorRef;
pub use envelope::Envelope;
pub use handle::{ActorHandle, spawn};
pub use pool::ActorPool;
pub use registry::ActorRegistry;
pub use stats::ActorStatsSnapshot;

#[cfg(feature = "system")]
pub use id::ActorId;
#[cfg(feature = "system")]
pub use context::Context;
#[cfg(feature = "system")]
pub use supervisor::{ActorSystem, SpawnOptions, SupervisionEvent};
#[cfg(feature = "system")]
pub use status::{ActorNode, ActorStatus, StatusMsg, StatusRef};
```

> `context.rs`, `supervisor.rs`, and `status.rs` do not exist yet. To keep this
> task compiling on its own, create them as empty stubs now and fill them in
> later tasks:
>
> - `kathputli/src/context.rs`: `//! Actor context (placeholder).`
> - `kathputli/src/supervisor.rs`: `//! Supervisor (placeholder).`
> - `kathputli/src/status.rs`: `//! Status actor (placeholder).`
>
> The re-exports above reference types that don't exist yet, so for THIS task
> comment out the four `#[cfg(feature = "system")] pub use { context/supervisor/
> status }` lines (keep the `id` ones and the `pub mod` lines). Uncomment each
> re-export in the task that defines its types.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p kathputli --features system id::`
Expected: PASS (`ids_are_unique_and_increasing`).
Run: `cargo build -p kathputli` (default features)
Expected: builds with no new deps compiled.

- [ ] **Step 5: Commit**

```bash
git add kathputli/Cargo.toml kathputli/src/lib.rs kathputli/src/id.rs \
        kathputli/src/context.rs kathputli/src/supervisor.rs kathputli/src/status.rs
git commit -m "feat: add system/serde features, ActorId, module scaffolding"
```

---

## Task 4: Supervision tree + `ActorSystem` skeleton + events

**Files:**
- Modify: `kathputli/src/supervisor.rs`
- Test: inline `#[cfg(test)]` in `supervisor.rs`

**Interfaces:**
- Consumes: `ActorId`, `ActorStats` (via `Arc`), `CancellationToken`, `tokio::sync::broadcast`.
- Produces:
  - `pub struct ActorSystem { inner: Arc<Inner> }` (`Clone`).
  - `pub enum SupervisionEvent { Spawned{id,name,parent}, Restarted{id,restarts}, Failed{id,ancestry,error}, Stopped{id} }` (`Clone, Debug`).
  - `pub struct SpawnOptions { pub name: String, pub max_restarts: u32, pub buffer: usize }` + `Default` (`max_restarts=3`, `buffer=16`, `name=""`).
  - `ActorSystem::events(&self) -> broadcast::Receiver<SupervisionEvent>`.
  - pub(crate) tree ops: `register`, `deregister`, `ancestry`, `emit`, `read_tree` (used by later tasks + status).
  - pub(crate) `SupervisedEntry { name: Arc<str>, parent: Option<ActorId>, children: Vec<ActorId>, token: CancellationToken, stats: Arc<ActorStats>, restarts: Arc<AtomicU32>, depth_probe: Arc<dyn Fn() -> usize + Send + Sync> }`.

- [ ] **Step 1: Write the failing test** — in `kathputli/src/supervisor.rs`:

```rust
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
        matches!(ev, SupervisionEvent::Stopped { id: got } if got == id);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p kathputli --features system supervisor::`
Expected: FAIL — `ActorSystem`, `SupervisedEntry`, etc. not found.

- [ ] **Step 3: Implement** — replace `kathputli/src/supervisor.rs` placeholder with:

```rust
//! Supervising actor system: lifecycle tree, restart, escalation.

use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::id::ActorId;
use crate::stats::ActorStats;

/// Lifecycle events broadcast by the system. Subscribe via [`ActorSystem::events`].
#[derive(Clone, Debug)]
pub enum SupervisionEvent {
    Spawned { id: ActorId, name: String, parent: Option<ActorId> },
    Restarted { id: ActorId, restarts: u32 },
    Failed { id: ActorId, ancestry: Vec<ActorId>, error: String },
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
    /// Set once the status actor is up (see Task 7). `None` in `bare()`.
    pub(crate) status: Mutex<Option<crate::status::StatusRef>>,
    /// Set once the root actor is up (see Task 7).
    pub(crate) root: Mutex<Option<ActorId>>,
}

/// The supervising actor system. Cheap to clone (`Arc` inside).
#[derive(Clone)]
pub struct ActorSystem {
    pub(crate) inner: Arc<Inner>,
}

impl ActorSystem {
    /// A system with no root/status actors — used by tree unit tests and as the
    /// base for [`start`](ActorSystem::start) (see Task 7).
    pub(crate) fn bare() -> Self {
        let (events, _) = broadcast::channel(256);
        ActorSystem {
            inner: Arc::new(Inner {
                tree: Mutex::new(HashMap::new()),
                events,
                status: Mutex::new(None),
                root: Mutex::new(None),
            }),
        }
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
}

#[cfg(test)]
pub(crate) struct TreeView {
    pub(crate) children: Vec<ActorId>,
}
```

> The test's `tree[&p].children` indexes a `HashMap<ActorId, TreeView>`; that is
> why `read_tree` returns `TreeView`. Adjust the test imports if needed.

- [ ] **Step 4: Uncomment the supervisor re-export** in `kathputli/src/lib.rs`:

```rust
#[cfg(feature = "system")]
pub use supervisor::{ActorSystem, SpawnOptions, SupervisionEvent};
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p kathputli --features system supervisor::`
Expected: PASS (three tree/event tests).

- [ ] **Step 6: Commit**

```bash
git add kathputli/src/supervisor.rs kathputli/src/lib.rs
git commit -m "feat: supervision tree, SupervisionEvent, ActorSystem skeleton"
```

---

## Task 5: `Context<M>` + `init`/`update` spawn with restart loop

**Files:**
- Modify: `kathputli/src/context.rs`, `kathputli/src/supervisor.rs`
- Test: `kathputli/tests/test_system.rs` (create)

**Interfaces:**
- Consumes: `ActorSystem`, `SupervisedEntry`, `ActorHandle`, `ActorRef`, `ActorStats`, `SpawnOptions`, `SupervisionEvent`, `futures_util::FutureExt`.
- Produces:
  - `pub struct Context<M: Send + 'static>` (`Clone`) with: `id() -> ActorId`, `name() -> &str`, `parent_id() -> Option<ActorId>`, `myself() -> ActorHandle<M>`, `system() -> &ActorSystem`. (`spawn`/`spawn_once`/`status` added in Task 6/7.)
  - `ActorSystem::spawn_supervised<M,S,I,U,Fut>(name, parent, parent_token, opts, init, update) -> ActorRef<M>` (pub(crate) core engine).
  - `ActorSystem::spawn<M,S,I,U,Fut>(&self, name: impl Into<String>, init, update) -> ActorRef<M>` (public; child of root — wired fully in Task 7; for now spawns a rootless top-level actor).

- [ ] **Step 1: Implement `Context<M>`** — replace `kathputli/src/context.rs`:

```rust
//! Per-actor context: Hewitt's axioms exposed as plain values.

use std::sync::Arc;

use crate::handle::ActorHandle;
use crate::id::ActorId;
use crate::supervisor::ActorSystem;

/// Handed to every `update` / `spawn_once` body. Lets an actor create children,
/// reach itself, and learn its parent — without any base class.
pub struct Context<M: Send + 'static> {
    pub(crate) id: ActorId,
    pub(crate) name: Arc<str>,
    pub(crate) parent: Option<ActorId>,
    pub(crate) token: tokio_util::sync::CancellationToken,
    pub(crate) myself: ActorHandle<M>,
    pub(crate) system: ActorSystem,
}

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

impl<M: Send + 'static> Context<M> {
    pub fn id(&self) -> ActorId {
        self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn parent_id(&self) -> Option<ActorId> {
        self.parent
    }
    /// A handle to this actor's own mailbox (self-send / scheduling).
    pub fn myself(&self) -> ActorHandle<M> {
        self.myself.clone()
    }
    pub fn system(&self) -> &ActorSystem {
        &self.system
    }
}
```

- [ ] **Step 2: Uncomment the context re-export** in `kathputli/src/lib.rs`:

```rust
#[cfg(feature = "system")]
pub use context::Context;
```

- [ ] **Step 3: Make `ActorHandle` constructible by the supervisor** — `ActorHandle`'s fields are `pub(crate)`, so `supervisor.rs` can build `ActorHandle { sender, stats }` directly. Confirm by reading `kathputli/src/handle.rs:18-21`. No change needed.

- [ ] **Step 4: Write the failing test** — create `kathputli/tests/test_system.rs`:

```rust
#![cfg(feature = "system")]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use kathputli::supervisor::ActorSystem;
use tokio::sync::oneshot;

enum CounterMsg {
    Inc,
    Get(oneshot::Sender<u64>),
    Boom, // triggers a panic to exercise restart
}

#[tokio::test]
async fn update_folds_state() {
    let sys = ActorSystem::bare();
    let actor = sys.spawn(
        "counter",
        |_ctx| 0u64,
        |count, msg, _ctx| async move {
            match msg {
                CounterMsg::Inc => count + 1,
                CounterMsg::Get(reply) => {
                    let _ = reply.send(count);
                    count
                }
                CounterMsg::Boom => panic!("boom"),
            }
        },
    );
    for _ in 0..3 {
        actor.tell(CounterMsg::Inc).unwrap();
    }
    let got = actor.ask(CounterMsg::Get).await.unwrap();
    assert_eq!(got, 3);
}

#[tokio::test]
async fn restarts_on_panic_then_keeps_serving() {
    let sys = ActorSystem::bare();
    let inits = Arc::new(AtomicU32::new(0));
    let inits2 = inits.clone();
    let actor = sys.spawn(
        "resilient",
        move |_ctx| {
            inits2.fetch_add(1, Ordering::SeqCst);
            0u64
        },
        |count, msg, _ctx| async move {
            match msg {
                CounterMsg::Inc => count + 1,
                CounterMsg::Get(reply) => {
                    let _ = reply.send(count);
                    count
                }
                CounterMsg::Boom => panic!("boom"),
            }
        },
    );
    actor.tell(CounterMsg::Boom).unwrap(); // panic → restart, fresh state
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    actor.tell(CounterMsg::Inc).unwrap();
    let got = actor.ask(CounterMsg::Get).await.unwrap();
    assert_eq!(got, 1, "state reset after restart, then one Inc");
    assert!(inits.load(Ordering::SeqCst) >= 2, "init ran again on restart");
}

#[tokio::test]
async fn stops_after_max_restarts_and_escalates() {
    let sys = ActorSystem::bare();
    let mut events = sys.events();
    let actor = sys.spawn(
        "doomed",
        |_ctx| (),
        |_s, _msg: CounterMsg, _ctx| async move { panic!("always") },
    );
    // 4 booms: restarts 1,2,3 then exhaustion → Failed + Stopped.
    for _ in 0..4 {
        let _ = actor.tell(CounterMsg::Boom);
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    }
    let mut saw_failed = false;
    while let Ok(ev) = events.try_recv() {
        if matches!(ev, kathputli::supervisor::SupervisionEvent::Failed { .. }) {
            saw_failed = true;
        }
    }
    assert!(saw_failed, "expected a Failed escalation event");
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!actor.is_alive(), "actor must be dead after exhausting restarts");
}
```

- [ ] **Step 5: Run test to verify it fails**

Run: `cargo test -p kathputli --features system --test test_system`
Expected: FAIL — `no method named `spawn` found for ... ActorSystem`.

- [ ] **Step 6: Implement the restart engine** — append to `kathputli/src/supervisor.rs`:

```rust
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::Ordering;

use futures_util::FutureExt;
use tokio::sync::mpsc;

use crate::actor_ref::ActorRef;
use crate::context::Context;
use crate::handle::ActorHandle;

impl ActorSystem {
    /// Spawn a top-level supervised actor (child of root once Task 7 wires the
    /// root; until then `parent = None`).
    pub fn spawn<M, S, I, U, Fut>(
        &self,
        name: impl Into<String>,
        init: I,
        update: U,
    ) -> ActorRef<M>
    where
        M: Send + 'static,
        S: Send + 'static,
        I: Fn() -> S + Send + 'static,
        U: Fn(S, M, Context<M>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = S> + Send,
    {
        let parent = *self.inner.root.lock().unwrap();
        let parent_token = self.root_token();
        let opts = SpawnOptions { name: name.into(), ..Default::default() };
        self.spawn_supervised(parent, parent_token, opts, init, update)
    }

    /// The token new top-level actors descend from (root token if present, else
    /// a detached token).
    pub(crate) fn root_token(&self) -> CancellationToken {
        let root = *self.inner.root.lock().unwrap();
        match root {
            Some(id) => self
                .inner
                .tree
                .lock()
                .unwrap()
                .get(&id)
                .map(|e| e.token.clone())
                .unwrap_or_default(),
            None => CancellationToken::new(),
        }
    }

    /// Core engine: spawn a supervised `init`/`update` actor under `parent`.
    pub(crate) fn spawn_supervised<M, S, I, U, Fut>(
        &self,
        parent: Option<ActorId>,
        parent_token: CancellationToken,
        opts: SpawnOptions,
        init: I,
        update: U,
    ) -> ActorRef<M>
    where
        M: Send + 'static,
        S: Send + 'static,
        I: Fn() -> S + Send + 'static,
        U: Fn(S, M, Context<M>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = S> + Send,
    {
        let (tx, mut rx) = mpsc::channel::<M>(opts.buffer);
        let token = parent_token.child_token();
        let poison = CancellationToken::new();
        let stats = Arc::new(ActorStats::new());
        let restarts = Arc::new(AtomicU32::new(0));
        let id = ActorId::next();
        let name: Arc<str> = Arc::from(opts.name.as_str());

        let handle = ActorHandle { sender: tx.clone(), stats: stats.clone() };

        // Type-erased mailbox-depth probe for the status tree.
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
        self.emit(SupervisionEvent::Spawned {
            id,
            name: opts.name.clone(),
            parent,
        });

        let ctx = Context {
            id,
            name: name.clone(),
            parent,
            token: token.clone(),
            myself: handle.clone(),
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
                        let n = restarts.fetch_add(1, Ordering::Relaxed) + 1;
                        if n > max {
                            let ancestry = system.ancestry(id);
                            system.emit(SupervisionEvent::Failed {
                                id,
                                ancestry,
                                error: panic_message(&panic),
                            });
                            break; // exhausted → die, no auto-restart
                        }
                        system.emit(SupervisionEvent::Restarted { id, restarts: n });
                        // loop: re-run incarnation, SAME rx (mailbox preserved).
                    }
                }
            }
            // Cleanup: cancel (cascade children), deregister (no zombie), notify.
            loop_token.cancel();
            system.deregister(id);
            system.emit(SupervisionEvent::Stopped { id });
        });

        ActorRef::new_with_poison(handle, token, poison)
    }
}

/// One actor incarnation: fold messages into state until a clean exit.
async fn run_incarnation<M, S, I, U, Fut>(
    rx: &mut mpsc::Receiver<M>,
    token: &CancellationToken,
    poison: &CancellationToken,
    init: &I,
    update: &U,
    ctx: &Context<M>,
    stats: &Arc<ActorStats>,
) where
    M: Send + 'static,
    S: Send + 'static,
    I: Fn() -> S,
    U: Fn(S, M, Context<M>) -> Fut,
    Fut: Future<Output = S>,
{
    let mut state = init();
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

fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "panic".to_string()
    }
}
```

> Why `catch_unwind` instead of a child task + `JoinHandle`: `rx` stays in the
> supervising task's frame, so buffered messages and all sender handles survive
> a restart. Only the message being handled at panic time is lost.

- [ ] **Step 7: Run tests to verify they pass**

Run: `cargo test -p kathputli --features system --test test_system`
Expected: PASS (`update_folds_state`, `restarts_on_panic_then_keeps_serving`, `stops_after_max_restarts_and_escalates`).

> Panicking tasks print a backtrace to stderr — that is expected; the tests
> still pass. Run with `--features system` only.

- [ ] **Step 8: Commit**

```bash
git add kathputli/src/context.rs kathputli/src/supervisor.rs kathputli/src/lib.rs kathputli/tests/test_system.rs
git commit -m "feat: init/update supervised actors with restart loop"
```

---

## Task 6: Kamikaze `spawn_once` + `Context` child spawning

**Files:**
- Modify: `kathputli/src/supervisor.rs`, `kathputli/src/context.rs`
- Test: `kathputli/tests/test_system.rs` (append)

**Interfaces:**
- Produces: `ActorSystem::spawn_once<F,Fut>(&self, name, task) -> ActorRef<()>` where `F: Fn(Context<()>) -> Fut + Send + Sync + 'static`, `Fut: Future<Output=()> + Send`.
- Produces: `ActorSystem::spawn_once_supervised(parent, parent_token, opts, task) -> ActorRef<()>` (engine).
- Produces: `Context<M>::spawn<M2,...>(...) -> ActorRef<M2>` and `Context<M>::spawn_once<F,Fut>(...) -> ActorRef<()>` (children of the current actor; cascade via `self.token.child_token()`).

- [ ] **Step 1: Write the failing test** — append to `kathputli/tests/test_system.rs`:

```rust
#[tokio::test]
async fn spawn_once_runs_then_dies() {
    let sys = ActorSystem::bare();
    let (tx, rx) = oneshot::channel();
    let actor = sys.spawn_once("job", move |_ctx| {
        let tx = tx;
        async move {
            let _ = tx.send(99u32);
        }
    });
    let got = rx.await.unwrap();
    assert_eq!(got, 99);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!actor.is_alive(), "kamikaze actor reaped after completion");
}

#[tokio::test]
async fn child_cascades_on_parent_shutdown() {
    let sys = ActorSystem::bare();
    let (alive_tx, alive_rx) = oneshot::channel::<kathputli::ActorRef<()>>();
    let mut alive_tx = Some(alive_tx);
    let parent = sys.spawn(
        "parent",
        move |ctx| {
            // Spawn a long-lived child the first time init runs.
            let child = ctx.spawn_once("child", |cctx| async move {
                cctx.token_wait().await; // lives until cancelled (helper below)
            });
            if let Some(tx) = alive_tx.take() {
                let _ = tx.send(child);
            }
            ()
        },
        |s, _m: (), _ctx| async move { s },
    );
    let child = alive_rx.await.unwrap();
    assert!(child.is_alive());
    parent.shutdown(); // cancels parent token → child token (descendant) cancels
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!child.is_alive(), "child must cascade-stop with parent");
}
```

> The test uses `cctx.token_wait()` — add this small helper to `Context` in Step 3
> (an actor that parks until its token is cancelled). If you prefer not to add a
> public helper, replace the child body with
> `loop { tokio::time::sleep(std::time::Duration::from_millis(5)).await; }` and
> drop the `token_wait` step. The cascade assertion is the point.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p kathputli --features system --test test_system spawn_once`
Expected: FAIL — `no method named `spawn_once``.

- [ ] **Step 3: Implement `spawn_once`** — append to `kathputli/src/supervisor.rs`:

```rust
impl ActorSystem {
    /// Spawn a kamikaze (one-shot) actor: runs `task` to completion, then dies.
    /// Restart-on-panic still applies (up to `max_restarts`), then escalate+die.
    pub fn spawn_once<F, Fut>(&self, name: impl Into<String>, task: F) -> ActorRef<()>
    where
        F: Fn(Context<()>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send,
    {
        let parent = *self.inner.root.lock().unwrap();
        let parent_token = self.root_token();
        let opts = SpawnOptions { name: name.into(), ..Default::default() };
        self.spawn_once_supervised(parent, parent_token, opts, task)
    }

    pub(crate) fn spawn_once_supervised<F, Fut>(
        &self,
        parent: Option<ActorId>,
        parent_token: CancellationToken,
        opts: SpawnOptions,
        task: F,
    ) -> ActorRef<()>
    where
        F: Fn(Context<()>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send,
    {
        // A one-shot is an init/update actor whose body ignores messages and
        // returns after `task` completes. We model it directly for clarity.
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
        self.emit(SupervisionEvent::Spawned { id, name: opts.name.clone(), parent });

        let ctx = Context {
            id,
            name: name.clone(),
            parent,
            token: token.clone(),
            myself: handle.clone(),
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
                        let n = restarts.fetch_add(1, Ordering::Relaxed) + 1;
                        if n > max {
                            let ancestry = system.ancestry(id);
                            system.emit(SupervisionEvent::Failed {
                                id,
                                ancestry,
                                error: panic_message(&panic),
                            });
                            break;
                        }
                        system.emit(SupervisionEvent::Restarted { id, restarts: n });
                    }
                }
            }
            loop_token.cancel();
            system.deregister(id);
            system.emit(SupervisionEvent::Stopped { id });
        });

        ActorRef::new_with_poison(handle, token, poison)
    }
}
```

- [ ] **Step 4: Implement `Context` child spawning** — append to `impl<M> Context<M>` in `kathputli/src/context.rs`:

```rust
use std::future::Future;

use crate::actor_ref::ActorRef;
use crate::supervisor::SpawnOptions;

impl<M: Send + 'static> Context<M> {
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
        I: Fn() -> S + Send + 'static,
        U: Fn(S, M2, Context<M2>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = S> + Send,
    {
        let opts = SpawnOptions { name: name.into(), ..Default::default() };
        self.system
            .spawn_supervised(Some(self.id), self.token.child_token(), opts, init, update)
    }

    /// Spawn a kamikaze child of this actor.
    pub fn spawn_once<F, Fut>(&self, name: impl Into<String>, task: F) -> ActorRef<()>
    where
        F: Fn(Context<()>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send,
    {
        let opts = SpawnOptions { name: name.into(), ..Default::default() };
        self.system
            .spawn_once_supervised(Some(self.id), self.token.child_token(), opts, task)
    }

    /// Park until this actor's token is cancelled (handy for long-lived children
    /// in `spawn_once`).
    pub async fn token_wait(&self) {
        self.token.cancelled().await;
    }
}
```

> `spawn_supervised`/`spawn_once_supervised` are `pub(crate)`, so `Context` (same
> crate) can call them. The `spawn_supervised` engine passes `parent_token` and
> derives the child token internally; here we pass `self.token.child_token()` so
> the grandchild chain stays correct (a child's child descends from the child).

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p kathputli --features system --test test_system`
Expected: PASS (all prior + `spawn_once_runs_then_dies`, `child_cascades_on_parent_shutdown`).

- [ ] **Step 6: Commit**

```bash
git add kathputli/src/supervisor.rs kathputli/src/context.rs kathputli/tests/test_system.rs
git commit -m "feat: kamikaze spawn_once and context child spawning with cascade"
```

---

## Task 7: `ActorSystem::start` — root + status bootstrap

**Files:**
- Modify: `kathputli/src/supervisor.rs`, `kathputli/src/status.rs`, `kathputli/src/context.rs`
- Test: `kathputli/tests/test_system.rs` (append)

**Interfaces:**
- Produces: `ActorSystem::start() -> ActorSystem` — spawns a root actor and a status actor (child of root); `shutdown(&self)` cancels root (cascades all); `status(&self) -> StatusRef`.
- Produces: `Context<M>::status(&self) -> StatusRef`.
- Consumes: `StatusRef`, `status::spawn_status_actor` (Task 8 fills behavior; Task 7 may use a minimal status actor stub, then Task 8 enriches).

> Ordering note: Task 8 builds the status actor's query behavior. To keep Task 7
> independently testable, implement a **minimal** status actor here (an actor
> that accepts `StatusMsg` and answers `GetTree`/`GetActor` by reading the
> system tree), and let Task 8 add the event log + serde + richer fields. If you
> are executing strictly in order, you may instead merge Tasks 7 and 8 — they
> share the status module. The plan keeps them separate so the root/status
> bootstrap and the query surface get independent review gates.

- [ ] **Step 1: Add a `StatusRef` newtype + minimal status actor** — replace `kathputli/src/status.rs` placeholder:

```rust
//! Status actor: query the live actor tree.

use tokio::sync::oneshot;

use crate::actor_ref::ActorRef;
use crate::id::ActorId;
use crate::supervisor::ActorSystem;

/// Messages the status actor answers.
pub enum StatusMsg {
    GetTree(oneshot::Sender<Vec<ActorNode>>),
    GetActor(ActorId, oneshot::Sender<Option<ActorStatus>>),
}

/// Point-in-time status of one actor.
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
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ActorNode {
    pub status: ActorStatus,
    pub children: Vec<ActorNode>,
}

/// Cheap-clone handle to the status actor.
#[derive(Clone)]
pub struct StatusRef {
    inner: ActorRef<StatusMsg>,
}

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
        move || sys_for_init.clone(), // state = a system handle for reads
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
```

- [ ] **Step 2: Add snapshot readers to `ActorSystem`** — append to `kathputli/src/supervisor.rs`:

```rust
use crate::status::{ActorNode, ActorStatus};

impl ActorSystem {
    pub(crate) fn snapshot_actor(&self, id: ActorId) -> Option<ActorStatus> {
        let tree = self.inner.tree.lock().unwrap();
        tree.get(&id).map(|e| status_of(id, e))
    }

    pub(crate) fn snapshot_forest(&self) -> Vec<ActorNode> {
        let tree = self.inner.tree.lock().unwrap();
        let roots: Vec<ActorId> = tree
            .iter()
            .filter(|(_, e)| e.parent.is_none())
            .map(|(id, _)| *id)
            .collect();
        roots.into_iter().map(|id| build_node(&tree, id)).collect()
    }
}

fn status_of(id: ActorId, e: &SupervisedEntry) -> ActorStatus {
    let snap = e.stats.snapshot((e.depth_probe)());
    ActorStatus {
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
    tree: &std::collections::HashMap<ActorId, SupervisedEntry>,
    id: ActorId,
) -> ActorNode {
    let e = &tree[&id];
    let children = e
        .children
        .iter()
        .filter(|c| tree.contains_key(c))
        .map(|c| build_node(tree, *c))
        .collect();
    ActorNode { status: status_of(id, e), children }
}
```

- [ ] **Step 3: Implement `start` / `shutdown` / `status`** — append to `kathputli/src/supervisor.rs`:

```rust
use crate::status::{spawn_status_actor, StatusRef};

impl ActorSystem {
    /// Start a system: brings up a root actor and a status actor (child of root).
    pub fn start() -> Self {
        let sys = ActorSystem::bare();

        // Root actor: a parked actor that anchors the tree and shutdown.
        let root_ref = sys.spawn_supervised(
            None,
            CancellationToken::new(),
            SpawnOptions { name: "root".to_string(), ..Default::default() },
            || (),
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
        std::mem::forget(root_ref); // intentional: root lives for the system's life

        let status = spawn_status_actor(&sys, root_id);
        *sys.inner.status.lock().unwrap() = Some(status);

        sys
    }

    /// Handle to the status actor (panics if called on a `bare()` system).
    pub fn status(&self) -> StatusRef {
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
}
```

> `std::mem::forget(root_ref)` is deliberate: dropping the only `ActorRef` would
> close the root mailbox and the root would exit. An alternative that avoids
> `forget` (cleaner for clippy) is to store the root `ActorRef` in `Inner`
> behind `Mutex<Option<...>>`. Prefer storing it. If you store it, add a field
> `root_ref: Mutex<Option<ActorRef<()>>>` to `Inner`, set it in `start`, and
> drop the `forget`. **Do this** — `forget` will trip `clippy::mem_forget`.

- [ ] **Step 4: Add `Context::status`** — append to `impl<M> Context<M>` in `kathputli/src/context.rs`:

```rust
    /// Handle to the system's status actor.
    pub fn status(&self) -> crate::status::StatusRef {
        self.system.status()
    }
```

- [ ] **Step 5: Uncomment the status re-export** in `kathputli/src/lib.rs`:

```rust
#[cfg(feature = "system")]
pub use status::{ActorNode, ActorStatus, StatusMsg, StatusRef};
```

- [ ] **Step 6: Write/replace the test** — append to `kathputli/tests/test_system.rs`:

```rust
#[tokio::test]
async fn start_brings_up_root_and_status() {
    let sys = ActorSystem::start();
    let tree = sys.status().tree().await;
    // Forest has a single root; status actor is its child.
    assert_eq!(tree.len(), 1, "exactly one root");
    let root = &tree[0];
    assert_eq!(root.status.name, "root");
    assert!(root.status.children.iter().any(|c| c.status.name == "status"));
}

#[tokio::test]
async fn spawned_actor_appears_under_root() {
    let sys = ActorSystem::start();
    let _a = sys.spawn("worker", |_c| 0u8, |s, _m: (), _c| async move { s });
    let tree = sys.status().tree().await;
    let root = &tree[0];
    assert!(root.status.children.iter().any(|c| c.status.name == "worker"));
}

#[tokio::test]
async fn system_shutdown_stops_everything() {
    let sys = ActorSystem::start();
    let a = sys.spawn("worker", |_c| 0u8, |s, _m: (), _c| async move { s });
    assert!(a.is_alive());
    sys.shutdown();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert!(!a.is_alive(), "cascade from root stops workers");
}
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `cargo test -p kathputli --features system --test test_system`
Expected: PASS (all prior + the three bootstrap tests).

- [ ] **Step 8: Commit**

```bash
git add kathputli/src/supervisor.rs kathputli/src/status.rs kathputli/src/context.rs kathputli/src/lib.rs kathputli/tests/test_system.rs
git commit -m "feat: ActorSystem::start with root + status actor bootstrap"
```

---

## Task 8: Status event log (hybrid push) + serde verification

**Files:**
- Modify: `kathputli/src/status.rs`, `kathputli/src/supervisor.rs`
- Test: `kathputli/tests/test_system.rs` (append)

**Interfaces:**
- Produces: `StatusMsg::RecentEvents(oneshot::Sender<Vec<String>>)` — recent lifecycle events as formatted strings (the "live log"); `StatusRef::recent_events(&self) -> Vec<String>`.
- Produces: an internal forwarder task that subscribes to `ActorSystem::events()` and feeds each `SupervisionEvent` into the status actor.

- [ ] **Step 1: Write the failing test** — append to `kathputli/tests/test_system.rs`:

```rust
#[tokio::test]
async fn status_records_lifecycle_events() {
    let sys = ActorSystem::start();
    // Spawn then shut down a worker; both events should reach the log.
    let a = sys.spawn("ephemeral", |_c| 0u8, |s, _m: (), _c| async move { s });
    a.shutdown();
    tokio::time::sleep(std::time::Duration::from_millis(40)).await;
    let log = sys.status().recent_events().await;
    assert!(log.iter().any(|e| e.contains("Spawned") && e.contains("ephemeral")));
    assert!(log.iter().any(|e| e.contains("Stopped")));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p kathputli --features system --test test_system status_records`
Expected: FAIL — `no method named `recent_events``.

- [ ] **Step 3: Extend the status actor** — in `kathputli/src/status.rs`:

Add the new message variant and the state type:

```rust
pub enum StatusMsg {
    GetTree(oneshot::Sender<Vec<ActorNode>>),
    GetActor(ActorId, oneshot::Sender<Option<ActorStatus>>),
    RecentEvents(oneshot::Sender<Vec<String>>),
    /// Internal: fed by the event forwarder.
    Event(crate::supervisor::SupervisionEvent),
}
```

Add to `StatusRef`:

```rust
    pub async fn recent_events(&self) -> Vec<String> {
        self.inner
            .ask(StatusMsg::RecentEvents)
            .await
            .unwrap_or_default()
    }
```

Replace `spawn_status_actor` so its state carries both the system handle and a
bounded log, and so it starts an event-forwarder:

```rust
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
        move || StatusState { sys: sys_for_init.clone(), log: VecDeque::new() },
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
    let mut rx = system.events();
    let sink = status.raw().handle().clone();
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(ev) => {
                    if sink.tell(StatusMsg::Event(ev)).is_err() {
                        break; // status actor gone
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(_) => break,
            }
        }
    });

    status
}
```

> `SupervisionEvent` must be visible here; it already is via
> `crate::supervisor::SupervisionEvent`. Ensure `SupervisionEvent` derives
> `Debug` (it does — Task 4).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p kathputli --features system --test test_system`
Expected: PASS (all prior + `status_records_lifecycle_events`).

- [ ] **Step 5: Verify serde** — append to `kathputli/tests/test_system.rs`:

```rust
#[cfg(feature = "serde")]
#[tokio::test]
async fn status_snapshot_serializes() {
    let sys = ActorSystem::start();
    let tree = sys.status().tree().await;
    let json = serde_json::to_string(&tree).expect("serialize tree");
    assert!(json.contains("\"name\":\"root\""));
}
```

Add `serde_json = "1"` to `[dev-dependencies]` in `kathputli/Cargo.toml`.

- [ ] **Step 6: Run serde test**

Run: `cargo test -p kathputli --features system,serde --test test_system status_snapshot_serializes`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add kathputli/src/status.rs kathputli/src/supervisor.rs kathputli/Cargo.toml kathputli/tests/test_system.rs
git commit -m "feat: status actor event log + serde snapshot support"
```

---

## Task 9: Cross-cutting integration tests (no-zombie, one-for-one, mailbox-preserved)

**Files:**
- Test: `kathputli/tests/test_system.rs` (append)

**Interfaces:**
- Consumes: everything from Tasks 5–8. No new production code (if a test reveals a gap, fix it in the owning module and note it in the commit).

- [ ] **Step 1: Write the tests** — append to `kathputli/tests/test_system.rs`:

```rust
#[tokio::test]
async fn no_zombie_after_stop() {
    let sys = ActorSystem::start();
    let a = sys.spawn("temp", |_c| 0u8, |s, _m: (), _c| async move { s });
    let id = {
        // Find the worker id via the tree.
        let tree = sys.status().tree().await;
        fn find(nodes: &[kathputli::ActorNode], name: &str) -> Option<kathputli::ActorId> {
            for n in nodes {
                if n.status.name == name {
                    return Some(n.status.id);
                }
                if let Some(id) = find(&n.children, name) {
                    return Some(id);
                }
            }
            None
        }
        find(&tree, "temp").expect("worker present")
    };
    a.shutdown();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert!(sys.status().actor(id).await.is_none(), "entry must be reaped");
}

#[tokio::test]
async fn one_for_one_isolation() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    let sys = ActorSystem::start();
    let sib_inits = Arc::new(AtomicU32::new(0));

    // Sibling B: counts its init calls (should stay at 1 — never restarts).
    let b_inits = sib_inits.clone();
    let _b = sys.spawn(
        "sibling-b",
        move |_c| {
            b_inits.fetch_add(1, Ordering::SeqCst);
            0u64
        },
        |s, _m: (), _c| async move { s },
    );

    // Sibling A: panics on demand.
    enum AMsg { Boom }
    let a = sys.spawn(
        "sibling-a",
        |_c| 0u64,
        |s, m: AMsg, _c| async move {
            match m { AMsg::Boom => panic!("a down") }
            #[allow(unreachable_code)] s
        },
    );
    a.tell(AMsg::Boom).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    assert_eq!(sib_inits.load(Ordering::SeqCst), 1, "sibling B never restarted");
}

#[tokio::test]
async fn mailbox_preserved_across_restart() {
    let sys = ActorSystem::start();

    enum M { Boom, Inc, Get(tokio::sync::oneshot::Sender<u64>) }
    let a = sys.spawn(
        "preserve",
        |_c| 0u64,
        |count, m, _c| async move {
            match m {
                M::Boom => panic!("kaboom"),
                M::Inc => count + 1,
                M::Get(reply) => { let _ = reply.send(count); count }
            }
        },
    );
    // Queue Boom then several Incs back-to-back. Boom is handled (panic →
    // restart); the buffered Incs must survive and be handled by the new
    // incarnation.
    a.tell(M::Boom).unwrap();
    for _ in 0..4 {
        a.tell(M::Inc).unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_millis(40)).await;
    let got = a.ask(M::Get).await.unwrap();
    assert_eq!(got, 4, "buffered Incs survived the restart");
}
```

> If `mailbox_preserved_across_restart` fails (e.g. the Incs were lost), that
> indicates `rx` was not retained across `catch_unwind` — revisit Task 5's
> engine (the receiver must live in the supervising task frame, not be moved
> into the incarnation).

- [ ] **Step 2: Run tests**

Run: `cargo test -p kathputli --features system --test test_system`
Expected: PASS (all tests).

- [ ] **Step 3: Run the full matrix + clippy**

```bash
cargo test -p kathputli
cargo test -p kathputli --features system
cargo test -p kathputli --features system,serde
cargo clippy -p kathputli --all-features -- -D warnings
```
Expected: all PASS; clippy clean.

- [ ] **Step 4: Commit**

```bash
git add kathputli/tests/test_system.rs
git commit -m "test: cross-cutting supervision integration tests"
```

---

## Task 10: Docs + CRAP quality gate

**Files:**
- Modify: `kathputli/src/lib.rs` (crate docs), `kathputli/README.md`
- Create: `crap-baseline.json` (gitignored or committed per preference)

**Interfaces:** none (docs + tooling).

- [ ] **Step 1: Add crate-level docs** — at the top of `kathputli/src/lib.rs`, add a feature-gated doc section describing the actor system (example mirroring `update_folds_state`). Keep it short; show `ActorSystem::start`, `spawn`, `spawn_once`, `status().tree()`.

- [ ] **Step 2: Update README** — add an "Actor System (opt-in)" section to `kathputli/README.md` documenting the `system` and `serde` features, the `init`/`update` model, restart policy (default 3 → escalate → die), poison vs shutdown, and the status query. Include a runnable example.

- [ ] **Step 3: Build docs to verify**

Run: `cargo doc -p kathputli --all-features --no-deps`
Expected: builds with no warnings.

- [ ] **Step 4: Commit docs**

```bash
git add kathputli/src/lib.rs kathputli/README.md
git commit -m "docs: document the opt-in actor system"
```

- [ ] **Step 5: Install the CRAP tooling**

```bash
cargo binstall cargo-crap cargo-llvm-cov || cargo install cargo-crap cargo-llvm-cov
```
Expected: both tools available (`cargo crap --help`, `cargo llvm-cov --help`).

- [ ] **Step 6: Generate coverage with all features**

```bash
cargo llvm-cov --all-features --lcov --output-path lcov.info
```
Expected: `lcov.info` written.

- [ ] **Step 7: Run the CRAP analysis**

```bash
cargo crap --lcov lcov.info
```
Expected: a report ranking functions by CRAP score (complexity × under-coverage). Record the top offenders.

- [ ] **Step 8: Address high-CRAP functions**

For each flagged function (high complexity AND low coverage):
- If it is genuinely complex and central (e.g. `spawn_supervised`, `run_incarnation`), **add targeted tests** to bring coverage up rather than rewriting.
- If complexity is incidental (long match arms, nested conditionals that can be flattened), **simplify** — extract helpers, early-return, reduce branching.
- Do NOT chase the metric on trivial code. Note any deliberately-left items.

Re-run `cargo crap --lcov lcov.info` after changes to confirm scores dropped.

- [ ] **Step 9: Establish a baseline for the future**

```bash
cargo crap --lcov lcov.info --format json --output crap-baseline.json
```
Decide with the user whether to commit `crap-baseline.json` (for `--fail-regression` in CI) or gitignore `lcov.info`/coverage artifacts. Add `lcov.info` and coverage dirs to `.gitignore` regardless.

- [ ] **Step 10: Final verification + commit**

```bash
cargo test -p kathputli --all-features
cargo clippy -p kathputli --all-features -- -D warnings
git add kathputli/ .gitignore crap-baseline.json
git commit -m "chore: CRAP baseline and coverage-driven cleanup"
```

---

## Self-Review

**Spec coverage:**
- FP `init`/`update` model → Task 5. ✓
- `spawn_once` kamikaze → Task 6. ✓
- `Context<M>` (id/myself/parent/spawn/spawn_once/status/system) → Tasks 5–7. ✓
- Bounded restart (default 3) + escalate + die, no auto-restart past limit → Task 5 (+ test in Task 5). ✓
- Mailbox preserved across restart (catch_unwind, rx in supervisor frame) → Task 5 + verified Task 9. ✓
- No zombies (deregister + cancel on every exit) → Task 5/6 + verified Task 9. ✓
- One-for-one isolation → verified Task 9. ✓
- Cascade shutdown via child tokens → Task 6/7 + tests. ✓
- Poison pill (drain then die) → Task 2 (core) reused by system loop in Task 5. ✓
- `ActorSystem::start` root + default status child → Task 7. ✓
- Status query (`tree`/`actor`) + hybrid (pull structure/busy, push event log) → Tasks 7–8. ✓
- `busy`/idle stats → Task 1. ✓
- `serde` snapshot types → Tasks 7–8 (verified Task 8). ✓
- Feature gating (`system` pulls `futures-util`; `serde`) → Task 3. ✓
- Existing API unchanged/additive → Tasks 1–2 keep signatures; verified by running existing tests. ✓
- CRAP cleanup → Task 10. ✓

**Deviations from the spec (intentional, noted for the reviewer):**
- No separate `mailbox.rs` module: the receive loop is inlined in `supervisor.rs` (`run_incarnation`). The update model never hands a mailbox to user code, so a standalone `Mailbox` type would be dead code (YAGNI). Poison/drain still implemented.
- Status uses a single `SupervisionEvent` type for both escalation and the log (no separate `StatusEvent`). Structure/busy is pulled from the live tree; only the event log is pushed (via a forwarder). This matches the hybrid intent with less code.

**Placeholder scan:** The only placeholder-looking content is the explicit "delete this stub" note in Task 2 Step 1 (kept to warn against an invented helper) and the empty module stubs in Task 3 (filled in Tasks 5/7). Both are intentional and resolved within the plan.

**Type consistency:** `spawn_supervised`/`spawn_once_supervised` signatures, `Context<M>` fields, `SupervisedEntry` fields, `ActorStatus`/`ActorNode` fields, and `StatusMsg` variants are used identically across Tasks 4–9. `record_finish`/`is_busy` (Task 1) are consumed by Task 5's loop and Task 7's `status_of`. `new_with_poison` (Task 2) is used by Tasks 5–6.

**Known follow-ups (deferred, per spec Future Work):** per-parent failure handler callbacks, restart backoff/time-windows, optional `actor!` macro, trait→system adapter, state persistence.
