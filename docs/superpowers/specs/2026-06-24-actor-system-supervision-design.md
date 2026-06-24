# Actor System: Supervision, Lifecycle Registry & Status — Design

**Date:** 2026-06-24
**Status:** Approved (design phase)
**Crate:** `kathputli`

## Goal

Add an **opt-in** supervising actor system on top of the existing lean actor
primitives. It manages actor lifecycle, models a parent→child supervision tree,
restarts failed actors with a bounded policy, and exposes a queryable live status
of the whole tree for any UI to consume.

The whole feature lives behind a cargo feature (`system`, default off) so the
core stays minimal. Existing primitives (`Actor` trait, `spawn`, `ActorRef`,
`ActorHandle`, `ActorPool`, `ActorRegistry`) are **unchanged**.

## Design Philosophy

Two non-negotiables differentiate this from kameo / Akka:

1. **Simplicity** — functions, mailboxes, a supervision tree, a status actor.
   No typed-routing magic, no behavior DSL, no required macros.
2. **FP style** — actors are *functions*, not trait/interface implementations.
   The model is the Elm/F# `MailboxProcessor` fold-over-messages shape. Rust
   macros may later provide optional pseudo-functional sugar, but are not
   required by the core.

Hewitt's three actor axioms are exposed as plain values on a `Context`, never via
inheritance: an actor can **create** other actors, **send** to actors it knows,
and relate to its **parent** (which owns its lifetime and receives its failure
escalations).

## Non-Goals (explicitly out of scope)

- Networked / distributed actors.
- Actor state persistence (planned as a separate later feature).
- HTTP serving of status (status is a *query API* / actor; serving is the
  user's concern). `serde` derives are provided to make UI integration easy.
- Per-parent custom failure-handler callbacks (deferred; see Future Work).
- Restart time-windows / backoff (deferred; v1 uses a simple total count).
- Optional `actor!`/`behavior!` macro sugar and a trait→system adapter
  (deferred; see Future Work).

## Programming Model

### Normal actor: `init` + `update` (fold over messages)

The framework owns the receive loop. The user supplies:

- `init: Fn() -> State` — build initial state (also re-run on restart → fresh state).
- `update: Fn(State, Msg, Context<Msg>) -> impl Future<Output = State>` — fold one
  message into the next state. State is **moved in and out** (no `&mut self`, no
  trait, no borrow held across `.await`).

```rust
// Msg::Get carries a oneshot reply via the existing Envelope pattern.
system.spawn("counter", |_ctx| 0u64, |count, msg, _ctx| async move {
    match msg {
        Msg::Inc        => count + 1,
        Msg::Get(reply) => { let _ = reply.send(count); count }
    }
});
```

Rationale: moving `State` in/out (rather than `&mut State` or `&mut Mailbox`)
avoids Rust's hardest lifetime problem — handing a mutable borrow into a
*restartable* async closure — with no `for<'a>` HRTB and no boxing required in
the core. It is also the most FP shape.

### One-shot / kamikaze actor: `spawn_once`

A function that runs to completion and dies. Restart-on-panic still applies
(default 3); on exhaustion it escalates to the parent and dies.

```rust
system.spawn_once("import-job", |_ctx| async move { do_work().await });
```

This is the "fire-and-forget processing actor": does its work (with retry
chances), then is reaped. No special "kind" machinery — it is literally a
function that returns.

### Context<M>

Passed to `update` / `spawn_once` bodies. Typed to the actor's own message type.

```rust
impl<M: Send + 'static> Context<M> {
    fn id(&self) -> &ActorId;
    fn myself(&self) -> ActorHandle<M>;          // self-send / scheduling
    fn parent_id(&self) -> Option<&ActorId>;     // parent relationship
    fn spawn<M2>(&self, name, init, update) -> ActorRef<M2>;   // child of self
    fn spawn_once<F>(&self, name, task) -> ActorRef<()>;       // child of self
    fn system(&self) -> &ActorSystem;
    fn status(&self) -> &StatusRef;
}
```

`spawn` on a `Context` makes the new actor a **child** of the current actor
(token + tree link). To *send* to other actors you hold their `ActorRef` /
`ActorHandle` (captured in your closure or carried in messages) — there is no
magic global address book.

## Supervision & Restart

### Restart loop (internal, per supervised actor)

Each supervised actor runs in one task containing a restart loop:

```text
restarts = 0
loop:
    run ONE incarnation under catch_unwind:
        state = init()
        loop:
            select biased:
                token.cancelled()         -> clean break
                poison + drained          -> clean break
                msg = mailbox.recv():
                    Some(m) -> mark busy; state = update(state, m, ctx).await; mark idle
                    None    -> clean break        // all senders dropped
    on clean exit  -> break (reap)
    on panic:
        push StatusEvent::Failed; emit SupervisionEvent::Failed
        restarts += 1
        if restarts > max_restarts (default 3):
            escalate_to_parent(); push Stopped; break   // die, NO zombie
        else:
            push StatusEvent::Restarted(restarts)       // re-run incarnation
cleanup: remove tree entry; cancel token (cascade children); push Stopped
```

Key properties:

- **Mailbox survives restart.** The `mpsc::Receiver` lives in the supervisor
  frame (outside the incarnation passed to `catch_unwind`), so buffered messages
  and all `ActorHandle`/`ActorRef` senders remain valid across a restart. Only
  the single message being handled at panic time is lost (documented).
- **Fresh state on restart.** `init()` is re-run; no carried-over corrupt state.
- **No zombies.** Every exit path (clean or exhausted) removes the tree entry,
  cancels the token (cascading to children), and emits `Stopped`.
- **One-for-one.** A child's restart loop is independent; siblings are untouched.
- **`catch_unwind`** over the incarnation future uses `AssertUnwindSafe` (we
  accept that the dropped incarnation's actor/in-flight message are discarded).

### Failure escalation

- After exhausting restarts, the actor emits
  `SupervisionEvent::Failed { id, ancestry: Vec<ActorId>, error: String }` on a
  `tokio::sync::broadcast` channel exposed via `ActorSystem::events()`.
- Default subscriber logs at `tracing::error`. The actor then dies (reaped).
- **No automatic restart past the limit** — recovery requires a human code fix,
  by design.
- Parents relate to children via the tree (cascade shutdown + ancestry-tagged
  events). Per-parent programmatic failure handlers are deferred (Future Work).

### Spawn options

```rust
pub struct SpawnOptions {
    pub name: String,
    pub max_restarts: u32,   // default 3
    pub buffer: usize,       // mailbox capacity
}
```

`spawn` / `spawn_once` use defaults; `spawn_with(opts, ...)` for overrides.

## ActorSystem & Status Actor

### ActorSystem

```rust
let system = ActorSystem::start();        // spawns root actor + status child
system.spawn("worker", init, update);     // child of root
system.status();                          // StatusRef to the status actor
system.events();                          // broadcast::Receiver<SupervisionEvent>
system.shutdown();                        // cancel root -> cascade everything
```

`ActorSystem::start()` brings up a **root actor** and, as its **default child**,
the **status actor** — both alive the instant the system starts.

### Status actor (hybrid feed)

The status actor is an ordinary actor that folds `StatusEvent`s into a map and
answers queries.

- **Pushed (low-frequency lifecycle):** `Spawned { id, parent }`, `Restarted`,
  `Failed`, `Stopped`. This is the live log / history.
- **Pulled (high-frequency busy/idle):** on a query the status actor reads each
  registered actor's lock-free `ActorStats` atomics (held as `Arc<ActorStats>`,
  registered on the `Spawned` event) to compute current `busy` / `mailbox_depth`.
  Hot-path actors are never spammed with per-message status traffic.

### Query API

```rust
enum StatusMsg {
    GetTree(oneshot::Sender<Vec<ActorNode>>),
    GetActor(ActorId, oneshot::Sender<Option<ActorStatus>>),
}

// system.status().ask(StatusMsg::GetTree).await
```

Snapshot types (plain data; `serde` derives behind the `serde` feature):

```rust
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
    pub last_activity_ms: Option<u64>,
}

pub struct ActorNode {
    pub status: ActorStatus,
    pub children: Vec<ActorNode>,
}
```

## Poison Pill & Exit Semantics

- `poison()` (on `ActorRef` and `ActorSystem::poison(id)`): stop accepting **new**
  messages, **drain** what is buffered, then exit cleanly. Implemented via a
  dedicated poison `CancellationToken` watched in the select loop; when fired it
  closes the receiver (`rx.close()`) so buffered messages drain and `recv()`
  eventually returns `None` → clean exit.
- `shutdown()` (existing semantics): cancel ASAP between messages; the in-flight
  `update` completes first.
- Clean exits (reaped, **never** restarted): `shutdown`, poison-drain complete,
  all senders dropped (mailbox closed), or `spawn_once` body returns.

## Changes to Existing Code

`stats.rs` (additive, benefits the existing API too):

- Add `busy: AtomicBool` to `ActorStats`; set on handle start, clear on finish.
- Add `pub is_busy: bool` to `ActorStatsSnapshot`.

Everything else (`actor.rs`, `handle.rs`, `actor_ref.rs`, `pool.rs`,
`registry.rs`) is untouched.

## Module & Feature Layout

New cargo features in `kathputli/Cargo.toml`:

- `system` (default off) — gates all new modules; enables optional dep
  `futures-util` (used only for `FutureExt::catch_unwind`).
- `serde` (default off) — derives `Serialize` on the status snapshot types.

New modules (all gated by `system`):

- `system.rs` — `ActorSystem`, `SpawnOptions`, root + status bootstrap.
- `context.rs` — `Context<M>`, `ActorId`.
- `mailbox.rs` — `Mailbox<M>` wrapping `mpsc::Receiver`, busy/idle instrumentation.
- `supervisor.rs` — internal restart loop, supervision tree (type-erased
  lifecycle entries: `{ id, name, parent, children, token, alive, restarts,
  stats: Arc<ActorStats> }`), `SupervisionEvent`.
- `status.rs` — status actor, `StatusMsg`, `StatusEvent`, `ActorStatus`,
  `ActorNode`, `StatusRef`.

The supervision tree is **heterogeneous** (actors of different `Msg` types in one
tree). It stores **type-erased** lifecycle data — the cancellation token,
liveness flag, restart count, name/links, and `Arc<ActorStats>`. Typed
communication is via the `ActorRef<M>` the caller keeps; the tree never needs the
typed sender.

## Testing Strategy

Integration tests (new `tests/test_system.rs`), mirroring existing test style:

- Restart up to 3 then escalate+die (count `init` invocations; assert event).
- Mailbox preserved across restart (buffered message handled by the new
  incarnation; sender handle still valid).
- No zombie: after stop, actor is not `alive`, removed from the tree/status.
- Cascade shutdown: parent shutdown stops all descendants.
- One-for-one isolation: a child panic/restart leaves siblings running.
- Poison pill: buffered messages drained, then clean exit; new sends rejected.
- `spawn_once` kamikaze: runs once then reaped; panic → restart → exhaust → die.
- Status actor: `Spawned`/`Restarted`/`Failed`/`Stopped` recorded; `busy`/`idle`
  accuracy under load; `GetTree` returns correct nesting.
- `serde` feature: `ActorStatus`/`ActorNode` serialize.

## Implementation Order

1. `stats` busy flag (additive, no feature gate).
2. `supervisor` restart loop + `Context` + `Mailbox` + `spawn`/`spawn_once`
   (feature-gated), with `catch_unwind`.
3. Supervision tree + cascade + one-for-one + escalation `broadcast` events +
   `ActorSystem` + root actor.
4. Status actor (default child) + push events + pull atomics + query types +
   `serde`.
5. Poison pill.
6. *(Optional / later)* macro sugar + trait→system adapter + per-parent failure
   handlers + restart backoff/windows.

## Future Work

- Actor state persistence.
- Per-parent programmatic failure handlers (richer "parent decides" policy).
- Restart backoff and time-windowed restart limits.
- Optional `actor!`/`behavior!` macro for pseudo-functional ergonomics.
- Trait→system adapter so existing `Actor` impls can be supervised.
