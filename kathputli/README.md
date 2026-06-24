# kathputli

**कठपुतली** — Hindi for "marionette" or "puppet." Actors respond to messages like puppets respond to strings.

A minimal, typed-mailbox actor primitive for Rust, built on Tokio.

---

## Quick start

```toml
[dependencies]
kathputli = "0.1"
async-trait = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

```rust
use async_trait::async_trait;
use kathputli::{Actor, ActorHandle, spawn};
use tokio::sync::oneshot;

// 1. Define your message type
enum CounterMsg {
    Increment,
    Get(oneshot::Sender<u64>),
}

// 2. Define your actor
struct Counter {
    count: u64,
}

#[async_trait]
impl Actor for Counter {
    type Msg = CounterMsg;

    async fn handle(&mut self, msg: CounterMsg) {
        match msg {
            CounterMsg::Increment => self.count += 1,
            CounterMsg::Get(reply) => { let _ = reply.send(self.count); }
        }
    }
}

#[tokio::main]
async fn main() {
    // 3. Spawn the actor
    let actor_ref = spawn(Counter { count: 0 }, 32);
    let handle: ActorHandle<CounterMsg> = actor_ref.handle().clone();

    // tell: fire-and-forget
    handle.tell(CounterMsg::Increment).unwrap();

    // ask: request-response
    let count = handle.ask(|reply| CounterMsg::Get(reply)).await.unwrap();
    assert_eq!(count, 1);

    // Clean shutdown
    actor_ref.shutdown();
}
```

## Feature flags

| Feature | Description | Default |
|---------|-------------|---------|
| `tracing` | Emit a `trace`-level span around every `handle()` call. Zero overhead without this feature. | off |
| `system` | Opt-in supervised actor system: lifecycle tree, restart policies, status queries. Pulls in `futures-util` and `tokio-util`. | off |
| `serde` | Derives `serde::Serialize` on `ActorStatus` and `ActorNode` (requires `system`). | off |

## Actor System (opt-in)

Enable the `system` feature in `Cargo.toml`:

```toml
[dependencies]
kathputli = { version = "0.1", features = ["system"] }
# add "serde" to the features list for JSON-serializable status snapshots
```

### Model: `init` + `update`

Supervised actors are pure state machines. You provide two closures:

- **`init(ctx: Context<M>) -> S`** — called once (and on each restart) to
  produce the initial state. Takes a `Context` — use `|_ctx|` if you don't need it.
- **`update(state: S, msg: M, ctx: Context<M>) -> impl Future<Output = S>`** —
  called for each incoming message; returns the next state.

The mailbox is **preserved across restarts** — no messages are lost on panic.

### Restart policy

Actors restart up to `max_restarts` times (default **3**) on panic.
After the limit is exhausted the system emits `SupervisionEvent::Failed` and the
actor dies permanently. There is no automatic restart past the limit.

### Shutdown flavours

| Method | Behaviour |
|--------|-----------|
| `actor_ref.poison()` | Drain pending mailbox messages, then stop (graceful). |
| `sys.shutdown()` | Cancel root token immediately — cascades to all children. |

### `spawn_once` — kamikaze actors

`ActorSystem::spawn_once` (and `Context::spawn_once`) run a single async task to
completion and then die. The same restart policy applies: up to `max_restarts`
retries on panic, then escalate + die.

### Status queries

```rust,ignore
// Whole-tree snapshot (Vec<ActorNode> — a recursive tree)
let nodes = sys.status().tree().await;

// Single actor by id
let maybe = sys.status().actor(id).await;

// Recent lifecycle events as formatted strings (bounded at 256 entries)
let log = sys.status().recent_events().await;
```

### Example

```rust,ignore
use kathputli::ActorSystem;

#[tokio::main]
async fn main() {
    let sys = ActorSystem::start();

    // Stateful counter: init takes Context<M> (use _ctx if not needed)
    let _counter = sys.spawn(
        "counter",
        |_ctx| 0u64,                           // init
        |state, _msg: (), _ctx| async move {   // update
            state + 1
        },
    );

    // One-shot task (kamikaze): runs once and exits
    sys.spawn_once("greeter", |_ctx| async move {
        println!("hello once");
    });

    // Query the live supervision tree
    let nodes = sys.status().tree().await;
    println!("{} top-level actor(s)", nodes.len());

    // Graceful system-wide stop
    sys.shutdown();
}
```

### `Context<M>` API

Inside `update` (and `init`), the [`Context<M>`] gives access to:

| Method | Returns |
|--------|---------|
| `ctx.id()` | `ActorId` — this actor's unique id |
| `ctx.name()` | `&str` — registered name |
| `ctx.parent_id()` | `Option<ActorId>` |
| `ctx.myself()` | `ActorRef<M>` — self-message ref |
| `ctx.system()` | `&ActorSystem` |
| `ctx.spawn(name, init, update)` | `ActorRef<M2>` — supervised child |
| `ctx.spawn_once(name, task)` | `ActorRef<()>` — kamikaze child |
| `ctx.status()` | `StatusRef` — status actor handle |
| `ctx.token_wait().await` | Parks until this actor's token is cancelled |

## Design decisions

These decisions are recorded here because they travel with the crate when it moves to its own repository.

### Single crate + feature flags, not a crate family

Modularity ships as Cargo features (the same approach sqlx uses for postgres/sqlite/mysql/any), not as sibling crates. A crate family requires lockstep version maintenance; a feature flag does not.

### Planned `persist` feature

A future `persist` feature will enable event-sourced actors that integrate with the [`katha`](../katha) event-sourcing crate as an optional dependency. This is planned to be extracted from real usage in the PiHealth apps once the per-entity-actor pattern stabilizes — it is not speculative API design.

### Distributed/clustering is an explicit NON-GOAL

There is no remoting, no location transparency, no sharding, and there never will be. Cross-process boundaries should be explicit RPC. This constraint keeps the crate small enough to audit in an afternoon.

### Messaging semantics

- **`tell`** is fail-fast: it calls `try_send` and returns an error immediately when the mailbox is full or the actor is dead. No blocking.
- **`ask`** applies backpressure: it calls `send().await` and waits for the mailbox to accept the message, then awaits the reply.
- Messages are processed **sequentially**. An in-flight `handle()` always completes before shutdown — cancellation is only checked between messages.

### Known sharp edges / roadmap

- **Unsupervised actors** (`spawn` / `Actor` trait): a panic inside `handle()` still kills the actor task silently. Use the `system` feature for restart policies.
- **Planned future work:** per-parent failure-handler callbacks, restart backoff / time-windows, optional `actor!` macro, trait → system adapter, state persistence.

**Akka users:** this is deliberately `ActorRef` + mailbox only — no Behaviors, no DeathWatch, no Cluster. If you need those, use a different crate.

## Related crates in this workspace

- [`katha`](../katha) — event sourcing core (कथा, "story/narrative")
- [`katha-macros`](../katha-macros) — proc-macro helpers for katha
