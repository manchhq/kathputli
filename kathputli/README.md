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

### Known sharp edge / roadmap

A panic inside `handle()` currently kills the actor task silently. Planned additions:

1. **Restart-policy supervision** — an option to restart the actor on panic with configurable backoff.
2. **`ActorRef::terminated()` watch** — a future that resolves when the actor task exits.

**Akka users:** this is deliberately `ActorRef` + mailbox only — no Behaviors, no DeathWatch, no Cluster. If you need those, use a different crate.

## Related crates in this workspace

- [`katha`](../katha) — event sourcing core (कथा, "story/narrative")
- [`katha-macros`](../katha-macros) — proc-macro helpers for katha
