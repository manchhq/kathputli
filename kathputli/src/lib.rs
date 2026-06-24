//! # kathputli — typed-mailbox actor primitive for Rust
//!
//! A minimal, typed-mailbox actor system built on Tokio.
//!
//! ## Core API (always available)
//!
//! Use [`spawn`] + [`ActorHandle`] to create unsupervised actors via the
//! [`Actor`] trait.
//!
//! ## Actor System (opt-in, `features = ["system"]`)
//!
//! Enable the `system` feature (and optionally `serde`) to unlock a supervised
//! actor tree with restart policies, lifecycle events, and status queries.
//!
//! ### Model
//!
//! Actors are pure state machines: `init` produces the initial state from a
//! `Context`, and `update` folds each incoming message into the next state:
//!
//! ```text
//! init:   Context<M> -> S
//! update: (S, M, Context<M>) -> impl Future<Output = S>
//! ```
//!
//! ### Restart policy
//!
//! Each actor restarts up to `max_restarts` times (default **3**) on panic.
//! After the limit is reached, the system emits
//! `SupervisionEvent::Failed` and the actor dies permanently — it is never
//! auto-restarted past the limit. The mailbox is **preserved** across restarts
//! so no messages are lost.
//!
//! ### Shutdown flavours
//!
//! * **`poison()`** — drain pending messages, then stop (graceful).
//! * **`shutdown()`** on the system — cancel the root token, cascading all
//!   children to stop immediately.
//!
//! ### Quick example
//!
//! ```rust,ignore
//! use kathputli::ActorSystem;
//!
//! #[tokio::main]
//! async fn main() {
//!     let sys = ActorSystem::start();
//!
//!     // Spawn a stateful counter actor.
//!     // `init` receives a Context (use `_ctx` if unused).
//!     let counter = sys.spawn(
//!         "counter",
//!         |_ctx| 0u64,                          // init: Context<u64> -> u64
//!         |state, _msg: (), _ctx| async move {  // update: folds each message
//!             state + 1
//!         },
//!     );
//!
//!     // Spawn a one-shot (kamikaze) actor.
//!     sys.spawn_once("greeter", |_ctx| async move {
//!         println!("hello once");
//!     });
//!
//!     // Query the live supervision tree.
//!     let nodes = sys.status().tree().await;
//!     println!("{} top-level actor(s)", nodes.len());
//!
//!     sys.shutdown();
//! }
//! ```
//!
//! See `ActorSystem`, `Context`, `StatusRef` for the full API.

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
