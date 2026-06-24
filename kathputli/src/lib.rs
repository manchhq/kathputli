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
// #[cfg(feature = "system")]
// pub use status::{ActorNode, ActorStatus, StatusMsg, StatusRef};
