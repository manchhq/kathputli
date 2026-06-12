use async_trait::async_trait;

/// Core trait for all actors.
///
/// Implement this for your actor struct. The actor processes messages one at a
/// time in the order they arrive. State is owned exclusively by the actor.
#[async_trait]
pub trait Actor: Send + 'static {
    type Msg: Send + 'static;

    async fn handle(&mut self, msg: Self::Msg);
}
