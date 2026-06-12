use anyhow::Result;
use async_trait::async_trait;
use kathputli::{Actor, ActorRef};
use tokio::sync::oneshot;

// ---------------------------------------------------------------------------
// Test actor: simple counter
// ---------------------------------------------------------------------------

struct Counter {
    count: u32,
}

enum CounterMsg {
    Inc,
    Get(oneshot::Sender<u32>),
}

#[async_trait]
impl Actor for Counter {
    type Msg = CounterMsg;

    async fn handle(&mut self, msg: Self::Msg) {
        match msg {
            CounterMsg::Inc => self.count += 1,
            CounterMsg::Get(tx) => {
                let _ = tx.send(self.count);
            }
        }
    }
}

fn spawn_counter(initial: u32) -> ActorRef<CounterMsg> {
    kathputli::spawn(Counter { count: initial }, 64)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tell_reaches_actor() {
    let actor_ref = spawn_counter(0);
    for _ in 0..10 {
        actor_ref.tell(CounterMsg::Inc).unwrap();
    }
    // ask to flush — reply only arrives after all earlier tells are processed
    let count = actor_ref.ask(CounterMsg::Get).await.unwrap();
    assert_eq!(count, 10);
}

#[tokio::test]
async fn ask_returns_correct_reply() {
    let actor_ref = spawn_counter(42);
    let count = actor_ref.ask(CounterMsg::Get).await.unwrap();
    assert_eq!(count, 42);
}

#[tokio::test]
async fn shutdown_stops_processing() {
    let actor_ref = spawn_counter(0);

    // Confirm alive
    assert!(actor_ref.is_alive());

    actor_ref.shutdown();

    // is_alive immediately reflects cancellation
    assert!(!actor_ref.is_alive());

    // Give the loop a moment to exit
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // Further tells fail because the channel receiver is dropped after loop exit
    // (the mpsc sender will eventually error when the loop has exited)
    // We verify is_alive() is false as the primary indicator.
    assert!(!actor_ref.is_alive());
}

#[tokio::test]
async fn dead_actor_tell_returns_error() {
    let actor_ref = spawn_counter(0);

    // Shut down the actor — token is cancelled, loop exits, receiver is dropped
    actor_ref.shutdown();

    // Give the task time to process the cancellation and exit
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // try_send fails because the receiver (rx) is dropped after loop exit
    let result: Result<()> = actor_ref.tell(CounterMsg::Inc);
    assert!(result.is_err(), "expected error after actor loop exited");
}

#[tokio::test]
async fn handle_clone_communicates_independently() {
    let actor_ref = spawn_counter(0);
    let handle = actor_ref.handle().clone();

    // Increment via handle clone
    handle.tell(CounterMsg::Inc).unwrap();
    handle.tell(CounterMsg::Inc).unwrap();

    // Query via actor_ref
    let count = actor_ref.ask(CounterMsg::Get).await.unwrap();
    assert_eq!(count, 2);
}
