use async_trait::async_trait;
use kathputli::{Actor, ActorPool};
use proptest::prelude::*;
use tokio::sync::oneshot;

// ---------------------------------------------------------------------------
// Shared test actors
// ---------------------------------------------------------------------------

/// Collects every pushed u32 in arrival order.
struct Collector {
    items: Vec<u32>,
}

enum CollectorMsg {
    Push(u32),
    Get(oneshot::Sender<Vec<u32>>),
}

#[async_trait]
impl Actor for Collector {
    type Msg = CollectorMsg;

    async fn handle(&mut self, msg: Self::Msg) {
        match msg {
            CollectorMsg::Push(v) => self.items.push(v),
            CollectorMsg::Get(tx) => {
                let _ = tx.send(self.items.clone());
            }
        }
    }
}

/// Simple per-worker counter used by pool distribution tests.
#[derive(Default)]
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

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

proptest! {
    /// Messages sent via `tell` must arrive at the actor in exactly the order
    /// they were sent (single-producer, single-consumer channel guarantee).
    #[test]
    fn prop_messages_processed_in_order(msgs in proptest::collection::vec(0u32..1000, 0..50)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let actor_ref = kathputli::spawn(
                Collector { items: Vec::new() },
                64,
            );

            for &v in &msgs {
                actor_ref.tell(CollectorMsg::Push(v)).unwrap();
            }

            let received = actor_ref.ask(CollectorMsg::Get).await.unwrap();
            prop_assert_eq!(received, msgs);
            Ok(())
        }).map_err(|e: TestCaseError| e)?;
    }

    /// After sending n messages and then shutting down, the actor must not have
    /// processed more messages than were sent (no phantom delivery).
    #[test]
    fn prop_all_messages_delivered_before_shutdown(n in 1usize..=100) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let actor_ref = kathputli::spawn(
                Collector { items: Vec::new() },
                256,
            );

            for i in 0..n {
                actor_ref.tell(CollectorMsg::Push(i as u32)).unwrap();
            }

            // Flush: the ask arrives after all preceding tells in the mailbox
            let received = actor_ref.ask(CollectorMsg::Get).await.unwrap();

            prop_assert!(
                received.len() <= n,
                "processed {} but sent only {}",
                received.len(),
                n
            );
            Ok(())
        }).map_err(|e: TestCaseError| e)?;
    }

    /// Round-robin pool distributes exactly `per_worker` messages per worker
    /// when the total sent equals `per_worker * pool_size`.
    #[test]
    fn prop_pool_distributes_evenly(per_worker in 1u32..=30) {
        const POOL_SIZE: usize = 3;
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let pool: ActorPool<CounterMsg> =
                ActorPool::new(POOL_SIZE, Counter::default, 64);

            let total = per_worker * POOL_SIZE as u32;
            for _ in 0..total {
                pool.tell(CounterMsg::Inc).unwrap();
            }

            for worker in pool.workers() {
                let count = worker.ask(CounterMsg::Get).await.unwrap();
                prop_assert_eq!(count, per_worker);
            }
            Ok(())
        }).map_err(|e: TestCaseError| e)?;
    }

    /// Asking for an immutable quantity twice must return the same answer.
    #[test]
    fn prop_ask_idempotent(initial in 0u32..10_000) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let actor_ref = kathputli::spawn(
                Collector { items: vec![initial] },
                8,
            );

            let r1 = actor_ref.ask(CollectorMsg::Get).await.unwrap();
            let r2 = actor_ref.ask(CollectorMsg::Get).await.unwrap();
            prop_assert_eq!(r1, r2);
            Ok(())
        }).map_err(|e: TestCaseError| e)?;
    }
}
