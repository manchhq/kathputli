use async_trait::async_trait;
use kathputli::{Actor, ActorPool};
use tokio::sync::oneshot;

// ---------------------------------------------------------------------------
// Test actor: per-worker message counter
// ---------------------------------------------------------------------------

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
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pool_size_matches_construction() {
    let pool: ActorPool<CounterMsg> = ActorPool::new(5, Counter::default, 16);
    assert_eq!(pool.size(), 5);
}

#[tokio::test]
async fn pool_routes_round_robin() {
    const WORKERS: usize = 3;
    const PER_WORKER: u32 = 3;
    let pool: ActorPool<CounterMsg> = ActorPool::new(WORKERS, Counter::default, 32);

    // Send WORKERS * PER_WORKER messages — round-robin assigns PER_WORKER to each worker
    for _ in 0..(WORKERS as u32 * PER_WORKER) {
        pool.tell(CounterMsg::Inc).unwrap();
    }

    // Ask each worker for its individual count
    for worker in pool.workers() {
        let count = worker.ask(CounterMsg::Get).await.unwrap();
        assert_eq!(
            count, PER_WORKER,
            "each worker should receive exactly {PER_WORKER} messages"
        );
    }
}

#[tokio::test]
async fn pool_shutdown_all_stops_all_workers() {
    let pool: ActorPool<CounterMsg> = ActorPool::new(3, Counter::default, 16);

    pool.shutdown_all();

    for worker in pool.workers() {
        assert!(!worker.is_alive());
    }
}

#[tokio::test]
async fn pool_workers_accessor_returns_all() {
    let pool: ActorPool<CounterMsg> = ActorPool::new(4, Counter::default, 8);
    assert_eq!(pool.workers().len(), 4);
}
