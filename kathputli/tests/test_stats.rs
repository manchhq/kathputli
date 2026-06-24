use std::time::Duration;

use async_trait::async_trait;
use kathputli::{Actor, ActorPool, spawn};
use tokio::sync::oneshot;

// ---------------------------------------------------------------------------
// Test actor: replies to Ping, sleeps on Slow (to keep messages queued)
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Worker;

enum WorkerMsg {
    Ping(oneshot::Sender<()>),
    Slow,
}

#[async_trait]
impl Actor for Worker {
    type Msg = WorkerMsg;

    async fn handle(&mut self, msg: Self::Msg) {
        match msg {
            WorkerMsg::Ping(reply) => {
                let _ = reply.send(());
            }
            WorkerMsg::Slow => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}

// ---------------------------------------------------------------------------
// ActorRef::stats
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stats_start_idle() {
    let actor = spawn(Worker, 16);
    let stats = actor.stats();
    assert_eq!(stats.message_count, 0);
    assert_eq!(stats.mailbox_depth, 0);
    assert!(stats.last_activity.is_none());
    assert!(stats.idle_for().is_none());
}

#[tokio::test]
async fn stats_counts_handled_messages() {
    let actor = spawn(Worker, 16);
    for _ in 0..5 {
        actor.ask(WorkerMsg::Ping).await.expect("ping handled");
    }
    let stats = actor.stats();
    assert_eq!(stats.message_count, 5);
    assert!(stats.last_activity.is_some());
    assert!(stats.idle_for().is_some());
}

#[tokio::test]
async fn stats_reports_mailbox_depth() {
    let actor = spawn(Worker, 16);
    // Fire several slow messages without awaiting; the actor can only handle
    // one at a time, so the rest pile up in the mailbox.
    for _ in 0..5 {
        actor.tell(WorkerMsg::Slow).expect("enqueued");
    }
    let depth = actor.stats().mailbox_depth;
    assert!(depth >= 1, "expected queued messages, got depth {depth}");
    assert!(depth <= 5, "depth {depth} exceeds messages sent");
}

// ---------------------------------------------------------------------------
// ActorPool health
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pool_all_alive_on_start() {
    let pool: ActorPool<WorkerMsg> = ActorPool::new(4, || Worker, 16);
    assert_eq!(pool.alive_count(), 4);
    assert_eq!(pool.dead_workers(), 0);
}

#[tokio::test]
async fn pool_tracks_partial_shutdown() {
    let pool: ActorPool<WorkerMsg> = ActorPool::new(3, || Worker, 16);
    pool.workers()[0].shutdown();
    assert_eq!(pool.alive_count(), 2);
    assert_eq!(pool.dead_workers(), 1);
}

#[tokio::test]
async fn pool_all_dead_after_shutdown_all() {
    let pool: ActorPool<WorkerMsg> = ActorPool::new(4, || Worker, 16);
    pool.shutdown_all();
    assert_eq!(pool.alive_count(), 0);
    assert_eq!(pool.dead_workers(), 4);
}

// ---------------------------------------------------------------------------
// Stats busy flag
// ---------------------------------------------------------------------------

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
    assert!(
        !actor.stats().is_busy,
        "actor should be idle after handling"
    );
}
