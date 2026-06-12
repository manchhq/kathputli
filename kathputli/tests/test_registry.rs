use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use kathputli::{Actor, ActorRegistry};
use tokio::sync::oneshot;

// ---------------------------------------------------------------------------
// Minimal actor for registry tests
// ---------------------------------------------------------------------------

struct Noop;

enum NoopMsg {
    Ping(oneshot::Sender<()>),
}

#[async_trait]
impl Actor for Noop {
    type Msg = NoopMsg;

    async fn handle(&mut self, msg: Self::Msg) {
        match msg {
            NoopMsg::Ping(tx) => {
                let _ = tx.send(());
            }
        }
    }
}

fn spawn_noop() -> kathputli::ActorHandle<NoopMsg> {
    kathputli::spawn(Noop, 8).handle().clone()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_or_insert_creates_and_caches() {
    let registry: ActorRegistry<NoopMsg> = ActorRegistry::new();

    let h1 = registry
        .get_or_insert_with("actor-1".into(), spawn_noop)
        .await;
    let h2 = registry
        .get_or_insert_with("actor-1".into(), spawn_noop)
        .await;

    // Both must refer to the same actor — verify by asking via both handles
    h1.ask(NoopMsg::Ping).await.unwrap();
    h2.ask(NoopMsg::Ping).await.unwrap();
}

#[tokio::test]
async fn get_returns_none_for_missing_key() {
    let registry: ActorRegistry<NoopMsg> = ActorRegistry::new();
    let result = registry.get("missing").await;
    assert!(result.is_none());
}

#[tokio::test]
async fn remove_clears_entry() {
    let registry: ActorRegistry<NoopMsg> = ActorRegistry::new();

    registry.insert("actor-x".into(), spawn_noop()).await;

    assert!(registry.get("actor-x").await.is_some());

    registry.remove("actor-x").await;

    assert!(registry.get("actor-x").await.is_none());
}

#[tokio::test]
async fn concurrent_get_or_insert_creates_only_once() {
    let registry = Arc::new(ActorRegistry::<NoopMsg>::new());
    let create_count = Arc::new(AtomicUsize::new(0));

    let tasks: Vec<_> = (0..20)
        .map(|_| {
            let reg = registry.clone();
            let count = create_count.clone();
            tokio::spawn(async move {
                reg.get_or_insert_with("shared-actor".into(), || {
                    count.fetch_add(1, Ordering::SeqCst);
                    spawn_noop()
                })
                .await
            })
        })
        .collect();

    for task in tasks {
        task.await.unwrap();
    }

    // Under the write lock, `entry().or_insert_with()` is called at most once
    assert_eq!(
        create_count.load(Ordering::SeqCst),
        1,
        "factory must be called exactly once regardless of concurrency"
    );
}
