use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use async_trait::async_trait;
use kathputli::{Actor, ActorRegistry};
use tokio::sync::oneshot;

// ---------------------------------------------------------------------------
// Minimal actor for registry tests
// ---------------------------------------------------------------------------

struct Noop;

enum NoopMsg {
    Ping(oneshot::Sender<()>),
    /// Blocks inside `handle()` until the embedded receiver resolves. Lets a
    /// test pin an actor in the busy state for a deterministic window.
    Block(oneshot::Receiver<()>),
}

#[async_trait]
impl Actor for Noop {
    type Msg = NoopMsg;

    async fn handle(&mut self, msg: Self::Msg) {
        match msg {
            NoopMsg::Ping(tx) => {
                let _ = tx.send(());
            }
            NoopMsg::Block(rx) => {
                let _ = rx.await;
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

// ---------------------------------------------------------------------------
// Inspection (0.2.1)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn len_is_empty_and_contains_reflect_state() {
    let registry: ActorRegistry<NoopMsg> = ActorRegistry::new();

    assert_eq!(registry.len().await, 0);
    assert!(registry.is_empty().await);
    assert!(!registry.contains("a").await);

    registry.insert("a".into(), spawn_noop()).await;

    assert_eq!(registry.len().await, 1);
    assert!(!registry.is_empty().await);
    assert!(registry.contains("a").await);
    assert!(!registry.contains("b").await);
}

#[tokio::test]
async fn ids_and_snapshot_reflect_inserts() {
    let registry: ActorRegistry<NoopMsg> = ActorRegistry::new();
    registry.insert("a".into(), spawn_noop()).await;
    registry.insert("b".into(), spawn_noop()).await;

    let mut ids = registry.ids().await;
    ids.sort();
    assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);

    let snapshot = registry.snapshot().await;
    assert_eq!(snapshot.len(), 2);
    let mut snap_ids: Vec<String> = snapshot.into_iter().map(|(id, _)| id).collect();
    snap_ids.sort();
    assert_eq!(snap_ids, vec!["a".to_string(), "b".to_string()]);
}

// ---------------------------------------------------------------------------
// idle_for (0.2.1)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn idle_for_is_none_for_missing_and_freshly_spawned() {
    let registry: ActorRegistry<NoopMsg> = ActorRegistry::new();

    // Absent entry.
    assert!(registry.idle_for("missing").await.is_none());

    // Freshly spawned, never handled a message → conservatively unknown.
    registry.insert("fresh".into(), spawn_noop()).await;
    assert!(registry.idle_for("fresh").await.is_none());
}

#[tokio::test]
async fn idle_for_is_some_after_handling_a_message() {
    let registry: ActorRegistry<NoopMsg> = ActorRegistry::new();
    registry.insert("a".into(), spawn_noop()).await;

    registry
        .get("a")
        .await
        .unwrap()
        .ask(NoopMsg::Ping)
        .await
        .unwrap();

    assert!(registry.idle_for("a").await.is_some());
}

// ---------------------------------------------------------------------------
// evict_idle (0.2.1)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn evict_idle_removes_actor_idle_past_ttl() {
    let registry: ActorRegistry<NoopMsg> = ActorRegistry::new();
    registry.insert("idle".into(), spawn_noop()).await;

    // Handle one message so the actor has a last-activity timestamp.
    registry
        .get("idle")
        .await
        .unwrap()
        .ask(NoopMsg::Ping)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(80)).await;

    let evicted = registry.evict_idle(Duration::from_millis(40)).await;

    assert_eq!(evicted, vec!["idle".to_string()]);
    assert!(!registry.contains("idle").await);
    assert!(registry.is_empty().await);
}

#[tokio::test]
async fn evict_idle_keeps_busy_queued_and_recent() {
    let registry: ActorRegistry<NoopMsg> = ActorRegistry::new();
    for id in ["idle", "busy", "queued", "recent"] {
        registry.insert(id.into(), spawn_noop()).await;
    }

    // idle: handle a message, then let it age past the TTL below.
    registry
        .get("idle")
        .await
        .unwrap()
        .ask(NoopMsg::Ping)
        .await
        .unwrap();

    // busy: pin it inside handle() for the whole test.
    let (busy_tx, busy_rx) = oneshot::channel();
    registry
        .get("busy")
        .await
        .unwrap()
        .tell(NoopMsg::Block(busy_rx))
        .unwrap();

    // queued: one message in-flight (busy) plus one waiting in the mailbox.
    let (q_tx, q_rx) = oneshot::channel();
    let (q_tx2, q_rx2) = oneshot::channel();
    let queued = registry.get("queued").await.unwrap();
    queued.tell(NoopMsg::Block(q_rx)).unwrap();
    queued.tell(NoopMsg::Block(q_rx2)).unwrap();

    // Let busy/queued actually enter handle(), and let idle age.
    tokio::time::sleep(Duration::from_millis(80)).await;

    // recent: handled a message just now, well within the TTL.
    registry
        .get("recent")
        .await
        .unwrap()
        .ask(NoopMsg::Ping)
        .await
        .unwrap();

    // Sanity: queued really does have a waiting message.
    assert_eq!(
        registry.get("queued").await.unwrap().stats().mailbox_depth,
        1,
        "queued actor should have one message waiting in its mailbox"
    );

    let evicted = registry.evict_idle(Duration::from_millis(40)).await;

    assert_eq!(evicted, vec!["idle".to_string()]);
    assert!(!registry.contains("idle").await);
    assert!(registry.contains("busy").await);
    assert!(registry.contains("queued").await);
    assert!(registry.contains("recent").await);

    // Release the pinned actors so their tasks can exit cleanly.
    let _ = busy_tx.send(());
    let _ = q_tx.send(());
    let _ = q_tx2.send(());
}

#[tokio::test]
async fn snapshot_reflects_post_eviction_state() {
    let registry: ActorRegistry<NoopMsg> = ActorRegistry::new();
    registry.insert("keep".into(), spawn_noop()).await;
    registry.insert("drop".into(), spawn_noop()).await;

    registry
        .get("drop")
        .await
        .unwrap()
        .ask(NoopMsg::Ping)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(80)).await;

    let evicted = registry.evict_idle(Duration::from_millis(40)).await;
    assert_eq!(evicted, vec!["drop".to_string()]);

    let snap_ids: Vec<String> = registry.snapshot().await.into_iter().map(|(id, _)| id).collect();
    assert_eq!(snap_ids, vec!["keep".to_string()]);
}
