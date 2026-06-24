#![cfg(feature = "system")]

use kathputli::{ActorSystem, Context};
use std::time::Duration;
use tokio::time::timeout;

/// Helper: drain events until the predicate matches, timeout after `ms`.
async fn wait_event<F>(
    rx: &mut tokio::sync::broadcast::Receiver<kathputli::SupervisionEvent>,
    ms: u64,
    pred: F,
) -> bool
where
    F: Fn(&kathputli::SupervisionEvent) -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_millis(ms);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match timeout(remaining, rx.recv()).await {
            Ok(Ok(ev)) if pred(&ev) => return true,
            Ok(Ok(_)) => continue,
            _ => return false,
        }
    }
}

#[tokio::test]
async fn update_folds_state() {
    let sys = ActorSystem::new();
    let counter: kathputli::ActorRef<u32> = sys.spawn(
        "counter",
        |_ctx| 0u32,
        |state, msg: u32, _ctx: Context<u32>| async move { state + msg },
    );
    counter.tell(1).unwrap();
    counter.tell(2).unwrap();
    counter.tell(3).unwrap();
    // Give the actor time to process
    tokio::time::sleep(Duration::from_millis(50)).await;
    // Actor is still alive (no panic, no stop)
    assert!(counter.is_alive());
}

#[tokio::test]
async fn restarts_on_panic_then_keeps_serving() {
    let sys = ActorSystem::new();
    let mut events = sys.events();

    let actor: kathputli::ActorRef<String> = sys.spawn(
        "panicky",
        |_ctx| false, // have_panicked flag
        |panicked: bool, msg: String, _ctx: Context<String>| async move {
            if !panicked && msg == "boom" {
                panic!("intentional test panic");
            }
            panicked || msg == "boom"
        },
    );

    // Trigger a panic
    actor.tell("boom".to_string()).unwrap();

    // Wait for Restarted event
    assert!(
        wait_event(&mut events, 1000, |e| matches!(
            e,
            kathputli::SupervisionEvent::Restarted { .. }
        ))
        .await
    );

    // After restart the actor should still accept messages
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(actor.is_alive());
    actor.tell("hello".to_string()).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn stops_after_max_restarts_and_escalates() {
    let sys = ActorSystem::new();
    let mut events = sys.events();

    let actor: kathputli::ActorRef<()> = sys.spawn(
        "always_panics",
        |_ctx| (),
        |_state: (), _msg: (), _ctx: Context<()>| async { panic!("always panics") },
    );

    // Exhaust all 3 restarts (4 panics: initial + 3 restarts)
    for _ in 0..4 {
        let _ = actor.tell(());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Should eventually get Failed event
    assert!(
        wait_event(&mut events, 2000, |e| matches!(
            e,
            kathputli::SupervisionEvent::Failed { .. }
        ))
        .await
    );

    // Actor should be marked dead after failure (token cancelled in cleanup)
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!actor.is_alive());
}

#[tokio::test]
async fn supervised_poison_drains_and_stats_track() {
    use tokio::sync::oneshot;
    let sys = ActorSystem::new();
    enum M { Inc, Get(oneshot::Sender<u64>) }
    let a = sys.spawn("p", |_ctx| 0u64, |n, m, _c| async move {
        match m { M::Inc => n + 1, M::Get(r) => { let _ = r.send(n); n } }
    });
    for _ in 0..3 { a.tell(M::Inc).unwrap(); }
    // stats reflect handled messages
    let got = a.ask(M::Get).await.unwrap();
    assert_eq!(got, 3);
    assert!(a.stats().message_count >= 3, "supervised stats must count messages");
    // poison drains remaining then stops
    a.tell(M::Inc).unwrap();
    a.poison();
    tokio::time::sleep(std::time::Duration::from_millis(40)).await;
    assert!(!a.is_alive(), "poison should stop the supervised actor");
}

#[tokio::test]
async fn spawn_once_runs_then_dies() {
    use std::sync::{Arc, Mutex};
    use tokio::sync::oneshot;
    let sys = ActorSystem::new();
    let (tx, rx) = oneshot::channel();
    let tx = Arc::new(Mutex::new(Some(tx)));
    let actor = sys.spawn_once("job", move |_ctx| {
        let tx = tx.clone();
        async move {
            if let Some(sender) = tx.lock().unwrap().take() {
                let _ = sender.send(99u32);
            }
        }
    });
    let got = rx.await.unwrap();
    assert_eq!(got, 99);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!actor.is_alive(), "kamikaze actor reaped after completion");
}

#[tokio::test]
async fn start_brings_up_root_and_status() {
    let sys = ActorSystem::start();
    let tree = sys.status().tree().await;
    // Forest has a single root; status actor is its child.
    assert_eq!(tree.len(), 1, "exactly one root");
    let root = &tree[0];
    assert_eq!(root.status.name, "root");
    assert!(root.children.iter().any(|c| c.status.name == "status"));
}

#[tokio::test]
async fn spawned_actor_appears_under_root() {
    let sys = ActorSystem::start();
    let _a = sys.spawn("worker", |_c| 0u8, |s, _m: (), _c| async move { s });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let tree = sys.status().tree().await;
    let root = &tree[0];
    assert!(root.children.iter().any(|c| c.status.name == "worker"));
}

#[tokio::test]
async fn system_shutdown_stops_everything() {
    let sys = ActorSystem::start();
    let a = sys.spawn("worker", |_c| 0u8, |s, _m: (), _c| async move { s });
    assert!(a.is_alive());
    sys.shutdown();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert!(!a.is_alive(), "cascade from root stops workers");
}

#[tokio::test]
async fn child_cascades_on_parent_shutdown() {
    use std::sync::{Arc, Mutex};
    use tokio::sync::oneshot;
    let sys = ActorSystem::new();
    let (alive_tx, alive_rx) = oneshot::channel::<kathputli::ActorRef<()>>();
    let alive_tx = Arc::new(Mutex::new(Some(alive_tx)));
    let parent = sys.spawn(
        "parent",
        move |ctx| {
            // Spawn a long-lived child the first time init runs.
            let child = ctx.spawn_once("child", |cctx| async move {
                cctx.token_wait().await; // lives until cancelled
            });
            if let Some(tx) = alive_tx.lock().unwrap().take() {
                let _ = tx.send(child);
            }
            ()
        },
        |s, _m: (), _ctx| async move { s },
    );
    let child = alive_rx.await.unwrap();
    assert!(child.is_alive());
    parent.shutdown(); // cancels parent token → child token (descendant) cancels
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!child.is_alive(), "child must cascade-stop with parent");
}

#[tokio::test]
async fn status_records_lifecycle_events() {
    let sys = ActorSystem::start();
    // Spawn then shut down a worker; both events should reach the log.
    let a = sys.spawn("ephemeral", |_c| 0u8, |s, _m: (), _c| async move { s });
    a.shutdown();
    tokio::time::sleep(std::time::Duration::from_millis(40)).await;
    let log = sys.status().recent_events().await;
    assert!(log.iter().any(|e| e.contains("Spawned") && e.contains("ephemeral")));
    assert!(log.iter().any(|e| e.contains("Stopped")));
}

#[cfg(feature = "serde")]
#[tokio::test]
async fn status_snapshot_serializes() {
    let sys = ActorSystem::start();
    let tree = sys.status().tree().await;
    let json = serde_json::to_string(&tree).expect("serialize tree");
    assert!(json.contains("\"name\":\"root\""));
}

#[tokio::test]
async fn no_zombie_after_stop() {
    let sys = ActorSystem::start();
    let a = sys.spawn("temp", |_c| 0u8, |s, _m: (), _c| async move { s });
    let id = {
        // Find the worker id via the tree.
        let tree = sys.status().tree().await;
        fn find(nodes: &[kathputli::ActorNode], name: &str) -> Option<kathputli::ActorId> {
            for n in nodes {
                if n.status.name == name {
                    return Some(n.status.id);
                }
                if let Some(id) = find(&n.children, name) {
                    return Some(id);
                }
            }
            None
        }
        find(&tree, "temp").expect("worker present")
    };
    a.shutdown();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert!(sys.status().actor(id).await.is_none(), "entry must be reaped");
}

#[tokio::test]
async fn one_for_one_isolation() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    let sys = ActorSystem::start();
    let sib_inits = Arc::new(AtomicU32::new(0));

    // Sibling B: counts its init calls (should stay at 1 — never restarts).
    let b_inits = sib_inits.clone();
    let _b = sys.spawn(
        "sibling-b",
        move |_c| {
            b_inits.fetch_add(1, Ordering::SeqCst);
            0u64
        },
        |s, _m: (), _c| async move { s },
    );

    // Sibling A: panics on demand.
    enum AMsg { Boom }
    let a = sys.spawn(
        "sibling-a",
        |_c| 0u64,
        |s, m: AMsg, _c| async move {
            match m { AMsg::Boom => panic!("a down") }
            #[allow(unreachable_code)] s
        },
    );
    a.tell(AMsg::Boom).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    assert_eq!(sib_inits.load(Ordering::SeqCst), 1, "sibling B never restarted");
}

#[tokio::test]
async fn spawn_once_panics_then_restarts_then_escalates() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    let sys = ActorSystem::new();
    let mut events = sys.events();

    // A kamikaze actor that panics every time — should exhaust max_restarts (3),
    // then emit Failed and die.
    let run_count = Arc::new(AtomicU32::new(0));
    let rc = run_count.clone();
    let a = sys.spawn_once("panicky-once", move |_ctx| {
        let rc = rc.clone();
        async move {
            rc.fetch_add(1, Ordering::SeqCst);
            panic!("always panics");
        }
    });

    // Wait for the Failed event (exhausted 3 restarts + initial = 4 runs).
    assert!(
        wait_event(&mut events, 3000, |e| matches!(
            e,
            kathputli::SupervisionEvent::Failed { .. }
        ))
        .await,
        "should emit Failed after exhausting max_restarts"
    );

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!a.is_alive(), "spawn_once must die after escalation");
    // Ran at least initial + max_restarts = 4 times
    assert!(run_count.load(Ordering::SeqCst) >= 4);
}

#[tokio::test]
async fn mailbox_preserved_across_restart() {
    let sys = ActorSystem::start();

    enum M { Boom, Inc, Get(tokio::sync::oneshot::Sender<u64>) }
    let a = sys.spawn(
        "preserve",
        |_c| 0u64,
        |count, m, _c| async move {
            match m {
                M::Boom => panic!("kaboom"),
                M::Inc => count + 1,
                M::Get(reply) => { let _ = reply.send(count); count }
            }
        },
    );
    // Queue Boom then several Incs back-to-back. Boom is handled (panic →
    // restart); the buffered Incs must survive and be handled by the new
    // incarnation.
    a.tell(M::Boom).unwrap();
    for _ in 0..4 {
        a.tell(M::Inc).unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_millis(40)).await;
    let got = a.ask(M::Get).await.unwrap();
    assert_eq!(got, 4, "buffered Incs survived the restart");
}
