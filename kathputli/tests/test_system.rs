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
        || 0u32,
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
        || false, // have_panicked flag
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
        || (),
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
    let a = sys.spawn("p", || 0u64, |n, m, _c| async move {
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
