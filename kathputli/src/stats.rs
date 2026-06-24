use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Lock-free activity counters shared between an actor's receive loop and the
/// handles that observe it.
///
/// One instance is created per spawned actor (wrapped in `Arc`). The receive
/// loop is the only writer; handles only ever read via [`snapshot`].
pub(crate) struct ActorStats {
    message_count: AtomicU64,
    /// Millis since `base` at which the most recently handled message started.
    /// `0` means no message has been handled yet.
    last_activity_ms: AtomicU64,
    busy: AtomicBool,
    base: Instant,
}

impl ActorStats {
    pub(crate) fn new() -> Self {
        Self {
            message_count: AtomicU64::new(0),
            last_activity_ms: AtomicU64::new(0),
            busy: AtomicBool::new(false),
            base: Instant::now(),
        }
    }

    /// Record that a message has been dequeued and is about to be handled.
    /// Counting here (rather than after handling) keeps `message_count`
    /// consistent with an `ask` whose reply is sent from inside `handle`.
    pub(crate) fn record_start(&self) {
        self.message_count.fetch_add(1, Ordering::Relaxed);
        // `.max(1)` keeps a freshly-spawned actor whose first message lands at
        // t=0ms distinguishable from one that has never handled anything.
        let elapsed = self.base.elapsed().as_millis() as u64;
        self.last_activity_ms
            .store(elapsed.max(1), Ordering::Relaxed);
        self.busy.store(true, Ordering::Relaxed);
    }

    /// Record that the actor finished handling the current message.
    pub(crate) fn record_finish(&self) {
        self.busy.store(false, Ordering::Relaxed);
    }

    /// Build a point-in-time view. `mailbox_depth` is read by the caller from
    /// the live channel since it is not tracked here.
    pub(crate) fn snapshot(&self, mailbox_depth: usize) -> ActorStatsSnapshot {
        let ms = self.last_activity_ms.load(Ordering::Relaxed);
        let last_activity = (ms != 0).then(|| self.base + Duration::from_millis(ms));
        ActorStatsSnapshot {
            message_count: self.message_count.load(Ordering::Relaxed),
            mailbox_depth,
            last_activity,
            is_busy: self.busy.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time view of an actor's activity, returned by
/// [`ActorHandle::stats`](crate::ActorHandle::stats) and
/// [`ActorRef::stats`](crate::ActorRef::stats).
#[derive(Debug, Clone)]
pub struct ActorStatsSnapshot {
    /// Total messages dequeued for handling since the actor was spawned.
    pub message_count: u64,
    /// Approximate messages currently queued in the mailbox. Does not include a
    /// message actively being handled (it has already left the channel).
    pub mailbox_depth: usize,
    /// When the most recent message began handling, or `None` if the actor has
    /// not handled any message yet.
    pub last_activity: Option<Instant>,
    /// `true` if the actor is currently inside `handle()` / `update()`.
    pub is_busy: bool,
}

impl ActorStatsSnapshot {
    /// Time elapsed since the last message was handled, or `None` if the actor
    /// has been idle since it was spawned.
    pub fn idle_for(&self) -> Option<Duration> {
        self.last_activity.map(|t| t.elapsed())
    }
}
