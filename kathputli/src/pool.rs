use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;

use crate::{Actor, ActorRef};

/// A fixed-size pool of identical actors with round-robin dispatch.
///
/// All workers run concurrently. Messages are distributed one-at-a-time in
/// round-robin order. Use [`shutdown_all`] to cancel every worker at once.
pub struct ActorPool<Msg: Send + 'static> {
    workers: Vec<ActorRef<Msg>>,
    next: AtomicUsize,
}

impl<Msg: Send + 'static> ActorPool<Msg> {
    /// Spawn `size` workers using `factory` to create each actor instance.
    ///
    /// # Panics
    /// Panics if `size == 0`.
    pub fn new<A, F>(size: usize, factory: F, buffer: usize) -> Self
    where
        A: Actor<Msg = Msg>,
        F: Fn() -> A,
    {
        assert!(size > 0, "pool size must be at least 1");
        let workers = (0..size).map(|_| crate::spawn(factory(), buffer)).collect();
        Self {
            workers,
            next: AtomicUsize::new(0),
        }
    }

    /// Send a message to the next worker in round-robin order.
    pub fn tell(&self, msg: Msg) -> Result<()> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        self.workers[idx].tell(msg)
    }

    /// Cancel every worker's token, triggering clean shutdown for all.
    pub fn shutdown_all(&self) {
        for worker in &self.workers {
            worker.shutdown();
        }
    }

    pub fn size(&self) -> usize {
        self.workers.len()
    }

    pub fn workers(&self) -> &[ActorRef<Msg>] {
        &self.workers
    }

    /// Number of workers still alive (token not yet cancelled).
    pub fn alive_count(&self) -> usize {
        self.workers.iter().filter(|w| w.is_alive()).count()
    }

    /// Number of workers that have been shut down.
    pub fn dead_workers(&self) -> usize {
        self.workers.len() - self.alive_count()
    }
}
