use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::RwLock;

use crate::ActorHandle;
use crate::stats::ActorStatsSnapshot;

/// Named registry for actor handles — find-or-create pattern.
///
/// Useful for per-entity actors (e.g. one actor per patient ID).
/// All methods are async to avoid blocking while holding the lock.
///
/// # Ownership contract
///
/// The registry is intended to hold the **canonical** handle for each entity.
/// Callers fetch via [`get`](Self::get) / [`get_or_insert_with`](Self::get_or_insert_with),
/// use the clone for a single operation, and drop it — they should **not** stash
/// handle clones across long awaits.
///
/// The registry has no background reaper: it never evicts on its own. A parent
/// observes its children via [`snapshot`](Self::snapshot) / [`idle_for`](Self::idle_for)
/// and reclaims idle ones by calling [`evict_idle`](Self::evict_idle) on its own
/// schedule. Because `evict_idle` only removes actors that are not busy, have an
/// empty mailbox, and have been idle past the TTL, the window for a transient
/// duplicate (a caller still holding a clone of a just-evicted handle) is tiny.
/// Consumers whose actors mutate shared state should still use optimistic
/// concurrency on the write path (an entity-versioned append) so a transient
/// duplicate actor cannot double-commit.
pub struct ActorRegistry<Msg: Send + 'static> {
    actors: RwLock<HashMap<String, ActorHandle<Msg>>>,
}

impl<Msg: Send + 'static> Default for ActorRegistry<Msg> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Msg: Send + 'static> ActorRegistry<Msg> {
    pub fn new() -> Self {
        Self {
            actors: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get(&self, id: &str) -> Option<ActorHandle<Msg>> {
        self.actors.read().await.get(id).cloned()
    }

    pub async fn insert(&self, id: String, handle: ActorHandle<Msg>) {
        self.actors.write().await.insert(id, handle);
    }

    pub async fn remove(&self, id: &str) {
        self.actors.write().await.remove(id);
    }

    /// Returns an existing handle or calls `f` to create one and inserts it.
    pub async fn get_or_insert_with(
        &self,
        id: String,
        f: impl FnOnce() -> ActorHandle<Msg>,
    ) -> ActorHandle<Msg> {
        // Fast path: already exists
        {
            let read = self.actors.read().await;
            if let Some(handle) = read.get(&id) {
                return handle.clone();
            }
        }
        // Slow path: insert
        let mut write = self.actors.write().await;
        write.entry(id).or_insert_with(f).clone()
    }

    // -- Inspection ---------------------------------------------------------

    /// Number of live children currently registered.
    pub async fn len(&self) -> usize {
        self.actors.read().await.len()
    }

    /// `true` if no children are registered.
    pub async fn is_empty(&self) -> bool {
        self.actors.read().await.is_empty()
    }

    /// `true` if a child with this id is registered.
    pub async fn contains(&self, id: &str) -> bool {
        self.actors.read().await.contains_key(id)
    }

    /// Ids of every live child, in arbitrary order.
    pub async fn ids(&self) -> Vec<String> {
        self.actors.read().await.keys().cloned().collect()
    }

    /// `(id, stats)` for every live child — observability, and the raw material
    /// for a custom eviction policy beyond plain TTL.
    pub async fn snapshot(&self) -> Vec<(String, ActorStatsSnapshot)> {
        self.actors
            .read()
            .await
            .iter()
            .map(|(id, handle)| (id.clone(), handle.stats()))
            .collect()
    }

    // -- Eviction -----------------------------------------------------------

    /// Idle duration of one child, or `None` if it is absent or has never
    /// handled a message.
    pub async fn idle_for(&self, id: &str) -> Option<Duration> {
        self.actors
            .read()
            .await
            .get(id)
            .and_then(|handle| handle.stats().idle_for())
    }

    /// Remove and return the ids of children that are *evictable*, checked
    /// atomically under the write lock. The parent decides when to call this;
    /// the registry never sweeps on its own.
    ///
    /// A child is evictable when it is not busy, has an empty mailbox, and has
    /// been idle for at least `ttl`. A child that has never handled a message
    /// (no last-activity timestamp) is conservatively **kept** — it is likely
    /// mid-spawn. Removing an evictable child drops the registry's
    /// [`ActorHandle`]; since the predicate guarantees not-busy with an empty
    /// mailbox, dropping the last handle exits the actor's receive loop cleanly
    /// with no message loss.
    pub async fn evict_idle(&self, ttl: Duration) -> Vec<String> {
        let mut write = self.actors.write().await;
        let evictable: Vec<String> = write
            .iter()
            .filter(|(_, handle)| {
                let s = handle.stats();
                !s.is_busy && s.mailbox_depth == 0 && s.idle_for().is_some_and(|d| d >= ttl)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in &evictable {
            write.remove(id);
        }
        evictable
    }
}
