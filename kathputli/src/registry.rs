use std::collections::HashMap;

use tokio::sync::RwLock;

use crate::ActorHandle;

/// Named registry for actor handles — find-or-create pattern.
///
/// Useful for per-entity actors (e.g. one actor per patient ID).
/// All methods are async to avoid blocking while holding the lock.
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
}
