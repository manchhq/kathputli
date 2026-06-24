//! Process-unique actor identifiers.

use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// A process-unique, monotonically increasing actor id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ActorId(u64);

impl ActorId {
    /// Allocate the next unique id.
    pub(crate) fn next() -> Self {
        ActorId(NEXT_ID.fetch_add(1, Ordering::Relaxed))
    }

    /// The underlying numeric value.
    pub fn value(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for ActorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_unique_and_increasing() {
        let a = ActorId::next();
        let b = ActorId::next();
        assert_ne!(a, b);
        assert!(b.value() > a.value());
    }
}
