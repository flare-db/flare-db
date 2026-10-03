//! Beam `BagUserState` semantics.
//!
//! A bag user state is identified by the shared [`super::build_state_key`]
//! composite of `(transform_id, user_state_id, window, key)` and behaves as an
//! ordered bag of byte chunks: [`append`](BagState::append) concatenates a
//! chunk onto the end, [`get`](BagState::get) returns the concatenation of
//! all appended chunks, and [`clear`](BagState::clear) empties it.

use anyhow::Result;

use super::backend::StateBackend;

/// Beam `BagUserState` over the opaque [`StateBackend`] key/value storage.
#[derive(Clone)]
pub struct BagState {
    backend: StateBackend,
}

impl BagState {
    /// Wrap `backend` as Beam bag user state.
    pub fn new(backend: StateBackend) -> Self {
        Self { backend }
    }

    /// Read the concatenation of all values appended for `state_key`.
    pub async fn get(&self, state_key: &[u8]) -> Result<Vec<u8>> {
        Ok(self.backend.get(state_key).await?.unwrap_or_default())
    }

    /// Append `value` to the bag identified by `state_key`.
    ///
    /// The read-modify-write is serialized per key by [`StateBackend`], so
    /// concurrent appends cannot lose an update.
    pub async fn append(&self, state_key: &[u8], value: &[u8]) -> Result<()> {
        self.backend
            .read_modify_write(state_key, |mut current| {
                current.extend_from_slice(value);
                current
            })
            .await
    }

    /// Remove all values for `state_key`.
    pub async fn clear(&self, state_key: &[u8]) -> Result<()> {
        self.backend.delete(state_key).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{state::build_state_key, store::element_store::FlareElementStore};
    use std::sync::Arc;
    use tempfile::tempdir;

    async fn make_bag() -> (tempfile::TempDir, BagState) {
        let dir = tempdir().expect("failed to create tempdir warehouse");
        let warehouse = dir
            .path()
            .to_str()
            .expect("tempdir path is not valid utf8")
            .to_string();
        let store = FlareElementStore::new(warehouse, "testdb".to_string(), None)
            .await
            .expect("failed to construct FlareElementStore");
        (dir, BagState::new(StateBackend::new(Arc::new(store))))
    }

    #[tokio::test]
    async fn get_append_clear_roundtrip() {
        let (_dir, state) = make_bag().await;
        let key = build_state_key("transform", "user-state", b"window", b"key");

        // Missing state reads back empty.
        assert!(state.get(&key).await.unwrap().is_empty());

        // Appends concatenate in order.
        state.append(&key, b"hello").await.unwrap();
        state.append(&key, b" ").await.unwrap();
        state.append(&key, b"world").await.unwrap();
        assert_eq!(state.get(&key).await.unwrap(), b"hello world");

        // Clear empties the bag.
        state.clear(&key).await.unwrap();
        assert!(state.get(&key).await.unwrap().is_empty());

        // Appends after a clear start fresh.
        state.append(&key, b"again").await.unwrap();
        assert_eq!(state.get(&key).await.unwrap(), b"again");
    }

    #[tokio::test]
    async fn bags_are_isolated_between_keys() {
        let (_dir, state) = make_bag().await;
        let k1 = build_state_key("t", "s", b"w", b"k1");
        let k2 = build_state_key("t", "s", b"w", b"k2");

        state.append(&k1, b"one").await.unwrap();
        state.append(&k2, b"two").await.unwrap();

        assert_eq!(state.get(&k1).await.unwrap(), b"one");
        assert_eq!(state.get(&k2).await.unwrap(), b"two");

        state.clear(&k1).await.unwrap();
        assert!(state.get(&k1).await.unwrap().is_empty());
        assert_eq!(state.get(&k2).await.unwrap(), b"two");
    }
}
