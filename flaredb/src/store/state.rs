//! Beam Fn State API (`BagUserState`) semantics.
//!
//! This module owns the Beam-specific logic on top of the opaque key/value
//! primitives in [`FlareElementStore`]. A `BagUserState` is identified by a
//! composite of `(transform_id, user_state_id, window, key)` and behaves as an
//! ordered bag of byte chunks: `append` concatenates a chunk onto the end, `get`
//! returns the concatenation of all appended chunks, and `clear` empties it.
//!
//! State requests are serviced by a single, serialized driver (see
//! `engine/harness/state.rs`), so the read-modify-write in [`append_bag`] has no
//! concurrent accessors to race against.

use std::sync::Arc;

use anyhow::Result;

use crate::store::element_store::FlareElementStore;

/// Deterministic composite key for a bag-user-state entry.
///
/// Every component is length-prefixed (4-byte big-endian) so distinct
/// `(transform_id, user_state_id, window, key)` tuples always map to distinct,
/// collision-free keys, and raw binary window/key bytes cannot be ambiguous.
pub fn build_state_key(
    transform_id: &str,
    user_state_id: &str,
    window: &[u8],
    key: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        4 * 4 + transform_id.len() + user_state_id.len() + window.len() + key.len(),
    );
    for part in [
        transform_id.as_bytes(),
        user_state_id.as_bytes(),
        window,
        key,
    ] {
        out.extend_from_slice(&(part.len() as u32).to_be_bytes());
        out.extend_from_slice(part);
    }
    out
}

/// Beam `BagUserState` store backed by [`FlareElementStore`]'s state table.
#[derive(Clone)]
pub struct FlareStateStore {
    store: Arc<FlareElementStore>,
}

impl FlareStateStore {
    /// Wrap an element store, sharing its database/catalog and state table.
    pub fn new(store: Arc<FlareElementStore>) -> Self {
        Self { store }
    }

    /// Read the concatenation of all values appended for `state_key`.
    pub async fn get_bag(&self, state_key: &[u8]) -> Result<Vec<u8>> {
        Ok(self.store.state_get(state_key).await?.unwrap_or_default())
    }

    /// Append `value` to the bag identified by `state_key`.
    pub async fn append_bag(&self, state_key: &[u8], value: &[u8]) -> Result<()> {
        let mut current = self.store.state_get(state_key).await?.unwrap_or_default();
        current.extend_from_slice(value);
        self.store.state_put(state_key, &current).await
    }

    /// Remove all values for `state_key`.
    pub async fn clear_bag(&self, state_key: &[u8]) -> Result<()> {
        self.store.state_delete(state_key).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    async fn make_store() -> (tempfile::TempDir, Arc<FlareElementStore>) {
        let dir = tempdir().expect("failed to create tempdir warehouse");
        let warehouse = dir
            .path()
            .to_str()
            .expect("tempdir path is not valid utf8")
            .to_string();
        let store = FlareElementStore::new(warehouse, "testdb".to_string(), None)
            .await
            .expect("failed to construct FlareElementStore");
        (dir, Arc::new(store))
    }

    #[test]
    fn build_state_key_is_deterministic_and_collision_free() {
        let a = build_state_key("t", "s", b"w", b"k");
        let b = build_state_key("t", "s", b"w", b"k");
        assert_eq!(a, b);

        // Different components must produce different keys.
        assert_ne!(a, build_state_key("t2", "s", b"w", b"k"));
        assert_ne!(a, build_state_key("t", "s2", b"w", b"k"));
        assert_ne!(a, build_state_key("t", "s", b"w2", b"k"));
        assert_ne!(a, build_state_key("t", "s", b"w", b"k2"));

        // Length-prefixing prevents ambiguous concatenations.
        assert_ne!(
            build_state_key("ab", "c", b"", b""),
            build_state_key("a", "bc", b"", b"")
        );
    }

    #[tokio::test]
    async fn get_append_clear_roundtrip() {
        let (_dir, store) = make_store().await;
        let state = FlareStateStore::new(store);
        let key = build_state_key("transform", "user-state", b"window", b"key");

        // Missing state reads back empty.
        assert!(state.get_bag(&key).await.unwrap().is_empty());

        // Appends concatenate in order.
        state.append_bag(&key, b"hello").await.unwrap();
        state.append_bag(&key, b" ").await.unwrap();
        state.append_bag(&key, b"world").await.unwrap();
        assert_eq!(state.get_bag(&key).await.unwrap(), b"hello world");

        // Clear empties the bag.
        state.clear_bag(&key).await.unwrap();
        assert!(state.get_bag(&key).await.unwrap().is_empty());

        // Appends after a clear start fresh.
        state.append_bag(&key, b"again").await.unwrap();
        assert_eq!(state.get_bag(&key).await.unwrap(), b"again");
    }

    #[tokio::test]
    async fn bags_are_isolated_between_keys() {
        let (_dir, store) = make_store().await;
        let state = FlareStateStore::new(store);
        let k1 = build_state_key("t", "s", b"w", b"k1");
        let k2 = build_state_key("t", "s", b"w", b"k2");

        state.append_bag(&k1, b"one").await.unwrap();
        state.append_bag(&k2, b"two").await.unwrap();

        assert_eq!(state.get_bag(&k1).await.unwrap(), b"one");
        assert_eq!(state.get_bag(&k2).await.unwrap(), b"two");

        state.clear_bag(&k1).await.unwrap();
        assert!(state.get_bag(&k1).await.unwrap().is_empty());
        assert_eq!(state.get_bag(&k2).await.unwrap(), b"two");
    }
}
