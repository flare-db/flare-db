//! Durable user state.

pub use self::backend::StateBackend;
pub use self::bag::BagState;

use anyhow::Result;

use self::multimap::MultimapState;

/// Deterministic composite key for a user-state entry.
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

/// A Beam Fn user-state kind FlareDB can serve.
///
/// Note that the SDK harness backs `ValueState`, `CombiningState`, and
/// `SetState` with `BagUserState` on the wire (for example
/// `BagUserState.java` asserts `stateKey.hasBagUserState()`), so `Bag` is the
/// wire kind for those DoFn state types. SDK `MapState` uses the multimap user
/// state protocols below.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserStateKind {
    /// `StateKey.BagUserState`.
    Bag,
    /// `StateKey.MultimapKeysUserState`; enumerates and clears map keys.
    MultimapKeys,
    /// `StateKey.MultimapEntriesUserState`; reads all key/value groups.
    MultimapEntries,
    /// `StateKey.MultimapUserState`; reads or mutates values for one map key.
    MultimapValues,
}

impl UserStateKind {
    /// The `beam_fn_api::StateKey` variant name, for diagnostics and errors.
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::Bag => "bag_user_state",
            Self::MultimapKeys => "multimap_keys_user_state",
            Self::MultimapEntries => "multimap_entries_user_state",
            Self::MultimapValues => "multimap_user_state",
        }
    }
}

/// Transport-agnostic address of a Beam user-state cell.
///
/// Decoding the Fn API `StateKey` proto into this type keeps the proto out of
/// the state layer, mirroring how the harness owns transport framing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserStateAddress {
    /// Which Beam user-state kind this cell belongs to.
    pub kind: UserStateKind,
    /// Id of the `PTransform` that owns the `StateSpec`.
    pub transform_id: String,
    /// The `ParDoPayload.state_specs` local name identifying this state.
    pub user_state_id: String,
    /// The window encoded in a nested context, exactly as received.
    pub window: Vec<u8>,
    /// The user key encoded in a nested context, exactly as received.
    pub key: Vec<u8>,
    /// The map key for `MultimapUserState`; empty for other wire kinds.
    pub map_key: Vec<u8>,
}

impl UserStateAddress {
    /// Build an address from its decoded components.
    pub fn new(
        kind: UserStateKind,
        transform_id: String,
        user_state_id: String,
        window: Vec<u8>,
        key: Vec<u8>,
    ) -> Self {
        Self {
            kind,
            transform_id,
            user_state_id,
            window,
            key,
            map_key: Vec::new(),
        }
    }

    /// Build a multimap-state address, optionally selecting one encoded map key.
    pub fn new_multimap(
        kind: UserStateKind,
        transform_id: String,
        user_state_id: String,
        window: Vec<u8>,
        key: Vec<u8>,
        map_key: Vec<u8>,
    ) -> Self {
        Self {
            kind,
            transform_id,
            user_state_id,
            window,
            key,
            map_key,
        }
    }

    /// The opaque composite key addressing this cell.
    pub fn composite_key(&self) -> Vec<u8> {
        build_state_key(
            &self.transform_id,
            &self.user_state_id,
            &self.window,
            &self.key,
        )
    }
}

/// Dispatches a Beam user-state operation to the implementation for its kind.
///
/// This is the boundary between the Fn State request handler and the store. The
/// harness decodes a `StateKey` into a [`UserStateAddress`]. This type interprets
/// the address according to the Beam state semantics.
pub struct UserStateStore {
    backend: StateBackend,
    address: UserStateAddress,
}

impl UserStateStore {
    /// Bind `address` to `backend`.
    ///
    /// The backend is cheap to clone (it wraps shared handles), so a store may
    /// be constructed per request.
    pub fn new(backend: StateBackend, address: UserStateAddress) -> Self {
        Self { backend, address }
    }

    /// The address this store is bound to.
    pub fn address(&self) -> &UserStateAddress {
        &self.address
    }

    /// Read the cell's value, or empty when unset.
    ///
    /// For bag state this is the concatenation of all appended chunks, matching
    /// the Fn State `get` contract.
    pub async fn get(&self) -> Result<Vec<u8>> {
        let key = self.address.composite_key();
        match self.address.kind {
            UserStateKind::Bag => BagState::new(self.backend.clone()).get(&key).await,
            UserStateKind::MultimapKeys => {
                MultimapState::new(self.backend.clone()).keys(&key).await
            }
            UserStateKind::MultimapEntries => {
                MultimapState::new(self.backend.clone()).entries(&key).await
            }
            UserStateKind::MultimapValues => {
                MultimapState::new(self.backend.clone())
                    .values(&key, &self.address.map_key)
                    .await
            }
        }
    }

    /// Append `data` to the cell (bag semantics).
    pub async fn append(&self, data: &[u8]) -> Result<()> {
        let key = self.address.composite_key();
        match self.address.kind {
            UserStateKind::Bag => BagState::new(self.backend.clone()).append(&key, data).await,
            UserStateKind::MultimapKeys => Err(anyhow::anyhow!(
                "multimap keys append requires parsed map keys"
            )),
            UserStateKind::MultimapEntries => Err(anyhow::anyhow!(
                "append is not supported for multimap entries state"
            )),
            UserStateKind::MultimapValues => Err(anyhow::anyhow!(
                "multimap value append requires parsed map values"
            )),
        }
    }

    /// Clear the cell.
    pub async fn clear(&self) -> Result<()> {
        let key = self.address.composite_key();
        match self.address.kind {
            UserStateKind::Bag => BagState::new(self.backend.clone()).clear(&key).await,
            UserStateKind::MultimapKeys | UserStateKind::MultimapEntries => {
                MultimapState::new(self.backend.clone()).clear(&key).await
            }
            UserStateKind::MultimapValues => {
                MultimapState::new(self.backend.clone())
                    .clear_values(&key, &self.address.map_key)
                    .await
            }
        }
    }

    /// Append already-encoded map keys to multimap user state.
    pub async fn append_multimap_keys(&self, keys: &[Vec<u8>]) -> Result<()> {
        let key = self.address.composite_key();
        MultimapState::new(self.backend.clone())
            .append_keys(&key, keys)
            .await
    }

    /// Append already-encoded values for this address's map key.
    pub async fn append_multimap_values(&self, values: &[Vec<u8>]) -> Result<()> {
        let key = self.address.composite_key();
        MultimapState::new(self.backend.clone())
            .append_values(&key, &self.address.map_key, values)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn user_state_address_composite_key_matches_build_state_key() {
        let address = UserStateAddress::new(
            UserStateKind::Bag,
            "transform".to_string(),
            "state".to_string(),
            b"window".to_vec(),
            b"key".to_vec(),
        );

        assert_eq!(
            address.composite_key(),
            build_state_key("transform", "state", b"window", b"key")
        );
        assert_eq!(address.kind.wire_name(), "bag_user_state");
    }
}

pub mod backend {
    //! Opaque key/value storage backend for Beam user state.
    //!
    //! The backend stores one durable cell per state key. The key is the composite
    //! state key of opaque bytes. The value is one blob. A write is a
    //! last-write-wins upsert and a clear removes the cell. The cells live in the
    //! `state` database of the job's SlateDB store.
    //!
    //! This layer moves opaque bytes only. The Beam semantics belong to the
    //! per-state-type modules, such as [`super::bag`]: append is a
    //! read-modify-write and clear is a delete.

    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use anyhow::Result;
    use tokio::sync::Mutex;

    use crate::engine::kv::KvStore;

    /// Automatic flush.
    const STATE_FLUSH_THRESHOLD: usize = 512;

    const STATE_CACHE_MAX_ENTRIES: usize = 100_000;

    const STATE_CACHE_EVICT_TARGET: usize = STATE_CACHE_MAX_ENTRIES / 4 * 3;

    struct StateCache {
        /// Authoritative value per touched cell. `None` means that the cell is
        /// empty, deleted, or cleared.
        values: HashMap<Vec<u8>, Option<Vec<u8>>>,
        /// Cells whose value the store does not hold yet.
        dirty: HashSet<Vec<u8>>,
    }

    impl StateCache {
        /// Evict clean cells after an insert once the cache exceeds its bound.
        fn evict_if_needed(&mut self) {
            if self.values.len() > STATE_CACHE_MAX_ENTRIES {
                self.evict_clean_to_fit(STATE_CACHE_EVICT_TARGET);
            }
        }

        /// Drop clean cells until at most `target` remain.
        ///
        /// Dirty cells never go: their value is the client-visible state and it
        /// must survive until the commit. A fully dirty cache stays above
        /// `target`. The flush threshold keeps the dirty set small.
        fn evict_clean_to_fit(&mut self, target: usize) {
            if self.values.len() <= target {
                return;
            }
            let StateCache { values, dirty } = self;
            let mut excess = values.len() - target;
            values.retain(|key, _| {
                if excess > 0 && !dirty.contains(key) {
                    excess -= 1;
                    false
                } else {
                    true
                }
            });
        }
    }

    /// Opaque key/value storage for Beam user state.
    ///
    /// The cells live in a [`KvStore`]. The trait keeps the SlateDB dependency out
    /// of this module and gives the tests a seam.
    #[derive(Clone)]
    pub struct StateBackend {
        store: Arc<dyn KvStore>,
        /// Write-back cache plus the lock that serializes all state operations on
        /// it. A single lock is enough because requests are handled one at a time;
        /// it makes each read-modify-write atomic without a backing-store
        /// transaction.
        cache: Arc<Mutex<StateCache>>,
    }

    impl StateBackend {
        /// Store the cells in `store`.
        pub fn with_store(store: Arc<dyn KvStore>) -> Self {
            Self {
                store,
                cache: Arc::new(Mutex::new(StateCache {
                    values: HashMap::new(),
                    dirty: HashSet::new(),
                })),
            }
        }

        /// Read the value stored for `state_key`, or `None` when absent.
        ///
        /// Read from the write-back cache when the cell is there. Otherwise read
        /// from the store and cache the value.
        pub async fn get(&self, state_key: &[u8]) -> Result<Option<Vec<u8>>> {
            let mut cache = self.cache.lock().await;
            self.get_locked(&mut cache, state_key).await
        }

        async fn get_locked(
            &self,
            cache: &mut StateCache,
            state_key: &[u8],
        ) -> Result<Option<Vec<u8>>> {
            if let Some(value) = cache.values.get(state_key) {
                return Ok(value.clone());
            }
            let value = self.read_from_store(state_key).await?;
            cache.values.insert(state_key.to_vec(), value.clone());
            cache.evict_if_needed();
            Ok(value)
        }

        /// Read the value stored for `state_key` directly from the backing store.
        async fn read_from_store(&self, state_key: &[u8]) -> Result<Option<Vec<u8>>> {
            self.store.get(state_key).await
        }

        /// Upsert `value` for `state_key` (last-write-wins).
        pub async fn put(&self, state_key: &[u8], value: &[u8]) -> Result<()> {
            let mut cache = self.cache.lock().await;
            cache
                .values
                .insert(state_key.to_vec(), Some(value.to_vec()));
            cache.dirty.insert(state_key.to_vec());
            cache.evict_if_needed();
            self.maybe_flush(&mut cache).await
        }

        /// Delete the row for `state_key` (no-op when absent).
        pub async fn delete(&self, state_key: &[u8]) -> Result<()> {
            let mut cache = self.cache.lock().await;
            cache.values.insert(state_key.to_vec(), None);
            cache.dirty.insert(state_key.to_vec());
            cache.evict_if_needed();
            self.maybe_flush(&mut cache).await
        }

        /// Atomically apply `update` to the value stored at `state_key`.
        ///
        /// Serialized by the cache lock so a concurrent read-modify-write cannot
        /// lose an update; `update` must be pure.
        pub async fn read_modify_write<F>(&self, state_key: &[u8], update: F) -> Result<()>
        where
            F: FnOnce(Vec<u8>) -> Vec<u8>,
        {
            let mut cache = self.cache.lock().await;
            let current = self
                .get_locked(&mut cache, state_key)
                .await?
                .unwrap_or_default();
            let next = update(current);
            cache.values.insert(state_key.to_vec(), Some(next));
            cache.dirty.insert(state_key.to_vec());
            cache.evict_if_needed();
            self.maybe_flush(&mut cache).await
        }

        /// Flush once the pending set grows past [`STATE_FLUSH_THRESHOLD`].
        async fn maybe_flush(&self, cache: &mut StateCache) -> Result<()> {
            if cache.dirty.len() >= STATE_FLUSH_THRESHOLD {
                self.flush_locked(cache).await?;
            }
            Ok(())
        }

        /// Commit every dirty cell in one durable batch.
        ///
        /// The backend calls this method when the pending set grows and when the
        /// state stream ends. The cells then outlive the in-memory cache.
        pub async fn flush(&self) -> Result<()> {
            let mut cache = self.cache.lock().await;
            self.flush_locked(&mut cache).await
        }

        async fn flush_locked(&self, cache: &mut StateCache) -> Result<()> {
            if cache.dirty.is_empty() {
                return Ok(());
            }

            let writes: Vec<(Vec<u8>, Option<Vec<u8>>)> = cache
                .dirty
                .iter()
                .map(|key| (key.clone(), cache.values.get(key).cloned().flatten()))
                .collect();
            self.store.apply(&writes).await?;
            cache.dirty.clear();
            Ok(())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use tempfile::tempdir;

        /// Open a SlateDB state handle rooted at `dir`.
        async fn open_kv(dir: &std::path::Path) -> Arc<dyn KvStore> {
            let kv = crate::engine::kv::SlateKvStore::open(dir, "state")
                .await
                .expect("failed to open SlateDB state store");
            Arc::new(kv)
        }

        /// A state backend plus the SlateDB handle that it writes through.
        ///
        /// A test can open a second backend over the same handle. That backend
        /// then shows what the first one committed.
        async fn make_backend() -> (tempfile::TempDir, Arc<dyn KvStore>, StateBackend) {
            let dir = tempdir().expect("failed to create tempdir");
            let kv = open_kv(dir.path()).await;
            (dir, kv.clone(), StateBackend::with_store(kv))
        }

        #[tokio::test]
        async fn put_get_delete_roundtrip() {
            let (_dir, _kv, backend) = make_backend().await;

            // Missing key reads back None.
            assert!(backend.get(b"key").await.unwrap().is_none());

            // Put then get.
            backend.put(b"key", b"value-1").await.unwrap();
            assert_eq!(
                backend.get(b"key").await.unwrap().as_deref(),
                Some(&b"value-1"[..])
            );

            // Last write wins.
            backend.put(b"key", b"value-2").await.unwrap();
            assert_eq!(
                backend.get(b"key").await.unwrap().as_deref(),
                Some(&b"value-2"[..])
            );

            // Delete removes the key.
            backend.delete(b"key").await.unwrap();
            assert!(backend.get(b"key").await.unwrap().is_none());
        }

        #[tokio::test]
        async fn put_is_isolated_between_keys() {
            let (_dir, _kv, backend) = make_backend().await;

            backend.put(b"k1", b"one").await.unwrap();
            backend.put(b"k2", b"two").await.unwrap();

            assert_eq!(
                backend.get(b"k1").await.unwrap().as_deref(),
                Some(&b"one"[..])
            );
            assert_eq!(
                backend.get(b"k2").await.unwrap().as_deref(),
                Some(&b"two"[..])
            );

            // Deleting one key leaves the other untouched.
            backend.delete(b"k1").await.unwrap();
            assert!(backend.get(b"k1").await.unwrap().is_none());
            assert_eq!(
                backend.get(b"k2").await.unwrap().as_deref(),
                Some(&b"two"[..])
            );
        }

        #[tokio::test]
        async fn keyed_get_returns_the_matching_key_among_many() {
            let (_dir, _kv, backend) = make_backend().await;

            for i in 0..32u8 {
                backend.put(&[i], &[i, i]).await.unwrap();
            }

            assert_eq!(
                backend.get(&[7]).await.unwrap().as_deref(),
                Some(&[7u8, 7u8][..])
            );
            assert!(backend.get(&[99]).await.unwrap().is_none());
        }

        #[tokio::test]
        async fn read_modify_write_does_not_lose_concurrent_updates() {
            let (_dir, _kv, backend) = make_backend().await;
            let tasks = 16;

            let mut handles = Vec::new();
            for _ in 0..tasks {
                let backend = backend.clone();
                handles.push(tokio::spawn(async move {
                    backend
                        .read_modify_write(b"counter", |mut current| {
                            current.push(b'x');
                            current
                        })
                        .await
                        .unwrap();
                }));
            }
            for handle in handles {
                handle.await.unwrap();
            }

            let value = backend.get(b"counter").await.unwrap().unwrap();
            assert_eq!(value.len(), tasks, "every concurrent append must survive");
        }

        #[tokio::test]
        async fn flush_persists_the_write_back_cache_to_a_fresh_backend() {
            let (_dir, kv, backend) = make_backend().await;
            backend.put(b"k", b"v1").await.unwrap();
            backend
                .read_modify_write(b"k", |mut current| {
                    current.extend_from_slice(b"+v2");
                    current
                })
                .await
                .unwrap();

            // Writes live in the in-memory cache until flushed, so a backend that
            // has not been flushed sees nothing.
            let fresh = StateBackend::with_store(kv.clone());
            assert!(fresh.get(b"k").await.unwrap().is_none());

            backend.flush().await.unwrap();

            // After a flush the value is durable for a backend sharing the store.
            let reloaded = StateBackend::with_store(kv.clone());
            assert_eq!(
                reloaded.get(b"k").await.unwrap().as_deref(),
                Some(&b"v1+v2"[..])
            );
        }

        #[tokio::test]
        async fn dirty_writes_commit_automatically_past_the_threshold() {
            let (_dir, kv, backend) = make_backend().await;

            for i in 0..=STATE_FLUSH_THRESHOLD as u32 {
                backend.put(&i.to_be_bytes(), b"v").await.unwrap();
            }

            // Crossing the threshold flushed without an explicit `flush` call.
            let reloaded = StateBackend::with_store(kv.clone());
            assert_eq!(
                reloaded.get(&0u32.to_be_bytes()).await.unwrap().as_deref(),
                Some(&b"v"[..])
            );
        }

        #[tokio::test]
        async fn flush_commits_deletes_as_well_as_inserts() {
            let (_dir, kv, backend) = make_backend().await;
            backend.put(b"gone", b"value").await.unwrap();
            backend.flush().await.unwrap();

            backend.delete(b"gone").await.unwrap();
            backend.flush().await.unwrap();

            let reloaded = StateBackend::with_store(kv.clone());
            assert!(reloaded.get(b"gone").await.unwrap().is_none());
        }

        #[tokio::test]
        async fn flushed_state_survives_a_backend_restart() {
            let dir = tempdir().expect("failed to create tempdir");
            let backend = StateBackend::with_store(open_kv(dir.path()).await);
            backend.put(b"k", b"v1").await.unwrap();
            backend
                .read_modify_write(b"k", |mut current| {
                    current.extend_from_slice(b"+v2");
                    current
                })
                .await
                .unwrap();
            backend.flush().await.unwrap();
            drop(backend);

            // A fresh handle over the same directory sees the committed value.
            let reopened = StateBackend::with_store(open_kv(dir.path()).await);
            assert_eq!(
                reopened.get(b"k").await.unwrap().as_deref(),
                Some(&b"v1+v2"[..])
            );
        }

        #[tokio::test]
        async fn slate_state_store_roundtrips_and_batches() {
            let dir = tempdir().expect("failed to create tempdir");
            let store = open_kv(dir.path()).await;

            assert!(store.get(b"missing").await.unwrap().is_none());

            // One batched write for several cells.
            store
                .apply(&[
                    (b"k1".to_vec(), Some(b"v1".to_vec())),
                    (b"k2".to_vec(), Some(b"v2".to_vec())),
                ])
                .await
                .unwrap();
            assert_eq!(store.get(b"k1").await.unwrap().as_deref(), Some(&b"v1"[..]));
            assert_eq!(store.get(b"k2").await.unwrap().as_deref(), Some(&b"v2"[..]));

            // A delete in the batch removes only its own key.
            store.apply(&[(b"k1".to_vec(), None)]).await.unwrap();
            assert!(store.get(b"k1").await.unwrap().is_none());
            assert_eq!(store.get(b"k2").await.unwrap().as_deref(), Some(&b"v2"[..]));
        }

        #[test]
        fn eviction_drops_clean_cells_and_keeps_dirty_ones() {
            let mut cache = StateCache {
                values: HashMap::new(),
                dirty: HashSet::new(),
            };
            for i in 0..1_000u32 {
                cache
                    .values
                    .insert(i.to_be_bytes().to_vec(), Some(vec![1, 2, 3]));
            }
            let dirty_key = 7u32.to_be_bytes().to_vec();
            cache.dirty.insert(dirty_key.clone());

            cache.evict_clean_to_fit(100);

            assert_eq!(cache.values.len(), 100, "cache is trimmed to the target");
            assert!(
                cache.values.contains_key(&dirty_key),
                "dirty cell must survive eviction"
            );
        }

        #[test]
        fn evict_if_needed_leaves_a_small_cache_untouched() {
            let mut cache = StateCache {
                values: HashMap::new(),
                dirty: HashSet::new(),
            };
            cache.values.insert(b"k".to_vec(), Some(b"v".to_vec()));

            cache.evict_if_needed();

            assert_eq!(cache.values.len(), 1);
        }
    }
}

pub mod bag {
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
        use crate::engine::{kv::SlateKvStore, state::build_state_key};
        use std::sync::Arc;
        use tempfile::tempdir;

        async fn make_bag() -> (tempfile::TempDir, BagState) {
            let dir = tempdir().expect("failed to create tempdir warehouse");
            let kv = SlateKvStore::open(dir.path(), "state")
                .await
                .expect("failed to open SlateDB state store");
            (dir, BagState::new(StateBackend::with_store(Arc::new(kv))))
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
}

/// Beam `MapState` backing protocol.
///
/// The Java SDK exposes `MapState` through three Fn State wire protocols:
/// `multimap_keys_user_state` for key enumeration, `multimap_entries_user_state`
/// for all entries, and `multimap_user_state` for values under one map key. All
/// three protocol views share one durable cell so their updates remain coherent.
pub(crate) mod multimap {
    use anyhow::{Result, anyhow};
    use bytes::Buf;
    use std::io::Cursor;

    use super::backend::StateBackend;
    use crate::coders::StandardBeamCoders;

    #[derive(Debug, Default)]
    struct Entry {
        key: Vec<u8>,
        values: Vec<Vec<u8>>,
    }

    /// Durable multimap state over the opaque [`StateBackend`].
    #[derive(Clone)]
    pub struct MultimapState {
        backend: StateBackend,
    }

    impl MultimapState {
        pub fn new(backend: StateBackend) -> Self {
            Self { backend }
        }

        pub async fn keys(&self, state_key: &[u8]) -> Result<Vec<u8>> {
            let mut out = Vec::new();
            for entry in self.read(state_key).await? {
                out.extend_from_slice(&entry.key);
            }
            Ok(out)
        }

        pub async fn values(&self, state_key: &[u8], map_key: &[u8]) -> Result<Vec<u8>> {
            let mut out = Vec::new();
            if let Some(entry) = self
                .read(state_key)
                .await?
                .into_iter()
                .find(|entry| entry.key == map_key)
            {
                for value in entry.values {
                    out.extend_from_slice(&value);
                }
            }
            Ok(out)
        }

        /// Encode entries as the concatenation of `KV<K, Iterable<V>>` values.
        ///
        /// The Java harness decodes this stream with `IterableCoder`, which
        /// writes a four-byte big-endian collection size before the values.
        pub async fn entries(&self, state_key: &[u8]) -> Result<Vec<u8>> {
            let mut out = Vec::new();
            for entry in self.read(state_key).await? {
                out.extend_from_slice(&entry.key);
                let value_count = i32::try_from(entry.values.len()).map_err(|_| {
                    anyhow!("multimap state contains too many values for one map key")
                })?;
                out.extend_from_slice(&value_count.to_be_bytes());
                for value in entry.values {
                    out.extend_from_slice(&value);
                }
            }
            Ok(out)
        }

        pub async fn append_keys(&self, state_key: &[u8], keys: &[Vec<u8>]) -> Result<()> {
            let mut entries = self.read(state_key).await?;
            for key in keys {
                if !entries.iter().any(|entry| entry.key == *key) {
                    entries.push(Entry {
                        key: key.clone(),
                        values: Vec::new(),
                    });
                }
            }
            self.write(state_key, entries).await
        }

        pub async fn append_values(
            &self,
            state_key: &[u8],
            map_key: &[u8],
            values: &[Vec<u8>],
        ) -> Result<()> {
            let mut entries = self.read(state_key).await?;
            let entry = match entries.iter().position(|entry| entry.key == map_key) {
                Some(index) => &mut entries[index],
                None => {
                    entries.push(Entry {
                        key: map_key.to_vec(),
                        values: Vec::new(),
                    });
                    entries.last_mut().expect("entry was just inserted")
                }
            };
            entry.values.extend_from_slice(values);
            self.write(state_key, entries).await
        }

        pub async fn clear_values(&self, state_key: &[u8], map_key: &[u8]) -> Result<()> {
            let mut entries = self.read(state_key).await?;
            entries.retain(|entry| entry.key != map_key);
            self.write(state_key, entries).await
        }

        pub async fn clear(&self, state_key: &[u8]) -> Result<()> {
            self.backend.delete(state_key).await
        }

        async fn read(&self, state_key: &[u8]) -> Result<Vec<Entry>> {
            match self.backend.get(state_key).await? {
                Some(data) => decode_entries(&data),
                None => Ok(Vec::new()),
            }
        }

        async fn write(&self, state_key: &[u8], entries: Vec<Entry>) -> Result<()> {
            self.backend.put(state_key, &encode_entries(&entries)).await
        }
    }

    /// Split concatenated nested-coder values without re-encoding them.
    pub(crate) fn split_encoded_values(
        coder: &StandardBeamCoders,
        data: &[u8],
    ) -> Result<Vec<Vec<u8>>> {
        let mut cursor = Cursor::new(data);
        let mut values = Vec::new();
        while (cursor.position() as usize) < data.len() {
            let start = cursor.position() as usize;
            coder
                .decode(&mut cursor)
                .map_err(|err| anyhow!("failed to decode multimap state value: {err}"))?;
            let end = cursor.position() as usize;
            if end <= start {
                return Err(anyhow!("multimap state coder consumed no bytes"));
            }
            values.push(data[start..end].to_vec());
        }
        Ok(values)
    }

    fn encode_entries(entries: &[Entry]) -> Vec<u8> {
        let capacity = entries
            .iter()
            .map(|entry| {
                8 + entry.key.len()
                    + entry
                        .values
                        .iter()
                        .map(|value| 4 + value.len())
                        .sum::<usize>()
            })
            .sum();
        let mut out = Vec::with_capacity(capacity);
        put_u32(&mut out, entries.len());
        for entry in entries {
            put_bytes(&mut out, &entry.key);
            put_u32(&mut out, entry.values.len());
            for value in &entry.values {
                put_bytes(&mut out, value);
            }
        }
        out
    }

    fn decode_entries(data: &[u8]) -> Result<Vec<Entry>> {
        let mut cursor = Cursor::new(data);
        let count = read_u32(&mut cursor)?;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let key = read_bytes(&mut cursor)?;
            let value_count = read_u32(&mut cursor)?;
            let mut values = Vec::with_capacity(value_count);
            for _ in 0..value_count {
                values.push(read_bytes(&mut cursor)?);
            }
            entries.push(Entry { key, values });
        }
        if cursor.position() as usize != data.len() {
            return Err(anyhow!("multimap state cell has trailing bytes"));
        }
        Ok(entries)
    }

    fn put_u32(out: &mut Vec<u8>, value: usize) {
        out.extend_from_slice(&(value as u32).to_be_bytes());
    }

    fn put_bytes(out: &mut Vec<u8>, value: &[u8]) {
        put_u32(out, value.len());
        out.extend_from_slice(value);
    }

    fn read_u32(cursor: &mut Cursor<&[u8]>) -> Result<usize> {
        if cursor.remaining() < 4 {
            return Err(anyhow!("multimap state cell has a truncated length"));
        }
        Ok(cursor.get_u32() as usize)
    }

    fn read_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Vec<u8>> {
        let len = read_u32(cursor)?;
        if cursor.remaining() < len {
            return Err(anyhow!("multimap state cell has a truncated value"));
        }
        let mut value = vec![0u8; len];
        cursor.copy_to_slice(&mut value);
        Ok(value)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::engine::kv::SlateKvStore;
        use std::sync::Arc;
        use tempfile::tempdir;

        async fn make_state() -> (tempfile::TempDir, MultimapState) {
            let dir = tempdir().expect("failed to create tempdir");
            let kv = SlateKvStore::open(dir.path(), "state")
                .await
                .expect("failed to open state store");
            (
                dir,
                MultimapState::new(StateBackend::with_store(Arc::new(kv))),
            )
        }

        #[tokio::test]
        async fn keys_values_entries_and_clear_share_one_cell() {
            let (_dir, state) = make_state().await;
            let cell = b"map-cell";
            state
                .append_keys(cell, &[b"\x01a".to_vec(), b"\x01b".to_vec()])
                .await
                .unwrap();
            state
                .append_values(cell, b"\x01a", &[b"\x02one".to_vec(), b"\x02two".to_vec()])
                .await
                .unwrap();

            assert_eq!(state.keys(cell).await.unwrap(), b"\x01a\x01b".to_vec());
            assert_eq!(
                state.values(cell, b"\x01a").await.unwrap(),
                b"\x02one\x02two".to_vec()
            );
            assert_eq!(
                state.entries(cell).await.unwrap(),
                b"\x01a\x00\x00\x00\x02\x02one\x02two\x01b\x00\x00\x00\x00".to_vec()
            );

            state.clear_values(cell, b"\x01a").await.unwrap();
            assert_eq!(state.keys(cell).await.unwrap(), b"\x01b".to_vec());
            state.clear(cell).await.unwrap();
            assert!(state.keys(cell).await.unwrap().is_empty());
        }
    }
}
