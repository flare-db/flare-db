//! Shared opaque key/value storage for runner subsystems that need fast point
//! access: Beam user state and user timers.
//!
//! [`SlateKvStore`] stores each cell in an embedded SlateDB LSM on
//! `object_store`. SlateDB is a log-structured merge tree. A `put` lands in an
//! in-memory WAL and memtable. A background flush writes the memtable to an SST
//! in object storage. Background compaction keeps the read cost bounded and the
//! memory flat.
//!
//! A job's store lives under `<warehouse>/.slate_state/<db>/`, one database per
//! subsystem (`state` and `timers`).
//!
//! The element store keeps `__flare_cursor` in a separate Paimon table. A cursor
//! must share the durability of the PCollection it points into.

use std::sync::Arc;

use anyhow::Result;

/// Opaque key/value store for fast point access.
#[async_trait::async_trait]
pub trait KvStore: Send + Sync {
    /// Read the value for `key`, or `None` when absent.
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Apply a batch of last-write-wins upserts (`Some`) and deletes (`None`) as
    /// a single durable unit.
    async fn apply(&self, writes: &[(Vec<u8>, Option<Vec<u8>>)]) -> Result<()>;

    /// All live `(key, value)` pairs, in key order.
    async fn scan(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
}

/// SlateDB (embedded LSM on object storage) backing for a key/value namespace.
pub struct SlateKvStore {
    db: slatedb::Db,
}

impl SlateKvStore {
    /// Open a SlateDB database rooted at `dir`, creating it if needed. `name`
    /// namespaces this store within `dir` (e.g. `"state"` or `"timers"`).
    pub async fn open(dir: &std::path::Path, name: &str) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::local::LocalFileSystem::new_with_prefix(dir)?);
        let db = slatedb::Db::open(name, object_store).await?;
        Ok(Self { db })
    }
}

#[async_trait::async_trait]
impl KvStore for SlateKvStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get(key).await?.map(|value| value.to_vec()))
    }

    async fn apply(&self, writes: &[(Vec<u8>, Option<Vec<u8>>)]) -> Result<()> {
        for (key, value) in writes {
            match value {
                Some(value) => {
                    self.db.put(key, value.clone()).await?;
                }
                None => {
                    self.db.delete(key).await?;
                }
            }
        }
        self.db.flush().await?;
        Ok(())
    }

    async fn scan(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        let mut iter = self.db.scan(..).await?;
        while let Some(item) = iter.next().await? {
            out.push((item.key.to_vec(), item.value.to_vec()));
        }
        Ok(out)
    }
}
