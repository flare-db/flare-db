I'll read the wiki pages in parallel to gather the content.Here's the full docs converted into a `SKILL.md` (agent skill format with YAML frontmatter):

````markdown
---
name: paimon-rust
description: Guide for working with Apache Paimon Rust — the native Rust implementation of the Paimon streaming data lake platform. Use when reading/writing Paimon tables, integrating with DataFusion, configuring storage backends, using vector/full-text search, or working with the C/Python/Go bindings.
---

# Apache Paimon Rust Skill

Apache Paimon Rust is a native Rust implementation of Apache Paimon, a streaming data lake platform providing data ingestion, changelog tracking, and real-time analytics. Version 0.4.0, MSRV Rust 1.91.0. Most APIs are async (require `tokio`).

## Workspace Layout

| Component | Path | Purpose |
|-----------|------|---------|
| Core | `crates/paimon` | Paimon format spec, I/O, table APIs, search |
| DataFusion | `crates/integrations/datafusion` | `TableProvider` / `CatalogProvider` / `SQLContext` for SQL |
| REST server | `crates/paimon-rest-server` | Paimon REST Catalog server |
| Bindings | `bindings/c`, `bindings/python`, `bindings/go` | FFI for C, Python (`pypaimon-rust`), Go |
| Benchmarks | `benchmarks/tpcds` | TPC-DS-derived benchmark suite |

## Core API Patterns

### Entry point: Catalog
Create a catalog via `CatalogFactory::create(Options)`:
- FileSystem catalog (default): set `CatalogOptions::WAREHOUSE` to e.g. `s3://bucket/paimon`
- REST catalog: set `CatalogOptions::METASTORE = "rest"` and `CatalogOptions::URI`

```rust
use paimon::{CatalogFactory, CatalogOptions, Options};

let mut options = Options::new();
options.set(CatalogOptions::WAREHOUSE, "s3://my-bucket/paimon");
options.set("s3.access-key-id", "AKIA...");
options.set("s3.secret-access-key", "secret...");
options.set("s3.region", "us-east-1");
let catalog = CatalogFactory::create(options).await?;
let table = catalog.get_table(identifier).await?;
```

### Read pipeline: scan-then-read
`Table` → `ReadBuilder` (projection/filters) → `TableScan::plan()` → `Plan` of `DataSplit`s → `TableRead::to_arrow()` → `ArrowRecordBatchStream`. Predicate pushdown splits filters into partition, bucket, and data predicates.

### Write pipeline: write-then-commit
`Table` → `WriteBuilder` → `TableWrite::write(batch)` → `prepare_commit()` → `Vec<CommitMessage>` → `TableCommit::commit()` (atomic snapshot creation, conflict-aware). Bucket assignment via `FixedBucketAssigner` or `DynamicBucketAssigner`; writers: `DataFileWriter` (append-only), `KeyValueFileWriter` (primary key), `PostponeFileWriter`, `DataEvolutionWriter` (partial updates).

### Key types
- `TableSchema` (`spec/schema.rs`): fields, `partition_keys`, `primary_keys`, options; evolved via `SchemaChange`
- `DataType` enum (`spec/types.rs`): primitives + `Array`/`Map`/`Multiset`/`Row` + `Variant`, `Blob`, `Vector`
- `Snapshot` → `ManifestList` → `ManifestFile` (Avro) → `ManifestEntry` (ADD/DELETE) → `DataFileMeta` (with `BinaryTableStats` for pruning). Snapshots stored as JSON.
- `BinaryRow`: internal row serialization for partition/primary keys

## Feature Flags & Storage

| Feature | Backend |
|---------|---------|
| `storage-fs`, `storage-memory` | defaults |
| `storage-s3`, `storage-oss`, `storage-gcs`, `storage-azdls`, `storage-hdfs` | cloud/HDFS |
| `storage-all` | everything |
| `fulltext`, `vortex` | full-text search; Vortex format |

I/O is built on OpenDAL: `FileIOBuilder` → `FileIO` → `InputFile`/`OutputFile` wrapping `opendal::Operator`. Hadoop-style keys (e.g. `fs.s3a.endpoint`) are normalized by `normalize_storage_config()`. Optional local disk cache: `CatalogOptions::LOCAL_CACHE_ENABLED`, `LOCAL_CACHE_DIR`, `LOCAL_CACHE_MAX_SIZE`; fail-open on cache errors. `RESTTokenFileIO` handles token refresh for REST catalogs.

File formats: Parquet (primary), ORC, Avro (manifests), Mosaic, Vortex.

## Table Semantics

- **Merge engines** (PK tables): `Deduplicate`, `PartialUpdate`, `Aggregation`, `FirstRow` — configured in `CoreOptions`; reads do sort-merge across files per bucket
- **Deletion Vectors**: bitmaps tracking deleted rows to avoid read-time merges
- **Data Evolution**: hidden `_ROW_ID` column enables partial-column updates; `DataEvolutionReader` merges partial files at read time
- Time travel via snapshot history; `VERSION AS OF` in SQL

## DataFusion Integration (`paimon-datafusion`)

- `PaimonTableProvider` (implements `TableProvider`) → `PaimonTableScan` (`ExecutionPlan`) with predicate pushdown, split distribution across partitions, statistics reporting
- `SQLContext` wraps `SessionContext`, intercepting Paimon DDL/DML: `CREATE TABLE ... WITH (...)`, `ALTER TABLE`, `TRUNCATE`, `MERGE INTO`, `UPDATE`, `DELETE`, `VERSION AS OF`, `CALL sys.*`
- System tables: `snapshots`, `manifests`, `files`, `schemas`

## Search

- **Vector search**: `VectorSearchBuilder` → `PkVectorOrchestrator` (per-bucket ANN + global top-K merge); backends `vindex` (Rust-native) and `Lumina` (Paimon); optional exact fallback for un-indexed files
- **Full-text search**: `FullTextSearchBuilder` (requires `limit` and `text_column`; errors `ConfigInvalid` otherwise) via `paimon-ftindex-core`
- **Hybrid search**: `HybridSearchBuilder` fuses results with Reciprocal Rank Fusion
- Two-phase: recall (row IDs + scores) then materialization via `TableRead`

## Special Types

- **BLOB**: small blobs inline; large ones in dedicated `.blob` files referenced by `BlobDescriptor` (uri/offset/length); `BlobReadLimiter` for admission control
- **Variant**: semi-structured JSON with "shredding" — hot sub-fields stored as physical columns (`ShreddingFormatWriter`/`ShreddingFormatReader`)
- **Vector**: dense fixed-size vectors; dedicated files use `.vector.` filename suffix
