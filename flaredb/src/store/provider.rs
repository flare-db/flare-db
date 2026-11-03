//! `FlareTableProvider`: a DataFusion `TableProvider` over a pinned incremental
//! snapshot range of a Paimon table, plus the two plans a GroupByKey needs.
//!
//! No Beam semantics live here. It knows snapshots, buckets, columns, filters
//! and sizes. Window decoding, key encoding, triggers and state stay with the
//! caller.
//!
//! The provider exposes both a hash aggregate helper and a shared-pool sort
//! helper. `GroupStrategy` is a file-size estimate, not a semantic guarantee:
//! callers must preserve the information their operator needs before selecting
//! the aggregate path. In particular, GBK trigger, window-merge and output-time
//! behavior can require per-element values and timestamps, so its current path
//! uses the provider scan and a semantics-preserving sort.

use std::{collections::BTreeMap, fmt, path::Path, sync::Arc};

use anyhow::Result as AnyResult;
use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::{DataFusionError, Result as DfResult, ScalarValue},
    dataframe::DataFrame,
    datasource::{TableProvider, TableType},
    execution::{memory_pool::FairSpillPool, runtime_env::RuntimeEnvBuilder},
    functions_aggregate::expr_fn::{array_agg, max, min},
    logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown},
    physical_expr::{
        EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr,
        expressions::Column as PhysColumn,
    },
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream,
        common::spawn_buffered,
        empty::EmptyExec,
        execution_plan::{Boundedness, EmissionType},
        sorts::sort::SortExec,
        stream::RecordBatchStreamAdapter,
    },
    prelude::{SessionConfig, SessionContext, col},
};
use futures::{StreamExt, stream};
use paimon::{
    IncrementalScanMode, Table,
    spec::{Datum, Predicate, PredicateBuilder},
    table::{ArrowRecordBatchStream, DataSplit},
};
use tokio::sync::OnceCell;

// Snapshot range

/// Snapshot range `(start, end]`. `end` is the value to store as the reader's
/// cursor once the scan has been consumed and its output committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotRange {
    pub start: i64,
    pub end: i64,
}

impl SnapshotRange {
    /// The range a reader at `cursor` should consume, or `None` when nothing is
    /// new. A cursor older than the earliest retained snapshot is clamped
    /// forward (at-least-once), same as `FlareElementStore`.
    pub async fn resolve(table: &Table, cursor: Option<i64>) -> AnyResult<Option<Self>> {
        let manager = table.snapshot_manager();
        let Some(end) = manager.get_latest_snapshot_id().await? else {
            return Ok(None);
        };
        let earliest = manager.earliest_snapshot_id().await?.unwrap_or(end);
        let floor = earliest - 1;
        let start = cursor.map_or(floor, |cursor| cursor.max(floor));
        Ok((start < end).then_some(Self { start, end }))
    }
}

// Statistics and strategy

/// Size of one bucket's data in the snapshot range, from Paimon file metadata.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BucketStats {
    pub rows: u64,
    pub file_bytes: u64,
}

impl BucketStats {
    fn add(self, other: Self) -> Self {
        Self {
            rows: self.rows + other.rows,
            file_bytes: self.file_bytes + other.file_bytes,
        }
    }
}

/// The one place that reads Paimon file metadata.
/// ADAPT: accessor and field names depend on your paimon-rust version.
fn split_stats(split: &DataSplit) -> BucketStats {
    split
        .data_files()
        .iter()
        .fold(BucketStats::default(), |acc, file| {
            acc.add(BucketStats {
                rows: file.row_count as u64,
                file_bytes: file.file_size as u64,
            })
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupStrategy {
    /// Hash aggregation through DataFusion. Fast, memory grows with live groups.
    Hash,
    /// Per-partition external sort. Bounded memory, slower.
    Sort,
}

/// Pick hash aggregation only when the heaviest partition's estimated in-memory
/// size fits comfortably in its share of the pool; otherwise sort and spill.
///
/// `expansion` is the in-memory / compressed-on-disk ratio for your data. It is
/// a guess until you measure it: start near 4.0 and tune. Headroom is a flat
/// 50% for hash tables and list builders.
pub fn choose_strategy(
    stats: &[(i32, BucketStats)],
    partitions: usize,
    pool_bytes: usize,
    expansion: f64,
) -> GroupStrategy {
    if stats.is_empty() {
        return GroupStrategy::Hash;
    }
    let per_bucket: BTreeMap<i32, u64> = stats
        .iter()
        .map(|(bucket, stats)| (*bucket, stats.file_bytes))
        .collect();
    let heaviest = group_buckets(per_bucket, partitions)
        .into_iter()
        .map(|group| group.into_iter().sum::<u64>())
        .max()
        .unwrap_or(0);
    let budget = (pool_bytes / partitions.max(1)) as f64 * 0.5;
    if (heaviest as f64) * expansion <= budget {
        GroupStrategy::Hash
    } else {
        GroupStrategy::Sort
    }
}

// Session

/// A session sized for GroupByKey: ONE shared spill-capable pool for all
/// partitions, bucket-count parallelism, and larger Arrow batches.
pub fn gbk_session(
    memory_bytes: usize,
    spill_dir: &Path,
    spill_limit_bytes: u64,
    partitions: usize,
    batch_size: usize,
    sort_spill_reservation_bytes: usize,
) -> DfResult<SessionContext> {
    let runtime = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(FairSpillPool::new(memory_bytes)))
            .with_temp_file_path(spill_dir)
            .with_max_temp_directory_size(spill_limit_bytes)
            .build()?,
    );
    let config = SessionConfig::new()
        .with_target_partitions(partitions.max(1))
        .with_batch_size(batch_size)
        .with_sort_spill_reservation_bytes(sort_spill_reservation_bytes);
    Ok(SessionContext::new_with_config_rt(config, runtime))
}

// Provider

/// DataFusion view of one incremental range of one Paimon table.
pub struct FlareTableProvider {
    table: Arc<Table>,
    schema: SchemaRef,
    range: SnapshotRange,
    /// Column Paimon buckets by. When projected, the scan declares hash
    /// partitioning on it.
    bucket_key: Option<String>,
    /// Upper bound on DataFusion partitions. Parallelism times per-partition
    /// memory is the trade you are choosing.
    max_partitions: usize,
    stats: OnceCell<Arc<Vec<(i32, BucketStats)>>>,
}

impl FlareTableProvider {
    /// `schema` is the Arrow schema of the Paimon table. List child names are
    /// normalized to `element`, which is what Paimon's reader emits, because
    /// DataFusion compares full list field identity.
    pub fn new(
        table: Table,
        schema: &Schema,
        range: SnapshotRange,
        bucket_key: Option<String>,
        max_partitions: usize,
    ) -> Self {
        Self {
            table: Arc::new(table),
            schema: normalize_list_child_names(schema),
            range,
            bucket_key,
            max_partitions: max_partitions.max(1),
            stats: OnceCell::new(),
        }
    }

    pub fn range(&self) -> SnapshotRange {
        self.range
    }

    pub fn max_partitions(&self) -> usize {
        self.max_partitions
    }

    /// Per-bucket sizes for this range, planned once and cached. Plans with a
    /// single projected column, so it reads manifests only, not data.
    pub async fn bucket_stats(&self) -> AnyResult<Arc<Vec<(i32, BucketStats)>>> {
        self.stats
            .get_or_try_init(|| async {
                let first = self.schema.field(0).name().as_str();
                let by_bucket = self.plan(&[first], None).await?;
                let stats = by_bucket
                    .iter()
                    .map(|(bucket, splits)| {
                        let total = splits
                            .iter()
                            .map(split_stats)
                            .fold(BucketStats::default(), BucketStats::add);
                        (*bucket, total)
                    })
                    .collect();
                Ok(Arc::new(stats))
            })
            .await
            .cloned()
    }

    /// Hash vs sort for this range under a pool of `pool_bytes`.
    pub async fn strategy(&self, pool_bytes: usize, expansion: f64) -> AnyResult<GroupStrategy> {
        let stats = self.bucket_stats().await?;
        Ok(choose_strategy(
            &stats,
            self.max_partitions,
            pool_bytes,
            expansion,
        ))
    }

    /// Plan the incremental scan once and group the data splits by bucket.
    async fn plan(
        &self,
        projection: &[&str],
        filter: Option<Predicate>,
    ) -> AnyResult<BTreeMap<i32, Arc<Vec<DataSplit>>>> {
        let mut read_builder = self.table.new_read_builder();
        read_builder.with_projection(projection)?;
        if let Some(filter) = filter {
            read_builder.with_filter(filter);
        }
        let plan = read_builder
            .new_incremental_scan(IncrementalScanMode::Auto, self.range.start, self.range.end)
            .plan()
            .await?;

        // ADAPT: requires `DataSplit: Clone`.
        let mut by_bucket: BTreeMap<i32, Vec<DataSplit>> = BTreeMap::new();
        for split in plan.data_splits() {
            by_bucket
                .entry(split.bucket())
                .or_default()
                .push(split.clone());
        }
        let by_bucket = by_bucket
            .into_iter()
            .map(|(bucket, splits)| (bucket, Arc::new(splits)))
            .collect();
        Ok(by_bucket)
    }
}

impl fmt::Debug for FlareTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlareTableProvider")
            .field("range", &self.range)
            .field("bucket_key", &self.bucket_key)
            .field("max_partitions", &self.max_partitions)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for FlareTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        let builder = PredicateBuilder::new(self.table.schema().fields());
        Ok(filters
            .iter()
            .map(|filter| {
                if to_paimon_predicate(&builder, filter).is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let projected: SchemaRef = match projection {
            Some(indices) => Arc::new(self.schema.project(indices)?),
            None => Arc::clone(&self.schema),
        };
        if projected.fields().is_empty() {
            return Err(DataFusionError::NotImplemented(
                "FlareTableProvider does not support an empty projection".to_string(),
            ));
        }
        let names: Vec<&str> = projected
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();

        let builder = PredicateBuilder::new(self.table.schema().fields());
        let mut pushed: Vec<Predicate> = filters
            .iter()
            .filter_map(|filter| to_paimon_predicate(&builder, filter))
            .collect();
        let filter = match pushed.len() {
            0 => None,
            1 => pushed.pop(),
            // ADAPT: assumes `Predicate::and(Vec<Predicate>)`, mirroring `Predicate::or`.
            _ => Some(Predicate::and(pushed)),
        };

        let by_bucket = self
            .plan(&names, filter)
            .await
            .map_err(|error| DataFusionError::External(error.into()))?;
        if by_bucket.is_empty() {
            return Ok(Arc::new(EmptyExec::new(projected)));
        }

        let partitions = group_buckets(by_bucket, self.max_partitions);
        let partitioning = match self
            .bucket_key
            .as_deref()
            .and_then(|key| projected.index_of(key).ok().map(|index| (key, index)))
        {
            // Rows with equal bucket-key values are co-located. This is NOT
            // DataFusion's hash function, so only rely on it for single-input
            // operators (sort, aggregate), never to co-partition with another input.
            Some((key, index)) => Partitioning::Hash(
                vec![Arc::new(PhysColumn::new(key, index)) as Arc<dyn PhysicalExpr>],
                partitions.len(),
            ),
            None => Partitioning::UnknownPartitioning(partitions.len()),
        };

        let projection_names: Arc<Vec<String>> =
            Arc::new(names.iter().map(|name| name.to_string()).collect());
        Ok(Arc::new(FlareScanExec::new(
            projected,
            Arc::clone(&self.table),
            projection_names,
            partitions,
            partitioning,
            self.range,
        )))
    }
}

/// Round-robin whole buckets into at most `max_partitions` groups. A bucket is
/// never split, so key co-location holds at any partition count.
fn group_buckets<T>(by_bucket: BTreeMap<i32, T>, max_partitions: usize) -> Vec<Vec<T>> {
    let count = max_partitions.max(1).min(by_bucket.len());
    let mut groups: Vec<Vec<T>> = (0..count).map(|_| Vec::new()).collect();
    for (index, (_, item)) in by_bucket.into_iter().enumerate() {
        groups[index % count].push(item);
    }
    groups
}

// Physical scan

/// The physical scan: one partition per group of buckets.
pub struct FlareScanExec {
    schema: SchemaRef,
    /// `TableRead<'a>` borrows from its `ReadBuilder`, so it cannot live in a
    /// `'static` plan node. Keep the owned table and the projection, and build
    /// the reader where it is used (`open_bucket`).
    table: Arc<Table>,
    projection: Arc<Vec<String>>,
    /// partition -> buckets in that partition -> that bucket's data splits.
    partitions: Vec<Vec<Arc<Vec<DataSplit>>>>,
    range: SnapshotRange,
    props: Arc<PlanProperties>,
}

impl FlareScanExec {
    fn new(
        schema: SchemaRef,
        table: Arc<Table>,
        projection: Arc<Vec<String>>,
        partitions: Vec<Vec<Arc<Vec<DataSplit>>>>,
        partitioning: Partitioning,
        range: SnapshotRange,
    ) -> Self {
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            schema,
            table,
            projection,
            partitions,
            range,
            props,
        }
    }
}

impl fmt::Debug for FlareScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlareScanExec")
            .field("range", &self.range)
            .field("partitions", &self.partitions.len())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for FlareScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FlareScanExec: snapshots=({}, {}], partitions={}",
            self.range.start,
            self.range.end,
            self.partitions.len()
        )
    }
}

impl ExecutionPlan for FlareScanExec {
    fn name(&self) -> &str {
        "FlareScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    /// Streams the partition's buckets one after another, so only one bucket's
    /// files are open at a time. Re-executable: nothing is consumed from `self`.
    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let buckets = self.partitions.get(partition).cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "FlareScanExec has {} partitions, asked for {partition}",
                self.partitions.len()
            ))
        })?;
        let table = Arc::clone(&self.table);
        let projection = Arc::clone(&self.projection);
        let batches = stream::iter(buckets).flat_map(move |splits| {
            match open_bucket(&table, &projection, &splits) {
                Ok(batches) => batches
                    .map(|batch| batch.map_err(|e| DataFusionError::External(Box::new(e))))
                    .boxed(),
                Err(error) => {
                    stream::once(async move { Err(DataFusionError::External(error.into())) })
                        .boxed()
                }
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            batches,
        )))
    }
}

/// Open one bucket's splits as a lazy Arrow stream.
///
/// The `ReadBuilder` and `TableRead` are locals that are dropped on return, so
/// this relies on the returned stream not borrowing them. That matches how
/// `FlareElementStore::open_incremental_arrow_by_bucket` already returns streams
/// after dropping its reader.
///
/// The Paimon filter is not re-applied here. It already pruned the planned
/// splits, and the pushdown is `Inexact`, so DataFusion re-checks every row.
fn open_bucket(
    table: &Table,
    projection: &[String],
    splits: &[DataSplit],
) -> AnyResult<ArrowRecordBatchStream> {
    let names: Vec<&str> = projection.iter().map(String::as_str).collect();
    let mut read_builder = table.new_read_builder();
    read_builder.with_projection(&names)?;
    let read = read_builder.new_read()?;
    Ok(read.to_arrow(splits)?)
}

// GroupByKey plans

/// Output column names for the timestamp aggregates added by `grouped_values`.
pub const MIN_TIMESTAMP_COLUMN: &str = "flare_gbk_min_timestamp";
pub const MAX_TIMESTAMP_COLUMN: &str = "flare_gbk_max_timestamp";

/// What to group. Names are columns of the provider's schema.
pub struct GroupSpec<'a> {
    /// Group-by columns, e.g. the encoded key and the window key.
    pub group_columns: &'a [&'a str],
    /// Column collected into a list per group.
    pub value_column: &'a str,
    /// When set, adds min and max of this column per group (for output
    /// timestamps: earliest or latest in pane).
    pub timestamp_column: Option<&'a str>,
}

/// Hash-aggregation plan: `scan -> aggregate(array_agg, min, max)`.
///
/// Output columns: the group columns, the value column as a list, and
/// optionally `MIN/MAX_TIMESTAMP_COLUMN`. Execute with `execute_stream()` or
/// `execute_stream_partitioned()`.
///
/// `array_agg` keeps NULLs by default, which is what you want: Beam `Void`
/// values are stored as null booleans and must stay in the iterable.
///
/// Check the physical plan with `contains_repartition` before relying on this.
/// If DataFusion inserts a hash repartition, it did not accept the scan's
/// partitioning as sufficient and you are paying a shuffle.
pub fn grouped_values(
    ctx: &SessionContext,
    provider: Arc<FlareTableProvider>,
    spec: &GroupSpec<'_>,
) -> DfResult<DataFrame> {
    let group_exprs: Vec<Expr> = spec.group_columns.iter().map(|name| col(*name)).collect();
    let mut aggregates = vec![array_agg(col(spec.value_column)).alias(spec.value_column)];
    if let Some(timestamp) = spec.timestamp_column {
        aggregates.push(min(col(timestamp)).alias(MIN_TIMESTAMP_COLUMN));
        aggregates.push(max(col(timestamp)).alias(MAX_TIMESTAMP_COLUMN));
    }
    ctx.read_table(provider)?.aggregate(group_exprs, aggregates)
}

/// True when the plan contains a `RepartitionExec` (a shuffle).
pub fn contains_repartition(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.name() == "RepartitionExec"
        || plan
            .children()
            .iter()
            .any(|child| contains_repartition(child))
}

/// Scan `provider` and sort every partition independently, in parallel, under
/// the session's single memory pool and spill configuration.
///
/// Returns one sorted stream per partition. Because the scan is partitioned by
/// bucket, equal bucket-key values are always inside one returned stream, so a
/// caller can form groups from each stream alone with no global merge.
///
/// Each stream is wrapped in `spawn_buffered`, so partitions sort concurrently
/// even if the caller consumes them one at a time. Size the pool for that:
/// `FairSpillPool` gives each live sort roughly `pool / partitions`.
pub async fn sorted_partition_streams(
    ctx: &SessionContext,
    provider: &FlareTableProvider,
    projection: Option<&Vec<usize>>,
    sort_columns: &[&str],
    buffered_batches: usize,
) -> DfResult<Vec<SendableRecordBatchStream>> {
    let state = ctx.state();
    let scan = provider.scan(&state, projection, &[], None).await?;
    let schema = scan.schema();

    let sort_exprs = sort_columns
        .iter()
        .map(|name| {
            let index = schema.index_of(name)?;
            Ok(PhysicalSortExpr::new(
                Arc::new(PhysColumn::new(name, index)),
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            ))
        })
        .collect::<DfResult<Vec<_>>>()?;
    // ADAPT: DataFusion >= 50 returns Option (non-empty ordering); <= 49 returns Self.
    let ordering = LexOrdering::new(sort_exprs)
        .ok_or_else(|| DataFusionError::Plan("at least one sort column is required".into()))?;

    // preserve_partitioning keeps one sort per scan partition and adds no merge.
    let sort = SortExec::new(ordering, scan).with_preserve_partitioning(true);
    let partitions = sort.properties().output_partitioning().partition_count();
    let task_ctx = ctx.task_ctx();
    (0..partitions)
        .map(|partition| {
            let stream = sort.execute(partition, Arc::clone(&task_ctx))?;
            Ok(spawn_buffered(stream, buffered_batches))
        })
        .collect()
}

// Filter translation and schema helpers

/// Translate the few filter shapes whose Paimon predicates are known to exist:
/// `col > i64`, `col IS NULL`, and `OR` of those. Anything else stays in
/// DataFusion. Extend this as the Paimon builder allows.
fn to_paimon_predicate(builder: &PredicateBuilder, expr: &Expr) -> Option<Predicate> {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Gt,
            right,
        }) => {
            let (Expr::Column(column), Expr::Literal(ScalarValue::Int64(Some(value)), ..)) =
                (left.as_ref(), right.as_ref())
            else {
                return None;
            };
            builder.greater_than(&column.name, Datum::Long(*value)).ok()
        }
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Or,
            right,
        }) => Some(Predicate::or(vec![
            to_paimon_predicate(builder, left)?,
            to_paimon_predicate(builder, right)?,
        ])),
        Expr::IsNull(inner) => {
            let Expr::Column(column) = inner.as_ref() else {
                return None;
            };
            builder.is_null(&column.name).ok()
        }
        _ => None,
    }
}

/// Paimon's reader names list children `element`; the writer-side Arrow schema
/// names them `item`. DataFusion compares the whole list field, so rename.
fn normalize_list_child_names(schema: &Schema) -> SchemaRef {
    let fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .map(|field| {
            Arc::new(Field::new(
                field.name(),
                normalize_type(field.data_type()),
                field.is_nullable(),
            ))
        })
        .collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

fn normalize_type(data_type: &DataType) -> DataType {
    let child = |child: &Arc<Field>| {
        Arc::new(Field::new(
            "element",
            normalize_type(child.data_type()),
            child.is_nullable(),
        ))
    };
    match data_type {
        DataType::List(c) => DataType::List(child(c)),
        DataType::LargeList(c) => DataType::LargeList(child(c)),
        DataType::FixedSizeList(c, size) => DataType::FixedSizeList(child(c), *size),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(buckets: &[(i32, u64)]) -> Vec<(i32, BucketStats)> {
        buckets
            .iter()
            .map(|(bucket, bytes)| {
                (
                    *bucket,
                    BucketStats {
                        rows: bytes / 100,
                        file_bytes: *bytes,
                    },
                )
            })
            .collect()
    }

    #[test]
    fn buckets_are_never_split_and_partitions_are_capped() {
        let by_bucket: BTreeMap<i32, &str> = (0..32).map(|b| (b, "splits")).collect();
        let groups = group_buckets(by_bucket, 8);
        assert_eq!(groups.len(), 8);
        assert!(groups.iter().all(|group| group.len() == 4));
    }

    #[test]
    fn fewer_buckets_than_partitions_gives_one_partition_per_bucket() {
        let by_bucket: BTreeMap<i32, &str> = (0..3).map(|b| (b, "splits")).collect();
        assert_eq!(group_buckets(by_bucket, 16).len(), 3);
    }

    #[test]
    fn small_input_picks_hash() {
        // 32 buckets x 10 MB, 8 partitions, 4x expansion = 160 MB per partition
        // against a 4 GB pool / 8 = 512 MB share, halved to 256 MB.
        let s = stats(&(0..32).map(|b| (b, 10 << 20)).collect::<Vec<_>>());
        assert_eq!(choose_strategy(&s, 8, 4 << 30, 4.0), GroupStrategy::Hash);
    }

    #[test]
    fn large_input_picks_sort() {
        let s = stats(&(0..32).map(|b| (b, 200 << 20)).collect::<Vec<_>>());
        assert_eq!(choose_strategy(&s, 8, 4 << 30, 4.0), GroupStrategy::Sort);
    }

    #[test]
    fn one_hot_bucket_forces_sort_even_when_the_total_is_small() {
        let mut buckets: Vec<(i32, u64)> = (0..31).map(|b| (b, 1 << 20)).collect();
        buckets.push((31, 900 << 20));
        assert_eq!(
            choose_strategy(&stats(&buckets), 8, 4 << 30, 4.0),
            GroupStrategy::Sort
        );
    }

    #[test]
    fn list_child_names_are_normalized_recursively() {
        let schema = Schema::new(vec![Field::new(
            "v",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
                true,
            ))),
            true,
        )]);
        let normalized = normalize_list_child_names(&schema);
        let DataType::List(outer) = normalized.field(0).data_type() else {
            panic!("expected list");
        };
        assert_eq!(outer.name(), "element");
        let DataType::List(inner) = outer.data_type() else {
            panic!("expected nested list");
        };
        assert_eq!(inner.name(), "element");
    }
}
