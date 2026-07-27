use crate::utils::errors::BatchConfigError;
use std::collections::HashMap;
use std::time::{Duration, Instant};

const MAX_DATA_POINTS: usize = 100;
const MAX_GROWTH_FACTOR: usize = 2;
const WARMUP_BATCH_COUNT: usize = 1;

/// Dynamic batch-size estimation for amortizing a fixed cost over a variable
/// number of elements. Inspired by Apache Beam BatchElements utility.
///
/// Tracks `(batch_size, elapsed)` observations and estimates batch size dynamically
/// that will drive the measured cost model
/// `time = fixed_cost + num_elements * per_element_cost`
/// toward the configured target.
pub struct BatchSizeEstimator {
    config: BatchConfig,
    data: Vec<(f64, f64)>, // (batch_size, elapsed_ms)
    replay_last_batch_size: Option<usize>,
    batch_size_num_seen: HashMap<usize, usize>,
    ignore_next_timing: bool,
}

impl BatchSizeEstimator {
    pub fn new(config: BatchConfig) -> Self {
        Self {
            config,
            data: Vec::new(),
            replay_last_batch_size: None,
            batch_size_num_seen: HashMap::new(),
            ignore_next_timing: false,
        }
    }

    /// Records a timing sample for a batch of the given size. If the very
    /// first observation at a given batch size is still in its warm-up
    /// window, the timing is discarded and that batch size is replayed on
    /// the next call to `next_batch_size` instead.
    pub fn record(&mut self, batch_size: usize, elapsed: Duration) {
        let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
        if self.ignore_next_timing {
            self.ignore_next_timing = false;
            self.replay_last_batch_size = Some(batch_size.min(self.config.max_batch_size));
        } else {
            self.data.push((batch_size as f64, elapsed_ms));
            if self.data.len() >= MAX_DATA_POINTS {
                self.thin_data();
            }
        }
    }

    /// Convenience wrapper: times `f`, records the sample, and returns `f`'s
    /// result. This is the main entry point for wiring the estimator around
    /// an arbitrary function.
    pub fn time<T>(&mut self, batch_size: usize, f: impl FnOnce() -> T) -> T {
        let start = Instant::now();
        let result = f();
        self.record(batch_size, start.elapsed());
        result
    }

    fn thin_data(&mut self) {
        // drop one point from the first quarter, then one from the first half of what remains.
        let len = self.data.len();
        if len >= 4 {
            self.data.remove(rand::random_range(0..len / 4));
        }
        let len = self.data.len();
        if len >= 2 {
            self.data.remove(rand::random_range(0..len / 2));
        }
    }

    pub fn ignore_next_timing(&mut self) {
        self.ignore_next_timing = true;
    }

    /// y = a + b*x.
    fn linear_regression(xs: &[f64], ys: &[f64]) -> (f64, f64) {
        let n = xs.len() as f64;
        let xbar = xs.iter().sum::<f64>() / n;
        let ybar = ys.iter().sum::<f64>() / n;

        if xbar == 0.0 {
            return (ybar, 0.0);
        }

        let all_same = xs.iter().all(|&x| x == xs[0]);
        if all_same {
            return (0.0, ybar / xbar);
        }

        let mut num = 0.0;
        let mut den = 0.0;
        for i in 0..xs.len() {
            num += (xs[i] - xbar) * (ys[i] - ybar);
            den += (xs[i] - xbar) * (xs[i] - xbar);
        }
        let b = num / den;
        let a = ybar - b * xbar;
        (a, b)
    }

    fn calculate_next_batch_size(&self) -> usize {
        let cfg = &self.config;

        if cfg.min_batch_size == cfg.max_batch_size {
            return cfg.min_batch_size;
        }
        if self.data.is_empty() {
            return cfg.min_batch_size;
        }
        if self.data.len() < 2 {
            return (cfg.min_batch_size * MAX_GROWTH_FACTOR)
                .min(cfg.max_batch_size)
                .max(cfg.min_batch_size + 1);
        }

        // Trim top 20% by batch size (outlier control).
        let mut sorted = self.data.clone();
        sorted.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        let trim_size = (sorted.len() * 4 / 5).max(20).min(sorted.len());
        let trimmed = &sorted[..trim_size];

        let xs: Vec<f64> = trimmed.iter().map(|p| p.0).collect();
        let ys: Vec<f64> = trimmed.iter().map(|p| p.1).collect();
        let (a_raw, b_raw) = Self::linear_regression(&xs, &ys);
        let a = a_raw.max(1e-10);
        let b = b_raw.max(1e-20);

        let last_batch_size = self.data.last().unwrap().0;
        let cap = ((last_batch_size as usize) * MAX_GROWTH_FACTOR).min(cfg.max_batch_size);

        let mut target = cfg.max_batch_size as f64;

        let target_duration_ms = cfg.target_batch_duration_secs * 1000.0;

        if let Some(with_fixed) = cfg.target_batch_duration_secs_with_fixed_cost {
            let target_with_fixed_ms = with_fixed * 1000.0;
            target = target.min((target_with_fixed_ms - a) / b);
        }

        if cfg.target_batch_duration_secs > 0.0 {
            target = target.min(target_duration_ms / b);
        }

        if cfg.target_batch_overhead > 0.0 {
            target = target.min((a / b) * (1.0 / cfg.target_batch_overhead - 1.0));
        }

        // Jitter, once there's enough history to make it safe to wobble.
        if self.data.len() > 10 {
            let jitter_frac = cfg.variance * 2.0 * (rand::random::<f64>() - 0.5);
            target += target * jitter_frac;
        }
        let jitter_floor = (self.data.len() % 2) as f64;

        (target
            .min(cap as f64)
            .max(cfg.min_batch_size as f64 + jitter_floor)) as usize
    }

    /// Returns the next batch size to use. If a batch size's timing was
    /// discarded due to warm-up, replays that same size before computing a
    /// fresh estimate.
    pub fn next_batch_size(&mut self) -> usize {
        let result = match self.replay_last_batch_size.take() {
            Some(size) => size,
            None => self.calculate_next_batch_size(),
        };

        let seen = self.batch_size_num_seen.entry(result).or_insert(0);
        *seen += 1;
        if *seen <= WARMUP_BATCH_COUNT {
            self.ignore_next_timing();
        }

        result
    }
}

/// Configuration controlling how batch sizes are selected and adapted.
#[derive(Debug, Clone)]
pub struct BatchConfig {
    pub min_batch_size: usize,
    pub max_batch_size: usize,
    pub target_batch_overhead: f64,
    pub target_batch_duration_secs: f64,
    /// None == unset
    pub target_batch_duration_secs_with_fixed_cost: Option<f64>,
    pub variance: f64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            min_batch_size: 1,
            max_batch_size: 10_000,
            target_batch_overhead: 0.05,
            target_batch_duration_secs: 10.0,
            target_batch_duration_secs_with_fixed_cost: None,
            variance: 0.25,
        }
    }
}

impl BatchConfig {
    pub fn builder() -> BatchConfigBuilder {
        BatchConfigBuilder::default()
    }
}

#[derive(Debug, Clone)]
pub struct BatchConfigBuilder {
    inner: BatchConfig,
}

impl Default for BatchConfigBuilder {
    fn default() -> Self {
        Self {
            inner: BatchConfig::default(),
        }
    }
}

impl BatchConfigBuilder {
    pub fn min_batch_size(mut self, v: usize) -> Self {
        self.inner.min_batch_size = v;
        self
    }
    pub fn max_batch_size(mut self, v: usize) -> Self {
        self.inner.max_batch_size = v;
        self
    }
    pub fn target_batch_overhead(mut self, v: f64) -> Self {
        self.inner.target_batch_overhead = v;
        self
    }
    pub fn target_batch_duration_secs(mut self, v: f64) -> Self {
        self.inner.target_batch_duration_secs = v;
        self
    }
    pub fn target_batch_duration_secs_with_fixed_cost(mut self, v: f64) -> Self {
        self.inner.target_batch_duration_secs_with_fixed_cost = Some(v);
        self
    }
    pub fn variance(mut self, v: f64) -> Self {
        self.inner.variance = v;
        self
    }

    pub fn build(self) -> Result<BatchConfig, BatchConfigError> {
        let c = &self.inner;
        if c.min_batch_size > c.max_batch_size {
            return Err(BatchConfigError::MinExceedsMax(
                c.min_batch_size,
                c.max_batch_size,
            ));
        }
        if !(c.target_batch_overhead > 0.0 && c.target_batch_overhead <= 1.0) {
            return Err(BatchConfigError::InvalidOverhead(c.target_batch_overhead));
        }
        if c.target_batch_duration_secs <= 0.0 {
            return Err(BatchConfigError::InvalidDuration(
                c.target_batch_duration_secs,
            ));
        }
        if let Some(v) = c.target_batch_duration_secs_with_fixed_cost {
            if v <= 0.0 {
                return Err(BatchConfigError::InvalidFixedCostDuration(v));
            }
        }
        Ok(self.inner)
    }
}

/// Buffers items of type `T` and flushes them to a caller-supplied closure
/// once the dynamically estimated target batch size is reached. Works with
/// any function or service call: an HTTP client, a DB writer, a model
/// inference call — anything with a fixed-cost-plus-per-element-cost shape.
pub struct AdaptiveBatcher<T> {
    estimator: BatchSizeEstimator,
    buffer: Vec<T>,
    target: usize,
}

impl<T> AdaptiveBatcher<T> {
    pub fn new(config: BatchConfig) -> Self {
        let mut estimator = BatchSizeEstimator::new(config);
        let target = estimator.next_batch_size();
        Self {
            estimator,
            buffer: Vec::new(),
            target,
        }
    }

    /// Adds an item. If this pushes the buffer to its target size, the
    /// batch is flushed through `process` and its result returned.
    pub fn push<F, R>(&mut self, item: T, process: F) -> Option<R>
    where
        F: FnOnce(&[T]) -> R,
    {
        self.buffer.push(item);
        if self.buffer.len() >= self.target {
            Some(self.flush(process).unwrap())
        } else {
            None
        }
    }

    /// Flushes whatever is currently buffered (e.g. at end-of-stream),
    /// regardless of whether the target size has been reached.
    pub fn flush<F, R>(&mut self, process: F) -> Option<R>
    where
        F: FnOnce(&[T]) -> R,
    {
        if self.buffer.is_empty() {
            return None;
        }
        let batch = std::mem::take(&mut self.buffer);
        let batch_size = batch.len();
        let result = self.estimator.time(batch_size, || process(&batch));
        self.target = self.estimator.next_batch_size();
        Some(result)
    }

    /// Current number of buffered-but-not-yet-flushed items.
    pub fn pending(&self) -> usize {
        self.buffer.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn fixed_batch_size_when_min_equals_max() {
        let config = BatchConfig::builder()
            .min_batch_size(50)
            .max_batch_size(50)
            .build()
            .unwrap();
        let mut est = BatchSizeEstimator::new(config);
        for _ in 0..5 {
            assert_eq!(est.next_batch_size(), 50);
            est.record(50, Duration::from_millis(1));
        }
    }

    #[test]
    fn grows_from_cold_start() {
        let config = BatchConfig::builder()
            .min_batch_size(1)
            .max_batch_size(1000)
            .build()
            .unwrap();
        let mut est = BatchSizeEstimator::new(config);
        let first = est.next_batch_size();
        assert_eq!(first, 1);
        est.record(first, Duration::from_micros(500));
        let second = est.next_batch_size();
        assert!(second >= first);
    }

    #[test]
    fn converges_toward_target_duration() {
        // Simulate a fixed cost of 5ms + 0.1ms/element, targeting ~50ms
        // batches, and check the estimator settles near the elements count
        // that produces that duration (~450 elements).
        let config = BatchConfig::builder()
            .min_batch_size(1)
            .max_batch_size(100_000)
            .target_batch_duration_secs(0.05)
            .target_batch_overhead(0.05) // default; duration constraint should bind, not overhead
            .variance(0.0)
            .build()
            .unwrap();
        let mut est = BatchSizeEstimator::new(config);

        let mut last_size = 0;
        for _ in 0..60 {
            let size = est.next_batch_size();
            last_size = size;
            let simulated_ms = 5.0 + size as f64 * 0.1;
            est.record(size, Duration::from_secs_f64(simulated_ms / 1000.0));
        }

        // per-element cost is 0.1ms, target duration 50ms => ~500 elements
        assert!(
            last_size > 200 && last_size < 900,
            "expected convergence near ~500, got {last_size}"
        );
    }

    #[test]
    fn adaptive_batcher_flushes_and_reports_size() {
        let config = BatchConfig::builder()
            .min_batch_size(4)
            .max_batch_size(4)
            .build()
            .unwrap();
        let mut batcher = AdaptiveBatcher::new(config);

        let mut flushed_sizes = Vec::new();
        for i in 0..10u32 {
            if let Some(size) = batcher.push(i, |batch: &[u32]| batch.len()) {
                flushed_sizes.push(size);
            }
        }
        let tail = batcher.flush(|batch: &[u32]| batch.len());

        assert_eq!(flushed_sizes, vec![4, 4]);
        assert_eq!(tail, Some(2));
    }

    #[test]
    fn wires_around_a_real_function_call() {
        // Stands in for "any service/function": e.g. a batched RPC call.
        fn downstream_call(batch: &[u32]) -> u32 {
            sleep(Duration::from_micros(200));
            batch.iter().sum()
        }

        let config = BatchConfig::builder()
            .min_batch_size(1)
            .max_batch_size(200)
            .target_batch_duration_secs(0.01)
            .build()
            .unwrap();
        let mut batcher = AdaptiveBatcher::new(config);

        let mut total = 0u32;
        for i in 0..500u32 {
            if let Some(sum) = batcher.push(i, downstream_call) {
                total += sum;
            }
        }
        if let Some(sum) = batcher.flush(downstream_call) {
            total += sum;
        }

        assert_eq!(total, (0..500u32).sum::<u32>());
    }
}
