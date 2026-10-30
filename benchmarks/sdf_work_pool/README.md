# SDF Worker-Pool Benchmark

This benchmark compares the same fixed amount of slow work in two forms:

- a normal `ParDo` bundle, which executes serially;
- a long-running Splittable DoFn restriction, which FlareDB can split only when
  predicted completion savings exceed the measured split overhead;
- a short Splittable DoFn workload that should avoid predictive splitting.

Start FlareDB with an isolated base directory and a Python environment that has
Apache Beam plus the local `flarerunner` package, then run:

```sh
python3 benchmarks/sdf_work_pool/benchmark.py
```

The script performs two measured runs of each workload, verifies that every
block was processed exactly once, and writes the elapsed times, maximum observed
concurrency, and throughput to `build/sdf-work-pool-benchmark.json`. Comparison
of the long and short SDF runs shows whether splitting is limited to work with
enough remaining runtime to amortize scheduling overhead.

Benchmark behavior and workload size are intentionally fixed. The production
split and polling policies are automatic; only worker count and per-worker
bundle concurrency retain their existing environment overrides.
