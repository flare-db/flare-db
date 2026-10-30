# SDF Worker-Pool Benchmark

This benchmark compares the same fixed amount of slow work in two forms:

- a normal `ParDo` bundle, which executes serially;
- a Splittable DoFn restriction, which FlareDB's fixed worker-pool policy can
  split and execute across available workers.

Start FlareDB with an isolated base directory and a Python environment that has
Apache Beam plus the local `flarerunner` package, then run:

```sh
python3 benchmarks/sdf_work_pool/benchmark.py
```

The script performs two measured runs of each workload, verifies that every
block was processed exactly once, and writes the elapsed times, maximum observed
concurrency, and throughput to `build/sdf-work-pool-benchmark.json`.

Benchmark behavior and workload size are intentionally fixed. The production
worker count, per-worker bundle limit, queue limits, split depth, and timing
policy are not configurable.
