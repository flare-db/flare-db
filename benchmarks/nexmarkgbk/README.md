# Nexmark GBK Benchmark

Benchmarks Apache Beam [`GroupByKey`](https://beam.apache.org/documentation/programming-guide/#groupbykey) performance on the [Flare runner](https://github.com/flare-db/flaredb) using the Nexmark event stream.

## Overview

- Generates Nexmark bid events deterministically using the Beam `NexmarkGenerator`
- Groups bids by auction ID (`GroupByKey<Long, Long>`) and validates every group against pre-computed expected results (count, sum, min, max, xor hash, sum hash)
- Validates completeness: no missing, duplicate, or miscounted groups
- Reports end-to-end throughput, group-size distribution, and per-bundle GBK timing

```

## How to Run

1. build project (from the repository root)
```sh
./gradlew :nexmarkgbk:shadowJar
```

2. Set your jar path in pipeline options. 
```java
// Eg:
options.setUberJar(
        "/home/ganesh/flare-db/flareio/flare-db/benchmarks/nexmarkgbk/build/libs/nexmarkgbk-0.1.0-all.jar");
```

3. Create flaredb release build
```sh
cd flaredb
cargo build --release
```

4. Spin up flaredb instance
```sh
./flareup-dev.sh --release
```

5. Run the pipeline (from the repository root)
```sh
./gradlew :nexmarkgbk:run
```

Benchmark results will be generated at `build/nexmark-gbk-benchmark.txt` in the `nexmarkgbk` module.

### Options

| Option | Default | Description |
|---|---|---|
| `numEvents` | `1000000` | Number of Nexmark events to generate |
| `benchmarkOutputPath` | `build/nexmark-gbk-benchmark.txt` | Path for the benchmark summary table |

## Output

Writes a summary table to `benchmarkOutputPath` containing:

- Total events / bid events
- Runtime (ms) and throughput (events/s)
- Auction group count and group-size statistics (min, max, avg)
- Per-auction breakdown (bid count, avg/min/max price, total bid value)
- GBK timing (logged per bundle during execution)

Validation failures halt the pipeline with an `IllegalStateException` detailing the mismatch.

## Pipeline Structure

```
Impulse → GenerateEvents → Filter(bids) → MapToKV
  → GroupByKey → ValidatePerAuction (checks each group, records timing)
  → CollectIDs → GroupByKey → ValidateCompleteness (checks all groups accounted for)
```

## Dependencies

- Rust 1.97.0
- Cargo
- Java 17

The uber-jar produced by `./gradlew :nexmarkgbk:shadowJar` includes all dependencies (Gradle Shadow plugin, equivalent to the previous Maven Shade setup).
