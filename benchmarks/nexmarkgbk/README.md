# Nexmark GBK Benchmark

Benchmarks Apache Beam [`GroupByKey`](https://beam.apache.org/documentation/programming-guide/#groupbykey) performance on the [Flare runner](https://github.com/flare-db/flaredb) using the Nexmark event stream.

## Overview

- Generates Nexmark bid events deterministically using the Beam `NexmarkGenerator`
- Groups bids by auction ID (`GroupByKey<Long, Long>`) and validates every group against pre-computed expected results (count, sum, min, max, xor hash, sum hash)
- Validates completeness: no missing, duplicate, or miscounted groups
- Reports end-to-end throughput, group-size distribution, and per-bundle GBK timing

```

## How to Run

1. build project
```sh
mvn clean install
```

2. Set your jar path in pipeline options. 
```java
// Eg:
options.setUberJar(
        "/home/ganesh/flaredb-bench/flare-db/benchmarks/nexmarkgbk/target/nexmarkgbk-1.0-SNAPSHOT.jar");
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

5. Run the pipeline
```sh
cd benchmarks/nexmarkgbk
mvn exec:java -Dexec.mainClass="com.flaredb.bench.NexmarkGBK"
```

Benchmark results will be generated at target/nexmark-gbk-benchmark.txt

### Options

| Option | Default | Description |
|---|---|---|
| `numEvents` | `1000000` | Number of Nexmark events to generate |
| `benchmarkOutputPath` | `target/nexmark-gbk-benchmark.txt` | Path for the benchmark summary table |

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

The uber-jar produced by `mvn package` includes all dependencies via the Maven Shade Plugin.
