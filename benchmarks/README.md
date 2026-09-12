# FlareDB Benchmarks

This module contains the benchmarking infrastructure for FlareDB, based on the Apache Beam [Nexmark benchmark suite](https://beam.apache.org/documentation/sdks/java/testing/nexmark/).

## Overview

The Nexmark benchmark suite models an online auction system with three primary entities:
- **Person**: Submits items for auction or bids on auctions.
- **Auction**: Items listed for auction.
- **Bid**: Offers made on active auctions.

The `:nexmark` Gradle module ports batch-based queries with default global window for performance benchmarking on the `FlareRunner`.

### Supported Batch Queries (Default Global Window)
- **Query 0 (PASSTHROUGH)**: Pass-through event processing to measure serialization and pipeline overhead.
- **Query 1 (CURRENCY_CONVERSION)**: Converts bid prices from USD to Euros.
- **Query 2 (SELECTION)**: Filters bids matching specific auction ID modulo criteria.
- **Query 3 (LOCAL_ITEM_SUGGESTION)**: Joins category 10 auctions with persons in specific US states (OR, ID, CA) by seller ID.
- **Query 7 (HIGHEST_BID)**: Calculates global highest bids per period / fanout side input.
- **Query 8 (MONITOR_NEW_USERS)**: Selects people who created auctions in the dataset.
- **Query 9 (WINNING_BIDS)**: Joins auctions and bids to find winning bids above reserve price.
- **BOUNDED_SIDE_INPUT_JOIN**: Joins bid stream with bounded side input enrichment data.

---

## Running Benchmarks

### 1. Build Module & Fat JAR
```bash
./gradlew :nexmark:shadowJar
```

### 2. Run SMOKE Benchmark Suite
Runs all supported batch queries using default configuration parameters:
```bash
./gradlew :nexmark:run -Pnexmark.args="--suite=SMOKE"
```

### 3. Run Specific Query
Run Query 0 (PASSTHROUGH):
```bash
./gradlew :nexmark:run -Pnexmark.args="--query=0 --numEvents=100000"
```

Run Query 1 (CURRENCY_CONVERSION):
```bash
./gradlew :nexmark:run -Pnexmark.args="--query=1 --numEvents=100000"
```

Run Query 3 (LOCAL_ITEM_SUGGESTION):
```bash
./gradlew :nexmark:run -Pnexmark.args="--query=3 --numEvents=100000"
```

### 4. Custom Options
You can configure target endpoint (`--jobEndpoint`) and number of generated events (`--numEvents`):
```bash
./gradlew :nexmark:run -Pnexmark.args="--query=PASSTHROUGH --numEvents=500000 --jobEndpoint=127.0.0.1:8099"
```
