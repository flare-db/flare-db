# FlareDB ValidatesRunner Tests

This module executes the Apache Beam `ValidatesRunner` test suite against a running FlareDB Job Service.

## Prerequisites

Start a FlareDB instance (in another terminal or background):

```bash
./flareup-dev.sh --debug
```

By default, the runner connects to `127.0.0.1:8099`.

---

## Build

Build the module and package the shadow (uber) JAR:

```bash
./gradlew :validates-runner:shadowJar
```

---

## Running Tests

### Run All ValidatesRunner Tests

```bash
./gradlew :validates-runner:validatesRunner
```
