# Contributing to FlareDB

Thanks for contributing to FlareDB. This guide covers the repo layout and the commands you need to build, test, and run your changes.

## Repository structure

```
flare-db/
├── Cargo.toml            # Rust workspace: flaredb + flare-cli
├── settings.gradle       # Gradle build: all Java modules (single entry point)
├── gradle/               # Version catalog (deps/plugins), wrapper
├── flaredb/              # Rust engine
├── flare-cli/            # Rust CLI
├── beam-model-rs/        # Rust Beam model (generated from beam/model protos)
├── beam/model/           # Vendored Apache Beam protos (codegen input, not built)
├── runner-sdk/java/      # :flaredb-runner — Apache Beam runner for FlareDB
├── flare-io/java/        # :flareio-java — FlareDB I/O for Beam (Arrow/Flight SQL)
├── example/wordcount/    # :wordcount — WordCount pipeline example
├── example/flare-io-write/  # :flareio-write — FlareDB I/O write example pipeline
└── benchmarks/nexmarkgbk # :nexmarkgbk — Nexmark benchmarks
```

Java modules are declared in `settings.gradle` and `gradle/libs.versions.toml` pins all dependency versions in one place.

## Build

```sh
# Java: compile all modules
./gradlew build -x test

# Java: compile a single module (one of: flaredb-runner, flareio-java, wordcount, flareio-write, nexmarkgbk)
./gradlew :wordcount:compileJava
./gradlew :flaredb-runner:jar

# Rust: engine + CLI
cargo build -p flaredb -p flare-cli

# Rust: single crate
cargo build -p flaredb
```

## Test

```sh
# Java: run all tests
./gradlew test

# Java: single module
./gradlew :flareio-java:test
./gradlew :flaredb-runner:test

# Rust
cargo test -p flaredb

# Rust: tests of a single module
cargo test -p flaredb engine::
```

## Run

1. Build your changes:

   ```sh
   cargo build -p flaredb
   ```

2. Start a local FlareDB dev instance.

   ```sh
   ./flareup-dev.sh --debug
   ```

3. Submit a pipeline from another terminal:

   ```sh
   ./gradlew :wordcount:run
   ```

## Useful Gradle commands

```sh
./gradlew projects                 # list all modules
./gradlew :<module>:tasks          # tasks available in a module
./gradlew :wordcount:shadowJar     # build self-contained jar
./gradlew :flaredb-runner:spotlessApply   # format Java sources
```
