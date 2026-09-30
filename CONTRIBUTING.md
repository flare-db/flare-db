# Contributing to FlareDB

Thanks for contributing! This guide explains where to start, how the repository is organized, how to validate changes, and how to run FlareDB locally.

## Repository context

The repository has supplementary resources for contributors and coding agents to get started quicker.

- [`AGENTS.md`](AGENTS.md) — a quick map of the codebase, project rules, workflows, and search patterns.
- [`flaredb-architecture`](.agent/skills/flaredb-architecture/SKILL.md) — FlareDB’s job submission, graph/fusion, execution, harness, storage, SQL, and protocol architecture.
- [`beam-concepts`](.agent/skills/beam-concepts/SKILL.md) — Apache Beam and portable-runner vocabulary.


## Repository map

- `flaredb/` — Rust engine and gRPC services.
- `flare-cli/` — `flare` CLI for installing, starting, stopping, inspecting, and querying a FlareDB instance.
- `flare-sql/` — interactive SQL shell used by the CLI.
- `runner-sdk/java/flarerunner/` — Java Beam runner and Job/Artifact API client.
- `runner-sdk/python/flarerunner/` — Python Beam portable runner.
- `flare-io/java/flareio-java/` — Java Beam I/O connector using Arrow Flight SQL.
- `beam/model/` — vendored Apache Beam protocol sources.
- `beam-model-rs/` — Rust Beam model code-generation project and checked-in generated bindings; it is not an active workspace member.
- `example/` — Java and Python submission examples, including WordCount, Flatten, and CoGroupByKey.
- `benchmarks/nexmark/` — Nexmark benchmark module.

Rust workspace members are declared in the root `Cargo.toml`. Java modules are declared in `settings.gradle`; dependency versions are centralized in `gradle/libs.versions.toml`.

## Choose the right way to run FlareDB

There are two different local workflows:

### Normal use: the `flare` CLI

Use the CLI when you want to use FlareDB as an installed application or run pipelines without changing the Rust engine:

```sh
cargo run -p flare-cli -- init
cargo run -p flare-cli -- up
cargo run -p flare-cli -- sql
cargo run -p flare-cli -- logs
cargo run -p flare-cli -- down
```

After installing the CLI, the equivalent commands are `flare init`, `flare up`, `flare sql`, `flare logs`, and `flare down`. The CLI manages the instance under `~/.flaredb` and uses the configured/downloaded FlareDB binary and Beam worker.

### FlareDB engine development: `flareup-dev.sh`

Use `flareup-dev.sh` when you are changing the `flaredb` crate and need to run an instance from your local build. It prefers `target/debug/flaredb` (or `target/release/flaredb`) and captures server and worker logs for that development instance.

```sh
cargo build -p flaredb
./flareup-dev.sh --debug
```

Then, from another terminal, submit a pipeline:

```sh
./gradlew :wordcount:run
python3 example/python/wordcount.py
```

`flareup-dev.sh` is a developer launcher, not the normal user workflow. It requires Java and `wget`, may download the Beam worker JAR, and runs until interrupted. Stop it with `Ctrl-C`. Use the CLI for all other local-instance workflows, including CLI or SQL development that does not require the freshly built `flaredb` binary.

## Build systems

FlareDB uses separate build systems for its Rust engine, Java SDK/I/O modules, and Python runner.

### Cargo: Rust workspace

[Cargo](https://doc.rust-lang.org/cargo/) is Rust’s package manager and build tool. A Cargo **workspace** groups related packages so they can share a lockfile and be built or tested together. This repository’s active workspace is declared in the root `Cargo.toml` and contains:

- `flaredb` — the Rust engine and server binary/library.
- `flare-cli` — the `flare` command-line application.
- `flare-sql` — the SQL shell library used by the CLI.

A package can contain libraries and binaries. Use `-p <package>` to target one package; without it, Cargo operates on the active workspace. Rust module or test filters are passed after the package arguments.

```sh
cargo build                         # build the active workspace
cargo build -p flaredb               # build only the Rust engine
cargo build -p flare-cli             # build the CLI
cargo test -p flaredb engine::       # run matching flaredb tests
cargo test -p flaredb <test_name>    # run one focused test
cargo fmt --check                    # check Rust formatting
cargo clippy --workspace --all-targets
```

`beam-model-rs/` is a separate Cargo package used for Beam model code generation, but it is currently commented out of the root workspace.

### Gradle: Java multi-project build

[Gradle](https://docs.gradle.org/) is the Java build and task system used for the runner SDK, I/O connector, examples, and benchmark. The root `settings.gradle` declares the repository’s **subprojects**; each project is addressed as `:<project-name>`. The Gradle wrapper (`./gradlew`) selects the repository’s expected Gradle version, so prefer it over a globally installed `gradle` command.

Gradle tasks are named actions such as `build`, `test`, `jar`, `shadowJar`, and `spotlessCheck`. Run a task for every project with `./gradlew <task>`, or target one project with `./gradlew :<project>:<task>`.

Important projects include `:flaredb-runner`, `:flareio-java`, `:wordcount`, `:flareio-write`, `:flareio-read`, `:flatten`, `:co-groupby-key`, and `:nexmark`. Java uses JDK 17, and dependency versions are centralized in `gradle/libs.versions.toml`.

```sh
./gradlew projects                    # list configured subprojects
./gradlew build                       # build all Java subprojects
./gradlew :flaredb-runner:build       # build one project
./gradlew :flareio-java:test          # test one project
./gradlew :wordcount:shadowJar        # build the example's self-contained JAR
./gradlew spotlessCheck               # check Java formatting
./gradlew :flaredb-runner:spotlessApply
```

### Python package

The Python runner is a standalone package under `runner-sdk/python/flarerunner`. Install it in editable mode while developing it so imports use the working tree:

```sh
pip install -e runner-sdk/python/flarerunner
```

Its tests use Python’s standard `unittest` discovery. The package requires Apache Beam; its metadata currently specifies `apache-beam>=2.76.0`.

## Build commands

Use these short forms when you already understand which build system owns the code you changed:

```sh
# Rust
cargo build
cargo build -p flaredb

# Java
./gradlew build
./gradlew :flaredb-runner:build

# Python
pip install -e runner-sdk/python/flarerunner

## Tests and formatting

Run the narrowest relevant check first, then broaden it when a change crosses module or protocol boundaries.

```sh
# Rust
cargo test -p flaredb
cargo test -p flaredb engine::
cargo test -p flaredb <test_name>
cargo test

# Java
./gradlew :flaredb-runner:test
./gradlew :flareio-java:test
./gradlew test
./gradlew spotlessCheck
./gradlew :flaredb-runner:spotlessApply

# Python
cd runner-sdk/python/flarerunner
python -m unittest discover -s tests -p "*_test.py"
```

Rust CI runs `cargo build --verbose` and `cargo test --verbose`. Java CI runs `./gradlew spotlessCheck` and `./gradlew build`.

## Examples and integration checks

- Java WordCount: `./gradlew :wordcount:shadowJar` followed by `./gradlew :wordcount:run`.
- Python WordCount: install the editable runner, then run `python3 example/python/wordcount.py`.
- Runner-native fan-in: inspect `example/flatten`.
- Java join coder handling: inspect `example/co-groupby-key` and the runner coder-rewrite tests.
- FlareDB I/O: inspect `example/flare-io-read` and `example/flare-io-write`.

For Python, use a virtual environment when needed:

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install apache-beam
pip install -e runner-sdk/python/flarerunner
```

## Pull-request checklist

Before opening a pull request:

1. Create a focused set of changes.
2. Update or add a focused test when behavior changes.
3. Run formatting and the narrowest relevant tests.
4. Run broader Rust, Java, or Python checks when the change crosses those boundaries.
5. Check that generated files, protocol fields, URNs, and public interfaces were changed only through their source-of-truth path.
6. Include any setup requirements or known limitations in the pull request description.
