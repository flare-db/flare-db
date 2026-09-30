# FlareDB agent guide

FlareDB is a Rust, single-node Apache Beam portable runner plus a PCollection-backed table/query surface. Treat the implementation—not the README—as the source of truth. For the system map and execution trace, read [`.agent/skills/flaredb-architecture/SKILL.md`](.agent/skills/flaredb-architecture/SKILL.md) before changing an unfamiliar subsystem.

## Orient yourself

| Need to change… | Start here | Then inspect |
|---|---|---|
| Job submission, staging, or lifecycle | `flaredb/src/jobservice/` | Java/Python runners; `flaredb/src/main.rs` service wiring |
| Beam graph translation or stage fusion | `flaredb/src/fusion/` | `jobservice/job.rs`, `engine/{scheduler,dispatcher}.rs` |
| Harness/bundle execution or element transport | `flaredb/src/engine/` | `engine/harness/`, `coders/`, `store/` |
| Native runner transforms | `flaredb/src/transforms/` | `jobservice/urns.rs`, `fusion/pipeline.rs`, `store/` |
| Coders or Beam wire framing | `flaredb/src/coders/` | `engine/runtime.rs`, SDK coder rewrites/tests |
| PCollection persistence or SQL/Flight | `flaredb/src/store/`, `flaredb/src/io/server.rs` | `flare-io/java/`, `flare-sql/` |
| Java submission runner | `runner-sdk/java/flarerunner/` | `example/wordcount/`, Rust Job Service |
| Python submission runner | `runner-sdk/python/flarerunner/` | `example/python/wordcount.py`, Rust Job Service |
| Beam I/O connector | `flare-io/java/flareio-java/` | `flaredb/src/io/server.rs`, I/O examples |
| CLI / local-instance behavior | `flare-cli/`, `flare-sql/` | `flaredb/src/main.rs`, `flareup-dev.sh` |

The Java `WordCount` and Python `wordcount.py` examples are the best end-to-end submission references. `example/flatten` and `example/co-groupby-key` exercise runner-native fan-in and coder handling.

## Architecture in one minute

SDKs translate Beam pipelines to Runner API protobufs and submit them to FlareDB’s gRPC Job Service. FlareDB rewrites/fuses the pipeline into an executable graph. SDK-owned stages run in a Java or Python Beam SDK harness through Fn Control/Data APIs; Flare-owned stages (`Impulse`, `GroupByKey`, `Flatten`) execute in Rust over materialized PCollections. PCollections are converted through Arrow and stored as Paimon tables.

Read the [architecture skill](.agent/skills/flaredb-architecture/SKILL.md) for the actual types, flow, boundaries, lookup table, and search recipes. For Beam vocabulary only, use [`.agent/skills/beam-concepts/SKILL.md`](.agent/skills/beam-concepts/SKILL.md); do not duplicate Beam concepts into FlareDB changes.

## Development workflow

**Rust workspace** (`flaredb`, `flare-cli`, `flare-sql`):
```sh
cargo build
cargo test
cargo test -p flaredb engine::
cargo test -p flaredb <test_name>
```
CI runs `cargo build --verbose` and `cargo test --verbose`. No Rust formatter/linter is CI-enforced; use `cargo fmt --check` and `cargo clippy --workspace --all-targets` when relevant.

**Java/Gradle** (Java 17; modules are in `settings.gradle`):
```sh
./gradlew :flaredb-runner:test
./gradlew :flareio-java:test
./gradlew test
./gradlew spotlessCheck
./gradlew :flaredb-runner:spotlessApply
./gradlew :wordcount:shadowJar
./gradlew :wordcount:run
```
Java CI runs `./gradlew spotlessCheck` and `./gradlew build`.

**Python runner:**
```sh
pip install -e runner-sdk/python/flarerunner
cd runner-sdk/python/flarerunner && python -m unittest discover -s tests -p "*_test.py"
```

**Local integration:** build `flaredb`, then run `./flareup-dev.sh --debug` and submit `./gradlew :wordcount:run` or `python3 example/python/wordcount.py`. The launcher requires Java, `wget`, a local binary, and may download a worker JAR; it runs until interrupted.

## Working rules

1. **Trace before editing:** definition → implementation → call sites → closest test. Inspect the existing data/control path, especially across Runner API and Fn API boundaries.
2. **Use the established split:** SDK code belongs in harness stages; runner-native behavior is registered through `transforms::from_urn` and must fit graph/fusion/executor/store behavior. Do not create a parallel execution path.
3. **Make the smallest coherent change.** Update the narrowest regression test; broaden validation only when a change crosses Rust/SDK/protocol boundaries.
4. **Preserve protocol interfaces:** treat Beam URNs, Runner API/Fn API protobufs, coders, public runner options, and Flight SQL behavior as compatibility boundaries.
5. **Respect current capability boundaries:** the Fn State service is transport plumbing, not a state backend; Docker/external workers are declared but unimplemented. Confirm support in code before advertising or depending on it.
6. **Proto/model caution:** vendored inputs are `beam/model/`; `beam-model-rs/build.rs` generates checked-in bindings in `beam-model-rs/src/v1/`. But the active workspace excludes `beam-model-rs` and `flaredb` currently depends on crates.io `beam-model-rs = 2.70.0`. Verify which model is actually compiled before changing either source or generated bindings.

## High-value searches

```sh
# A Beam transform from URN through planning and native execution
rg 'beam:transform:…|PAR_DO_TRANSFORM|GROUP_BY_KEY_TRANSFORM' flaredb runner-sdk
rg 'from_urn|FlareTransform|ExecutableNode|ExecutableStage' flaredb/src

# A Runner API/Fn API type and its handling
rg 'PrepareJobRequest|ProcessBundleDescriptor|StateRequest' flaredb runner-sdk

# A coder/data boundary or PCollection storage path
rg 'StandardBeamCoders|WindowedValueCoder|process_(input|output)_elements' flaredb/src
rg 'FlareElementStore|write_beamrecord_batch|scan_collection' flaredb/src

# The closest regression test
rg 'transform name|URN|type/function name' flaredb/src runner-sdk/**/src/test
```

## Repository-local agent skills

Keep `AGENTS.md` as the operating entry point; open a skill only when its context is needed.

| Skill | Use it for |
|---|---|
| [`.agent/skills/flaredb-architecture/SKILL.md`](.agent/skills/flaredb-architecture/SKILL.md) | FlareDB’s actual job, fusion, execution, storage, and protocol architecture; read before changing an unfamiliar subsystem. |
| [`.agent/skills/beam-concepts/SKILL.md`](.agent/skills/beam-concepts/SKILL.md) | Apache Beam vocabulary and portable-model concepts, such as PCollections, PTransforms, Runner API, and Fn API. |
