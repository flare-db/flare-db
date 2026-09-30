---
name: flaredb-architecture
description: Maps FlareDB's Apache Beam portable-runner architecture, execution path, persistence, and subsystem entry points. Use before changing an unfamiliar FlareDB subsystem.
---

# FlareDB architecture map

## Mental model

FlareDB is a single-node Rust runtime that accepts Apache Beam portable pipelines and makes their `PCollection` boundaries materialized, queryable tables. Beam is the programming and portability interface; FlareDB owns pipeline planning, runner-native transforms, bundle orchestration, element persistence, and SQL/Flight exposure.

A Beam SDK does **not** send language-specific user code to Rust. It translates a pipeline into Beam Runner API protobufs. FlareDB keeps SDK-owned work as `ExecutableStage`s and asks a Java or Python SDK harness to execute that work through the Beam Fn API. FlareDB-specific execution begins where a stage is classified as runner-owned: today `Impulse`, `GroupByKey`, and `Flatten` are Rust `FlareTransform`s.

The important split is therefore:

```text
Runner API graph → fusion/executable graph
                     ├─ Worker stage: Fn Control/Data ↔ SDK harness
                     └─ Runner stage: Rust transform ↔ FlareElementStore
                                            ↓
                                      Arrow batches / Paimon tables
```

For Beam terminology such as PCollections, ParDo, windows, and portable APIs, use `../beam-concepts/SKILL.md`. This skill documents FlareDB’s implementation, not Beam theory.

## Repository map

```text
flaredb/                         Rust server and execution engine
  src/jobservice/                Beam Job + Artifact service; submission becomes a Job
  src/fusion/                    Runner API graph view, SDF rewrite, stage fusion, executable DAG
  src/engine/                    Scheduler, dispatcher, SDK bundle runtime, Fn API channels
  src/transforms/                Rust-native transforms and URN registry
  src/coders/                    Beam value/coder decoding and WindowedValue framing
  src/store/                     BeamRecord ↔ Arrow ↔ Paimon PCollection persistence
  src/io/server.rs               Arrow Flight SQL service over Paimon catalog
  src/worker/                    Java/Python harness process launcher
runner-sdk/java/flarerunner/     Java pipeline translation, artifact staging, Job API client
runner-sdk/python/flarerunner/   Python portable-runner-derived Job API client
flare-io/java/flareio-java/      Beam Row I/O using Arrow Flight / Flight SQL
flare-cli/                       Local install/up/down/logs commands
flare-sql/                       Interactive DataFusion/Paimon SQL REPL
beam/model/                      Vendored Beam proto sources
beam-model-rs/                   Proto build project and checked-in Rust bindings; not workspace-active
example/                         Submission/integration examples
```

`flaredb/src/main.rs` is the composition root. Its single Tonic server exposes Job Service, Artifact Staging, Fn Control/Data/Logging/State, and Flight SQL at the same endpoint.

## Components and boundaries

| Component | Responsibility | Relationship / boundary |
|---|---|---|
| Beam SDK + Flare runner SDK | Convert user pipeline to Runner API; prepare, stage artifacts, run | gRPC Beam Job API; Java rewrites opaque join coders before submission |
| `FlareJobService` | Stores prepared `ExecutableGraph`, chooses harness, drives a job | `prepare` calls `Job::new`; `run` spawns harness then dispatches graph |
| `fusion` | Normalizes, represents, and fuses Runner API transforms | `QueryablePipeline` → `FusedPipeline` → `ExecutableGraph` |
| `ExecutableGraph` | Petgraph DAG of `Runner`, `Worker`, or `Splittable` nodes | Edges carry `ConsumerMetaData`: PCollection, coder, producer/consumer IDs |
| `engine` + `harness` | Dependency scheduling and Fn bundle/data orchestration | Tonic Fn APIs are a protocol boundary; do not invent a second transport |
| `transforms` | Flare-native PCollection operations | `FlareTransform::execute(ExecutionContext)` reads/writes element-store collections |
| `store` | Materialize elements as Paimon tables | `BeamRecord` conversion is the coder/storage boundary; schema derives from first batch |
| `io` / `flare-io` / `flare-sql` | Query and external table I/O | Arrow Flight SQL and DataFusion/Paimon, separate from pipeline scheduling |

### Technologies actually in the execution path

- **Beam Runner API / generated `beam_model_rs` types:** `Pipeline`, `Components`, `PTransform`, coders, environments, `ProcessBundleDescriptor`, Job/Fn service types.
- **Beam Fn API:** control registers descriptors and sends `ProcessBundle`; data streams framed `Elements`; logging is routed to job logs. State service accepts a stream but has no request-processing/state backend.
- **Petgraph:** internal executable DAG and dependency edges.
- **Arrow:** in-memory `RecordBatch` representation and Flight IPC.
- **Paimon:** filesystem catalog and tables backing PCollections; FlareDB creates a per-job Paimon database.
- **DataFusion / `paimon-datafusion`:** native `GroupByKey` aggregation and SQL/Flight query planning/execution.

There is no Tonbo implementation in the active code path. The current catalog creation is `FileSystemCatalog`; do not assume remote object-storage behavior without finding an implementation/configuration path.

## Representative execution flow

### Job submission to graph

1. Java `FlareRunner.run` or Python `JobServiceHandle.submit` translates the SDK pipeline and sends `PrepareJobRequest` after any SDK-side coder rewrite/staging setup.
2. `jobservice/server.rs` `FlareJobService::prepare` validates `request.pipeline`, calls `Job::new`, and saves its `ExecutableGraph` in `JobStore`.
3. `Job::create_job` writes debug snapshots, expands splittable ParDo definitions using `ProtoOverrides` / `SplittableParDoExpander`, then calls `fuse_pipeline`.
4. `QueryablePipeline::new` builds a bipartite graph of primitive `PTransformNode` and `PCollectionNode` objects. `GreedyPipelineFuser` groups compatible SDK transforms by environment; runner stages remain separate. `ExecutableGraph::from` creates the runtime DAG.

### Run and materialization

5. `FlareJobService::run` detects Python process environments or defaults to Java, spawns that SDK harness through `WorkerManager`, waits for its Fn connection, then invokes `ExecutorDispatcher::prepare_pipeline` and `run_pipeline`.
6. `NodeScheduler` releases nodes only after every predecessor completes. `ExecutorDispatcher` runs ready nodes concurrently and marks them complete after success.
7. A **worker node** calls `BundleRuntime::register_bundle`, which adds Runner API source/sink data boundaries and sends a `ProcessBundleDescriptor` over Fn Control. `StageExecutor` sends stored input through Fn Data, receives output, decodes each WindowedValue with `StandardBeamCoders`, and writes `BeamRecord` batches to the output PCollection table.
8. A **runner node** builds/registers a descriptor, then invokes `FlareTransform::execute`. Its `ExecutionContext` identifies input and output PCollections in the same `FlareElementStore`.
9. `FlareElementStore` derives a `RecordTableSchema` from a PCollection’s first output batch, converts records to Arrow, commits them to Paimon, and scans them back for the next stage. PCollection tables are therefore the inter-stage exchange medium, not merely an output sink.

### Concrete transform trace: Impulse → ParDo → GroupByKey

```text
beam:transform:impulse:v1
  → QueryablePipeline root with no SDK environment
  → runner stage in FusedPipeline
  → ExecutableNode::Runner(from_urn(...))
  → StageExecutor calls Impulse::execute
  → FlareElementStore.write_beamrecord_batch(output PCollection)
  → downstream Worker ExecutableStage reads/materializes through Fn Data
  → worker outputs persisted to its output PCollection
  → GroupByKey runner node uses PaimonTableProvider + DataFusion aggregate
  → writes grouped Arrow batches to next PCollection table
```

The implementation anchors are `jobservice/job.rs`, `fusion/{pipeline,fuser}.rs`, `transforms/mod.rs`, `engine/{dispatcher,executor,runtime}.rs`, and `store/element_store.rs`.

## Module relationships: what a change pulls in

```text
JobService Prepare
  → Job::create_job
  → ProtoOverrides / QueryablePipeline / GreedyPipelineFuser
  → FusedPipeline + Components
  → ExecutableGraph
  → NodeScheduler + ExecutorDispatcher
  → StageExecutor
      ├─ BundleRuntime + harness control/data (SDK stages)
      └─ FlareTransform + FlareElementStore (runner stages)
```

```text
Worker output bytes
  → BundleRuntime::process_output_elements
  → StandardBeamCoders + WindowedValueCoder
  → BeamRecord
  → FlareElementStore::write_beamrecord_batch
  → Arrow RecordBatch
  → Paimon table
  → scan_collection / PaimonTableProvider / Flight SQL
```

- Adding a **native transform** requires more than an implementation: define/reference its URN in `jobservice/urns.rs`, register it in `transforms::from_urn`, confirm fuser classification/boundaries, ensure `ExecutableGraph` can reach it, and test coder/store behavior.
- Changing **fusion** affects both `ExecutableStage` descriptors and DAG dependencies. Inspect `fusion/pipeline.rs`, `fusion/fuser.rs`, `engine/scheduler.rs`, and the stage data-boundary construction in `engine/runtime.rs`.
- Changing **coders** must preserve exactly the harness framing. `process_input_elements` deliberately sends payload data followed by an empty `is_last` marker to work with both Java and Python harnesses. Review Java `PortableCoderRewrites` and Python pickled-coder handling too.
- Changing **storage schema** affects native transforms, SDK stage handoff, Flight I/O, and SQL. The schema registry is in-memory per store/job and Paimon has no native Arrow Null type (`Void` becomes nullable boolean).
- Changing **stateful behavior** needs a real backend plus Fn State request dispatch; `engine/harness/state.rs` only retains streams and exposes send/receive helpers today. Fusion already treats ParDo state/timers as fusion boundaries.

## Where to look

| Task | Start here | Then inspect |
|---|---|---|
| Pipeline ingestion / Job API | `flaredb/src/jobservice/server.rs` | `job.rs`, Java/Python runner submission code |
| Artifacts and staging | `flaredb/src/jobservice/artifact.rs` | `FlareArtifactResolver`, Python `stage` |
| URN or native transform | `flaredb/src/jobservice/urns.rs`, `transforms/mod.rs` | transform file, fuser, executable graph, tests |
| Graph construction | `flaredb/src/fusion/pipeline.rs` | `refs.rs`, `stage.rs`, `job.rs` |
| Fusion or SDF rewrite | `flaredb/src/fusion/fuser.rs`, `expander.rs` | `stage.rs`, `engine/sdf.rs` |
| Scheduling / run lifecycle | `flaredb/src/engine/{scheduler,dispatcher}.rs` | `executor.rs`, `jobservice/server.rs` |
| Harness protocol / bundles | `flaredb/src/engine/harness/` | `engine/runtime.rs`, `worker/manager.rs` |
| Element coding | `flaredb/src/coders/` | `engine/runtime.rs`, SDK coder rewrite tests |
| PCollection persistence | `flaredb/src/store/{record,element_store}.rs` | native transforms, `io/server.rs` |
| SQL / Flight | `flaredb/src/io/server.rs` | `flare-io/java/flareio-java`, `flare-sql` |
| Java runner | `runner-sdk/java/flarerunner/src/main/java/com/flaredb/runner/FlareRunner.java` | tests; `PortableCoderRewrites.java` |
| Python runner | `runner-sdk/python/flarerunner/flaredb_runner/flare_runner.py` | `tests/flare_runner_test.py`, `example/python/wordcount.py` |
| End-to-end behavior | `example/wordcount/` | Python WordCount, `example/flatten`, `example/co-groupby-key` |
| Proto/model changes | `beam/model/`, `beam-model-rs/build.rs` | root `Cargo.toml`, `flaredb/Cargo.toml`, lockfile |

## Tests and examples as architecture clues

- `fusion/fuser.rs` has pipeline-shape/fusion tests; start there for stage-boundary changes.
- `fusion/expander.rs` tests the splittable-ParDo rewrite; `engine/scheduler.rs` tests DAG readiness/fan-in.
- `store/element_store.rs` and `store/record.rs` have focused async/schema/record tests.
- `runner-sdk/java/flarerunner/src/test/` covers artifacts, job behavior, options, and coder rewrites.
- `flare-io/java/flareio-java/src/test/` covers Flight I/O conversion and validation.
- `runner-sdk/python/flarerunner/tests/flare_runner_test.py` mocks Job Service interactions.

Use `example/wordcount` for normal Java submission, `example/python/wordcount.py` for process-environment Python harness submission, `example/flatten` for runner fan-in, and `example/co-groupby-key` for opaque join coder handling.

## Search recipes

### Trace a Beam transform

1. `rg 'beam:transform:<name>|CONSTANT_NAME' flaredb runner-sdk example`
2. Locate the URN definition in `jobservice/urns.rs` and its graph/fuser treatment.
3. Follow `PTransformNode` through `QueryablePipeline`, `GreedyPipelineFuser`, then `ExecutableGraph`.
4. Determine the execution side: `ExecutableNode::Worker` means Fn harness; `Runner` means `transforms::from_urn` and `FlareTransform::execute`.
5. Trace its PCollection IDs and coders through `ConsumerMetaData`, `BundleRuntime`, and `FlareElementStore`.
6. Find an inline Rust test plus closest Java/Python example if it crosses an SDK boundary.

### Trace other interfaces

```sh
# A generated Beam protocol type through service implementation
rg 'PrepareJobRequest|ProcessBundleDescriptor|StateRequest' flaredb runner-sdk

# Trait definition, implementations, and call sites
rg 'trait FlareTransform|impl FlareTransform|\.execute\(' flaredb/src
rg 'trait Executor|impl Executor|execute_node' flaredb/src

# Fn transport and PCollection handoff
rg 'register_bundle|send_process_bundle_request|process_input_elements|process_output_elements' flaredb/src

# Storage and query path
rg 'FlareElementStore|write_beamrecord_batch|scan_collection|PaimonTableProvider' flaredb/src flare-io flare-sql

# Coder interpretation or SDK compatibility rewrite
rg 'StandardBeamCoders|WindowedValueCoder|length_prefix|pickled_python|javasdk' flaredb runner-sdk
```

Search from protocol representation toward execution, not from a guessed module name. For ambiguous behavior, follow **definition → implementation → call sites → closest test**.

## Constraints worth remembering

- `from_urn` currently recognizes only `Impulse`, `GroupByKey`, and `Flatten`; unknown runner-stage URNs panic. Do not assume a Beam primitive is natively executable merely because its constant appears in `urns.rs`.
- Worker runtimes are local Java/Python subprocesses. `Docker` and `External` variants return `unimplemented`.
- The Job Service runs work synchronously in `Run`; several Job API methods remain TODO. Treat capability claims in prose as less reliable than the service implementation.
- `NodeScheduler::output_edge_metadata` currently returns the first outgoing edge. Graph construction represents fan-out, but verify the execution path for fan-out changes.
- `beam/model/` is vendored source and `beam-model-rs/build.rs` can generate checked-in bindings, but `beam-model-rs` is commented out of the workspace and `flaredb` uses crates.io `beam-model-rs = 2.70.0`. Confirm the compiled model before modifying either.
