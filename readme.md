<div align="center">
  <img src="./assets/flaredb-readme.png" alt="Flare Logo" width="500"/>
  <h2>Apache Beam native streaming database</h2>
  <br><br>

  [![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](...)
  [![Maven Central](https://img.shields.io/maven-central/v/com.flare-db/flaredb-runner)](https://central.sonatype.com/artifact/com.flare-db/flaredb-runner)
  ![Apache Beam](https://img.shields.io/badge/Apache%20Beam-Runner-yellow?logo=apache)
</div>

**FlareDB** is a streaming database for building and running batch and streaming data pipelines. It uses Apache Beam as its programming interface. Beam provides a rich programming model for writing batch and streaming data pipelines in Java, Python, Go and SQL, while FlareDB provides a Rust based runtime to execute pipelines written with Beam SDKs.

Its based on a unified streams-and-tables architecture. Streams represent data in motion, while tables represent that same data as materialized state. FlareDB brings these concepts together in a single engine. As pipelines execute, PCollections transition naturally between streams and materialized table state, allowing FlareDB to unify data processing and storage within a single system.


#### Learn more about the architecture

For a deeper dive into FlareDB's design and execution model, check out this post: https://ganeshsivakumar.substack.com/p/flaredb.

⭐ New streaming systems don't come along that often. If you're curious to see where this project goes, consider starring the repository, it helps you keep track of updates and helps others discover it too.

## Getting Started

### 1. Install the FlareDB CLI

The FlareDB CLI provides commands to initialize, start, and manage FlareDB instances, as well as run Apache Beam pipelines on FlareDB.

If you are on **Linux or macOS** , please run the following command to install the CLI:

```bash
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/flare-db/flare-db/releases/download/flare-cli-v0.1.3/flare-cli-installer.sh | sh
```

For **Windows** run the following command in PowerShell:

```bash
powershell -ExecutionPolicy Bypass -c "irm https://github.com/flare-db/flare-db/releases/download/flare-cli-v0.1.3/flare-cli-installer.ps1 | iex"
```

Alternatively, you can download the CLI binary directly from the GitHub Releases page by selecting the appropriate binary for your platform.

### 2. Initialize FlareDB

After installing the CLI, run:

```bash
flare init
```

This command performs the initial setup by creating the required local directories and downloading the FlareDB binary and Apache Beam worker JAR.

The initialization only needs to be **completed once**. After that, you can use the `flare up` and `flare down` commands to manage the instance.

### 3. Start a FlareDB Instance

Start a local FlareDB instance with:

```bash
flare up
```

Once the instance is running, FlareDB is ready to accept pipeline jobs.


### 4. Configure Your Beam Pipeline

To run an Apache Beam pipeline on FlareDB, add the FlareDB Runner SDK as a dependency to your Beam project. The runner sdk submits the pipeline to the FlareDB instance as a Job.


Check out the WordCount example under `examples/` for a complete reference.

### 5. Run the Example

With FlareDB running, execute the WordCount example:

```bash
# compile wordcount pipeline
mvn clean install

# run the example
mvn exec:java -Dexec.mainClass="com.flaredb.example.WordCount"
```

The pipeline will be submitted to the local FlareDB instance and executed by the engine. Execution logs and pipeline output can be found in the logging directory created during startup.


### 6. Stop FlareDB instance

After executing pipelines, run this command to stop FlareDB instance

```bash
flare down
```

## Current Status

**FlareDB V0.1.0** is the first public release of FlareDB. It lays the foundation for a streaming database and its execution engine.

The initial release supports:

* Single-node execution of Apache Beam pipelines.
* Bounded sources on the Global Window.
* Native execution of core runner transforms, including `Impulse` and `GroupByKey`.
* Portable `DoFn` execution through the Beam SDK Harness.
* Apache Beam Portability Framework implementation.

### Roadmap

Upcoming releases will focus on expanding FlareDB's streaming execution capabilities:

* **Unbounded Sources** - Support for watermarks, event-time processing, windowing, and triggers.
* **Splittable DoFns** - Parallel work execution for I/Os.
* **Stateful Processing** - Implementation of the Apache Beam State API.
* **Native Transforms** - Additional runner-native implementations for element-wise, aggregation, and composite transforms.
* **Materialized Views** - Persist Beam `PCollections` as queryable table state for serving and analytics.


## License

FlareDB is licensed under the Apache License 2.0 
