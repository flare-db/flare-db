//! # FlareDB
//!
//! **FlareDB** is a streaming database for building and running batch and
//! streaming data pipelines. It uses Apache Beam as its programming interface.
//! Beam provides a rich programming model for expressing batch and streaming
//! data pipelines in Java, Python, Go and SQL, while FlareDB provides a Rust
//! based runtime to execute pipelines written with Beam SDKs.
//!
//! Its based on a unified streams-and-tables architecture. Streams represent
//! data in motion, while tables represent that same data as materialized state.
//! FlareDB brings these concepts together in a single engine. As pipelines
//! execute, PCollections transition naturally between streams and materialized
//! table state, allowing FlareDB to unify data processing and storage within a
//! single system.
//!
//! ## Getting Started
//!
//! ### 1. Start FlareDB
//!
//! Clone the repository and start a local FlareDB instance.
//!
//! ```text
//! git clone https://github.com/flare-db/flare-db.git
//! cd flare-db
//!
//! # run script
//! ./flareup.sh
//! ```
//!
//! The startup script creates the required directories, downloads the FlareDB
//! and Beam SDK worker binaries, and starts the local FlareDB instance. Once
//! the server is running, it is ready to accept Beam pipeline jobs.
//!
//! ### 2. Configure Your Beam Pipeline
//!
//! To run a Beam pipeline on FlareDB, add the FlareDB Runner SDK as a dependency
//! to your Apache Beam project. The Runner SDK submits the pipeline to the
//! running FlareDB instance for execution.
//!
//! See the `examples/` directory for a complete reference.
//!
//! ### 3. Run the Example
//!
//! With FlareDB running, execute the WordCount example:
//!
//! ```text
//! # compile wordcount pipeline
//! mvn clean install
//!
//! # run
//! mvn exec:java -Dexec.mainClass="com.flaredb.example.WordCount"
//! ```
//!
//! The pipeline will be submitted to the local FlareDB instance and executed by
//! the engine. Execution logs and pipeline output can be found in the logging
//! directory created during startup.
//!
//! Project repository:
//!
//! https://github.com/flare-db/flare-db
pub mod coders;
pub mod engine;
pub mod fusion;
pub mod io;
pub mod jobservice;
pub mod store;
pub mod transforms;
pub mod utils;
pub mod worker;

pub const DEFAULT_API_SERVICE_URL: &str = "127.0.0.1:8099";

/// Default maximum size for one gRPC message in either direction.
///
/// Beam element batches can legitimately exceed tonic's 4 MiB default. Keep the
/// limit configurable because bundle/output sizes depend on the SDK and workload.
pub const DEFAULT_GRPC_MAX_MESSAGE_SIZE_BYTES: usize = 64 * 1024 * 1024;

/// Resolve the process-wide gRPC message limit.
///
/// `FLAREDB_GRPC_MAX_MESSAGE_SIZE_BYTES` overrides the 64 MiB default. Invalid or
/// zero values are ignored so a typo cannot make the service unusable.
pub fn grpc_max_message_size_bytes() -> usize {
    grpc_max_message_size_bytes_from(
        std::env::var("FLAREDB_GRPC_MAX_MESSAGE_SIZE_BYTES")
            .ok()
            .as_deref(),
    )
}

fn grpc_max_message_size_bytes_from(configured: Option<&str>) -> usize {
    configured
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_GRPC_MAX_MESSAGE_SIZE_BYTES)
}

#[cfg(test)]
mod grpc_limits_tests {
    use super::*;

    #[test]
    fn grpc_message_limit_defaults_and_honors_a_positive_override() {
        assert_eq!(
            grpc_max_message_size_bytes_from(None),
            DEFAULT_GRPC_MAX_MESSAGE_SIZE_BYTES
        );
        assert_eq!(
            grpc_max_message_size_bytes_from(Some(" 16777216 ")),
            16 * 1024 * 1024
        );
        assert_eq!(
            grpc_max_message_size_bytes_from(Some("0")),
            DEFAULT_GRPC_MAX_MESSAGE_SIZE_BYTES
        );
        assert_eq!(
            grpc_max_message_size_bytes_from(Some("not-a-number")),
            DEFAULT_GRPC_MAX_MESSAGE_SIZE_BYTES
        );
    }
}
