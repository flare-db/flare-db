use std::io::Result;
use tonic_prost_build::configure;

fn main() -> Result<()> {
    configure()
       .build_client(true)
       .build_server(true)
       .protoc_arg("--experimental_allow_proto3_optional")
       .out_dir("src/v1")
       .compile_protos(
           &[
               "../beam/model/fn-execution/src/main/proto/org/apache/beam/model/fn_execution/v1/beam_fn_api.proto",
               "../beam/model/fn-execution/src/main/proto/org/apache/beam/model/fn_execution/v1/beam_provision_api.proto",
               "../beam/model/interactive/src/main/proto/org/apache/beam/model/interactive/v1/beam_interactive_api.proto",
               "../beam/model/job-management/src/main/proto/org/apache/beam/model/job_management/v1/beam_job_api.proto",
               "../beam/model/job-management/src/main/proto/org/apache/beam/model/job_management/v1/beam_expansion_api.proto",
               "../beam/model/job-management/src/main/proto/org/apache/beam/model/job_management/v1/beam_artifact_api.proto",
               "../beam/model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/beam_runner_api.proto",
               "../beam/model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/endpoints.proto",
               "../beam/model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/external_transforms.proto",
               "../beam/model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/metrics.proto",
               "../beam/model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/schema.proto",
               "../beam/model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/standard_window_fns.proto",
           ],
           &[
               "../beam/model/fn-execution/src/main/proto",
               "../beam/model/interactive/src/main/proto",
               "../beam/model/job-management/src/main/proto",
               "../beam/model/pipeline/src/main/proto",
           ],
       )?;

    Ok(())
}
