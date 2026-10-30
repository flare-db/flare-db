use dashmap::DashMap;
use std::process::Stdio;
use std::sync::Arc;
use tokio::process::{Child, Command};
use tonic::Status;

use crate::{jobservice::urns::beam_urns, utils::path::logs_dir};

fn runner_capabilities() -> &'static str {
    beam_urns::MULTIMAP_KEYS_VALUES_SIDE_INPUT_PROTOCOL
}

#[derive(Clone, Debug)]
pub struct WorkerLaunchConfig {
    pub worker_jar: String,
    pub connect_timeout_secs: u64,
    /// Fallback interpreter used to launch a Python SDK harness when the job's
    /// PROCESS environment does not advertise one. `None` means `python3` from
    /// `PATH`.
    pub python_bin: Option<String>,
}

#[derive(Clone, Debug)]
pub enum WorkerRuntime {
    Java { staged_jar: String },
    Python { python_bin: Option<String> },
    Docker { image: String },
    External { endpoint: String },
}

#[derive(Clone)]
pub struct WorkerManager {
    config: WorkerLaunchConfig,
    active_workers: Arc<DashMap<String, Child>>,
}

impl WorkerManager {
    pub fn new(config: WorkerLaunchConfig) -> Self {
        Self {
            config,
            active_workers: Arc::new(DashMap::new()),
        }
    }

    pub fn config(&self) -> &WorkerLaunchConfig {
        &self.config
    }

    pub async fn spawn_worker(
        &self,
        job_id: &str,
        worker_id: usize,
        runtime: &WorkerRuntime,
        staging_dir: &str,
        instance_id: &str,
        fn_endpoint: &str,
        pipeline_options: &str,
    ) -> Result<(), Status> {
        let logs_dir = logs_dir(instance_id, job_id);
        tokio::fs::create_dir_all(&logs_dir)
            .await
            .map_err(|e| Status::internal(format!("failed to create logs dir: {}", e)))?;

        let log_path = format!("{}/flare-worker-{worker_id}.log", logs_dir.display());
        let harness_id = format!("{job_id}-worker-{worker_id}");
        let worker_key = worker_key(job_id, worker_id);
        let worker_staging_dir = format!("{staging_dir}/worker-{worker_id}");
        tokio::fs::create_dir_all(&worker_staging_dir)
            .await
            .map_err(|e| Status::internal(format!("failed to create worker staging dir: {e}")))?;
        let stdout_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|e| Status::internal(format!("failed to open harness log file: {}", e)))?;
        let stderr_file = stdout_file
            .try_clone()
            .map_err(|e| Status::internal(format!("failed to clone harness log handle: {}", e)))?;

        match runtime {
            WorkerRuntime::Java { staged_jar } => {
                let worker_jar = &self.config.worker_jar;

                let worker_exists = tokio::fs::try_exists(worker_jar)
                    .await
                    .map_err(|e| Status::internal(format!("failed to stat worker jar: {}", e)))?;
                if !worker_exists {
                    return Err(Status::internal(format!(
                        "worker jar not found at {}",
                        worker_jar
                    )));
                }

                let staged_exists = tokio::fs::try_exists(staged_jar).await.map_err(|e| {
                    Status::internal(format!("failed to stat staged artifact: {}", e))
                })?;
                if !staged_exists {
                    return Err(Status::internal(format!(
                        "staged artifact not found at {}",
                        staged_jar
                    )));
                }

                // Java uses ';' as the classpath separator on Windows and ':' elsewhere.
                let classpath_sep = if cfg!(windows) { ";" } else { ":" };
                let classpath = format!("{}{}{}", worker_jar, classpath_sep, staged_jar);
                let mut cmd = Command::new("java");

                cmd.arg("--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED") // arrow vector needs it
                    .arg("-Dio.netty.tryReflectionSetAccessible=true")
                    .arg("-Dorg.slf4j.simpleLogger.defaultLogLevel=info") //debug logs
                    //.arg("-Dorg.slf4j.simpleLogger.log.org.apache.beam.fn.harness.state=debug")
                    .arg("-cp")
                    .arg(&classpath)
                    .arg("org.apache.beam.fn.harness.FnHarness")
                    .env("HARNESS_ID", &harness_id)
                    .env(
                        "CONTROL_API_SERVICE_DESCRIPTOR",
                        format!(r#"url: "{fn_endpoint}""#),
                    )
                    .env(
                        "LOGGING_API_SERVICE_DESCRIPTOR",
                        format!(r#"url: "{fn_endpoint}""#),
                    )
                    .env(
                        "DATA_API_SERVICE_DESCRIPTOR",
                        format!(r#"url: "{fn_endpoint}""#),
                    )
                    .env(
                        "STATE_API_SERVICE_DESCRIPTOR",
                        format!(r#"url: "{fn_endpoint}""#),
                    )
                    .env("PIPELINE_OPTIONS", pipeline_options)
                    .env("RUNNER_CAPABILITIES", runner_capabilities())
                    .stdout(Stdio::from(stdout_file))
                    .stderr(Stdio::from(stderr_file));

                let child = cmd
                    .spawn()
                    .map_err(|e| Status::internal(format!("failed to spawn harness: {}", e)))?;

                let pid = child.id();
                log::info!(
                    "spawned java harness: job_id={}, worker_id={}, pid={:?}, classpath={}, log={}",
                    job_id,
                    worker_id,
                    pid,
                    classpath,
                    log_path
                );

                self.active_workers.insert(worker_key, child);
                Ok(())
            }
            WorkerRuntime::Python { python_bin } => {
                let py_bin = python_bin.as_deref().unwrap_or("python3");
                let mut cmd = Command::new(py_bin);

                cmd.arg("-m")
                    .arg("apache_beam.runners.worker.sdk_worker_main")
                    .env("HARNESS_ID", &harness_id)
                    .env("WORKER_ID", &harness_id)
                    .env(
                        "CONTROL_API_SERVICE_DESCRIPTOR",
                        format!(r#"url: "{fn_endpoint}""#),
                    )
                    .env(
                        "LOGGING_API_SERVICE_DESCRIPTOR",
                        format!(r#"url: "{fn_endpoint}""#),
                    )
                    .env(
                        "DATA_API_SERVICE_DESCRIPTOR",
                        format!(r#"url: "{fn_endpoint}""#),
                    )
                    .env(
                        "STATE_API_SERVICE_DESCRIPTOR",
                        format!(r#"url: "{fn_endpoint}""#),
                    )
                    .env("SEMI_PERSISTENT_DIRECTORY", &worker_staging_dir)
                    .env("PIPELINE_OPTIONS", pipeline_options)
                    .env("RUNNER_CAPABILITIES", runner_capabilities())
                    .env("PYTHONUNBUFFERED", "1")
                    .stdout(Stdio::from(stdout_file))
                    .stderr(Stdio::from(stderr_file));

                let child = cmd.spawn().map_err(|e| {
                    Status::internal(format!(
                        "failed to spawn python harness ({}) : {}",
                        py_bin, e
                    ))
                })?;

                let pid = child.id();
                log::info!(
                    "spawned python harness: job_id={}, worker_id={}, pid={:?}, python={}, staging_dir={}, log={}",
                    job_id,
                    worker_id,
                    pid,
                    py_bin,
                    worker_staging_dir,
                    log_path
                );

                self.active_workers.insert(worker_key, child);
                Ok(())
            }
            WorkerRuntime::Docker { image } => {
                log::warn!(
                    "Docker worker runtime requested for image '{}', but docker runtime is not supported in in-process mode.",
                    image
                );
                Err(Status::unimplemented(
                    "Docker worker runtime is not implemented",
                ))
            }
            WorkerRuntime::External { endpoint } => {
                log::warn!(
                    "External worker runtime requested at endpoint '{}', but external runtime is not supported in in-process mode.",
                    endpoint
                );
                Err(Status::unimplemented(
                    "External worker runtime is not implemented",
                ))
            }
        }
    }

    pub async fn stop_worker(&self, job_id: &str, worker_id: usize) -> Result<(), Status> {
        let key = worker_key(job_id, worker_id);
        if let Some((_, mut child)) = self.active_workers.remove(&key) {
            log::info!(
                "stopping worker for job_id={}, worker_id={}, pid={:?}",
                job_id,
                worker_id,
                child.id()
            );
            child.kill().await.map_err(|e| {
                Status::internal(format!(
                    "failed to kill worker process for job {}: {}",
                    job_id, e
                ))
            })?;
        } else {
            log::warn!(
                "stop_worker called for unknown job_id={}, worker_id={}",
                job_id,
                worker_id
            );
        }
        Ok(())
    }

    pub async fn stop_workers(&self, job_id: &str) -> Result<(), Status> {
        let prefix = format!("{job_id}:");
        let keys: Vec<String> = self
            .active_workers
            .iter()
            .filter_map(|entry| {
                entry
                    .key()
                    .starts_with(&prefix)
                    .then(|| entry.key().clone())
            })
            .collect();
        let mut first_error = None;
        for key in keys {
            if let Some((_, mut child)) = self.active_workers.remove(&key) {
                log::info!("stopping worker process {key}, pid={:?}", child.id());
                if let Err(error) = child.kill().await
                    && first_error.is_none()
                {
                    first_error = Some(Status::internal(format!(
                        "failed to kill worker process {key}: {error}"
                    )));
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    pub async fn stop_all(&self) {
        let keys: Vec<String> = self
            .active_workers
            .iter()
            .map(|r| r.key().clone())
            .collect();
        for key in keys {
            if let Some((_, mut child)) = self.active_workers.remove(&key) {
                log::info!("stopping worker for job_id={}, pid={:?}", key, child.id());
                let _ = child.kill().await;
            }
        }
    }
}

fn worker_key(job_id: &str, worker_id: usize) -> String {
    format!("{job_id}:{worker_id}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn advertises_multimap_keys_values_protocol() {
        assert_eq!(
            runner_capabilities(),
            "beam:protocol:multimap_keys_values_side_input:v1"
        );
    }

    #[tokio::test]
    async fn stop_worker_reaps_the_harness_process() {
        let manager = WorkerManager::new(WorkerLaunchConfig {
            worker_jar: String::new(),
            connect_timeout_secs: 1,
            python_bin: None,
        });
        let child = Command::new("sh")
            .arg("-c")
            .arg("sleep 60")
            .spawn()
            .expect("spawn test worker");
        let pid = child.id().expect("test worker pid");
        manager
            .active_workers
            .insert(worker_key("job-with-broken-stream", 0), child);

        assert!(Path::new(&format!("/proc/{pid}")).exists());
        manager
            .stop_worker("job-with-broken-stream", 0)
            .await
            .expect("stop worker");
        assert!(
            !Path::new(&format!("/proc/{pid}")).exists(),
            "worker process {pid} remained after cleanup"
        );
    }

    #[tokio::test]
    async fn stop_workers_reaps_every_process_for_a_job() {
        let manager = WorkerManager::new(WorkerLaunchConfig {
            worker_jar: String::new(),
            connect_timeout_secs: 1,
            python_bin: None,
        });
        let mut pids = Vec::new();
        for worker_id in 0..2 {
            let child = Command::new("sh")
                .arg("-c")
                .arg("sleep 60")
                .spawn()
                .expect("spawn test worker");
            pids.push(child.id().expect("test worker pid"));
            manager
                .active_workers
                .insert(worker_key("job-with-pool", worker_id), child);
        }

        manager
            .stop_workers("job-with-pool")
            .await
            .expect("stop worker pool");
        assert!(manager.active_workers.is_empty());
        for pid in pids {
            assert!(
                !Path::new(&format!("/proc/{pid}")).exists(),
                "worker process {pid} remained after pool cleanup"
            );
        }
    }
}
