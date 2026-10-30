use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tonic::Status;

use crate::{jobservice::urns::beam_urns, utils::path::logs_dir};

fn runner_capabilities() -> &'static str {
    beam_urns::MULTIMAP_KEYS_VALUES_SIDE_INPUT_PROTOCOL
}

fn worker_log_path(logs_dir: &Path) -> PathBuf {
    logs_dir.join("flare-worker.log")
}

fn spawn_process_log_forwarders(child: &mut Child, log_path: PathBuf, worker_id: usize) {
    if let Some(stdout) = child.stdout.take() {
        spawn_process_log_forwarder(stdout, log_path.clone(), worker_id, "stdout");
    }
    if let Some(stderr) = child.stderr.take() {
        spawn_process_log_forwarder(stderr, log_path, worker_id, "stderr");
    }
}

fn spawn_process_log_forwarder<R>(
    reader: R,
    log_path: PathBuf,
    worker_id: usize,
    stream_name: &'static str,
) where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(error) = forward_process_output(reader, &log_path, worker_id, stream_name).await
        {
            log::warn!(
                "failed to forward worker process {stream_name}: worker_id={worker_id}, \
                 log={}, error={error}",
                log_path.display()
            );
        }
    });
}

async fn forward_process_output<R>(
    reader: R,
    log_path: &Path,
    worker_id: usize,
    stream_name: &str,
) -> std::io::Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut reader = BufReader::new(reader);
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .await?;
    let mut line = Vec::new();
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line).await? == 0 {
            return Ok(());
        }
        let message = String::from_utf8_lossy(&line);
        let message = message.trim_end_matches(['\r', '\n']);
        let formatted = format!(
            "[FLARE WORKER PROCESS] worker_id={worker_id} stream={stream_name} message={message}\n"
        );
        file.write_all(formatted.as_bytes()).await?;
        file.flush().await?;
    }
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

        let log_path = worker_log_path(&logs_dir);
        let harness_id = format!("{job_id}-worker-{worker_id}");
        let worker_key = worker_key(job_id, worker_id);
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
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped());

                let mut child = cmd
                    .spawn()
                    .map_err(|e| Status::internal(format!("failed to spawn harness: {}", e)))?;
                spawn_process_log_forwarders(&mut child, log_path.clone(), worker_id);

                let pid = child.id();
                log::info!(
                    "spawned java harness: job_id={}, worker_id={}, pid={:?}, classpath={}, log={}",
                    job_id,
                    worker_id,
                    pid,
                    classpath,
                    log_path.display()
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
                    // Python workers load the same staged main session from the
                    // shared artifact root. Creating one directory per worker is
                    // unnecessary and leaves empty job/instance artifacts behind.
                    .env("SEMI_PERSISTENT_DIRECTORY", staging_dir)
                    .env("PIPELINE_OPTIONS", pipeline_options)
                    .env("RUNNER_CAPABILITIES", runner_capabilities())
                    .env("PYTHONUNBUFFERED", "1")
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped());

                let mut child = cmd.spawn().map_err(|e| {
                    Status::internal(format!(
                        "failed to spawn python harness ({}) : {}",
                        py_bin, e
                    ))
                })?;
                spawn_process_log_forwarders(&mut child, log_path.clone(), worker_id);

                let pid = child.id();
                log::info!(
                    "spawned python harness: job_id={}, worker_id={}, pid={:?}, python={}, staging_dir={}, log={}",
                    job_id,
                    worker_id,
                    pid,
                    py_bin,
                    staging_dir,
                    log_path.display()
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
    use tokio::io::{AsyncWriteExt, duplex};

    #[test]
    fn advertises_multimap_keys_values_protocol() {
        assert_eq!(
            runner_capabilities(),
            "beam:protocol:multimap_keys_values_side_input:v1"
        );
    }

    #[tokio::test]
    async fn process_output_is_prefixed_and_shared_across_workers() {
        let temp = tempfile::tempdir().expect("temp dir");
        let log_path = worker_log_path(temp.path());

        let (mut first_writer, first_reader) = duplex(256);
        first_writer
            .write_all(b"first stdout line\nsecond stdout line\n")
            .await
            .expect("write first process output");
        drop(first_writer);
        forward_process_output(first_reader, &log_path, 2, "stdout")
            .await
            .expect("forward first process output");

        let (mut second_writer, second_reader) = duplex(256);
        second_writer
            .write_all(b"second worker stderr line\n")
            .await
            .expect("write second process output");
        drop(second_writer);
        forward_process_output(second_reader, &log_path, 5, "stderr")
            .await
            .expect("forward second process output");

        let contents = std::fs::read_to_string(&log_path).expect("read worker log");
        assert!(contents.contains(
            "[FLARE WORKER PROCESS] worker_id=2 stream=stdout message=first stdout line"
        ));
        assert!(contents.contains(
            "[FLARE WORKER PROCESS] worker_id=2 stream=stdout message=second stdout line"
        ));
        assert!(contents.contains(
            "[FLARE WORKER PROCESS] worker_id=5 stream=stderr message=second worker stderr line"
        ));
        assert_eq!(
            std::fs::read_dir(temp.path())
                .expect("read temp dir")
                .count(),
            1,
            "workers must share one job-level log file"
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
