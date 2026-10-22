use dashmap::DashMap;
use std::{
    fmt,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, BufReader},
    process::{Child, Command},
    sync::{Mutex, broadcast},
};
use tonic::Status;

use crate::{jobservice::urns::beam_urns, utils::path::logs_dir};

/// Stable identity for one harness process within a job.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct WorkerId(String);

impl WorkerId {
    pub(crate) fn from_string(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WorkerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Lifecycle notification emitted when a harness process exits.
#[derive(Clone, Debug)]
pub struct WorkerExit {
    pub job_id: String,
    pub worker_id: WorkerId,
    pub status: std::process::ExitStatus,
    pub intentional: bool,
}

/// Serializes all worker output for a job into the shared `flare-worker.log`.
#[derive(Clone, Debug, Default)]
pub struct WorkerLogRouter {
    sinks: Arc<DashMap<PathBuf, Arc<Mutex<std::fs::File>>>>,
}

impl WorkerLogRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn append_line(
        &self,
        log_path: &Path,
        worker_id: &WorkerId,
        source: &str,
        message: &str,
    ) -> std::io::Result<()> {
        let sink = self.sink(log_path).await?;
        let mut sink = sink.lock().await;
        use std::io::Write;
        writeln!(
            sink,
            "worker={} source={} message={}",
            worker_id, source, message
        )
    }

    async fn sink(&self, log_path: &Path) -> std::io::Result<Arc<Mutex<std::fs::File>>> {
        if let Some(sink) = self.sinks.get(log_path) {
            return Ok(Arc::clone(sink.value()));
        }
        if let Some(parent) = log_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)?;
        let sink = Arc::new(Mutex::new(file));
        self.sinks.insert(log_path.to_path_buf(), Arc::clone(&sink));
        Ok(sink)
    }
}

struct ActiveWorker {
    job_id: String,
    worker_id: WorkerId,
    pid: Option<u32>,
    child: Arc<Mutex<Child>>,
    intentional: Arc<AtomicBool>,
    monitor: tokio::task::JoinHandle<()>,
    forwarders: Vec<tokio::task::JoinHandle<()>>,
}

fn runner_capabilities() -> &'static str {
    beam_urns::MULTIMAP_KEYS_VALUES_SIDE_INPUT_PROTOCOL
}

#[derive(Clone, Debug)]
pub struct WorkerLaunchConfig {
    pub worker_jar: String,
    pub logs_dir: String,
    pub control_url: String,
    pub pipeline_options: String,
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
    active_workers: Arc<DashMap<String, ActiveWorker>>,
    worker_sequence: Arc<DashMap<String, Arc<AtomicUsize>>>,
    log_router: WorkerLogRouter,
    exit_tx: broadcast::Sender<WorkerExit>,
}

impl WorkerManager {
    pub fn new(config: WorkerLaunchConfig) -> Self {
        Self::with_log_router(config, WorkerLogRouter::new())
    }

    pub fn with_log_router(config: WorkerLaunchConfig, log_router: WorkerLogRouter) -> Self {
        let (exit_tx, _) = broadcast::channel(64);
        Self {
            config,
            active_workers: Arc::new(DashMap::new()),
            worker_sequence: Arc::new(DashMap::new()),
            log_router,
            exit_tx,
        }
    }

    pub fn config(&self) -> &WorkerLaunchConfig {
        &self.config
    }

    pub fn log_router(&self) -> WorkerLogRouter {
        self.log_router.clone()
    }

    pub fn subscribe_worker_exits(&self) -> broadcast::Receiver<WorkerExit> {
        self.exit_tx.subscribe()
    }

    fn next_worker_id(&self, job_id: &str) -> WorkerId {
        let sequence = self
            .worker_sequence
            .entry(job_id.to_string())
            .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
            .clone();
        WorkerId(format!(
            "{job_id}-w{}",
            sequence.fetch_add(1, Ordering::SeqCst)
        ))
    }

    pub async fn spawn_worker(
        &self,
        job_id: &str,
        runtime: &WorkerRuntime,
        staging_dir: &str,
        instance_id: &str,
    ) -> Result<WorkerId, Status> {
        let worker_id = self.next_worker_id(job_id);
        let logs_dir = logs_dir(instance_id, job_id);
        tokio::fs::create_dir_all(&logs_dir)
            .await
            .map_err(|e| Status::internal(format!("failed to create logs dir: {}", e)))?;

        let log_path = format!("{}/flare-worker.log", logs_dir.display());
        self.log_router
            .append_line(
                Path::new(&log_path),
                &worker_id,
                "runner",
                "starting harness",
            )
            .await
            .map_err(|e| Status::internal(format!("failed to initialize harness log: {}", e)))?;

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
                    .env("HARNESS_ID", worker_id.as_str())
                    .env("WORKER_ID", worker_id.as_str())
                    .env(
                        "CONTROL_API_SERVICE_DESCRIPTOR",
                        format!(r#"url: "{}""#, self.config.control_url),
                    )
                    .env(
                        "LOGGING_API_SERVICE_DESCRIPTOR",
                        format!(r#"url: "{}""#, self.config.control_url),
                    )
                    .env(
                        "DATA_API_SERVICE_DESCRIPTOR",
                        format!(r#"url: "{}""#, self.config.control_url),
                    )
                    .env(
                        "STATE_API_SERVICE_DESCRIPTOR",
                        format!(r#"url: "{}""#, self.config.control_url),
                    )
                    .env("PIPELINE_OPTIONS", &self.config.pipeline_options)
                    .env("RUNNER_CAPABILITIES", runner_capabilities())
                    .kill_on_drop(true)
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped());

                let mut child = cmd
                    .spawn()
                    .map_err(|e| Status::internal(format!("failed to spawn harness: {}", e)))?;

                let pid = child.id();
                log::info!(
                    "worker spawned: id={}, job={}, pid={:?}, runtime=java, classpath={}, log={}",
                    worker_id,
                    job_id,
                    pid,
                    classpath,
                    log_path
                );

                let forwarders = self.take_log_forwarders(
                    &mut child,
                    worker_id.clone(),
                    PathBuf::from(&log_path),
                )?;
                self.track_worker(job_id, worker_id.clone(), child, forwarders);
                Ok(worker_id)
            }
            WorkerRuntime::Python { python_bin } => {
                let py_bin = python_bin.as_deref().unwrap_or("python3");
                let mut cmd = Command::new(py_bin);

                cmd.arg("-m")
                    .arg("apache_beam.runners.worker.sdk_worker_main")
                    .env("HARNESS_ID", worker_id.as_str())
                    .env("WORKER_ID", worker_id.as_str())
                    .env(
                        "CONTROL_API_SERVICE_DESCRIPTOR",
                        format!(r#"url: "{}""#, self.config.control_url),
                    )
                    .env(
                        "LOGGING_API_SERVICE_DESCRIPTOR",
                        format!(r#"url: "{}""#, self.config.control_url),
                    )
                    .env(
                        "DATA_API_SERVICE_DESCRIPTOR",
                        format!(r#"url: "{}""#, self.config.control_url),
                    )
                    .env(
                        "STATE_API_SERVICE_DESCRIPTOR",
                        format!(r#"url: "{}""#, self.config.control_url),
                    )
                    .env("SEMI_PERSISTENT_DIRECTORY", staging_dir)
                    .env("PIPELINE_OPTIONS", &self.config.pipeline_options)
                    .env("RUNNER_CAPABILITIES", runner_capabilities())
                    .env("PYTHONUNBUFFERED", "1")
                    .kill_on_drop(true)
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped());

                let mut child = cmd.spawn().map_err(|e| {
                    Status::internal(format!(
                        "failed to spawn python harness ({}) : {}",
                        py_bin, e
                    ))
                })?;

                let pid = child.id();
                log::info!(
                    "worker spawned: id={}, job={}, pid={:?}, runtime=python, python={}, staging_dir={}, log={}",
                    worker_id,
                    job_id,
                    pid,
                    py_bin,
                    staging_dir,
                    log_path
                );

                let forwarders = self.take_log_forwarders(
                    &mut child,
                    worker_id.clone(),
                    PathBuf::from(&log_path),
                )?;
                self.track_worker(job_id, worker_id.clone(), child, forwarders);
                Ok(worker_id)
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

    fn take_log_forwarders(
        &self,
        child: &mut Child,
        worker_id: WorkerId,
        log_path: PathBuf,
    ) -> Result<Vec<tokio::task::JoinHandle<()>>, Status> {
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| Status::internal("python/java harness stdout pipe missing"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| Status::internal("python/java harness stderr pipe missing"))?;
        Ok(vec![
            self.forward_log_stream(stdout, worker_id.clone(), log_path.clone(), "stdout"),
            self.forward_log_stream(stderr, worker_id, log_path, "stderr"),
        ])
    }

    fn forward_log_stream<R>(
        &self,
        stream: R,
        worker_id: WorkerId,
        log_path: PathBuf,
        source: &'static str,
    ) -> tokio::task::JoinHandle<()>
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let router = self.log_router.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stream).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if let Err(error) = router
                    .append_line(&log_path, &worker_id, source, &line)
                    .await
                {
                    log::warn!(
                        "failed to write worker output id={}, source={}, error={}",
                        worker_id,
                        source,
                        error
                    );
                }
            }
        })
    }

    fn track_worker(
        &self,
        job_id: &str,
        worker_id: WorkerId,
        child: Child,
        forwarders: Vec<tokio::task::JoinHandle<()>>,
    ) {
        let pid = child.id();
        let child = Arc::new(Mutex::new(child));
        let intentional = Arc::new(AtomicBool::new(false));
        let active_workers = self.active_workers.clone();
        let exit_tx = self.exit_tx.clone();
        let monitor_worker_id = worker_id.clone();
        let monitor_job_id = job_id.to_string();
        let monitor_child = Arc::clone(&child);
        let monitor_intentional = Arc::clone(&intentional);
        let monitor = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let status = {
                    let mut child = monitor_child.lock().await;
                    child.try_wait()
                };
                match status {
                    Ok(Some(status)) => {
                        let intentional = monitor_intentional.load(Ordering::SeqCst);
                        active_workers.remove(monitor_worker_id.as_str());
                        let _ = exit_tx.send(WorkerExit {
                            job_id: monitor_job_id.clone(),
                            worker_id: monitor_worker_id.clone(),
                            status,
                            intentional,
                        });
                        break;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        active_workers.remove(monitor_worker_id.as_str());
                        log::warn!(
                            "failed polling worker id={} exit status: {}",
                            monitor_worker_id,
                            error
                        );
                        break;
                    }
                }
            }
        });
        self.active_workers.insert(
            worker_id.as_str().to_string(),
            ActiveWorker {
                job_id: job_id.to_string(),
                worker_id,
                pid,
                child,
                intentional,
                monitor,
                forwarders,
            },
        );
    }

    pub async fn stop_worker(&self, worker_id: &WorkerId) -> Result<(), Status> {
        if let Some((_, worker)) = self.active_workers.remove(worker_id.as_str()) {
            worker.intentional.store(true, Ordering::SeqCst);
            log::info!(
                "worker stopped: id={}, job={}, pid={:?}, reason=requested",
                worker.worker_id,
                worker.job_id,
                worker.pid
            );
            let kill_result = {
                let mut child = worker.child.lock().await;
                match child.try_wait() {
                    Ok(Some(_)) => Ok(()),
                    Ok(None) | Err(_) => child.kill().await,
                }
            };
            worker.monitor.abort();
            for forwarder in worker.forwarders {
                let _ = tokio::time::timeout(Duration::from_secs(1), forwarder).await;
            }
            kill_result.map_err(|e| {
                Status::internal(format!(
                    "failed to kill worker {} for job {}: {}",
                    worker.worker_id, worker.job_id, e
                ))
            })?;
        } else {
            log::debug!("stop_worker called for unknown worker_id={}", worker_id);
        }
        Ok(())
    }

    pub async fn stop_job_workers(&self, job_id: &str) -> Result<(), Status> {
        let worker_ids: Vec<WorkerId> = self
            .active_workers
            .iter()
            .filter(|entry| entry.job_id == job_id)
            .map(|entry| entry.worker_id.clone())
            .collect();
        let mut first_error = None;
        for worker_id in worker_ids {
            if let Err(error) = self.stop_worker(&worker_id).await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    pub async fn stop_all(&self) {
        let worker_ids: Vec<WorkerId> = self
            .active_workers
            .iter()
            .map(|entry| entry.worker_id.clone())
            .collect();
        for worker_id in worker_ids {
            let _ = self.stop_worker(&worker_id).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::tempdir;

    fn test_manager() -> WorkerManager {
        WorkerManager::new(WorkerLaunchConfig {
            worker_jar: String::new(),
            logs_dir: String::new(),
            control_url: String::new(),
            pipeline_options: "{}".to_string(),
            connect_timeout_secs: 1,
            python_bin: None,
        })
    }

    fn test_sleep_command(seconds: &str) -> Child {
        Command::new("sh")
            .arg("-c")
            .arg(format!("sleep {seconds}"))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn test worker")
    }

    fn track_test_worker(manager: &WorkerManager, job_id: &str) -> (WorkerId, u32) {
        let worker_id = manager.next_worker_id(job_id);
        let mut child = test_sleep_command("60");
        let pid = child.id().expect("test worker pid");
        let forwarders = manager
            .take_log_forwarders(
                &mut child,
                worker_id.clone(),
                PathBuf::from(format!("/tmp/{worker_id}.log")),
            )
            .expect("worker log forwarders");
        manager.track_worker(job_id, worker_id.clone(), child, forwarders);
        (worker_id, pid)
    }

    #[test]
    fn advertises_multimap_keys_values_protocol() {
        assert_eq!(
            runner_capabilities(),
            "beam:protocol:multimap_keys_values_side_input:v1"
        );
    }

    #[tokio::test]
    async fn stop_worker_reaps_the_harness_process() {
        let manager = test_manager();
        let (worker_id, pid) = track_test_worker(&manager, "job-with-broken-stream");

        assert!(Path::new(&format!("/proc/{pid}")).exists());
        manager.stop_worker(&worker_id).await.expect("stop worker");
        assert!(
            !Path::new(&format!("/proc/{pid}")).exists(),
            "worker process {pid} remained after cleanup"
        );
    }

    #[tokio::test]
    async fn stop_job_workers_reaps_every_worker() {
        let manager = test_manager();
        let workers = (0..3)
            .map(|_| track_test_worker(&manager, "multi-worker-job"))
            .collect::<Vec<_>>();
        assert!(
            workers
                .iter()
                .all(|(_, pid)| Path::new(&format!("/proc/{pid}")).exists())
        );

        manager
            .stop_job_workers("multi-worker-job")
            .await
            .expect("stop job workers");
        assert!(
            workers
                .iter()
                .all(|(_, pid)| !Path::new(&format!("/proc/{pid}")).exists())
        );
    }

    #[tokio::test]
    async fn unexpected_worker_exit_is_reported_with_id() {
        let manager = test_manager();
        let mut exits = manager.subscribe_worker_exits();
        let worker_id = manager.next_worker_id("short-job");
        let mut child = test_sleep_command("0.05");
        let forwarders = manager
            .take_log_forwarders(
                &mut child,
                worker_id.clone(),
                PathBuf::from("/tmp/unexpected-worker.log"),
            )
            .unwrap();
        manager.track_worker("short-job", worker_id.clone(), child, forwarders);

        let exit = tokio::time::timeout(Duration::from_secs(2), exits.recv())
            .await
            .expect("worker exit notification")
            .expect("worker exit event");
        assert_eq!(exit.worker_id, worker_id);
        assert!(!exit.intentional);
    }

    #[tokio::test]
    async fn worker_logs_share_one_file_and_identify_the_worker() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("flare-worker.log");
        let router = WorkerLogRouter::new();
        router
            .append_line(&path, &WorkerId("job-w0".to_string()), "stdout", "first")
            .await
            .unwrap();
        router
            .append_line(&path, &WorkerId("job-w1".to_string()), "stderr", "second")
            .await
            .unwrap();

        let contents = std::fs::read_to_string(path).unwrap();
        assert!(contents.contains("worker=job-w0 source=stdout message=first"));
        assert!(contents.contains("worker=job-w1 source=stderr message=second"));
    }

    #[tokio::test]
    async fn child_stdout_and_stderr_are_forwarded_with_worker_prefix() {
        let directory = tempdir().unwrap();
        let log_path = directory.path().join("flare-worker.log");
        let manager = test_manager();
        let worker_id = manager.next_worker_id("log-job");
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("echo child-stdout; echo child-stderr >&2")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let forwarders = manager
            .take_log_forwarders(&mut child, worker_id.clone(), log_path.clone())
            .unwrap();
        let status = child.wait().await.unwrap();
        assert!(status.success());
        for forwarder in forwarders {
            forwarder.await.unwrap();
        }

        let contents = std::fs::read_to_string(log_path).unwrap();
        assert!(contents.contains(&format!(
            "worker={worker_id} source=stdout message=child-stdout"
        )));
        assert!(contents.contains(&format!(
            "worker={worker_id} source=stderr message=child-stderr"
        )));
    }
}
