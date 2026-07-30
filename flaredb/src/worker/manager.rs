use std::process::Stdio;
use std::sync::Arc;
use tokio::process::{Child, Command};
use dashmap::DashMap;
use tonic::Status;

use crate::utils::path::logs_dir;

#[derive(Clone, Debug)]
pub struct WorkerLaunchConfig {
    pub worker_jar: String,
    pub logs_dir: String,
    pub control_url: String,
    pub pipeline_options: String,
    pub connect_timeout_secs: u64,
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
        staged_jar: &str,
        instance_id: &str,
    ) -> Result<(), Status> {
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

        let staged_exists = tokio::fs::try_exists(staged_jar)
            .await
            .map_err(|e| Status::internal(format!("failed to stat staged artifact: {}", e)))?;
        if !staged_exists {
            return Err(Status::internal(format!(
                "staged artifact not found at {}",
                staged_jar
            )));
        }

        let logs_dir = logs_dir(instance_id, job_id);
        tokio::fs::create_dir_all(&logs_dir)
            .await
            .map_err(|e| Status::internal(format!("failed to create logs dir: {}", e)))?;

        let log_path = format!("{}/flare-worker.log", logs_dir.display());
        let stdout_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|e| Status::internal(format!("failed to open harness log file: {}", e)))?;
        let stderr_file = stdout_file
            .try_clone()
            .map_err(|e| Status::internal(format!("failed to clone harness log handle: {}", e)))?;

        let classpath = format!("{}:{}", worker_jar, staged_jar);
        let mut cmd = Command::new("java");
        cmd.arg("-cp")
            .arg(&classpath)
            .arg("org.apache.beam.fn.harness.FnHarness")
            .env("HARNESS_ID", job_id)
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
            .stdout(Stdio::from(stdout_file))
            .stderr(Stdio::from(stderr_file));

        let child = cmd
            .spawn()
            .map_err(|e| Status::internal(format!("failed to spawn harness: {}", e)))?;

        let pid = child.id();
        log::info!(
            "spawned harness: job_id={}, pid={:?}, classpath={}, log={}",
            job_id,
            pid,
            classpath,
            log_path
        );

        self.active_workers.insert(job_id.to_string(), child);

        Ok(())
    }

    pub async fn stop_worker(&self, job_id: &str) -> Result<(), Status> {
        if let Some((_, mut child)) = self.active_workers.remove(job_id) {
            log::info!(
                "stopping worker for job_id={}, pid={:?}",
                job_id,
                child.id()
            );
            child.kill().await.map_err(|e| {
                Status::internal(format!(
                    "failed to kill worker process for job {}: {}",
                    job_id, e
                ))
            })?;
        } else {
            log::warn!("stop_worker called for unknown job_id={}", job_id);
        }
        Ok(())
    }

    pub async fn stop_all(&self) {
        let keys: Vec<String> = self.active_workers.iter().map(|r| r.key().clone()).collect();
        for key in keys {
            if let Some((_, mut child)) = self.active_workers.remove(&key) {
                log::info!(
                    "stopping worker for job_id={}, pid={:?}",
                    key,
                    child.id()
                );
                let _ = child.kill().await;
            }
        }
    }
}
