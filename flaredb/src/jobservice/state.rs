use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::utils::path;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JobState {
    pub id: String,
    pub worker_log: String,
    pub flaredb_log: String,
    pub graph: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct State {
    pub pid: u32,
    pub instance_id: String,
    pub port: u16,
    pub log_dir: String,
    pub jobs: Vec<JobState>,
}

pub fn state_path() -> PathBuf {
    path::base_dir().join("state.json")
}

pub fn load_state(path: &Path) -> Result<State> {
    let file = fs::File::open(path)
        .with_context(|| format!("failed to open state file {}", path.display()))?;
    let state = serde_json::from_reader(file)
        .with_context(|| format!("failed to parse state file {}", path.display()))?;
    Ok(state)
}

pub fn write_state(path: &Path, state: &State) -> Result<()> {
    let temp_path = path.with_extension("json.tmp");
    let mut file = fs::File::create(&temp_path)
        .with_context(|| format!("failed to create temp state file {}", temp_path.display()))?;
    serde_json::to_writer_pretty(&mut file, state)
        .with_context(|| format!("failed to serialize state to {}", temp_path.display()))?;
    file.flush()
        .with_context(|| format!("failed to flush temp state file {}", temp_path.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to sync temp state file {}", temp_path.display()))?;
    fs::rename(&temp_path, path).with_context(|| {
        format!(
            "failed to rename {} to {}",
            temp_path.display(),
            path.display()
        )
    })?;
    Ok(())
}

pub fn record_job_state(instance_id: &str, job_id: &str) -> Result<()> {
    let s_path = state_path();

    let logs_directory = path::logs_dir(instance_id, job_id);
    let worker_log_path = logs_directory.join("flare-worker.log");
    let flaredb_log_path = path::instance_dir(instance_id).join("logs").join("flare-server.log");
    let graph_path = path::debug_executable_graph_path(instance_id, job_id);

    let job_entry = JobState {
        id: job_id.to_string(),
        worker_log: worker_log_path.display().to_string(),
        flaredb_log: flaredb_log_path.display().to_string(),
        graph: graph_path.display().to_string(),
    };

    let mut state = if s_path.exists() {
        match load_state(&s_path) {
            Ok(s) => s,
            Err(_) => State {
                pid: std::process::id(),
                instance_id: instance_id.to_string(),
                port: 8099,
                log_dir: path::instance_dir(instance_id).join("logs").display().to_string(),
                jobs: Vec::new(),
            },
        }
    } else {
        State {
            pid: std::process::id(),
            instance_id: instance_id.to_string(),
            port: 8099,
            log_dir: path::instance_dir(instance_id).join("logs").display().to_string(),
            jobs: Vec::new(),
        }
    };

    if let Some(pos) = state.jobs.iter().position(|j| j.id == job_id) {
        state.jobs[pos] = job_entry;
    } else {
        state.jobs.push(job_entry);
    }

    write_state(&s_path, &state)?;
    Ok(())
}
