use std::io;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

static BASE_DIR_OVERRIDE: OnceLock<PathBuf> = OnceLock::new();

pub fn set_base_dir(dir: &str) {
    let _ = BASE_DIR_OVERRIDE.set(PathBuf::from(dir));
}

pub fn base_dir() -> PathBuf {
    // Prefer explicit override set at runtime
    if let Some(p) = BASE_DIR_OVERRIDE.get() {
        return p.clone();
    }
    // Allow overriding base dir via env var
    if let Ok(dir) = std::env::var("FLAREDB_BASE_DIR") {
        return PathBuf::from(dir);
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".flaredb")
}

pub fn instance_dir(instance_id: &str) -> PathBuf {
    base_dir().join(instance_id)
}

pub fn job_dir(instance_id: &str, job_id: &str) -> PathBuf {
    instance_dir(instance_id).join(job_id)
}

pub fn artifacts_dir(instance_id: &str, job_id: &str) -> PathBuf {
    job_dir(instance_id, job_id).join("artifacts")
}

pub fn artifacts_jar_dir(instance_id: &str, job_id: &str) -> PathBuf {
    artifacts_dir(instance_id, job_id).join("jar")
}

pub fn store_dir(instance_id: &str, job_id: &str) -> PathBuf {
    job_dir(instance_id, job_id).join("store")
}

pub fn logs_dir(instance_id: &str, job_id: &str) -> PathBuf {
    job_dir(instance_id, job_id).join("logs")
}

pub fn debug_executable_graph_path(instance_id: &str, job_id: &str) -> PathBuf {
    job_dir(instance_id, job_id)
        .join("debug")
        .join("executable_graph.dot")
}

pub fn ensure_dir(path: &Path) -> io::Result<()> {
    std::fs::create_dir_all(path)
}

pub fn ensure_parent_for_file(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        ensure_dir(parent)
    } else {
        Ok(())
    }
}
