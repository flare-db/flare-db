use anyhow::Result;
use clap::{Parser, Subcommand};

const FLAREDB_VERSION: &str = "0.1.8";

#[derive(Parser)]
#[command(name = "flare")]
#[command(version)]
#[command(about = "CLI to manage FlareDB")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    // Intial setup
    Init,
    // Start FlareDB instance
    Up,
    // Stop FlareDB instance
    Down,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init => init::init().await?,
        Commands::Up => server::up().await?,
        Commands::Down => server::down().await?,
    }

    Ok(())
}

pub mod init {
    use anyhow::bail;
    //#[cfg(not(unix))]
    //use anyhow::bail;
    use crate::FLAREDB_VERSION;
    use anyhow::{Context, Result};
    use indicatif::{ProgressBar, ProgressStyle};
    use std::fs;
    use std::io;
    use std::path::Path;
    use tokio::io::AsyncWriteExt;

    pub async fn init() -> Result<()> {
        let home_dir = dirs::home_dir().context("failed to determine home directory")?;
        let base_dir = home_dir.join(".flaredb");
        let bin_dir = base_dir.join("bin");
        let instances_dir = base_dir.join("instances");

        fs::create_dir_all(&base_dir).with_context(|| {
            format!("failed to create base directory at {}", base_dir.display())
        })?;
        fs::create_dir_all(&bin_dir)
            .with_context(|| format!("failed to create bin directory at {}", bin_dir.display()))?;
        fs::create_dir_all(&instances_dir).with_context(|| {
            format!(
                "failed to create instances directory at {}",
                instances_dir.display()
            )
        })?;

        let (asset_filename, archive_type) = detect_flaredb_asset()?;
        let binary_name = if cfg!(windows) {
            format!("flaredb-{}.exe", FLAREDB_VERSION)
        } else {
            format!("flaredb-{}", FLAREDB_VERSION)
        };
        let binary_path = bin_dir.join(&binary_name);

        if binary_path.exists() {
            println!("FlareDB binary already exists at {}", binary_path.display());
        } else {
            let archive_path = bin_dir.join(&asset_filename);
            let download_url = format!(
                "https://github.com/flare-db/flare-db/releases/download/flaredb-v{}/{}",
                FLAREDB_VERSION, asset_filename
            );

            download_with_progress(&download_url, &archive_path).await?;
            extract_archive(&archive_path, &binary_path, archive_type)
                .await
                .with_context(|| format!("failed to extract archive {}", archive_path.display()))?;
            fs::remove_file(&archive_path)
                .with_context(|| format!("failed to remove archive {}", archive_path.display()))?;
        }

        let worker_jar_name = "beam-sdks-java-harness-2.72.0-flare-bundled.jar";
        let worker_jar_path = bin_dir.join(worker_jar_name);
        let worker_url = "https://github.com/flare-db/flare-db/releases/download/beam-worker-java-2.72.0/beam-sdks-java-harness-2.72.0-flare-bundled.jar";

        if worker_jar_path.exists() {
            println!("Worker jar already exists at {}", worker_jar_path.display());
        } else {
            download_with_progress(worker_url, &worker_jar_path).await?;
        }

        Ok(())
    }

    enum ArchiveType {
        TarXz,
        Zip,
    }

    fn detect_flaredb_asset() -> Result<(String, ArchiveType)> {
        let os = std::env::consts::OS;
        let arch = std::env::consts::ARCH;

        match (os, arch) {
            ("macos", "aarch64") => Ok((
                "flaredb-aarch64-apple-darwin.tar.xz".to_string(),
                ArchiveType::TarXz,
            )),
            ("macos", "x86_64") => Ok((
                "flaredb-x86_64-apple-darwin.tar.xz".to_string(),
                ArchiveType::TarXz,
            )),
            ("windows", "x86_64") => Ok((
                "flaredb-x86_64-pc-windows-msvc.zip".to_string(),
                ArchiveType::Zip,
            )),
            ("linux", "aarch64") => Ok((
                "flaredb-aarch64-unknown-linux-gnu.tar.xz".to_string(),
                ArchiveType::TarXz,
            )),
            ("linux", "x86_64") => Ok((
                "flaredb-x86_64-unknown-linux-gnu.tar.xz".to_string(),
                ArchiveType::TarXz,
            )),
            _ => bail!("unsupported platform: {}/{}", os, arch),
        }
    }

    async fn download_with_progress(url: &str, destination: &Path) -> Result<()> {
        println!("Downloading {} to {}", url, destination.display());

        let mut response = reqwest::Client::new()
            .get(url)
            .send()
            .await
            .with_context(|| format!("failed to download {url}"))?;

        let total_size = response
            .content_length()
            .with_context(|| format!("no content-length header from {url}"))?;

        let pb = ProgressBar::new(total_size);
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})",
            )
            .unwrap()
            .progress_chars("=> "),
        );

        let mut dest_file = tokio::fs::File::create(destination)
            .await
            .with_context(|| format!("failed to create {}", destination.display()))?;

        while let Some(chunk) = response
            .chunk()
            .await
            .with_context(|| format!("failed to read response body from {url}"))?
        {
            dest_file
                .write_all(&chunk)
                .await
                .with_context(|| format!("failed to write to {}", destination.display()))?;
            pb.inc(chunk.len() as u64);
        }

        dest_file
            .flush()
            .await
            .with_context(|| format!("failed to flush {}", destination.display()))?;

        pb.finish_with_message("done");
        Ok(())
    }

    async fn extract_archive(path: &Path, dest: &Path, archive_type: ArchiveType) -> Result<()> {
        let path = path.to_owned();
        let dest = dest.to_owned();

        tokio::task::spawn_blocking(move || match archive_type {
            ArchiveType::TarXz => extract_tar_xz(&path, &dest),
            ArchiveType::Zip => extract_zip(&path, &dest),
        })
        .await
        .context("archive extraction task failed")??;

        Ok(())
    }

    fn extract_tar_xz(archive_path: &Path, dest: &Path) -> Result<()> {
        let file = fs::File::open(archive_path)
            .with_context(|| format!("failed to open archive {}", archive_path.display()))?;
        let decoder = xz2::read::XzDecoder::new(file);
        let mut archive = tar::Archive::new(decoder);
        let source_binary_name = if cfg!(windows) {
            "flaredb.exe"
        } else {
            "flaredb"
        };

        let mut found = false;
        for entry in archive.entries().with_context(|| {
            format!(
                "failed to read entries from archive {}",
                archive_path.display()
            )
        })? {
            let mut entry = entry.with_context(|| {
                format!(
                    "failed to read entry from archive {}",
                    archive_path.display()
                )
            })?;
            let path = entry
                .path()
                .with_context(|| "failed to determine archive entry path")?;
            if path.file_name().and_then(|name| name.to_str()) == Some(source_binary_name) {
                let mut out = fs::File::create(dest)
                    .with_context(|| format!("failed to create {}", dest.display()))?;
                io::copy(&mut entry, &mut out)
                    .with_context(|| format!("failed to extract {}", dest.display()))?;
                set_executable(dest)?;
                found = true;
                break;
            }
        }

        if !found {
            bail!(
                "binary {} not found inside archive {}",
                source_binary_name,
                archive_path.display()
            );
        }

        Ok(())
    }

    fn extract_zip(archive_path: &Path, dest: &Path) -> Result<()> {
        let file = fs::File::open(archive_path)
            .with_context(|| format!("failed to open archive {}", archive_path.display()))?;
        let mut archive = zip::ZipArchive::new(file)
            .with_context(|| format!("failed to open zip archive {}", archive_path.display()))?;
        let source_binary_name = if cfg!(windows) {
            "flaredb.exe"
        } else {
            "flaredb"
        };

        let mut found = false;
        for i in 0..archive.len() {
            let mut entry = archive
                .by_index(i)
                .with_context(|| format!("failed to read zip entry {}", i))?;
            if entry.name().ends_with(source_binary_name) {
                let mut out = fs::File::create(dest)
                    .with_context(|| format!("failed to create {}", dest.display()))?;
                io::copy(&mut entry, &mut out)
                    .with_context(|| format!("failed to extract {}", dest.display()))?;
                set_executable(dest)?;
                found = true;
                break;
            }
        }

        if !found {
            bail!(
                "binary {} not found inside zip {}",
                source_binary_name,
                archive_path.display()
            );
        }

        Ok(())
    }

    fn set_executable(path: &Path) -> Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(path)
                .with_context(|| format!("failed to read permissions for {}", path.display()))?
                .permissions();
            perms.set_mode(0o755);
            fs::set_permissions(path, perms).with_context(|| {
                format!("failed to set executable permission on {}", path.display())
            })?;
        }
        Ok(())
    }
}

mod server {
    use super::process_control;
    use super::state;
    use crate::FLAREDB_VERSION;
    use anyhow::{Context, Result, bail};
    use std::fs::{self, OpenOptions};
    use std::process::Stdio;
    use tokio::net::TcpStream;
    use tokio::process::Command;
    use tokio::time::{Duration, sleep};
    use uuid::Uuid;

    const PORT: u16 = 8099;
    const WORKER_JAR_NAME: &str = "beam-sdks-java-harness-2.72.0-flare-bundled.jar";

    pub async fn up() -> Result<()> {
        let home_dir = dirs::home_dir().context("failed to determine home directory")?;
        let base_dir = home_dir.join(".flaredb");
        let bin_dir = base_dir.join("bin");
        let instances_dir = base_dir.join("instances");
        let state_path = state::state_path(&base_dir);

        if state_path.exists() {
            let existing_state = state::load_state(&state_path)?;
            if process_control::is_alive(existing_state.pid) {
                bail!(
                    "FlareDB already running (pid {}, instance {}). Run 'flare down' first.",
                    existing_state.pid,
                    existing_state.instance_id
                );
            }

            println!(
                "Found stale state for pid {} from instance {}. Removing stale state.",
                existing_state.pid, existing_state.instance_id
            );
            fs::remove_file(&state_path).with_context(|| {
                format!("failed to remove stale state {}", state_path.display())
            })?;
        }

        let binary_name = if cfg!(windows) {
            format!("flaredb-{}.exe", FLAREDB_VERSION)
        } else {
            format!("flaredb-{}", FLAREDB_VERSION)
        };
        let binary_path = bin_dir.join(&binary_name);
        let worker_jar_path = bin_dir.join(WORKER_JAR_NAME);

        if !binary_path.exists() {
            bail!(
                "Missing FlareDB binary {}. Run 'flare init' first.",
                binary_path.display()
            );
        }
        if !worker_jar_path.exists() {
            bail!(
                "Missing worker jar {}. Run 'flare init' first.",
                worker_jar_path.display()
            );
        }

        fs::create_dir_all(&instances_dir).with_context(|| {
            format!(
                "failed to create instances directory {}",
                instances_dir.display()
            )
        })?;

        let instance_id = Uuid::new_v4().to_string();
        let instance_log_dir = instances_dir.join(&instance_id).join("logs");
        fs::create_dir_all(&instance_log_dir).with_context(|| {
            format!(
                "failed to create instance log dir {}",
                instance_log_dir.display()
            )
        })?;

        let log_file_path = instance_log_dir.join("flare-server.log");
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_file_path)
            .with_context(|| {
                format!(
                    "failed to create server log file {}",
                    log_file_path.display()
                )
            })?;
        let log_file_err = log_file.try_clone().with_context(|| {
            format!(
                "failed to clone log file handle for {}",
                log_file_path.display()
            )
        })?;

        let mut command = Command::new(&binary_path);
        command
            .arg(&base_dir)
            .env("RUST_LOG", "info")
            .env("FLAREDB_INSTANCE_ID", &instance_id)
            .env("WORKER_JAR_PATH", &worker_jar_path)
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(log_file_err))
            .kill_on_drop(false);

        let child = command.spawn().with_context(|| {
            format!(
                "failed to spawn flaredb server from {}",
                binary_path.display()
            )
        })?;
        let pid = child
            .id()
            .context("failed to obtain PID of spawned flaredb process")?;
        drop(child);

        let ready = wait_for_port_ready(PORT, Duration::from_millis(500), 60).await;
        if !ready {
            let _ = process_control::terminate_forceful(pid);
            bail!(
                "FlareDB did start. Check log at {}",
                log_file_path.display()
            );
        }

        let state = state::State {
            pid,
            instance_id: instance_id.clone(),
            port: PORT,
            log_dir: instance_log_dir.display().to_string(),
            jobs: Vec::new(),
        };
        state::write_state(&state_path, &state)
            .with_context(|| format!("failed to write state file {}", state_path.display()))?;

        println!("Flared up! 🔥🔥");
        println!("");
        println!("  Instance ID         : {}", instance_id);
        println!("  PID                 : {}", pid);
        println!("  FlareDB server log  : {}", log_file_path.display());
        println!("  State file          : {}", state_path.display());

        Ok(())
    }

    pub async fn down() -> Result<()> {
        let home_dir = dirs::home_dir().context("failed to determine home directory")?;
        let base_dir = home_dir.join(".flaredb");
        let state_path = state::state_path(&base_dir);

        if !state_path.exists() {
            println!("FlareDB is not running.");
            return Ok(());
        }

        let state = state::load_state(&state_path)
            .with_context(|| format!("failed to read state file {}", state_path.display()))?;

        if !process_control::is_alive(state.pid) {
            println!(
                "Found stale state for pid {}. Removing state file.",
                state.pid
            );
            fs::remove_file(&state_path)
                .with_context(|| format!("failed to remove state file {}", state_path.display()))?;
            return Ok(());
        }

        process_control::terminate_graceful(state.pid).with_context(|| {
            format!("failed to request graceful shutdown for pid {}", state.pid)
        })?;

        let mut attempts = 0;
        while attempts < 20 && process_control::is_alive(state.pid) {
            sleep(Duration::from_millis(250)).await;
            attempts += 1;
        }

        if process_control::is_alive(state.pid) {
            process_control::terminate_forceful(state.pid)
                .with_context(|| format!("failed to forcefully terminate pid {}", state.pid))?;

            let mut attempts = 0;
            while attempts < 20 && process_control::is_alive(state.pid) {
                sleep(Duration::from_millis(250)).await;
                attempts += 1;
            }
        }

        if process_control::is_alive(state.pid) {
            bail!("FlareDB process {} did not stop", state.pid);
        }

        let port_closed = wait_for_port_closed(state.port, Duration::from_millis(250), 60).await;
        if !port_closed {
            bail!("FlareDB port {} did not release after shutdown", state.port);
        }

        fs::remove_file(&state_path)
            .with_context(|| format!("failed to remove state file {}", state_path.display()))?;

        println!("FlareDB stopped and state file removed.");
        Ok(())
    }

    async fn wait_for_port_ready(port: u16, interval: Duration, attempts: usize) -> bool {
        for _ in 0..attempts {
            if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                return true;
            }
            sleep(interval).await;
        }
        false
    }

    async fn wait_for_port_closed(port: u16, interval: Duration, attempts: usize) -> bool {
        for _ in 0..attempts {
            if TcpStream::connect(("127.0.0.1", port)).await.is_err() {
                return true;
            }
            sleep(interval).await;
        }
        false
    }
}

mod state {
    use anyhow::{Context, Result};
    use serde::{Deserialize, Serialize};
    use std::fs;
    use std::io::Write;
    use std::path::{Path, PathBuf};

    #[derive(Serialize, Deserialize)]
    pub struct JobState {
        pub id: String,
        pub worker_log: String,
        pub flaredb_log: String,
        pub graph: String,
    }

    #[derive(Serialize, Deserialize)]
    pub struct State {
        pub pid: u32,
        pub instance_id: String,
        pub port: u16,
        pub log_dir: String,
        pub jobs: Vec<JobState>,
    }

    pub fn state_path(base_dir: &Path) -> PathBuf {
        base_dir.join("state.json")
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
}

mod process_control {
    #[cfg(not(unix))]
    use anyhow::bail;
    use anyhow::{Context, Result};
    #[cfg(unix)]
    use sysinfo::Signal;
    use sysinfo::{Pid, ProcessesToUpdate, System};

    fn refresh_system() -> System {
        let mut system = System::new();
        system.refresh_processes(ProcessesToUpdate::All, true);
        system
    }

    fn pid_from_u32(pid: u32) -> Pid {
        Pid::from(pid as usize)
    }

    pub fn is_alive(pid: u32) -> bool {
        let system = refresh_system();
        system.process(pid_from_u32(pid)).is_some()
    }

    pub fn terminate_graceful(pid: u32) -> Result<()> {
        let mut system = System::new();
        system.refresh_processes(ProcessesToUpdate::All, true);
        let process = system
            .process(pid_from_u32(pid))
            .with_context(|| format!("process {} not found", pid))?;

        #[cfg(unix)]
        {
            process
                .kill_with(Signal::Term)
                .with_context(|| format!("failed to send SIGTERM to pid {}", pid))?;
        }

        #[cfg(not(unix))]
        {
            let killed = process.kill();
            if !killed {
                bail!("failed to terminate pid {}", pid);
            }
        }

        Ok(())
    }

    pub fn terminate_forceful(pid: u32) -> Result<()> {
        let mut system = System::new();
        system.refresh_processes(ProcessesToUpdate::All, true);
        let process = system
            .process(pid_from_u32(pid))
            .with_context(|| format!("process {} not found", pid))?;

        #[cfg(unix)]
        {
            process
                .kill_with(Signal::Kill)
                .with_context(|| format!("failed to send SIGKILL to pid {}", pid))?;
        }

        #[cfg(not(unix))]
        {
            let killed = process.kill();
            if !killed {
                bail!("failed to terminate pid {}", pid);
            }
        }

        Ok(())
    }
}
