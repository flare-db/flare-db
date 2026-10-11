use anyhow::Result;
use clap::{Parser, Subcommand};

use flare_sql;

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
    /// Initial setup: downloads the latest FlareDB release and the Beam Java worker jar.
    Init,
    /// Start a FlareDB instance.
    Up,
    /// Stop the running FlareDB instance.
    Down,
    /// Launch the interactive SQL shell.
    Sql,
    // Show installed versions and available updates.
    //
    // Prints the installed flare-cli, FlareDB, and beam-java-worker versions,
    // then checks R2 for newer releases and reports how to upgrade each.
    //
    // Usage:
    //   flare version
    #[command(
        name = "version",
        about = "Show installed versions and available updates.",
        long_about = "Show installed versions and available updates.\n\nPrints the installed flare-cli, FlareDB, and beam-java-worker versions, then checks R2 for newer releases and reports how to upgrade each."
    )]
    Version,
    // Update FlareDB and/or the Beam Java worker jar.
    //
    // With no flags, both are updated. If a newer flare-cli exists, this
    // points you at the install script instead of self-updating.
    //
    // Usage:
    //   flare update                 # update everything
    //   flare update --db            # update only the FlareDB binary
    //   flare update --java-worker   # update only the worker jar
    #[command(
        about = "Update FlareDB and/or the Beam Java worker jar.",
        long_about = "Update FlareDB and/or the Beam Java worker jar.\n\nWith no flags, both are updated. If a newer flare-cli exists, this points you at the install script instead of self-updating.",
        after_help = "Examples:\n  flare update                 # update everything\n  flare update --db            # update only the FlareDB binary\n  flare update --java-worker   # update only the worker jar"
    )]
    Update {
        /// Update the FlareDB server binary.
        #[arg(long)]
        db: bool,
        /// Update the Beam Java worker jar.
        #[arg(long = "java-worker")]
        java_worker: bool,
    },
    // View or stream job logs.
    //
    // Usage:
    //   flare logs               # stream logs for the most recent job
    //   flare logs <JOB_ID>      # stream logs for a specific job ID
    #[command(
        about = "View or stream job logs.",
        after_help = "Examples:\n  flare logs               # stream logs for the most recent job\n  flare logs <JOB_ID>      # stream logs for a specific job ID"
    )]
    Logs {
        /// Job ID to view logs for (positional). If omitted, defaults to the most recent job.
        #[arg(value_name = "JOB_ID")]
        job_id_pos: Option<String>,

        /// Job ID to view logs for. If omitted, defaults to the most recent job.
        #[arg(short = 'j', long = "jobid", value_name = "JOB_ID")]
        job_id_flag: Option<String>,

        /// Follow log output (stream continuously)
        #[arg(short, long, default_value_t = true)]
        follow: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init => init::init().await?,
        Commands::Up => server::up().await?,
        Commands::Down => server::down().await?,
        Commands::Sql => flare_sql::run().await?,
        Commands::Version => release::version().await?,
        Commands::Update { db, java_worker } => release::update(db, java_worker).await?,
        Commands::Logs {
            job_id_pos,
            job_id_flag,
            follow,
        } => {
            let job_id = job_id_flag.or(job_id_pos);
            logs::stream_logs(job_id, follow).await?
        }
    }

    Ok(())
}

/// Version discovery and updates for the three FlareDB artifacts published to
/// R2 (https://install.flare-db.com): the FlareDB binary, the Beam Java worker
/// jar, and the `flare` CLI itself.
pub mod release {
    use crate::{process_control, state};
    use anyhow::{Context, Result, bail};
    use semver::Version;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    const DEFAULT_BASE_URL: &str = "https://install.flare-db.com";
    const BINARY_PREFIX: &str = "flaredb-";
    const WORKER_JAR_PREFIX: &str = "beam-sdks-java-harness-";
    const WORKER_JAR_SUFFIX: &str = "-flare-bundled.jar";

    pub struct Installed {
        pub version: Version,
        pub path: PathBuf,
    }

    /// A release artifact the CLI can discover, report, and update.
    #[derive(Clone, Copy, PartialEq, Eq)]
    pub enum Component {
        /// The FlareDB server binary.
        Db,
        /// The Beam Java worker jar.
        JavaWorker,
        /// The `flare` CLI itself.
        Cli,
    }

    impl Component {
        /// URL path segment under `base_url` (e.g. `flaredb`).
        fn key(&self) -> &'static str {
            match self {
                Component::Db => "flaredb",
                Component::JavaWorker => "beam-worker-java",
                Component::Cli => "cli",
            }
        }

        /// Human-readable name for messages.
        fn label(&self) -> &'static str {
            match self {
                Component::Db => "FlareDB",
                Component::JavaWorker => "beam-java-worker",
                Component::Cli => "flare-cli",
            }
        }

        /// URL of this component's `latest.txt`.
        fn latest_url(&self) -> String {
            format!("{}/{}/latest.txt", base_url(), self.key())
        }

        /// The latest published version of this component.
        pub async fn latest_version(&self) -> Result<Version> {
            let url = self.latest_url();
            let text = http_get_text(&url)
                .await
                .with_context(|| format!("could not check the latest {} version", self.label()))?;
            let text = text.trim();
            Version::parse(text.trim_start_matches('v'))
                .with_context(|| format!("unexpected content in {url}: {text:?}"))
        }
    }

    /// Where releases are downloaded from. `FLARE_BASE_URL` overrides it (mirrors, testing).
    pub fn base_url() -> String {
        std::env::var("FLARE_BASE_URL")
            .unwrap_or_else(|_| DEFAULT_BASE_URL.to_string())
            .trim_end_matches('/')
            .to_string()
    }

    pub fn base_dir() -> Result<PathBuf> {
        Ok(dirs::home_dir()
            .context("failed to determine home directory")?
            .join(".flaredb"))
    }

    /// File name of the installed binary for a version, e.g. `flaredb-0.3.3`.
    pub fn binary_file_name(version: &Version) -> String {
        if cfg!(windows) {
            format!("{BINARY_PREFIX}{version}.exe")
        } else {
            format!("{BINARY_PREFIX}{version}")
        }
    }

    /// Parses `flaredb-0.3.3` (or `flaredb-0.3.3.exe`). Anything else, such as
    /// leftover `.tmp` files or archives, returns None.
    pub fn parse_binary_name(name: &str) -> Option<Version> {
        let rest = name.strip_prefix(BINARY_PREFIX)?;
        let rest = rest.strip_suffix(".exe").unwrap_or(rest);
        Version::parse(rest).ok()
    }

    /// Scans `bin_dir` for files whose name `parse` accepts, sorted oldest to newest.
    fn scan_installed(
        bin_dir: &Path,
        parse: impl Fn(&str) -> Option<Version>,
    ) -> Result<Vec<Installed>> {
        let mut found = Vec::new();
        let entries = match fs::read_dir(bin_dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(found),
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("failed to read directory {}", bin_dir.display()));
            }
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(version) = parse(&name) {
                found.push(Installed { version, path });
            }
        }
        found.sort_by(|a, b| a.version.cmp(&b.version));
        Ok(found)
    }

    /// Installed FlareDB binaries, sorted from oldest to newest.
    pub fn installed(bin_dir: &Path) -> Result<Vec<Installed>> {
        scan_installed(bin_dir, parse_binary_name)
    }

    /// The newest installed FlareDB version, if any.
    pub fn current(bin_dir: &Path) -> Result<Option<Installed>> {
        Ok(installed(bin_dir)?.pop())
    }

    /// File name of the worker jar for a version.
    pub fn worker_jar_name(version: &Version) -> String {
        format!("{WORKER_JAR_PREFIX}{version}{WORKER_JAR_SUFFIX}")
    }

    /// Parses `beam-sdks-java-harness-2.76.0-flare-bundled.jar`. Anything else returns None.
    pub fn parse_worker_jar_name(name: &str) -> Option<Version> {
        let rest = name.strip_prefix(WORKER_JAR_PREFIX)?;
        let rest = rest.strip_suffix(WORKER_JAR_SUFFIX)?;
        Version::parse(rest).ok()
    }

    /// Installed worker jars, sorted from oldest to newest.
    pub fn installed_java_worker(bin_dir: &Path) -> Result<Vec<Installed>> {
        scan_installed(bin_dir, parse_worker_jar_name)
    }

    /// The newest installed worker jar, if any.
    pub fn current_java_worker(bin_dir: &Path) -> Result<Option<Installed>> {
        Ok(installed_java_worker(bin_dir)?.pop())
    }

    pub async fn http_get_text(url: &str) -> Result<String> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .context("failed to build HTTP client")?;
        let response = client
            .get(url)
            .send()
            .await
            .with_context(|| format!("failed to reach {url}"))?
            .error_for_status()
            .with_context(|| format!("unexpected response from {url}"))?;
        response
            .text()
            .await
            .with_context(|| format!("failed to read response from {url}"))
    }

    /// `flare version` — installed versions plus available updates.
    pub async fn version() -> Result<()> {
        let bin_dir = base_dir()?.join("bin");
        let cli =
            Version::parse(env!("CARGO_PKG_VERSION")).context("could not parse CLI version")?;
        let db = current(&bin_dir)?;
        let worker = current_java_worker(&bin_dir)?;

        println!("Current versions:");
        println!("flare-cli:         {cli}");
        match &db {
            Some(c) => println!("FlareDB:           {}", c.version),
            None => println!("FlareDB:           none"),
        }
        match &worker {
            Some(w) => println!("beam-java-worker:  {}", w.version),
            None => println!("beam-java-worker:  none"),
        }

        println!();
        println!("Checking for updates...");
        println!();
        println!("Status:");
        println!();

        report_update(Component::Db, db.map(|i| i.version)).await;
        report_update(Component::JavaWorker, worker.map(|w| w.version)).await;
        report_update(Component::Cli, Some(cli)).await;

        Ok(())
    }

    /// Fetches the latest version for `component` and reports how `current` compares.
    async fn report_update(component: Component, current: Option<Version>) {
        let label = component.label();
        let latest = match component.latest_version().await {
            Ok(v) => v,
            Err(e) => {
                println!("{label}: could not check for updates ({e:#})");
                return;
            }
        };
        let hint = match component {
            Component::Db => "flare update --db",
            Component::JavaWorker => "flare update --java-worker",
            Component::Cli => "curl -sSL https://install.flare-db.com | bash",
        };
        match current {
            None => println!("{label}: {latest} available but not installed (run '{hint}')"),
            Some(c) if latest > c => {
                println!("{label}: update available {c} -> {latest} (run '{hint}')")
            }
            Some(c) if latest < c => {
                println!("{label}: newer than latest release ({c} vs {latest})")
            }
            Some(c) => println!("{label}: up to date ({c})"),
        }
    }

    /// Prints an install-script hint only when a newer CLI exists.
    async fn suggest_cli_update() {
        let Ok(current) = Version::parse(env!("CARGO_PKG_VERSION")) else {
            return;
        };
        let Ok(latest) = Component::Cli.latest_version().await else {
            return;
        };
        if latest > current {
            println!();
            println!("A newer flare-cli ({latest}) is available (you have {current}).");
            println!("Update it by running the install script:");
            println!("  curl -sSL https://install.flare-db.com | bash");
        }
    }

    /// Removes every installed binary except `keep`. Failures are reported, not fatal.
    pub fn prune_except(all: &[Installed], keep: &Version) -> Vec<String> {
        let mut removed = Vec::new();
        for old in all {
            if &old.version == keep {
                continue;
            }
            match fs::remove_file(&old.path) {
                Ok(()) => removed.push(old.version.to_string()),
                Err(e) => eprintln!("warning: could not remove {}: {e}", old.path.display()),
            }
        }
        removed
    }

    /// `flare update [--db] [--java-worker]`
    pub async fn update(db: bool, java_worker: bool) -> Result<()> {
        let base_dir = base_dir()?;
        let bin_dir = base_dir.join("bin");

        // Never swap binaries under a running instance.
        let state_path = state::state_path(&base_dir);
        if state_path.exists() {
            if let Ok(st) = state::load_state(&state_path) {
                if process_control::is_alive(st.pid) {
                    bail!(
                        "FlareDB is running (pid {}). Run 'flare down' first, then 'flare update'.",
                        st.pid
                    );
                }
            }
        }

        // With no flags, update everything that can be updated.
        let (update_db, update_java) = match (db, java_worker) {
            (false, false) => (true, true),
            (db, java) => (db, java),
        };

        fs::create_dir_all(&bin_dir)
            .with_context(|| format!("failed to create bin directory {}", bin_dir.display()))?;

        if update_db {
            update_db_component(&bin_dir).await?;
        }
        if update_java {
            update_java_worker_component(&bin_dir).await?;
        }

        suggest_cli_update().await;
        Ok(())
    }

    async fn update_db_component(bin_dir: &Path) -> Result<()> {
        let latest = Component::Db.latest_version().await?;
        let all = installed(bin_dir)?;

        if let Some(highest) = all.last().map(|i| i.version.clone()) {
            if highest >= latest {
                println!("FlareDB {highest} is already the latest version.");
                // Tidy up any older binaries left behind.
                for v in prune_except(&all, &highest) {
                    println!("Removed old version {v}");
                }
                return Ok(());
            }
            println!("Updating FlareDB {highest} -> {latest}");
        } else {
            println!("Installing FlareDB {latest}");
        }

        let new_path = crate::init::install_flaredb(&latest, bin_dir).await?;

        // Only now that the new binary is in place and verified, remove the old ones.
        let removed = prune_except(&all, &latest);
        println!();
        println!("FlareDB {latest} installed at {}", new_path.display());
        for v in &removed {
            println!("Removed old version {v}");
        }
        println!("Run 'flare up' to start it.");
        Ok(())
    }

    async fn update_java_worker_component(bin_dir: &Path) -> Result<()> {
        let latest = Component::JavaWorker.latest_version().await?;
        let all = installed_java_worker(bin_dir)?;

        if let Some(highest) = all.last().map(|w| w.version.clone()) {
            if highest >= latest {
                println!("beam-java-worker {highest} is already the latest version.");
                for v in prune_except(&all, &highest) {
                    println!("Removed old version {v}");
                }
                return Ok(());
            }
            println!("Updating beam-java-worker {highest} -> {latest}");
        } else {
            println!("Installing beam-java-worker {latest}");
        }

        let new_path = crate::init::install_java_worker(&latest, bin_dir).await?;
        let removed = prune_except(&all, &latest);
        println!(
            "beam-java-worker {latest} installed at {}",
            new_path.display()
        );
        for v in &removed {
            println!("Removed old version {v}");
        }
        println!("Restart 'flare up' to pick up the new worker jar.");
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn temp_dir() -> PathBuf {
            let dir = std::env::temp_dir().join(format!("flare-test-{}", uuid::Uuid::new_v4()));
            fs::create_dir_all(&dir).unwrap();
            dir
        }

        #[test]
        fn parses_binary_names() {
            assert_eq!(
                parse_binary_name("flaredb-0.3.2"),
                Some(Version::new(0, 3, 2))
            );
            assert_eq!(
                parse_binary_name("flaredb-0.3.2.exe"),
                Some(Version::new(0, 3, 2))
            );
            assert!(parse_binary_name("flaredb-0.3.2.tmp").is_none());
            assert!(parse_binary_name("flaredb-x86_64-unknown-linux-gnu.tar.xz").is_none());
            assert!(parse_binary_name("beam-sdks-java-harness-2.76.0-flare-bundled.jar").is_none());
            assert!(parse_binary_name("flaredb").is_none());
        }

        #[test]
        fn picks_highest_by_semver_not_by_string() {
            let dir = temp_dir();
            for v in ["0.3.9", "0.3.10", "0.3.2", "0.4.0-rc.1"] {
                fs::write(dir.join(format!("flaredb-{v}")), b"x").unwrap();
            }
            fs::write(dir.join("flaredb-0.9.9.tmp"), b"x").unwrap();
            fs::write(dir.join("beam.jar"), b"x").unwrap();
            let cur = current(&dir).unwrap().unwrap();
            assert_eq!(cur.version, Version::parse("0.4.0-rc.1").unwrap());
            let all: Vec<String> = installed(&dir)
                .unwrap()
                .iter()
                .map(|i| i.version.to_string())
                .collect();
            assert_eq!(all, ["0.3.2", "0.3.9", "0.3.10", "0.4.0-rc.1"]);
            fs::remove_dir_all(&dir).unwrap();
        }

        #[test]
        fn prune_keeps_only_requested_version() {
            let dir = temp_dir();
            for v in ["0.3.2", "0.3.3"] {
                fs::write(dir.join(format!("flaredb-{v}")), b"x").unwrap();
            }
            let all = installed(&dir).unwrap();
            let removed = prune_except(&all, &Version::new(0, 3, 3));
            assert_eq!(removed, ["0.3.2"]);
            assert!(!dir.join("flaredb-0.3.2").exists());
            assert!(dir.join("flaredb-0.3.3").exists());
            fs::remove_dir_all(&dir).unwrap();
        }

        #[test]
        fn missing_bin_dir_means_nothing_installed() {
            let dir = std::env::temp_dir().join(format!("flare-none-{}", uuid::Uuid::new_v4()));
            assert!(current(&dir).unwrap().is_none());
        }

        #[test]
        fn parses_worker_jar_names() {
            assert_eq!(
                parse_worker_jar_name("beam-sdks-java-harness-2.76.0-flare-bundled.jar"),
                Some(Version::new(2, 76, 0))
            );
            assert!(
                parse_worker_jar_name("beam-sdks-java-harness-2.76.0-flare-bundled.jar.tmp")
                    .is_none()
            );
            assert!(parse_worker_jar_name("flaredb-2.76.0").is_none());
            assert!(parse_worker_jar_name("beam.jar").is_none());
        }

        #[test]
        fn picks_newest_worker_jar() {
            let dir = temp_dir();
            for v in ["2.74.0", "2.76.0", "2.75.1"] {
                fs::write(
                    dir.join(format!("beam-sdks-java-harness-{v}-flare-bundled.jar")),
                    b"x",
                )
                .unwrap();
            }
            // A FlareDB binary must not be picked up as a worker jar.
            fs::write(dir.join("flaredb-0.3.2"), b"x").unwrap();
            let cur = current_java_worker(&dir).unwrap().unwrap();
            assert_eq!(cur.version, Version::parse("2.76.0").unwrap());
            fs::remove_dir_all(&dir).unwrap();
        }
    }
}

pub mod init {
    use anyhow::bail;
    //#[cfg(not(unix))]
    //use anyhow::bail;
    use crate::release;
    use anyhow::{Context, Result};
    use indicatif::{ProgressBar, ProgressStyle};
    use semver::Version;
    use sha2::{Digest, Sha256};
    use std::fs;
    use std::io;
    use std::io::Read;
    use std::path::{Path, PathBuf};
    use std::time::Duration;
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

        match release::current(&bin_dir)? {
            Some(installed) => {
                println!(
                    "FlareDB {} already exists at {}",
                    installed.version,
                    installed.path.display()
                );
                println!("Run 'flare version' to check for updates, or 'flare update' to upgrade.");
            }
            None => {
                let latest = release::Component::Db.latest_version().await?;
                println!("Installing FlareDB {latest}");
                install_flaredb(&latest, &bin_dir).await?;
            }
        }

        match release::current_java_worker(&bin_dir)? {
            Some(installed) => {
                println!("Worker jar already exists at {}", installed.path.display());
                println!(
                    "Run 'flare version' to check for updates, or 'flare update --java-worker' to upgrade."
                );
            }
            None => {
                let latest = release::Component::JavaWorker.latest_version().await?;
                println!("Installing beam-java-worker {latest}");
                install_java_worker(&latest, &bin_dir).await?;
            }
        }

        Ok(())
    }

    /// Downloads FlareDB `version` from R2, verifies its checksum, and installs it
    /// as `<bin_dir>/flaredb-<version>`. Nothing is left behind if any step fails.
    pub async fn install_flaredb(version: &Version, bin_dir: &Path) -> Result<PathBuf> {
        let (asset_filename, archive_type) = detect_flaredb_asset()?;
        let binary_name = release::binary_file_name(version);
        let final_path = bin_dir.join(&binary_name);
        let tmp_binary = bin_dir.join(format!("{binary_name}.tmp"));
        let archive_path = bin_dir.join(&asset_filename);
        let url = format!(
            "{}/flaredb/{}/{}",
            release::base_url(),
            version,
            asset_filename
        );

        let result: Result<()> = async {
            download_with_progress(&url, &archive_path).await?;
            verify_sha256(&archive_path, &format!("{url}.sha256")).await?;
            extract_archive(&archive_path, &tmp_binary, archive_type)
                .await
                .with_context(|| format!("failed to extract archive {}", archive_path.display()))?;
            fs::rename(&tmp_binary, &final_path).with_context(|| {
                format!(
                    "failed to move {} to {}",
                    tmp_binary.display(),
                    final_path.display()
                )
            })?;
            Ok(())
        }
        .await;

        // Always clean up scratch files (the .tmp is already gone after a successful rename).
        let _ = fs::remove_file(&archive_path);
        let _ = fs::remove_file(&tmp_binary);

        result?;
        Ok(final_path)
    }

    /// Downloads the Beam Java worker jar `version` from R2, verifies its checksum,
    /// and installs it as `<bin_dir>/<jar name>`. Nothing is left behind on failure.
    pub async fn install_java_worker(version: &Version, bin_dir: &Path) -> Result<PathBuf> {
        let jar_name = release::worker_jar_name(version);
        let final_path = bin_dir.join(&jar_name);
        let tmp_path = bin_dir.join(format!("{jar_name}.tmp"));
        let url = format!(
            "{}/beam-worker-java/{}/{}",
            release::base_url(),
            version,
            jar_name
        );

        let result: Result<()> = async {
            download_with_progress(&url, &tmp_path).await?;
            verify_sha256(&tmp_path, &format!("{url}.sha256")).await?;
            fs::rename(&tmp_path, &final_path).with_context(|| {
                format!(
                    "failed to move {} to {}",
                    tmp_path.display(),
                    final_path.display()
                )
            })?;
            Ok(())
        }
        .await;

        // Always clean up the scratch file (already gone after a successful rename).
        let _ = fs::remove_file(&tmp_path);

        result?;
        Ok(final_path)
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

        let mut response = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(15))
            .build()
            .context("failed to build HTTP client")?
            .get(url)
            .send()
            .await
            .with_context(|| format!("failed to download {url}"))?
            // Without this, a 404 page would be saved as if it were the file.
            .error_for_status()
            .with_context(|| format!("download failed for {url}"))?;

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

    /// Lowercase hex encoding of a byte slice.
    fn to_hex(bytes: &[u8]) -> String {
        use std::fmt::Write;
        let mut s = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            let _ = write!(s, "{b:02x}");
        }
        s
    }

    /// Compares the archive's SHA-256 with the `.sha256` file published next to it.
    async fn verify_sha256(archive: &Path, checksum_url: &str) -> Result<()> {
        let text = release::http_get_text(checksum_url)
            .await
            .context("could not download the checksum file")?;
        let expected = text
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_lowercase();

        let path = archive.to_owned();
        let actual = tokio::task::spawn_blocking(move || -> Result<String> {
            let mut file = fs::File::open(&path)
                .with_context(|| format!("failed to open {}", path.display()))?;
            let mut hasher = Sha256::new();
            let mut buf = [0u8; 64 * 1024];
            loop {
                let n = file
                    .read(&mut buf)
                    .with_context(|| format!("failed to hash {}", path.display()))?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            Ok(to_hex(&hasher.finalize()))
        })
        .await
        .context("checksum task failed")??;

        if expected != actual {
            bail!("checksum mismatch (expected {expected}, got {actual}). Nothing was installed.");
        }
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
        let mut reader = std::io::BufReader::new(file);
        let mut decompressed = Vec::new();
        lzma_rs::xz_decompress(&mut reader, &mut decompressed)
            .map_err(|e| anyhow::anyhow!("failed to decompress xz: {:?}", e))?;
        let mut archive = tar::Archive::new(&decompressed[..]);
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
    use super::release;
    use super::state;
    use anyhow::{Context, Result, bail};
    use std::fs::{self, OpenOptions};
    use std::process::Stdio;
    use tokio::net::TcpStream;
    use tokio::process::Command;
    use tokio::time::{Duration, sleep};
    use uuid::Uuid;

    const PORT: u16 = 8099;

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

        // Use the newest installed FlareDB version.
        let binary_path = match release::current(&bin_dir)? {
            Some(installed) => installed.path,
            None => bail!(
                "Missing FlareDB binary in {}. Run 'flare init' first.",
                bin_dir.display()
            ),
        };
        let worker_jar_path = match release::current_java_worker(&bin_dir)? {
            Some(installed) => installed.path,
            None => bail!(
                "Missing worker jar in {}. Run 'flare init' first.",
                bin_dir.display()
            ),
        };

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
                "FlareDB did not start. Check log at {}",
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

        println!();
        println!("\x1b[1mFlared up! 🔥\x1b[0m");
        println!();
        println!();
        println!("A <JOB_ID> is generated automatically for each submitted job.");
        println!("Use the <JOB_ID> to view logs for a specific job.");
        println!();
        println!("Usage:");
        println!("  flare logs                  # Stream logs for the most recent job");
        println!("  flare logs <JOB_ID>         # Stream logs for a specific job");
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

        println!("FlareDB instance stopped");
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

pub mod logs {
    use anyhow::{Context, Result, bail};
    use std::io::Write;
    use std::path::PathBuf;
    use std::time::Duration;
    use tokio::io::AsyncBufReadExt;
    use tokio::time::sleep;

    use crate::state;

    pub async fn stream_logs(target_job_id: Option<String>, follow: bool) -> Result<()> {
        let home_dir = dirs::home_dir().context("failed to determine home directory")?;
        let base_dir = home_dir.join(".flaredb");
        let state_path = state::state_path(&base_dir);

        let state_opt = if state_path.exists() {
            state::load_state(&state_path).ok()
        } else {
            None
        };

        let clean_id = |id: &str| -> String {
            let s = id
                .trim()
                .trim_matches(|c| c == '"' || c == '\'' || c == '=');
            if s.starts_with('-') {
                s.trim_start_matches('-').to_string()
            } else {
                s.to_string()
            }
        };

        let is_match = |job_id: &str, req: &str| -> bool {
            let j = job_id.to_lowercase();
            let r = clean_id(req).to_lowercase();
            if j.is_empty() || r.is_empty() {
                return false;
            }
            j == r
                || j.contains(&r)
                || r.contains(&j)
                || (j.starts_with("jobid") && j[5..].starts_with(&r))
                || (r.starts_with("jobid") && r[5..].starts_with(&j))
                || (r.starts_with("obid") && j.contains(&r[4..]))
        };

        let (resolved_job_id, log_path) = match target_job_id {
            Some(ref req_id) => {
                let mut found = None;

                // 1. Search state.json
                if let Some(ref st) = state_opt {
                    if let Some(j) = st.jobs.iter().find(|j| is_match(&j.id, req_id)) {
                        found = Some((j.id.clone(), PathBuf::from(&j.worker_log)));
                    }
                }

                // 2. Search filesystem instances directory
                if found.is_none() {
                    let instances_dir = base_dir.join("instances");
                    if instances_dir.exists() {
                        if let Ok(inst_entries) = std::fs::read_dir(&instances_dir) {
                            'search: for inst_entry in inst_entries.flatten() {
                                let jobs_dir = inst_entry.path().join("jobs");
                                if jobs_dir.exists() {
                                    if let Ok(job_entries) = std::fs::read_dir(&jobs_dir) {
                                        for job_entry in job_entries.flatten() {
                                            let folder_name =
                                                job_entry.file_name().to_string_lossy().to_string();
                                            if is_match(&folder_name, req_id) {
                                                let log1 = job_entry
                                                    .path()
                                                    .join("logs")
                                                    .join("flare-worker.log");
                                                let log2 = job_entry
                                                    .path()
                                                    .join("logs")
                                                    .join("flare-worker.logs");
                                                if log1.exists() {
                                                    found = Some((folder_name, log1));
                                                    break 'search;
                                                } else if log2.exists() {
                                                    found = Some((folder_name, log2));
                                                    break 'search;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                match found {
                    Some(res) => res,
                    None => {
                        let mut available = Vec::new();
                        if let Some(ref st) = state_opt {
                            available.extend(st.jobs.iter().map(|j| j.id.clone()));
                        }
                        if available.is_empty() {
                            let instances_dir = base_dir.join("instances");
                            if let Ok(inst_entries) = std::fs::read_dir(&instances_dir) {
                                for inst_entry in inst_entries.flatten() {
                                    let jobs_dir = inst_entry.path().join("jobs");
                                    if let Ok(job_entries) = std::fs::read_dir(&jobs_dir) {
                                        for job_entry in job_entries.flatten() {
                                            available.push(
                                                job_entry.file_name().to_string_lossy().to_string(),
                                            );
                                        }
                                    }
                                }
                            }
                        }

                        if !available.is_empty() {
                            bail!(
                                "Job ID '{}' not found. Available jobs:\n  {}",
                                req_id,
                                available.join("\n  ")
                            );
                        } else {
                            bail!(
                                "Job ID '{}' not found and no jobs were found in instance logs.",
                                req_id
                            );
                        }
                    }
                }
            }
            None => {
                let mut latest_job: Option<(String, PathBuf, std::time::SystemTime)> = None;

                if let Some(ref st) = state_opt {
                    if let Some(j) = st.jobs.last() {
                        let p = PathBuf::from(&j.worker_log);
                        if p.exists() {
                            let mtime = std::fs::metadata(&p)
                                .and_then(|m| m.modified())
                                .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
                            latest_job = Some((j.id.clone(), p, mtime));
                        }
                    }
                }

                if latest_job.is_none() {
                    let instances_dir = base_dir.join("instances");
                    if instances_dir.exists() {
                        if let Ok(inst_entries) = std::fs::read_dir(&instances_dir) {
                            for inst_entry in inst_entries.flatten() {
                                let jobs_dir = inst_entry.path().join("jobs");
                                if let Ok(job_entries) = std::fs::read_dir(&jobs_dir) {
                                    for job_entry in job_entries.flatten() {
                                        let folder_name =
                                            job_entry.file_name().to_string_lossy().to_string();
                                        let log1 =
                                            job_entry.path().join("logs").join("flare-worker.log");
                                        let log2 =
                                            job_entry.path().join("logs").join("flare-worker.logs");
                                        let target_log = if log1.exists() {
                                            Some(log1)
                                        } else if log2.exists() {
                                            Some(log2)
                                        } else {
                                            None
                                        };
                                        if let Some(log_path) = target_log {
                                            let mtime = std::fs::metadata(&log_path)
                                                .and_then(|m| m.modified())
                                                .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
                                            if latest_job
                                                .as_ref()
                                                .map_or(true, |(_, _, best_time)| {
                                                    mtime > *best_time
                                                })
                                            {
                                                latest_job = Some((folder_name, log_path, mtime));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                match latest_job {
                    Some((id, path, _)) => {
                        println!("Targeting most recent job: {}", id);
                        (id, path)
                    }
                    None => {
                        bail!("No jobs found. Run a job first or specify --job-id <job-id>.");
                    }
                }
            }
        };

        println!(
            "Streaming logs for job {} from {}...",
            resolved_job_id,
            log_path.display()
        );

        let mut attempts = 0;
        while !log_path.exists() && attempts < 20 {
            sleep(Duration::from_millis(250)).await;
            attempts += 1;
        }

        if !log_path.exists() {
            bail!("Log file not found at {}", log_path.display());
        }

        let file = tokio::fs::File::open(&log_path)
            .await
            .with_context(|| format!("failed to open log file {}", log_path.display()))?;

        let mut reader = tokio::io::BufReader::new(file);
        let mut line = String::new();
        let mut stdout = std::io::stdout();

        loop {
            line.clear();
            let bytes_read = reader
                .read_line(&mut line)
                .await
                .with_context(|| format!("failed to read line from {}", log_path.display()))?;

            if bytes_read > 0 {
                print!("{}", line);
                let _ = stdout.flush();
            } else {
                if !follow {
                    break;
                }
                sleep(Duration::from_millis(100)).await;
            }
        }

        Ok(())
    }
}
