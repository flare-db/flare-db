use anyhow::Result;
use clap::{Parser, Subcommand};

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
    Init,

    Up,

    Down,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init => init::init().await?,
        Commands::Up => {}
        Commands::Down => {}
    }

    Ok(())
}

pub mod init {
    use anyhow::{Context, Result, bail};
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
        let flaredb_version = "0.1.4";
        let binary_name = if cfg!(windows) {
            format!("flaredb-{}.exe", flaredb_version)
        } else {
            format!("flaredb-{}", flaredb_version)
        };
        let binary_path = bin_dir.join(&binary_name);

        if binary_path.exists() {
            println!("FlareDB binary already exists at {}", binary_path.display());
        } else {
            let archive_path = bin_dir.join(&asset_filename);
            let download_url = format!(
                "https://github.com/flare-db/flare-db/releases/download/flaredb-v0.1.4/{}",
                asset_filename
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
