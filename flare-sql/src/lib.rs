pub mod commands;
pub mod repl;

use anyhow::{Context, Result};
use std::path::PathBuf;
use std::sync::Arc;

use paimon::{CatalogOptions, FileSystemCatalog, Options};
use paimon_datafusion::SQLContext;

/// Run the interactive SQL shell.
///
/// The catalog is rooted at FlareDB's `flare_warehouse` directory so the
/// shell operates on the same warehouse as the running FlareDB instance.
pub async fn run() -> Result<()> {
    let warehouse_path = flare_warehouse_dir();

    // Ensure the warehouse directory exists before the catalog writes to it.
    std::fs::create_dir_all(&warehouse_path)
        .context("Failed to create flare warehouse directory")?;

    let warehouse_uri = format!("file://{}", warehouse_path.display());

    // Initialize a filesystem catalog pointing at the warehouse.
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, &warehouse_uri);
    let catalog =
        Arc::new(FileSystemCatalog::new(options).context("Failed to create filesystem catalog")?);

    // Create the SQL context and register the catalog as "default".
    let mut ctx = SQLContext::new();
    ctx.register_catalog("default", catalog)
        .await
        .context("Failed to register catalog")?;

    // Launch the interactive REPL.
    repl::run(ctx).await
}

fn flare_warehouse_dir() -> PathBuf {
    let base_dir = match std::env::var("FLAREDB_BASE_DIR") {
        Ok(dir) => PathBuf::from(dir),
        Err(_) => {
            let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
            PathBuf::from(home).join(".flaredb")
        }
    };
    base_dir.join("flare_warehouse")
}
