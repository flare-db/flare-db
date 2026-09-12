use std::time::Instant;

use anyhow::{Context, Result};
use datafusion::arrow::array::BooleanBuilder;
use datafusion::arrow::{
    array::StringArray, compute::filter_record_batch, record_batch::RecordBatch,
    util::pretty::pretty_format_batches,
};
use paimon_datafusion::SQLContext;
use rustyline::{DefaultEditor, error::ReadlineError};

use crate::commands;

const PROMPT: &str = "flare> ";
const CONTINUATION_PROMPT: &str = "  ... | ";

/// Launch the interactive Read-Eval-Print Loop.
pub async fn run(ctx: SQLContext) -> Result<()> {
    let mut rl = DefaultEditor::new()?;

    // Set up persistent command history.
    let history_path = history_file()?;
    if let Some(parent) = history_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let _ = rl.load_history(&history_path);

    println!("FlareDB Interactive SQL Shell");
    println!("Type 'help' for usage hints.\n");

    loop {
        // Read the first line.
        let first_line = match rl.readline(PROMPT) {
            Ok(line) => line,
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("Bye!");
                rl.save_history(&history_path)?;
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        let first_trimmed = first_line.trim();

        // Handle built-in commands immediately (no semicolon needed).
        if let Some(cmd) = commands::parse(first_trimmed) {
            match cmd {
                commands::Command::Help => {
                    println!("{}", commands::help_text());
                }
                commands::Command::Exit => {
                    println!("Bye!");
                    rl.save_history(&history_path)?;
                    return Ok(());
                }
            }
            continue;
        }

        // Accumulate input across lines until we see a terminating semicolon.
        let mut buffer = first_line;
        if !buffer.trim_end().ends_with(';') {
            loop {
                match rl.readline(CONTINUATION_PROMPT) {
                    Ok(line) => {
                        // A line ending with ';' signals a complete statement.
                        if line.trim_end().ends_with(';') {
                            buffer.push(' ');
                            buffer.push_str(&line);
                            rl.add_history_entry(buffer.trim_end())?;
                            break;
                        }
                        // Multi-line: join with a space.
                        buffer.push(' ');
                        buffer.push_str(&line);
                    }
                    Err(ReadlineError::Interrupted) => {
                        println!("^C");
                        buffer.clear();
                        break;
                    }
                    Err(ReadlineError::Eof) => {
                        println!("Bye!");
                        rl.save_history(&history_path)?;
                        return Ok(());
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        } else {
            // Single-line statement: semicolon on the first line.
            rl.add_history_entry(buffer.trim_end())?;
        }

        let input = buffer.trim().to_string();

        // Skip empty input (e.g. after Ctrl+C).
        if input.is_empty() {
            continue;
        }

        // Everything else is SQL.
        execute_sql(&ctx, &input).await;
    }
}

/// Execute a single SQL statement through SQLContext and display the result.
async fn execute_sql(ctx: &SQLContext, sql: &str) {
    let start = Instant::now();

    match ctx.sql(sql).await {
        Ok(df) => match df.collect().await {
            Ok(batches) => {
                // Hide DataFusion internal catalogs from metadata queries.
                let batches = filter_internal_schemas(batches);
                let elapsed = start.elapsed();
                format_result(&batches, elapsed.as_millis());
            }
            Err(e) => {
                print_sql_error(&e.to_string());
            }
        },
        Err(e) => {
            print_sql_error(&e.to_string());
        }
    }
}

/// Format a vector of RecordBatches for display.
fn format_result(batches: &[RecordBatch], elapsed_ms: u128) {
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    // DDL / DML that returns the standard `ok_result()` empty DataFrame
    // with a single `result` column containing "OK".
    if let Some(batch) = batches.first() {
        if is_ok_response(batch) {
            println!("\nQuery OK ({elapsed_ms} ms)");
            return;
        }
    }

    // Empty result set (e.g. DML that affects 0 rows, or SELECT with no matches).
    if total_rows == 0 {
        println!("\nEmpty set ({elapsed_ms} ms)");
        return;
    }

    // Pretty-print via DataFusion's built-in formatter.
    match pretty_format_batches(batches) {
        Ok(table) => {
            println!();
            println!("{table}");
            if total_rows == 1 {
                println!("1 row returned ({elapsed_ms} ms)\n");
            } else {
                println!("{total_rows} rows returned ({elapsed_ms} ms)\n");
            }
        }
        Err(e) => {
            println!("\nError formatting result: {e}");
        }
    }
}

/// Detect the standard DDL/DML response: a single-row, single-column
/// RecordBatch whose sole value is "OK".
fn is_ok_response(batch: &RecordBatch) -> bool {
    if batch.num_columns() != 1 || batch.num_rows() != 1 {
        return false;
    }
    if batch.schema().field(0).name() != "result" {
        return false;
    }
    batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .is_some_and(|arr| arr.value(0) == "OK")
}

/// Hide DataFusion internal catalogs from metadata queries like
/// `SHOW TABLES` so users only see their own tables.
fn filter_internal_schemas(batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
    const HIDDEN: &[&str] = &["information_schema", "datafusion"];

    batches
        .into_iter()
        .filter_map(|batch| {
            let schema = batch.schema();
            let cat_idx = schema.index_of("table_catalog").ok();
            let sch_idx = schema.index_of("table_schema").ok();

            match (cat_idx, sch_idx) {
                (Some(ci), Some(si)) => {
                    let cats = batch
                        .column(ci)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("table_catalog must be Utf8");
                    let schemas = batch
                        .column(si)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("table_schema must be Utf8");

                    let mut builder = BooleanBuilder::with_capacity(batch.num_rows());
                    for i in 0..batch.num_rows() {
                        let keep = !(HIDDEN.contains(&cats.value(i))
                            || HIDDEN.contains(&schemas.value(i)));
                        builder.append_value(keep);
                    }
                    let mask = builder.finish();

                    match filter_record_batch(&batch, &mask) {
                        Ok(f) if f.num_rows() > 0 => Some(f),
                        _ => None,
                    }
                }
                _ => Some(batch),
            }
        })
        .collect()
}

/// Print a human-readable SQL error.
fn print_sql_error(msg: &str) {
    // DataFusion errors typically arrive as multi-line strings;
    // strip redundant blank lines for readability.
    let cleaned = msg.trim();
    println!("\nError:\n\n{cleaned}\n");
}

/// Path to the persistent history file.
fn history_file() -> Result<std::path::PathBuf> {
    let home = dirs::home_dir().context("Could not determine home directory")?;
    Ok(home.join(".flaredb").join("sql_history.txt"))
}
