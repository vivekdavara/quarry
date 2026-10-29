//! `quarry`: the command-line front end.
//!
//! Exit status: 0 on success, 1 when the engine reports an error (printed to stderr as `error: ...`), 2 for a
//! malformed command line.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use quarry::arrow::record_batch::RecordBatch;
use quarry::arrow::util::pretty::pretty_format_batches;
use quarry::datasource::{DEFAULT_BATCH_SIZE, TableProvider};
use quarry::exec;
use quarry::optimizer::optimize;
use quarry::parquet::arrow::ArrowWriter;
use quarry::sql::{self, BoundStatement};
use quarry::{Catalog, QuarryError, QueryResult, Result};

#[derive(Debug, Parser)]
#[command(name = "quarry", version = quarry::VERSION, about = "A SQL query engine over Parquet and CSV")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Print a Parquet or CSV file's columns and types (and, for Parquet, its rows and row groups).
    Schema { file: PathBuf },
    /// Print the first rows of a Parquet or CSV file.
    Head {
        file: PathBuf,
        /// How many rows.
        #[arg(short = 'n', long, default_value_t = 10)]
        rows: usize,
    },
    /// Bind a query against the given tables and print its logical plan as bound, before the optimizer (nothing is
    /// read but the files' schemas).
    Plan {
        /// A table: `name=path`, or just `path` to name it after the file (`data/lineitem.parquet` is `lineitem`).
        #[arg(short, long = "table", value_name = "NAME=PATH")]
        tables: Vec<String>,
        /// The query (`EXPLAIN SELECT ...` works too).
        sql: String,
    },
    /// Run a query against the given tables and print its result as a table (`EXPLAIN SELECT ...` prints the
    /// optimized plan as it runs instead: hash or nested-loop joins, top-k sorts).
    Query {
        /// A table: `name=path`, or just `path` to name it after the file.
        #[arg(short, long = "table", value_name = "NAME=PATH")]
        tables: Vec<String>,
        /// Write the result to this Parquet file instead of printing it: its column names and types, NULLs and floats
        /// exactly (for scripts, such as scripts/differential.py). An existing file is replaced.
        #[arg(short, long, value_name = "FILE")]
        output: Option<PathBuf>,
        /// The most rows a batch holds, from the scans up. The answer doesn't depend on it; scripts/differential.py
        /// varies it to check that against DuckDB.
        #[arg(long, value_name = "ROWS", default_value_t = DEFAULT_BATCH_SIZE as u32,
              value_parser = clap::value_parser!(u32).range(1..))]
        batch_size: u32,
        /// The query (after `--` if it starts with `-`, as a SQL comment does).
        sql: String,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli.command) {
        Ok(output) => {
            print!("{output}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(1)
        }
    }
}

fn run(command: Command) -> Result<String> {
    match command {
        Command::Schema { file } => schema(&file),
        Command::Head { file, rows } => head(&file, rows),
        Command::Plan { tables, sql } => {
            let plan = match sql::bind(&catalog(&tables)?, &sql)? {
                BoundStatement::Query(plan) | BoundStatement::Explain(plan) => plan,
            };
            Ok(plan.to_string())
        }
        Command::Query {
            tables,
            output,
            batch_size,
            sql,
        } => match (sql::bind(&catalog(&tables)?, &sql)?, output) {
            (BoundStatement::Explain(_), Some(_)) => Err(QuarryError::Plan(
                "--output takes a query's rows; EXPLAIN prints a plan".to_string(),
            )),
            (BoundStatement::Explain(plan), None) => Ok(exec::explain(&optimize(&plan)?)),
            (BoundStatement::Query(plan), output) => {
                let result = exec::collect_with_batch_size(&optimize(&plan)?, batch_size as usize)?;
                let rows = result.num_rows();
                let noun = if rows == 1 { "row" } else { "rows" };
                match output {
                    Some(path) => {
                        write_parquet(&path, &result)?;
                        Ok(format!("{rows} {noun} written to {}\n", path.display()))
                    }
                    None => Ok(format!("{}\n{rows} {noun}\n", result.to_table()?)),
                }
            }
        },
    }
}

/// A catalog of `--table` arguments.
fn catalog(tables: &[String]) -> Result<Catalog> {
    let mut catalog = Catalog::new();
    for spec in tables {
        let (name, path) = parse_table(spec)?;
        catalog.register_file(&name, path)?;
    }
    Ok(catalog)
}

/// `name=path`, or a bare path named after its file stem.
fn parse_table(spec: &str) -> Result<(String, PathBuf)> {
    if let Some((name, path)) = spec.split_once('=') {
        if name.is_empty() || path.is_empty() {
            return Err(QuarryError::Table(format!(
                "--table {spec}: expected NAME=PATH"
            )));
        }
        return Ok((name.to_string(), PathBuf::from(path)));
    }
    let path = PathBuf::from(spec);
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .filter(|s| !s.is_empty());
    match stem {
        Some(stem) => Ok((stem.to_string(), path.clone())),
        None => Err(QuarryError::Table(format!(
            "--table {spec}: can't name a table after this path"
        ))),
    }
}

/// The result as one Parquet file (a single row group per batch the query made, uncompressed: these files are small
/// and short-lived).
fn write_parquet(path: &Path, result: &QueryResult) -> Result<()> {
    let file = std::fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, result.schema.clone(), None)?;
    for batch in &result.batches {
        writer.write(batch)?;
    }
    writer.close()?;
    Ok(())
}

fn open(file: &Path) -> Result<Arc<dyn TableProvider>> {
    let mut catalog = Catalog::new();
    catalog.register_file("file", file)?;
    Ok(catalog.table("file").expect("just registered"))
}

fn schema(file: &Path) -> Result<String> {
    let table = open(file)?;
    let mut out = String::new();
    for field in table.schema().fields() {
        let null = if field.is_nullable() { "" } else { " not null" };
        out.push_str(&format!("{}: {}{null}\n", field.name(), field.data_type()));
    }
    let stats = table.statistics();
    if let Some(rows) = stats.num_rows {
        out.push_str(&format!("rows: {rows}\n"));
    }
    if !stats.row_group_rows.is_empty() {
        let groups: Vec<String> = stats.row_group_rows.iter().map(u64::to_string).collect();
        out.push_str(&format!(
            "row groups: {} ({} rows)\n",
            groups.len(),
            groups.join(", ")
        ));
    }
    Ok(out)
}

fn head(file: &Path, rows: usize) -> Result<String> {
    let table = open(file)?;
    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut have = 0;
    if rows > 0 {
        for batch in table.scan(None, rows.min(quarry::datasource::DEFAULT_BATCH_SIZE))? {
            let batch = batch?;
            let take = batch.num_rows().min(rows - have);
            have += take;
            batches.push(batch.slice(0, take));
            if have == rows {
                break;
            }
        }
    }
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(table.schema()));
    }
    Ok(format!("{}\n", pretty_format_batches(&batches)?))
}
