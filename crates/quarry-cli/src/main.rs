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
use quarry::datasource::TableProvider;
use quarry::exec;
use quarry::sql::{self, BoundStatement};
use quarry::{Catalog, QuarryError, Result};

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
    /// Bind a query against the given tables and print its logical plan (nothing is read but the files' schemas).
    Plan {
        /// A table: `name=path`, or just `path` to name it after the file (`data/lineitem.parquet` is `lineitem`).
        #[arg(short, long = "table", value_name = "NAME=PATH")]
        tables: Vec<String>,
        /// The query (`EXPLAIN SELECT ...` works too).
        sql: String,
    },
    /// Run a query against the given tables and print its result as a table (`EXPLAIN SELECT ...` prints the plan
    /// instead).
    Query {
        /// A table: `name=path`, or just `path` to name it after the file.
        #[arg(short, long = "table", value_name = "NAME=PATH")]
        tables: Vec<String>,
        /// The query.
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
        Command::Query { tables, sql } => match sql::bind(&catalog(&tables)?, &sql)? {
            BoundStatement::Explain(plan) => Ok(plan.to_string()),
            BoundStatement::Query(plan) => {
                let result = exec::collect(&plan)?;
                let rows = result.num_rows();
                let noun = if rows == 1 { "row" } else { "rows" };
                Ok(format!("{}\n{rows} {noun}\n", result.to_table()?))
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
