//! `quarry`: the command-line front end.
//!
//! Exit status: 0 on success, 1 when the engine reports an error (printed to stderr as `error: ...`), 2 for a
//! malformed command line. `quarry repl` exits 1 if any statement or command in the session failed.

mod repl;

use std::io::IsTerminal;
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
    /// Read SQL statements (ending in `;`) and commands (`.help` lists them) from the terminal, or from a script on
    /// standard input, and run each against the given tables.
    Repl {
        /// A table: `name=path`, or just `path` to name it after the file (`.load` adds more).
        #[arg(short, long = "table", value_name = "NAME=PATH")]
        tables: Vec<String>,
        /// The most rows a batch holds, from the scans up.
        #[arg(long, value_name = "ROWS", default_value_t = DEFAULT_BATCH_SIZE as u32,
              value_parser = clap::value_parser!(u32).range(1..))]
        batch_size: u32,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    if let Command::Repl { tables, batch_size } = cli.command {
        return repl_main(&tables, batch_size as usize);
    }
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
            (BoundStatement::Query(plan), Some(path)) => {
                let result = exec::collect_with_batch_size(&optimize(&plan)?, batch_size as usize)?;
                write_parquet(&path, &result)?;
                Ok(format!(
                    "{} written to {}\n",
                    rows(result.num_rows()),
                    path.display()
                ))
            }
            (statement, None) => run_bound(statement, batch_size as usize),
        },
        Command::Repl { .. } => unreachable!("main runs the REPL"),
    }
}

/// `quarry repl`: the session's tables from `--table`, then lines from standard input.
fn repl_main(tables: &[String], batch_size: usize) -> ExitCode {
    let mut session = repl::Session::new(batch_size);
    for spec in tables {
        if let Err(e) = session.load(spec) {
            eprintln!("error: {e}");
            return ExitCode::from(1);
        }
    }
    let interactive = std::io::stdin().is_terminal();
    if interactive {
        println!(
            "quarry {}: SQL over Parquet and CSV. End each statement with ';'; .help lists the commands.",
            quarry::VERSION
        );
    }
    let (stdin, mut stdout, mut stderr) = (std::io::stdin(), std::io::stdout(), std::io::stderr());
    match repl::run(
        &mut session,
        stdin.lock(),
        &mut stdout,
        &mut stderr,
        interactive,
    ) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(1),
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(1)
        }
    }
}

/// Runs one SQL statement against the catalog as `quarry query` does: a query's rows as a table and their count,
/// or `EXPLAIN`'s optimized plan.
fn run_sql(catalog: &Catalog, sql: &str, batch_size: usize) -> Result<String> {
    run_bound(sql::bind(catalog, sql)?, batch_size)
}

fn run_bound(statement: BoundStatement, batch_size: usize) -> Result<String> {
    match statement {
        BoundStatement::Explain(plan) => Ok(exec::explain(&optimize(&plan)?)),
        BoundStatement::Query(plan) => {
            let result = exec::collect_with_batch_size(&optimize(&plan)?, batch_size)?;
            Ok(format!(
                "{}\n{}\n",
                result.to_table()?,
                rows(result.num_rows())
            ))
        }
    }
}

fn rows(n: usize) -> String {
    if n == 1 {
        "1 row".to_string()
    } else {
        format!("{n} rows")
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
            return Err(QuarryError::Table(format!("{spec}: expected NAME=PATH")));
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
            "{spec}: can't name a table after this path"
        ))),
    }
}

/// The result as one Parquet file (a single row group per batch the query made, uncompressed: these files are small
/// and short-lived).
fn write_parquet(path: &Path, result: &QueryResult) -> Result<()> {
    let file = std::fs::File::create(path)
        .map_err(|e| QuarryError::Table(format!("--output {}: {e}", path.display())))?;
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
    Ok(describe(open(file)?.as_ref()))
}

/// A table's columns and types, one a line, and for Parquet its rows and row groups.
fn describe(table: &dyn TableProvider) -> String {
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
    out
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
