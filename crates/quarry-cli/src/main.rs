//! `quarry`: the command-line front end.

use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "quarry", version = quarry::VERSION, about = "A SQL query engine over Parquet and CSV")]
struct Cli {}

fn main() {
    let _cli = Cli::parse();
}
