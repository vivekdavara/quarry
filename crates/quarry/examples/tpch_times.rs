//! Times the TPC-H-style queries (`crates/quarry-tpch`) over the tables `tpch-gen` wrote, in this process (no CLI
//! start-up), and prints a Markdown table.
//!
//! ```text
//! cargo run --release -p quarry --example tpch_times -- DIR [RUNS]
//! ```
//!
//! Each query runs once to warm up (the files in the page cache), then RUNS times (5 by default); the table has the
//! median and the fastest run. A run during which the machine slept (macOS's monotonic clock stops in sleep, the wall
//! clock doesn't) is reported, so it can be thrown away. `scripts/tpch_duckdb_times.py` times DuckDB the same way.

use std::path::Path;
use std::time::{Duration, Instant, SystemTime};

use quarry::Catalog;
use quarry_tpch::{QUERIES, TABLES};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (dir, runs) = match args.as_slice() {
        [dir] => (dir.as_str(), 5),
        [dir, runs] => match runs.parse::<usize>() {
            Ok(n) if n > 0 => (dir.as_str(), n),
            _ => usage(),
        },
        _ => usage(),
    };
    let mut catalog = Catalog::new();
    for table in TABLES {
        catalog
            .register_file(table, Path::new(dir).join(format!("{table}.parquet")))
            .unwrap();
    }
    let lines = catalog
        .table("lineitem")
        .unwrap()
        .statistics()
        .num_rows
        .expect("a Parquet file knows its row count");
    println!("{lines} lines, median of {runs} runs after one warm-up run");
    println!();
    println!("| Query | Result rows | Median | Fastest |");
    println!("|---|---|---|---|");
    for query in QUERIES {
        let result_rows = quarry::query(&catalog, query.sql).unwrap().num_rows();
        let mut times: Vec<Duration> = Vec::with_capacity(runs);
        for _ in 0..runs {
            let (wall, mono) = (SystemTime::now(), Instant::now());
            let result = quarry::query(&catalog, query.sql).unwrap();
            let elapsed = mono.elapsed();
            let slept = wall.elapsed().unwrap_or_default().saturating_sub(elapsed);
            if slept > Duration::from_millis(100) {
                eprintln!("{}: the machine slept {slept:?} during a run", query.name);
            }
            assert_eq!(result.num_rows(), result_rows);
            times.push(elapsed);
        }
        times.sort();
        // The middle run; for an even RUNS, the mean of the two middle ones (as Python's statistics.median).
        let n = times.len();
        let median = if n % 2 == 1 {
            times[n / 2]
        } else {
            (times[n / 2 - 1] + times[n / 2]) / 2
        };
        println!(
            "| {} {} | {result_rows} | {:.1} ms | {:.1} ms |",
            query.name,
            query.title,
            median.as_secs_f64() * 1e3,
            times[0].as_secs_f64() * 1e3,
        );
    }
}

fn usage() -> ! {
    eprintln!("usage: tpch_times DIR [RUNS (at least 1)]");
    std::process::exit(2);
}
