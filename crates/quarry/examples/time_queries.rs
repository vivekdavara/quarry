//! Times a fixed set of queries over an orders file made by `gen_orders`, in this process (no CLI start-up), and
//! prints a Markdown table.
//!
//! ```text
//! cargo run --release -p quarry --example time_queries -- PATH [RUNS]
//! ```
//!
//! Each query runs once to warm up (the file in the page cache), then RUNS times (5 by default); the table has the
//! median and the fastest run, and rows per second at the median. A run during which the machine slept (macOS's
//! monotonic clock stops in sleep, the wall clock doesn't) is reported, so it can be thrown away.

use std::time::{Duration, Instant, SystemTime};

use quarry::Catalog;

const QUERIES: &[(&str, &str)] = &[
    ("count", "SELECT count(*) FROM orders"),
    (
        "filter + count",
        "SELECT count(*) FROM orders WHERE status = 'shipped' AND total > 500",
    ),
    (
        "group by status (3 groups)",
        "SELECT status, count(*), sum(total), avg(qty) FROM orders GROUP BY status ORDER BY status",
    ),
    (
        "group by customer (10,000 groups), top 5",
        "SELECT customer, sum(total) AS spent FROM orders WHERE placed >= '2023-01-01' \
         GROUP BY customer ORDER BY spent DESC LIMIT 5",
    ),
    (
        "arithmetic + filter",
        "SELECT sum(total * qty), avg(qty * 2 + 1) FROM orders WHERE placed >= '2023-01-01'",
    ),
    (
        "count distinct",
        "SELECT count(DISTINCT customer) FROM orders",
    ),
    (
        "sort everything, top 10",
        "SELECT order_id, total FROM orders ORDER BY total DESC, order_id LIMIT 10",
    ),
];

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (path, runs) = match args.as_slice() {
        [_, path] => (path.as_str(), 5),
        [_, path, runs] => (path.as_str(), runs.parse().expect("RUNS is a number")),
        _ => {
            eprintln!("usage: time_queries PATH [RUNS]");
            std::process::exit(2);
        }
    };
    let mut catalog = Catalog::new();
    catalog.register_file("orders", path).unwrap();
    let rows = catalog
        .table("orders")
        .unwrap()
        .statistics()
        .num_rows
        .expect("a Parquet file knows its row count");
    println!("| Query | Result rows | Median | Fastest | Rows/s (median) |");
    println!("|---|---|---|---|---|");
    for (name, sql) in QUERIES {
        let result_rows = quarry::query(&catalog, sql).unwrap().num_rows();
        let mut times: Vec<Duration> = Vec::with_capacity(runs);
        for _ in 0..runs {
            let (wall, mono) = (SystemTime::now(), Instant::now());
            let result = quarry::query(&catalog, sql).unwrap();
            let elapsed = mono.elapsed();
            let slept = wall.elapsed().unwrap_or_default().saturating_sub(elapsed);
            if slept > Duration::from_millis(100) {
                eprintln!("{name}: the machine slept {slept:?} during a run");
            }
            assert_eq!(result.num_rows(), result_rows);
            times.push(elapsed);
        }
        times.sort();
        let median = times[times.len() / 2];
        let per_second = rows as f64 / median.as_secs_f64();
        println!(
            "| {name} | {result_rows} | {:.1} ms | {:.1} ms | {:.0} M |",
            median.as_secs_f64() * 1e3,
            times[0].as_secs_f64() * 1e3,
            per_second / 1e6
        );
    }
}
