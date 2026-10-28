//! Times a fixed set of queries over an orders file made by `gen_orders`, in this process (no CLI start-up), and
//! prints a Markdown table.
//!
//! ```text
//! cargo run --release -p quarry --example time_queries -- PATH [RUNS] [--customers PATH] [--bound]
//! ```
//!
//! Each query runs once to warm up (the file in the page cache), then RUNS times (5 by default); the table has the
//! median and the fastest run, and rows per second at the median. A run during which the machine slept (macOS's
//! monotonic clock stops in sleep, the wall clock doesn't) is reported, so it can be thrown away. `--customers` adds
//! the join query (over `gen_orders`'s customers file); `--bound` runs the plans as bound, without the optimizer.

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
    (
        "order_id range (1 row group of 10)",
        "SELECT count(*), sum(total) FROM orders WHERE order_id <= 1000000",
    ),
];

const JOIN: (&str, &str) = (
    "join customers (10,000 rows), group by region",
    "SELECT c.region, count(*), sum(o.total) FROM orders o JOIN customers c ON o.customer = c.customer \
     GROUP BY c.region ORDER BY c.region",
);

fn main() {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let bound = args.iter().any(|a| a == "--bound");
    args.retain(|a| a != "--bound");
    let customers = match args.iter().position(|a| a == "--customers") {
        Some(i) if i + 1 < args.len() => {
            let path = args.remove(i + 1);
            args.remove(i);
            Some(path)
        }
        Some(_) => usage(),
        None => None,
    };
    let (path, runs) = match args.as_slice() {
        [path] => (path.as_str(), 5),
        [path, runs] => (path.as_str(), runs.parse().expect("RUNS is a number")),
        _ => usage(),
    };
    let mut catalog = Catalog::new();
    catalog.register_file("orders", path).unwrap();
    let mut queries = QUERIES.to_vec();
    if let Some(customers) = &customers {
        catalog.register_file("customers", customers).unwrap();
        queries.push(JOIN);
    }
    let run = |sql: &str| {
        let plan = quarry::sql::plan(&catalog, sql).unwrap();
        let plan = if bound {
            plan
        } else {
            quarry::optimizer::optimize(&plan).unwrap()
        };
        quarry::exec::collect(&plan).unwrap()
    };
    let rows = catalog
        .table("orders")
        .unwrap()
        .statistics()
        .num_rows
        .expect("a Parquet file knows its row count");
    println!("| Query | Result rows | Median | Fastest | Rows/s (median) |");
    println!("|---|---|---|---|---|");
    for (name, sql) in queries {
        let result_rows = run(sql).num_rows();
        let mut times: Vec<Duration> = Vec::with_capacity(runs);
        for _ in 0..runs {
            let (wall, mono) = (SystemTime::now(), Instant::now());
            let result = run(sql);
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

fn usage() -> ! {
    eprintln!("usage: time_queries PATH [RUNS] [--customers PATH] [--bound]");
    std::process::exit(2);
}
