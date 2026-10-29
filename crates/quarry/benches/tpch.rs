//! Criterion benchmarks: each TPC-H-style query (`crates/quarry-tpch`) end to end, from SQL text to rows, over Parquet
//! files at scale 0.1 (600,000 lines; `QUARRY_BENCH_SCALE` sets another), and the same queries planned only (parsed,
//! bound and optimized; no file is read, since registering a table read its footer once).
//!
//! ```text
//! cargo bench -p quarry --bench tpch
//! cargo bench -p quarry --bench tpch -- q19     # one query
//! ```
//!
//! The tables are generated into a temporary directory first (about half a second at scale 0.1). The files are read
//! from the OS's cache after the first run, so the numbers are decoding and computing, not disk.

use std::hint::black_box;
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use quarry::Catalog;
use quarry::optimizer::optimize;
use quarry_tpch::{QUERIES, TABLES};

fn scale() -> f64 {
    match std::env::var("QUARRY_BENCH_SCALE") {
        Ok(v) => v
            .parse()
            .unwrap_or_else(|_| panic!("QUARRY_BENCH_SCALE={v:?} is not a number")),
        Err(_) => 0.1,
    }
}

fn benches(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let scale = scale();
    quarry_tpch::generate(scale, dir.path()).unwrap();
    let mut catalog = Catalog::new();
    for table in TABLES {
        catalog
            .register_file(table, dir.path().join(format!("{table}.parquet")))
            .unwrap();
    }

    let mut run = c.benchmark_group(format!("tpch-sf{scale}"));
    run.sample_size(20).measurement_time(Duration::from_secs(4));
    for query in QUERIES {
        run.bench_function(query.name, |b| {
            b.iter(|| quarry::query(&catalog, black_box(query.sql)).unwrap())
        });
    }
    run.finish();

    let mut plan = c.benchmark_group("plan");
    for query in QUERIES {
        plan.bench_function(query.name, |b| {
            b.iter(|| {
                optimize(&quarry::sql::plan(&catalog, black_box(query.sql)).unwrap()).unwrap()
            })
        });
    }
    plan.finish();
}

criterion_group!(tpch, benches);
criterion_main!(tpch);
