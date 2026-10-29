//! The TPC-H-style queries (`crates/quarry-tpch`) over the scale-0.01 tables, against DuckDB 2.0.0's answers: the same
//! column names, the same rows in the same order, integers, strings and dates equal, floats within a relative 1e-9
//! (DuckDB adds a sum's terms in another order). Each query runs at several batch sizes, and none of their plans is a
//! nested loop.

use std::io::Cursor;

use quarry::Catalog;
use quarry::arrow::array::{Array, AsArray, RecordBatch};
use quarry::arrow::compute::concat_batches;
use quarry::arrow::csv::ReaderBuilder;
use quarry::arrow::datatypes::{DataType, Float64Type};
use quarry::exec::{collect_with_batch_size, explain};
use quarry::optimizer::optimize;
use quarry_tpch::{ANSWER_SCALE, QUERIES, TABLES};

/// The tables in a temporary directory, and a catalog of them.
fn tables() -> (tempfile::TempDir, Catalog) {
    let dir = tempfile::tempdir().unwrap();
    quarry_tpch::generate(ANSWER_SCALE, dir.path()).unwrap();
    let mut catalog = Catalog::new();
    for table in TABLES {
        catalog
            .register_file(table, dir.path().join(format!("{table}.parquet")))
            .unwrap();
    }
    (dir, catalog)
}

/// The largest relative difference between two floats this test accepts.
const TOLERANCE: f64 = 1e-9;

/// Checks a result against a DuckDB answer and returns the largest relative difference of its floats.
fn compare(name: &str, result: &RecordBatch, answer: &str) -> f64 {
    let header = answer.lines().next().unwrap();
    let names: Vec<&str> = result
        .schema_ref()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(names.join(","), header, "{name}: column names");
    let reader = ReaderBuilder::new(result.schema())
        .with_header(true)
        .build(Cursor::new(answer))
        .unwrap();
    let batches: Vec<RecordBatch> = reader.map(Result::unwrap).collect();
    let expected = concat_batches(&result.schema(), &batches).unwrap();
    assert_eq!(result.num_rows(), expected.num_rows(), "{name}: rows");
    let mut worst = 0.0f64;
    for (i, (got, want)) in result.columns().iter().zip(expected.columns()).enumerate() {
        assert_eq!(got.null_count(), 0, "{name}: column {i} has NULLs");
        if got.data_type() == &DataType::Float64 {
            let (got, want) = (
                got.as_primitive::<Float64Type>(),
                want.as_primitive::<Float64Type>(),
            );
            for (row, (a, b)) in got.values().iter().zip(want.values()).enumerate() {
                let diff = (a - b).abs() / a.abs().max(b.abs()).max(f64::MIN_POSITIVE);
                assert!(
                    diff <= TOLERANCE,
                    "{name}: row {row}, column {i}: {a} here, {b} in DuckDB"
                );
                worst = worst.max(diff);
            }
        } else {
            assert_eq!(got.as_ref(), want.as_ref(), "{name}: column {i}");
        }
    }
    worst
}

#[test]
fn every_query_gives_duckdbs_answer_at_every_batch_size() {
    let (_dir, catalog) = tables();
    for query in QUERIES {
        let plan = optimize(&quarry::sql::plan(&catalog, query.sql).unwrap()).unwrap();
        for size in [7, 1000, 8192] {
            let result = collect_with_batch_size(&plan, size)
                .unwrap_or_else(|e| panic!("{}: {e}", query.name));
            let result = concat_batches(&result.schema, &result.batches).unwrap();
            let worst = compare(query.name, &result, query.answer);
            if size == 8192 {
                println!(
                    "{}: {} rows, floats within {worst:e}",
                    query.name,
                    result.num_rows()
                );
            }
        }
    }
}

#[test]
fn no_query_runs_a_nested_loop() {
    let (_dir, catalog) = tables();
    for query in QUERIES {
        let plan = optimize(&quarry::sql::plan(&catalog, query.sql).unwrap()).unwrap();
        let explained = explain(&plan);
        assert!(
            !explained.contains("NestedLoopJoin"),
            "{}:\n{explained}",
            query.name
        );
    }
}

#[test]
fn every_answer_has_rows() {
    // An empty answer would compare equal to an engine that returns nothing; each query's parameters select rows at
    // this scale.
    for query in QUERIES {
        assert!(query.answer.lines().count() > 1, "{}", query.name);
    }
}
