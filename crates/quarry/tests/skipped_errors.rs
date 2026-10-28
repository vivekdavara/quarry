//! What the optimizer doesn't read or compute, it can't fail on (DESIGN.md, "The optimizer": errors in data a plan
//! skips). Each case here fails as bound and succeeds optimized, and DuckDB 2.0 gives the optimized answer (each
//! checked by hand on DuckDB). All three were found by the day-3 review: `execute.rs` requires bound and optimized
//! plans to give the same rows or the same error, and these are the documented exceptions it doesn't reach.

use std::sync::Arc;

use quarry::Catalog;
use quarry::arrow::array::{Int64Array, RecordBatch};
use quarry::arrow::datatypes::{DataType, Field, Schema};
use quarry::datasource::{MemTable, ParquetTable};
use quarry::exec::collect;
use quarry::parquet::arrow::ArrowWriter;
use quarry::parquet::file::properties::WriterProperties;

/// The query's row count as bound and as optimized (or each one's error).
fn bound_and_optimized(c: &Catalog, sql: &str) -> (Result<usize, String>, Result<usize, String>) {
    let bound = quarry::sql::plan(c, sql).unwrap();
    let optimized = quarry::optimizer::optimize(&bound).unwrap();
    let run = |plan| {
        collect(plan)
            .map(|r| r.num_rows())
            .map_err(|e| e.to_string())
    };
    (run(&bound), run(&optimized))
}

fn int_table(columns: &[(&str, Vec<Option<i64>>)]) -> RecordBatch {
    let schema = Schema::new(
        columns
            .iter()
            .map(|(n, _)| Field::new(*n, DataType::Int64, true))
            .collect::<Vec<_>>(),
    );
    let arrays = columns
        .iter()
        .map(|(_, v)| Arc::new(Int64Array::from(v.clone())) as _)
        .collect();
    RecordBatch::try_new(Arc::new(schema), arrays).unwrap()
}

#[test]
fn a_skipped_row_group_is_not_read_so_its_rows_cannot_fail() {
    // p(x, y) = (1, 0), (10, 1), a row group each. `x > 5` rules out the first group by its statistics, so the
    // division by its y = 0 is never computed. DuckDB 2.0: no rows (and without `AND x > 5`, the same error).
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("p.parquet");
    let batch = int_table(&[
        ("x", vec![Some(1), Some(10)]),
        ("y", vec![Some(0), Some(1)]),
    ]);
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1))
        .build();
    let mut w = ArrowWriter::try_new(
        std::fs::File::create(&path).unwrap(),
        batch.schema(),
        Some(props),
    )
    .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let mut c = Catalog::new();
    c.register("p", Arc::new(ParquetTable::try_new(&path).unwrap()))
        .unwrap();
    let (bound, optimized) = bound_and_optimized(&c, "SELECT x FROM p WHERE 10 % y = 1 AND x > 5");
    assert!(bound.unwrap_err().contains("division by zero"));
    assert_eq!(optimized, Ok(0));
    let (bound, optimized) = bound_and_optimized(&c, "SELECT x FROM p WHERE 10 % y = 1");
    assert!(bound.unwrap_err().contains("division by zero"));
    assert!(optimized.unwrap_err().contains("division by zero"));
}

#[test]
fn a_condition_moved_below_a_join_drops_rows_before_a_later_conjunct_sees_them() {
    // As bound, `a.x > 0 AND 10 % (b.v - a.y) = 1` computes the division where `a.x > 0` is NULL (AND is lazy only
    // past a FALSE). Moved below the join as a filter, `a.x > 0` drops that row first. DuckDB 2.0: no rows.
    let mut c = Catalog::new();
    let a = int_table(&[
        ("k", vec![Some(1)]),
        ("x", vec![None]),
        ("y", vec![Some(5)]),
    ]);
    let b = int_table(&[("k", vec![Some(1)]), ("v", vec![Some(5)])]);
    c.register("a", Arc::new(MemTable::from_batch(a))).unwrap();
    c.register("b", Arc::new(MemTable::from_batch(b))).unwrap();
    let (bound, optimized) = bound_and_optimized(
        &c,
        "SELECT a.k FROM a JOIN b ON a.k = b.k WHERE a.x > 0 AND 10 % (b.v - a.y) = 1",
    );
    assert!(bound.unwrap_err().contains("division by zero"));
    assert_eq!(optimized, Ok(0));
}

#[test]
fn a_csv_column_no_one_reads_is_not_parsed() {
    // 1,000 integer rows (what inference reads), then a b that isn't one. The bound scan parses every column and
    // fails; the optimized one reads only a. DuckDB 2.0, with 30,000 rows before it: count(*) is 30,001, count(b)
    // fails.
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c.csv");
    let mut text = String::from("a,b\n");
    for i in 0..1000 {
        text.push_str(&format!("{i},{i}\n"));
    }
    text.push_str("1000,oops\n");
    std::fs::write(&path, text).unwrap();
    let mut c = Catalog::new();
    c.register_file("c", &path).unwrap();
    for (sql, rows) in [
        ("SELECT count(*) FROM c", 1),
        ("SELECT a FROM c WHERE a > 999", 1),
    ] {
        let (bound, optimized) = bound_and_optimized(&c, sql);
        assert!(bound.unwrap_err().contains("oops"), "{sql}");
        assert_eq!(optimized, Ok(rows), "{sql}");
    }
    let (bound, optimized) = bound_and_optimized(&c, "SELECT count(b) FROM c");
    assert!(bound.unwrap_err().contains("oops"));
    assert!(optimized.unwrap_err().contains("oops"));
}
