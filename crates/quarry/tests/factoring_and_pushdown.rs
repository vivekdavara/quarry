//! OR factoring (optimizer/factor.rs) followed by predicate pushdown must not make a query fail that succeeds as
//! bound. Found by the day-4 review: until 2026-10-29's fix, factoring an OR whose remainder could fail let pushdown move
//! that remainder onto rows the join key had kept from it.

use std::sync::Arc;

use quarry::Catalog;
use quarry::arrow::array::{Int64Array, RecordBatch};
use quarry::arrow::datatypes::{DataType, Field, Schema};
use quarry::datasource::MemTable;
use quarry::exec::collect;

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

/// l(k) = 1; r(k, v, w) = (1, 1, 0), (2, 0, 0). The r row with v = 0 matches no l row.
fn catalog() -> Catalog {
    let mut c = Catalog::new();
    let l = int_table(&[("k", vec![Some(1)])]);
    let r = int_table(&[
        ("k", vec![Some(1), Some(2)]),
        ("v", vec![Some(1), Some(0)]),
        ("w", vec![Some(0), Some(0)]),
    ]);
    c.register("l", Arc::new(MemTable::from_batch(l))).unwrap();
    c.register("r", Arc::new(MemTable::from_batch(r))).unwrap();
    c
}

/// As bound, each branch's `10 % r.v` is computed only for pairs whose `l.k = r.k` isn't FALSE, so the r row with
/// v = 0 (k = 2, which no l row has) is never divided by. Had `l.k = r.k` been factored out, the rest, `(10 % r.v = 0
/// OR r.w = 1)`, would read r only, and pushdown would move it below the join onto every r row, and fail. The OR can
/// fail, so it isn't factored, and every form gives the bound plan's one row (DuckDB 2.0 gives one row for the LEFT
/// join and fails the other two; README, "Review of day 4").
#[test]
fn pushdown_never_moves_a_factored_ors_failing_remainder_onto_rows_the_join_key_excluded() {
    let c = catalog();
    let mut wrong = Vec::new();
    for sql in [
        // A comma join's WHERE (TPC-H Q19's shape).
        "SELECT l.k FROM l, r WHERE (l.k = r.k AND 10 % r.v = 0) OR (l.k = r.k AND r.w = 1)",
        // An inner join's ON.
        "SELECT l.k FROM l JOIN r ON (l.k = r.k AND 10 % r.v = 0) OR (l.k = r.k AND r.w = 1)",
        // A LEFT join's ON: its right-side conjuncts go down to the right.
        "SELECT l.k FROM l LEFT JOIN r ON (l.k = r.k AND 10 % r.v = 0) OR (l.k = r.k AND r.w = 1)",
    ] {
        let (bound, optimized) = bound_and_optimized(&c, sql);
        assert_eq!(bound, Ok(1), "{sql}: bound");
        if optimized != bound {
            wrong.push(format!("{sql}: bound {bound:?}, optimized {optimized:?}"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// The same with a cast instead of a modulo (found by the second day-4 review: treating every expression as unable to
/// fail passed every other test). As bound, `CAST(r.s AS BIGINT)` runs only on pairs whose keys match; factored, the
/// rest would read r alone, go below the join, and cast r's 'x'.
#[test]
fn an_or_with_a_cast_isnt_factored() {
    use quarry::arrow::array::StringArray;
    let mut c = Catalog::new();
    let l = int_table(&[("k", vec![Some(1)])]);
    let r = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("w", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 5])),
            Arc::new(StringArray::from(vec!["1", "x"])),
            Arc::new(Int64Array::from(vec![0, 0])),
        ],
    )
    .unwrap();
    c.register("l", Arc::new(MemTable::from_batch(l))).unwrap();
    c.register("r", Arc::new(MemTable::from_batch(r))).unwrap();
    for sql in [
        "SELECT l.k FROM l, r WHERE (l.k = r.k AND CAST(r.s AS BIGINT) = 1) OR (l.k = r.k AND r.w = 1)",
        "SELECT l.k FROM l LEFT JOIN r ON (l.k = r.k AND CAST(r.s AS BIGINT) = 1) OR (l.k = r.k AND r.w = 1)",
    ] {
        let (bound, optimized) = bound_and_optimized(&c, sql);
        assert_eq!(bound, Ok(1), "{sql}: bound");
        assert_eq!(optimized, bound, "{sql}");
    }
}
