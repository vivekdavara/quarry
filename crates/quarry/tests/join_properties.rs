//! Random joins of two random tables, with or without a WHERE, run by the engine at a random batch size (as bound and as
//! optimized) and computed a second time by a nested loop in plain Rust that evaluates `ON` row by row with SQL's rules (`=` is NULL when either side is NULL; -0.0 equals
//! 0.0 and NaN equals NaN). The engine's output order is defined (the left input's, each left row's matches in the
//! right input's order, a LEFT join's unmatched row in its place), so the rows must be the same in the same order.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema};
use proptest::prelude::*;
use quarry::datasource::MemTable;
use quarry::exec::collect_with_batch_size;
use quarry::{Catalog, sql};

/// A row of a(k, f, v) or b(k, f, v): a small integer key, a float, an integer value; all nullable.
type Row = (Option<i64>, Option<f64>, Option<i64>);

const KEYS: &[i64] = &[0, 1, 2, -1];
const FLOATS: &[f64] = &[0.0, -0.0, 1.5, f64::NAN, -f64::NAN, 2.0];
const VALUES: &[i64] = &[0, 1, -1, 5, 3];

fn nullable<T: std::fmt::Debug + Clone + 'static>(
    values: &'static [T],
) -> impl Strategy<Value = Option<T>> {
    prop_oneof![1 => Just(None), 4 => proptest::sample::select(values).prop_map(Some)]
}

fn rows(max: usize) -> impl Strategy<Value = Vec<Row>> {
    proptest::collection::vec((nullable(KEYS), nullable(FLOATS), nullable(VALUES)), 0..max)
}

fn table(rows: &[Row]) -> Arc<MemTable> {
    let schema = Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("f", DataType::Float64, true),
        Field::new("v", DataType::Int64, true),
    ]);
    let k: ArrayRef = Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.0)));
    let f: ArrayRef = Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.1)));
    let v: ArrayRef = Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.2)));
    Arc::new(MemTable::from_batch(
        RecordBatch::try_new(Arc::new(schema), vec![k, f, v]).unwrap(),
    ))
}

/// One conjunct of a random `ON`, as SQL and as the reference evaluates it (`None` is NULL).
#[derive(Debug, Clone, Copy)]
enum Conjunct {
    KeysEqual,
    FloatsEqual,
    KeyPlusOneEqual,
    ValueLess,
    LeftValueAbove(i64),
    RightValueAbove(i64),
    Constant(bool),
}

impl Conjunct {
    fn sql(self) -> String {
        match self {
            Conjunct::KeysEqual => "a.k = b.k".into(),
            Conjunct::FloatsEqual => "b.f = a.f".into(),
            Conjunct::KeyPlusOneEqual => "a.k + 1 = b.k".into(),
            Conjunct::ValueLess => "a.v < b.v".into(),
            Conjunct::LeftValueAbove(n) => format!("a.v > {n}"),
            Conjunct::RightValueAbove(n) => format!("b.v > {n}"),
            Conjunct::Constant(c) => (if c { "TRUE" } else { "FALSE" }).into(),
        }
    }

    fn eval(self, a: &Row, b: &Row) -> Option<bool> {
        let float_eq = |x: f64, y: f64| (x.is_nan() && y.is_nan()) || x == y;
        match self {
            Conjunct::KeysEqual => Some(a.0? == b.0?),
            Conjunct::FloatsEqual => Some(float_eq(b.1?, a.1?)),
            Conjunct::KeyPlusOneEqual => Some(a.0? + 1 == b.0?),
            Conjunct::ValueLess => Some(a.2? < b.2?),
            Conjunct::LeftValueAbove(n) => Some(a.2? > n),
            Conjunct::RightValueAbove(n) => Some(b.2? > n),
            Conjunct::Constant(c) => Some(c),
        }
    }
}

fn conjuncts() -> impl Strategy<Value = Vec<Conjunct>> {
    let one = prop_oneof![
        4 => Just(Conjunct::KeysEqual),
        2 => Just(Conjunct::FloatsEqual),
        1 => Just(Conjunct::KeyPlusOneEqual),
        2 => Just(Conjunct::ValueLess),
        1 => proptest::sample::select(VALUES).prop_map(Conjunct::LeftValueAbove),
        1 => proptest::sample::select(VALUES).prop_map(Conjunct::RightValueAbove),
        1 => any::<bool>().prop_map(Conjunct::Constant),
    ];
    proptest::collection::vec(one, 1..4)
}

/// SQL's AND over the conjuncts, left to right: FALSE if any is FALSE, else NULL if any is NULL, else TRUE.
fn holds(conjuncts: &[Conjunct], a: &Row, b: &Row) -> bool {
    let mut all = Some(true);
    for c in conjuncts {
        match c.eval(a, b) {
            Some(false) => return false,
            None => all = None,
            Some(true) => {}
        }
    }
    all == Some(true)
}

/// The rows the join must give: (a.k, a.f, a.v, b.k, b.f, b.v), each float by its bits.
type Out = [Option<u64>; 6];

fn out(a: &Row, b: Option<&Row>) -> Out {
    let i = |x: Option<i64>| x.map(|x| x as u64);
    let f = |x: Option<f64>| x.map(f64::to_bits);
    [
        i(a.0),
        f(a.1),
        i(a.2),
        b.and_then(|b| i(b.0)),
        b.and_then(|b| f(b.1)),
        b.and_then(|b| i(b.2)),
    ]
}

/// The join's rows, then the WHERE's: a LEFT join's unmatched row has NULLs for b's columns, which WHERE sees.
fn reference(
    left: &[Row],
    right: &[Row],
    on: &[Conjunct],
    outer: bool,
    where_: Option<&[Conjunct]>,
) -> Vec<Out> {
    const NULLS: Row = (None, None, None);
    let mut rows = Vec::new();
    for a in left {
        let before = rows.len();
        for b in right {
            if holds(on, a, b) {
                rows.push((a, Some(b)));
            }
        }
        if outer && rows.len() == before {
            rows.push((a, None));
        }
    }
    rows.into_iter()
        .filter(|(a, b)| where_.is_none_or(|w| holds(w, a, b.unwrap_or(&NULLS))))
        .map(|(a, b)| out(a, b))
        .collect()
}

fn engine_rows(result: &quarry::QueryResult) -> Vec<Out> {
    let mut rows = Vec::new();
    for batch in &result.batches {
        for r in 0..batch.num_rows() {
            let mut row = [None; 6];
            for (c, cell) in row.iter_mut().enumerate() {
                let col = batch.column(c);
                if col.is_null(r) {
                    continue;
                }
                *cell = Some(match col.data_type() {
                    DataType::Int64 => col.as_primitive::<Int64Type>().value(r) as u64,
                    DataType::Float64 => col.as_primitive::<Float64Type>().value(r).to_bits(),
                    other => panic!("unexpected column type {other}"),
                });
            }
            rows.push(row);
        }
    }
    rows
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(
        std::env::var("PROPTEST_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(1000)
    ))]

    #[test]
    fn joins_agree_with_a_nested_loop(
        left in rows(12),
        right in rows(12),
        on in conjuncts(),
        where_ in proptest::option::of(conjuncts()),
        outer in any::<bool>(),
        batch_size in 1usize..8,
    ) {
        let mut catalog = Catalog::new();
        catalog.register("a", table(&left)).unwrap();
        catalog.register("b", table(&right)).unwrap();
        let condition = on.iter().map(|c| c.sql()).collect::<Vec<_>>().join(" AND ");
        let kind = if outer { "LEFT JOIN" } else { "JOIN" };
        let mut query = format!("SELECT a.k, a.f, a.v, b.k, b.f, b.v FROM a {kind} b ON {condition}");
        if let Some(w) = &where_ {
            query += " WHERE ";
            query += &w.iter().map(|c| c.sql()).collect::<Vec<_>>().join(" AND ");
        }
        let bound = sql::plan(&catalog, &query).unwrap();
        // Optimized, the one-sided conditions move below the join and the constants fold; the rows must not change.
        let optimized = quarry::optimizer::optimize(&bound).unwrap();
        for plan in [&bound, &optimized] {
            let result = collect_with_batch_size(plan, batch_size)
                .unwrap_or_else(|e| panic!("{query}: {e}"));
            prop_assert!(result.batches.iter().all(|b| b.num_rows() <= batch_size));
            prop_assert_eq!(
                engine_rows(&result),
                reference(&left, &right, &on, outer, where_.as_deref()),
                "{} at batch size {}\n{}", query, batch_size, plan
            );
        }
    }
}
