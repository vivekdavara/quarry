//! Random GROUP BY and ORDER BY queries over random rows, run by the engine at a random batch size and computed a
//! second time by the references below, in plain Rust over the rows: grouping by value with SQL's rules (-0.0 is 0.0,
//! NaNs are one value, NULL is a group of its own), and a stable sort with NULLs placed as asked. Grouped results must
//! hold the same rows in any order; sorted ones the same rows in the same order. Or both must fail.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Date32Type, Field, Float64Type, Int64Type, Schema};
use proptest::prelude::*;
use quarry::datasource::MemTable;
use quarry::exec::collect_with_batch_size;
use quarry::{Catalog, sql};

/// A value as the reference sees it. A float keeps its bits, so -0.0 and 0.0 differ in results (as DuckDB outputs
/// them); `same` is SQL's equality for grouping.
#[derive(Debug, Clone, PartialEq)]
enum V {
    Null,
    Int(i64),
    Float(u64),
    Str(String),
    Date(i32),
}

fn float(x: f64) -> V {
    V::Float(x.to_bits())
}

/// Equal as GROUP BY and DISTINCT see it: -0.0 is 0.0, all NaNs are one value, NULL equals NULL.
fn same(a: &V, b: &V) -> bool {
    match (a, b) {
        (V::Float(x), V::Float(y)) => {
            let (x, y) = (f64::from_bits(*x), f64::from_bits(*y));
            (x.is_nan() && y.is_nan()) || x == y
        }
        _ => a == b,
    }
}

/// The order MIN and MAX use: numbers by value with NaN the greatest, strings by bytes.
fn less(a: &V, b: &V) -> bool {
    match (a, b) {
        (V::Int(x), V::Int(y)) => x < y,
        (V::Float(x), V::Float(y)) => {
            let (x, y) = (f64::from_bits(*x), f64::from_bits(*y));
            !x.is_nan() && (y.is_nan() || x < y)
        }
        (V::Str(x), V::Str(y)) => x.as_bytes() < y.as_bytes(),
        (V::Date(x), V::Date(y)) => x < y,
        _ => panic!("comparing {a:?} with {b:?}"),
    }
}

/// t(k INT8-ish BIGINT, g VARCHAR, f DOUBLE, v BIGINT, d DATE), all nullable.
type Row = (
    Option<i64>,
    Option<&'static str>,
    Option<f64>,
    Option<i64>,
    Option<i32>,
);

const KEYS: &[i64] = &[0, 1, 2, -1];
const NAMES: &[&str] = &["a", "b", "B", "ä", ""];
// Multiples of 1/4 add up exactly in any order, so float sums don't depend on it.
const FLOATS: &[f64] = &[0.0, -0.0, 0.25, 1.5, -2.75, f64::NAN, 3.0];
const VALUES: &[i64] = &[0, 1, -1, 5, -7, 1 << 62, -(1 << 62)];
const DATES: &[i32] = &[0, 19_782, -1, 19_723];

fn nullable<T: std::fmt::Debug + Clone + 'static>(
    values: &'static [T],
) -> impl Strategy<Value = Option<T>> {
    prop_oneof![1 => Just(None), 5 => proptest::sample::select(values).prop_map(Some)]
}

fn rows() -> impl Strategy<Value = Vec<Row>> {
    let row = (
        nullable(KEYS),
        nullable(NAMES),
        nullable(FLOATS),
        nullable(VALUES),
        nullable(DATES),
    );
    proptest::collection::vec(row, 0..30)
}

fn batch(rows: &[Row]) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("g", DataType::Utf8, true),
        Field::new("f", DataType::Float64, true),
        Field::new("v", DataType::Int64, true),
        Field::new("d", DataType::Date32, true),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(rows.iter().map(|r| r.0).collect::<Int64Array>()),
        Arc::new(rows.iter().map(|r| r.1).collect::<StringArray>()),
        Arc::new(rows.iter().map(|r| r.2).collect::<Float64Array>()),
        Arc::new(rows.iter().map(|r| r.3).collect::<Int64Array>()),
        Arc::new(rows.iter().map(|r| r.4).collect::<Date32Array>()),
    ];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

/// A row's columns as values: k, g, f, v, d.
fn cells(r: &Row) -> [V; 5] {
    [
        r.0.map_or(V::Null, V::Int),
        r.1.map_or(V::Null, |s| V::Str(s.to_string())),
        r.2.map_or(V::Null, float),
        r.3.map_or(V::Null, V::Int),
        r.4.map_or(V::Null, V::Date),
    ]
}

const COLUMNS: [&str; 5] = ["k", "g", "f", "v", "d"];

/// One aggregate: function, column, DISTINCT.
#[derive(Debug, Clone, Copy)]
struct Agg {
    func: &'static str,
    column: Option<usize>,
    distinct: bool,
}

impl Agg {
    fn sql(&self) -> String {
        match self.column {
            None => "count(*)".to_string(),
            Some(c) => format!(
                "{}({}{})",
                self.func,
                if self.distinct { "DISTINCT " } else { "" },
                COLUMNS[c]
            ),
        }
    }
}

fn aggregates() -> impl Strategy<Value = Vec<Agg>> {
    let one = prop_oneof![
        Just(Agg {
            func: "count",
            column: None,
            distinct: false
        }),
        (
            prop::sample::select(&["count", "min", "max"][..]),
            0usize..5,
            any::<bool>()
        )
            .prop_map(|(func, c, distinct)| Agg {
                func,
                column: Some(c),
                distinct
            }),
        // SUM and AVG take numbers: f or v.
        (
            prop::sample::select(&["sum", "avg"][..]),
            prop::sample::select(&[2usize, 3][..]),
            any::<bool>()
        )
            .prop_map(|(func, c, distinct)| Agg {
                func,
                column: Some(c),
                distinct
            }),
    ];
    proptest::collection::vec(one, 1..5)
}

/// The reference: groups in order of first appearance, each a key and its rows' cells.
fn reference(rows: &[Row], keys: &[usize], aggs: &[Agg]) -> Result<Vec<Vec<V>>, String> {
    let mut groups: Vec<(Vec<V>, Vec<[V; 5]>)> = Vec::new();
    if keys.is_empty() {
        groups.push((vec![], vec![]));
    }
    for r in rows {
        let c = cells(r);
        let key: Vec<V> = keys.iter().map(|&k| c[k].clone()).collect();
        // A group's key is its first row's value.
        match groups
            .iter_mut()
            .find(|(k, _)| k.iter().zip(&key).all(|(a, b)| same(a, b)))
        {
            Some((_, members)) => members.push(c),
            None => groups.push((key, vec![c])),
        }
    }
    let mut out = Vec::new();
    for (key, members) in groups {
        let mut row = key;
        for agg in aggs {
            row.push(compute(agg, &members)?);
        }
        out.push(row);
    }
    Ok(out)
}

fn compute(agg: &Agg, members: &[[V; 5]]) -> Result<V, String> {
    let Some(c) = agg.column else {
        return Ok(V::Int(members.len() as i64));
    };
    let mut values: Vec<V> = members
        .iter()
        .map(|m| m[c].clone())
        .filter(|v| *v != V::Null)
        .collect();
    // DISTINCT keeps each value's first occurrence; MIN and MAX ignore it (DuckDB's choice, and the same answer up
    // to which of two equal values is returned).
    if agg.distinct && agg.func != "min" && agg.func != "max" {
        let mut seen: Vec<V> = Vec::new();
        values.retain(|v| {
            let new = !seen.iter().any(|s| same(s, v));
            if new {
                seen.push(v.clone());
            }
            new
        });
    }
    if agg.func == "count" {
        return Ok(V::Int(values.len() as i64));
    }
    if values.is_empty() {
        return Ok(V::Null);
    }
    match agg.func {
        "min" | "max" => {
            // Among equal values (-0.0 and 0.0) the last wins, as in DuckDB.
            let mut best = values[0].clone();
            for v in &values[1..] {
                let better = if agg.func == "min" {
                    !less(&best, v)
                } else {
                    !less(v, &best)
                };
                if better {
                    best = v.clone();
                }
            }
            Ok(best)
        }
        "sum" | "avg" => match &values[0] {
            V::Int(_) => {
                let sum: i128 = values
                    .iter()
                    .map(|v| match v {
                        V::Int(x) => i128::from(*x),
                        _ => unreachable!(),
                    })
                    .sum();
                if agg.func == "avg" {
                    Ok(float(sum as f64 / values.len() as f64))
                } else {
                    i64::try_from(sum)
                        .map(V::Int)
                        .map_err(|_| "overflow".to_string())
                }
            }
            V::Float(_) => {
                // From +0.0, in row order, as the engine adds.
                let sum: f64 = values
                    .iter()
                    .map(|v| match v {
                        V::Float(x) => f64::from_bits(*x),
                        _ => unreachable!(),
                    })
                    .fold(0.0, |a, b| a + b);
                Ok(float(if agg.func == "avg" {
                    sum / values.len() as f64
                } else {
                    sum
                }))
            }
            v => Err(format!("sum of {v:?}")),
        },
        f => Err(format!("unknown aggregate {f}")),
    }
}

fn engine_rows(result: &quarry::QueryResult) -> Vec<Vec<V>> {
    let mut out = Vec::new();
    for b in &result.batches {
        for i in 0..b.num_rows() {
            out.push(
                b.columns()
                    .iter()
                    .map(|a| {
                        if a.logical_nulls().is_some_and(|n| n.is_null(i)) {
                            return V::Null;
                        }
                        match a.data_type() {
                            DataType::Int64 => V::Int(a.as_primitive::<Int64Type>().value(i)),
                            DataType::Float64 => float(a.as_primitive::<Float64Type>().value(i)),
                            DataType::Utf8 => V::Str(a.as_string::<i32>().value(i).to_string()),
                            DataType::Date32 => V::Date(a.as_primitive::<Date32Type>().value(i)),
                            t => panic!("unexpected type {t}"),
                        }
                    })
                    .collect(),
            );
        }
    }
    out
}

fn sorted(mut rows: Vec<Vec<V>>) -> Vec<String> {
    let mut text: Vec<String> = rows.drain(..).map(|r| format!("{r:?}")).collect();
    text.sort();
    text
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(
        std::env::var("PROPTEST_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(1000)
    ))]

    #[test]
    fn group_by_agrees_with_the_reference(
        rows in rows(),
        keys in prop::sample::subsequence(vec![0usize, 1, 2, 4], 0..=2),
        aggs in aggregates(),
        batch_size in 1usize..8,
    ) {
        let mut catalog = Catalog::new();
        catalog.register("t", Arc::new(MemTable::from_batch(batch(&rows)))).unwrap();
        let mut select: Vec<String> = keys.iter().map(|&k| COLUMNS[k].to_string()).collect();
        select.extend(aggs.iter().map(Agg::sql));
        let mut query = format!("SELECT {} FROM t", select.join(", "));
        if !keys.is_empty() {
            let names: Vec<&str> = keys.iter().map(|&k| COLUMNS[k]).collect();
            query += &format!(" GROUP BY {}", names.join(", "));
        }
        let plan = sql::plan(&catalog, &query).unwrap();
        let engine = collect_with_batch_size(&plan, batch_size);
        match (engine, reference(&rows, &keys, &aggs)) {
            (Ok(result), Ok(want)) => {
                let got = engine_rows(&result);
                prop_assert_eq!(sorted(got), sorted(want), "{}\nrows {:?}", query, rows);
            }
            (Err(_), Err(_)) => {}
            (Ok(result), Err(e)) => {
                prop_assert!(false, "{query}\nrows {rows:?}\nengine {:?}\nreference fails: {e}", engine_rows(&result))
            }
            (Err(e), Ok(want)) => prop_assert!(false, "{query}\nrows {rows:?}\nengine fails: {e}\nreference {want:?}"),
        }
    }
}

#[test]
fn the_reference_groups_floats_as_sql_does() {
    let values: Vec<V> = [-0.0, 0.0, f64::NAN, -f64::NAN, 1.5]
        .iter()
        .map(|&x| float(x))
        .collect();
    let mut distinct: Vec<&V> = Vec::new();
    for v in &values {
        if !distinct.iter().any(|d| same(d, v)) {
            distinct.push(v);
        }
    }
    assert_eq!(distinct, [&float(-0.0), &float(f64::NAN), &float(1.5)]);
    let text: HashSet<String> = values.iter().map(|v| format!("{v:?}")).collect();
    assert_eq!(
        text.len(),
        5,
        "results tell -0.0 from 0.0, and NaNs by their bits"
    );
}

/// One ORDER BY key: a column, descending, NULLS FIRST.
type Key = (usize, bool, bool);

/// The reference sort: stable, so ties keep the table's order, then OFFSET and LIMIT. Rows are output as their ids.
fn sorted_ids(rows: &[Row], keys: &[Key], limit: Option<usize>, offset: usize) -> Vec<Vec<V>> {
    let mut order: Vec<usize> = (0..rows.len()).collect();
    let cells: Vec<[V; 5]> = rows.iter().map(cells).collect();
    order.sort_by(|&x, &y| {
        for &(c, desc, nulls_first) in keys {
            let (a, b) = (&cells[x][c], &cells[y][c]);
            let o = match (a, b) {
                (V::Null, V::Null) => std::cmp::Ordering::Equal,
                (V::Null, _) => {
                    if nulls_first {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    }
                }
                (_, V::Null) => {
                    if nulls_first {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                }
                _ => {
                    let o = if less(a, b) {
                        std::cmp::Ordering::Less
                    } else if less(b, a) {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Equal
                    };
                    if desc { o.reverse() } else { o }
                }
            };
            if o != std::cmp::Ordering::Equal {
                return o;
            }
        }
        std::cmp::Ordering::Equal
    });
    order
        .into_iter()
        .skip(offset)
        .take(limit.unwrap_or(usize::MAX))
        .map(|i| vec![V::Int(i as i64)])
        .collect()
}

fn keys() -> impl Strategy<Value = Vec<Key>> {
    proptest::collection::vec((0usize..5, any::<bool>(), any::<bool>()), 1..4)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(
        std::env::var("PROPTEST_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(1000)
    ))]

    #[test]
    fn order_by_agrees_with_a_stable_sort(
        rows in rows(),
        keys in keys(),
        limit in proptest::option::of(0usize..12),
        offset in 0usize..6,
        batch_size in 1usize..8,
    ) {
        // An id column numbers the rows, so the output says which row came where.
        let ids: Vec<i64> = (0..rows.len() as i64).collect();
        let base = batch(&rows);
        let mut fields = vec![Field::new("id", DataType::Int64, false)];
        fields.extend(base.schema().fields().iter().map(|f| f.as_ref().clone()));
        let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(ids))];
        columns.extend(base.columns().iter().cloned());
        let table = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", Arc::new(MemTable::from_batch(table))).unwrap();
        let order: Vec<String> = keys
            .iter()
            .map(|&(c, desc, nulls_first)| {
                format!(
                    "{} {} NULLS {}",
                    COLUMNS[c],
                    if desc { "DESC" } else { "ASC" },
                    if nulls_first { "FIRST" } else { "LAST" }
                )
            })
            .collect();
        let mut query = format!("SELECT id FROM t ORDER BY {}", order.join(", "));
        if let Some(n) = limit {
            query += &format!(" LIMIT {n}");
        }
        query += &format!(" OFFSET {offset}");
        let plan = sql::plan(&catalog, &query).unwrap();
        let result = collect_with_batch_size(&plan, batch_size).unwrap();
        prop_assert!(result.batches.iter().all(|b| b.num_rows() <= batch_size));
        prop_assert_eq!(
            engine_rows(&result),
            sorted_ids(&rows, &keys, limit, offset),
            "{}\nrows {:?}",
            query,
            rows
        );
    }
}
