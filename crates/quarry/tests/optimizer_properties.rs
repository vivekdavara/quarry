//! Random whole queries (comma, inner and LEFT joins; WHERE with constants, comparisons, OR and IS NULL; GROUP BY with
//! aggregates or a select list of expressions; ORDER BY; LIMIT) over random rows, run bound and optimized at a random
//! batch size. No rule may change the rows or their order: every rule keeps its node's input order, a hash join
//! gives the same order as the filtered nested loop it replaces, and groups come out in order of first appearance.
//! Where both plans fail there is nothing to compare; where only one does, it must be a documented case (DESIGN.md,
//! "The optimizer"), which these queries can't reach: they have no expression that fails.

use std::sync::Arc;

use proptest::prelude::*;
use quarry::Catalog;
use quarry::arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use quarry::arrow::datatypes::{DataType, Field, Schema};
use quarry::arrow::util::display::array_value_to_string;
use quarry::datasource::MemTable;
use quarry::exec::collect_with_batch_size;
use quarry::plan::LogicalPlan;

type Row = (Option<i64>, Option<&'static str>, Option<f64>);

const INTS: &[i64] = &[0, 1, 2, -1];
const STRS: &[&str] = &["p", "q", ""];
const FLOATS: &[f64] = &[0.0, -0.0, 1.5, f64::NAN];

fn nullable<T: std::fmt::Debug + Clone + 'static>(
    values: &'static [T],
) -> impl Strategy<Value = Option<T>> {
    prop_oneof![1 => Just(None), 4 => proptest::sample::select(values).prop_map(Some)]
}

fn rows() -> impl Strategy<Value = Vec<Row>> {
    proptest::collection::vec((nullable(INTS), nullable(STRS), nullable(FLOATS)), 0..10)
}

/// A table of (`a` BIGINT, `<s>` VARCHAR, `<f>` DOUBLE) under the given column names.
fn table(rows: &[Row], names: [&str; 3]) -> Arc<MemTable> {
    let schema = Schema::new(vec![
        Field::new(names[0], DataType::Int64, true),
        Field::new(names[1], DataType::Utf8, true),
        Field::new(names[2], DataType::Float64, true),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.0))),
        Arc::new(StringArray::from_iter(rows.iter().map(|r| r.1))),
        Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.2))),
    ];
    Arc::new(MemTable::from_batch(
        RecordBatch::try_new(Arc::new(schema), columns).unwrap(),
    ))
}

/// A condition over t(a, b, c) and, with a join, u(a, x, y): comparisons with constants or across tables,
/// constants that fold (to TRUE, FALSE or NULL), IS NULL, and AND/OR/NOT of those.
fn condition(joined: bool) -> BoxedStrategy<String> {
    // Each column with constants of its own type, so the comparisons bind.
    let ints: &[&str] = &["0", "1", "-1", "NULL"];
    let strs: &[&str] = &["'p'", "''", "NULL"];
    let floats: &[&str] = &["1.5", "-0.0", "0", "NULL"];
    let mut columns: Vec<(&str, &[&str])> = vec![("t.a", ints), ("t.b", strs), ("t.c", floats)];
    if joined {
        columns.extend([("u.a", ints), ("u.x", strs), ("u.y", floats)]);
    }
    let names: Vec<&str> = columns.iter().map(|(c, _)| *c).collect();
    let pairs: Vec<(&str, &str)> = if joined {
        vec![
            ("t.a", "u.a"),
            ("t.b", "u.x"),
            ("t.c", "u.y"),
            ("t.a", "u.y"),
        ]
    } else {
        vec![("t.a", "t.c")]
    };
    let ops = proptest::sample::select(&["=", "<>", "<", ">="][..]);
    let with_constant = proptest::sample::select(columns).prop_flat_map(move |(c, ks)| {
        (ops.clone(), proptest::sample::select(ks)).prop_map(move |(op, k)| format!("{c} {op} {k}"))
    });
    let ops = proptest::sample::select(&["=", "<>", "<", ">="][..]);
    let leaf = prop_oneof![
        3 => with_constant,
        2 => (proptest::sample::select(pairs), ops).prop_map(|((l, r), op)| format!("{l} {op} {r}")),
        1 => proptest::sample::select(names).prop_map(|c| format!("{c} IS NULL")),
        1 => proptest::sample::select(&["1 = 1", "2 < 1", "NULL = 1", "(1 + 1) * 3 = 6"][..])
            .prop_map(str::to_string),
    ];
    leaf.prop_recursive(2, 6, 2, |inner| {
        prop_oneof![
            (inner.clone(), inner.clone()).prop_map(|(a, b)| format!("({a}) AND ({b})")),
            (inner.clone(), inner.clone()).prop_map(|(a, b)| format!("({a}) OR ({b})")),
            // ORs whose branches share a conjunct, for the factoring rule: in different places, and a branch that
            // is only the shared conjunct.
            (inner.clone(), inner.clone(), inner.clone())
                .prop_map(|(s, a, b)| format!("(({s}) AND ({a})) OR (({b}) AND ({s}))")),
            (inner.clone(), inner.clone()).prop_map(|(s, a)| format!("({s}) OR (({a}) AND ({s}))")),
            inner.prop_map(|a| format!("NOT ({a})")),
        ]
    })
    .boxed()
}

#[derive(Debug, Clone)]
struct Query {
    from: String,
    joined: bool,
    filter: Option<String>,
    grouped: bool,
    order: bool,
    limit: Option<usize>,
}

impl Query {
    fn sql(&self) -> String {
        let select = match (self.grouped, self.joined) {
            (true, true) => "t.b, count(*), sum(u.a), min(t.c)",
            (true, false) => "t.b, count(*), sum(t.a), max(t.c)",
            (false, true) => "t.a, t.c * 2 AS c2, u.x, u.a + 1 AS a1",
            (false, false) => "t.a, t.b, t.c * 2 AS c2",
        };
        let mut sql = format!("SELECT {select} FROM {}", self.from);
        if let Some(f) = &self.filter {
            sql += &format!(" WHERE {f}");
        }
        if self.grouped {
            sql += " GROUP BY t.b";
        }
        if self.order {
            sql += if self.grouped {
                " ORDER BY 2 DESC, 1"
            } else {
                " ORDER BY 1, 2 DESC"
            };
        }
        if let Some(n) = self.limit {
            sql += &format!(" LIMIT {n}");
        }
        sql
    }
}

fn query() -> impl Strategy<Value = Query> {
    let from = prop_oneof![
        Just(("t".to_string(), false)),
        Just(("t, u".to_string(), true)),
        condition(true).prop_map(|on| (format!("t JOIN u ON {on}"), true)),
        condition(true).prop_map(|on| (format!("t LEFT JOIN u ON {on}"), true)),
        Just(("t JOIN u ON t.a = u.a".to_string(), true)),
        Just(("t LEFT JOIN u ON t.a = u.a AND u.y > 0".to_string(), true)),
    ];
    from.prop_flat_map(|(from, joined)| {
        (
            proptest::option::of(condition(joined)),
            any::<bool>(),
            any::<bool>(),
            proptest::option::of(0usize..6),
        )
            .prop_map(move |(filter, grouped, order, limit)| Query {
                from: from.clone(),
                joined,
                filter,
                grouped,
                order,
                limit,
            })
    })
}

fn values(plan: &LogicalPlan, batch_size: usize) -> Result<Vec<Vec<String>>, String> {
    let result = collect_with_batch_size(plan, batch_size).map_err(|e| e.to_string())?;
    let mut rows = Vec::new();
    for batch in &result.batches {
        for r in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|c| array_value_to_string(c, r).unwrap())
                    .collect(),
            );
        }
    }
    Ok(rows)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(
        std::env::var("PROPTEST_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(1000)
    ))]

    #[test]
    fn optimizing_never_changes_the_rows(
        t in rows(),
        u in rows(),
        query in query(),
        batch_size in 1usize..6,
    ) {
        let mut catalog = Catalog::new();
        catalog.register("t", table(&t, ["a", "b", "c"])).unwrap();
        catalog.register("u", table(&u, ["a", "x", "y"])).unwrap();
        let sql = query.sql();
        let bound = quarry::sql::plan(&catalog, &sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        let optimized = quarry::optimizer::optimize(&bound).unwrap();
        let (b, o) = (values(&bound, batch_size), values(&optimized, batch_size));
        prop_assert!(b.is_ok() && o.is_ok(), "{sql}\nbound {b:?}\noptimized {o:?}\n{optimized}");
        prop_assert_eq!(b, o, "{}\n{}", sql, optimized);
    }
}
