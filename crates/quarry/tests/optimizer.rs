//! What each optimizer rule does to a plan, checked on the printed plan (DESIGN.md, "The optimizer"). That the rows
//! don't change is checked by `execute.rs`, which runs every query with and without the optimizer.

use std::sync::Arc;

use quarry::Catalog;
use quarry::arrow::array::RecordBatch;
use quarry::arrow::datatypes::{DataType, Field, Schema};
use quarry::datasource::MemTable;

fn catalog() -> Catalog {
    let table = |fields: Vec<Field>| {
        Arc::new(MemTable::from_batch(RecordBatch::new_empty(Arc::new(
            Schema::new(fields),
        ))))
    };
    let mut c = Catalog::new();
    c.register(
        "t",
        table(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, true),
            Field::new("d", DataType::Int64, true),
        ]),
    )
    .unwrap();
    c.register(
        "u",
        table(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("x", DataType::Utf8, true),
            Field::new("y", DataType::Int64, true),
        ]),
    )
    .unwrap();
    c
}

/// The optimized plan of a query, as printed.
fn optimized(sql: &str) -> String {
    let plan = quarry::sql::plan(&catalog(), sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    quarry::optimizer::optimize(&plan)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .to_string()
}

fn lines(plan: &[&str]) -> String {
    plan.iter().map(|l| format!("{l}\n")).collect()
}

#[test]
fn scans_read_only_the_columns_used_above_them() {
    assert_eq!(
        optimized("SELECT count(*) FROM t"),
        lines(&[
            "Projection: count(*)",
            "  Aggregate: group_by=[] aggregates=[count(*)]",
            "    Scan: t projection=[]",
        ])
    );
    // The filter's column is read too, and every column above is renumbered (c is now the scan's column 1).
    assert_eq!(
        optimized("SELECT c * 2 FROM t WHERE d > 1"),
        lines(&[
            "Projection: t.c * 2.0 AS \"c * 2\"",
            "  Filter: t.d > 1",
            "    Scan: t projection=[c, d]",
        ])
    );
    // ORDER BY a column the select list doesn't name: the sort's key is read, and dropped after the sort.
    assert_eq!(
        optimized("SELECT b FROM t ORDER BY d DESC LIMIT 2"),
        lines(&[
            "Limit: skip=0 fetch=2",
            "  Projection: t.b",
            "    Sort: __order_0 DESC NULLS LAST",
            "      Projection: t.b, t.d AS __order_0",
            "        Scan: t projection=[b, d]",
        ])
    );
    // A join reads each side's columns that its ON and the select list use.
    assert_eq!(
        optimized("SELECT u.x FROM t JOIN u ON t.d = u.y"),
        lines(&[
            "Projection: u.x",
            "  Join: Inner on t.d = u.y",
            "    Scan: t projection=[d]",
            "    Scan: u projection=[x, y]",
        ])
    );
}

#[test]
fn constants_are_computed_once_and_filters_that_always_or_never_hold_go_away() {
    assert_eq!(
        optimized("SELECT a + (1 + 2) * 3 AS v FROM t WHERE 1 < 2"),
        lines(&["Projection: t.a + 9 AS v", "  Scan: t projection=[a]"])
    );
    // AND and OR with a constant operand: FALSE AND x never computes x, TRUE AND x is x.
    assert_eq!(
        optimized("SELECT a FROM t WHERE 1 > 2 AND a = 1"),
        lines(&["Projection: t.a", "  Empty: no rows"])
    );
    assert_eq!(
        optimized("SELECT a FROM t WHERE (1 < 2 AND a = 1) OR 2 < 1"),
        lines(&[
            "Projection: t.a",
            "  Filter: t.a = 1",
            "    Scan: t projection=[a]",
        ])
    );
    assert_eq!(
        optimized("SELECT a FROM t WHERE d > 0 OR 1 < 2"),
        lines(&[
            "Projection: t.a",
            "  Filter: t.d > 0 OR TRUE",
            "    Scan: t projection=[a, d]",
        ]),
        "x OR TRUE stays: x is computed on every row and may fail"
    );
    assert_eq!(
        optimized("SELECT a FROM t WHERE 2 < 1"),
        lines(&["Projection: t.a", "  Empty: no rows"])
    );
    assert_eq!(
        optimized("SELECT a FROM t WHERE NULL"),
        lines(&["Projection: t.a", "  Empty: no rows"])
    );
    // A constant that fails stays, to fail only if a row reaches it; its operands are folded.
    assert_eq!(
        optimized("SELECT CASE WHEN a > 0 THEN 1 ELSE 7 % (2 - 2) END FROM t"),
        lines(&[
            "Projection: CASE WHEN t.a > 0 THEN 1 ELSE 7 % 0 END AS \"CASE WHEN a > 0 THEN 1 ELSE 7 % (2 - 2) END\"",
            "  Scan: t projection=[a]",
        ])
    );
}

#[test]
fn conditions_move_below_joins_to_the_side_they_read() {
    // A comma join with an equality in WHERE becomes an inner hash join; one-sided conditions go to their side.
    assert_eq!(
        optimized(
            "SELECT t.b, u.x FROM t, u WHERE t.a = u.a AND u.y > 3 AND t.c < 1.5 AND t.d + u.y = 7"
        ),
        lines(&[
            "Projection: t.b, u.x",
            "  Join: Inner on t.a = u.a AND t.d + u.y = 7",
            "    Filter: t.c < 1.5",
            "      Scan: t projection=[a, b, c, d]",
            "    Filter: u.y > 3",
            "      Scan: u projection=[a, x, y]",
        ])
    );
    // An inner join's own ON moves the same way, and WHERE's conditions come after ON's on each side.
    assert_eq!(
        optimized("SELECT t.b FROM t JOIN u ON t.a = u.a AND u.y > 1 WHERE u.y < 5"),
        lines(&[
            "Projection: t.b",
            "  Join: Inner on t.a = u.a",
            "    Scan: t projection=[a, b]",
            "    Filter: u.y > 1 AND u.y < 5",
            "      Scan: u projection=[a, y]",
        ])
    );
    // Three tables: each condition goes down as far as its columns allow.
    assert_eq!(
        optimized("SELECT t.b FROM t, u, u AS v WHERE t.a = u.a AND u.y = v.y AND v.x = 'k'"),
        lines(&[
            "Projection: t.b",
            "  Join: Inner on u.y = v.y",
            "    Join: Inner on t.a = u.a",
            "      Scan: t projection=[a, b]",
            "      Scan: u projection=[a, y]",
            "    Filter: v.x = 'k'",
            "      Scan: u AS v projection=[x, y]",
        ])
    );
}

#[test]
fn a_left_join_moves_only_what_cannot_change_which_left_rows_come_out() {
    // WHERE: the left's conditions go down; the right's stay above (they see the NULLs of unmatched rows).
    // ON: the right's go down to the right; the left's stay (they decide matching, not which left rows come out).
    assert_eq!(
        optimized(
            "SELECT t.b, u.x FROM t LEFT JOIN u ON t.a = u.a AND u.y > 1 AND t.d > 2 \
             WHERE t.c < 1.5 AND u.x IS NULL"
        ),
        lines(&[
            "Projection: t.b, u.x",
            "  Filter: u.x IS NULL",
            "    Join: Left on t.a = u.a AND t.d > 2",
            "      Filter: t.c < 1.5",
            "        Scan: t projection=[a, b, c, d]",
            "      Filter: u.y > 1",
            "        Scan: u projection=[a, x, y]",
        ])
    );
}
