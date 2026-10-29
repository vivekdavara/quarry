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
fn conjuncts_every_or_branch_has_move_out_of_the_or() {
    // TPC-H Q19's shape: the join key in each branch. Factored out, it is a hash join key, and a one-sided conjunct
    // common to the branches goes down to its side.
    assert_eq!(
        optimized(
            "SELECT t.b FROM t, u WHERE (t.a = u.a AND t.d > 1 AND u.x = 'k') \
             OR (u.x = 'k' AND u.y < 3 AND t.a = u.a)"
        ),
        lines(&[
            "Projection: t.b",
            "  Join: Inner on t.a = u.a AND (t.d > 1 OR u.y < 3)",
            "    Scan: t projection=[a, b, d]",
            "    Filter: u.x = 'k'",
            "      Scan: u projection=[a, x, y]",
        ])
    );
    // A branch with nothing left makes the OR TRUE wherever the common conjuncts hold: `a OR (a AND b)` is `a`.
    // Conjuncts repeated in a branch are factored once. Three branches.
    assert_eq!(
        optimized(
            "SELECT b FROM t WHERE (a = 1 AND a = 1 AND d > 0) OR (d > 0 AND a = 1) \
             OR (a = 1 AND b = 'x' AND d > 0)"
        ),
        lines(&[
            "Projection: t.b",
            "  Filter: t.a = 1 AND t.d > 0",
            "    Scan: t projection=[a, b, d]",
        ])
    );
    // NOT and IS NULL can't fail either; once `a = 1` is absorbed, `a` isn't read.
    assert_eq!(
        optimized(
            "SELECT b FROM t WHERE (b IS NULL AND NOT d > 0 AND a = 1) OR (NOT d > 0 AND b IS NULL)"
        ),
        lines(&[
            "Projection: t.b",
            "  Filter: t.b IS NULL AND NOT t.d > 0",
            "    Scan: t projection=[b, d]",
        ])
    );
    // A conjunct that can fail stays where it is: factored out, it would run on rows its branch's earlier conjuncts
    // kept from it (here, the zeros of d).
    assert_eq!(
        optimized("SELECT b FROM t WHERE (d <> 0 AND 10 % d = 1) OR (a > 5 AND 10 % d = 1)"),
        lines(&[
            "Projection: t.b",
            "  Filter: t.d <> 0 AND 10 % t.d = 1 OR t.a > 5 AND 10 % t.d = 1",
            "    Scan: t projection=[a, b, d]",
        ])
    );
    // A nested OR's branches are branches too, and `b = 'x'` shares nothing with the others: unchanged.
    assert_eq!(
        optimized(
            "SELECT b FROM t WHERE (a = 1 AND d > 0) OR (a = 2 AND d > 0 AND d = 1 OR b = 'x')"
        ),
        lines(&[
            "Projection: t.b",
            "  Filter: t.a = 1 AND t.d > 0 OR (t.a = 2 AND t.d > 0 AND t.d = 1 OR t.b = 'x')",
            "    Scan: t projection=[a, b, d]",
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

/// The optimized plan as `EXPLAIN` prints it: with the operator that runs each join and sort.
fn explained(sql: &str) -> String {
    let plan = quarry::sql::plan(&catalog(), sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    quarry::exec::explain(&quarry::optimizer::optimize(&plan).unwrap())
}

#[test]
fn explain_names_the_join_and_sort_operators_that_run() {
    // A comma join with an equality is a hash join; the rest of the condition is its residual.
    assert_eq!(
        explained("SELECT t.b FROM t, u WHERE t.a = u.a AND t.d < u.y"),
        lines(&[
            "Projection: t.b",
            "  HashJoin: Inner keys=[t.a = u.a] residual=t.d < u.y",
            "    Scan: t projection=[a, b, d]",
            "    Scan: u projection=[a, y]",
        ])
    );
    // Keys written either way round, and expressions of one side, are keys; the right's key is over the right input.
    assert_eq!(
        explained("SELECT t.b FROM t LEFT JOIN u ON u.y = t.d + 1 AND t.b = u.x")
            .lines()
            .nth(1)
            .unwrap(),
        "  HashJoin: Left keys=[t.d + 1 = u.y, t.b = u.x]"
    );
    // No equality between the sides: a nested loop.
    assert_eq!(
        explained("SELECT count(*) FROM t JOIN u ON t.a < u.a OR t.b = u.x")
            .lines()
            .nth(2)
            .unwrap(),
        "    NestedLoopJoin: Inner on t.a < u.a OR t.b = u.x"
    );
    assert_eq!(
        explained("SELECT count(*) FROM t, u")
            .lines()
            .nth(2)
            .unwrap(),
        "    NestedLoopJoin: Cross"
    );
    // A sort under a LIMIT (through the projections between them) is a top-k of LIMIT + OFFSET rows; without one,
    // a full sort.
    assert_eq!(
        explained("SELECT b FROM t ORDER BY d DESC LIMIT 2 OFFSET 3"),
        lines(&[
            "Limit: skip=3 fetch=2",
            "  Projection: t.b",
            "    TopK: k=5 __order_0 DESC NULLS LAST",
            "      Projection: t.b, t.d AS __order_0",
            "        Scan: t projection=[b, d]",
        ])
    );
    for sql in [
        "SELECT b FROM t ORDER BY d",
        "SELECT b FROM t ORDER BY d OFFSET 2",
    ] {
        let plan = explained(sql);
        assert!(plan.contains("Sort: ") && !plan.contains("TopK"), "{plan}");
    }
}
