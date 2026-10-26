//! The binder against DESIGN.md: each rule binds a query and compares the printed plan, or checks the error.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use quarry::datasource::MemTable;
use quarry::plan::LogicalPlan;
use quarry::sql::{self, BoundStatement};
use quarry::{Catalog, QuarryError};

/// Two empty tables: `t` with a column of each type class, and `u`, which shares the column name `a` with `t`.
fn catalog() -> Catalog {
    let t = Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int32, true),
        Field::new("f", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("d", DataType::Date32, true),
        Field::new("flag", DataType::Boolean, true),
        Field::new("Mixed_Case", DataType::Int64, true),
        Field::new("big", DataType::UInt64, true),
    ]);
    let u = Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("x", DataType::Utf8, true),
    ]);
    // From the day-1 review: a Float32 column, and a column whose name is an expression's text.
    let w = Schema::new(vec![
        Field::new("x + 1", DataType::Int64, false),
        Field::new("x", DataType::Int64, false),
        Field::new("f32", DataType::Float32, false),
    ]);
    let mut catalog = Catalog::new();
    for (name, schema) in [("t", t), ("u", u), ("w", w)] {
        let batch = RecordBatch::new_empty(Arc::new(schema));
        catalog
            .register(name, Arc::new(MemTable::from_batch(batch)))
            .unwrap();
    }
    catalog
}

fn bind(query: &str) -> LogicalPlan {
    match sql::plan(&catalog(), query) {
        Ok(plan) => plan,
        Err(e) => panic!("{query}: {e}"),
    }
}

/// The printed plan, without the trailing newline.
fn plan(query: &str) -> String {
    bind(query).to_string().trim_end().to_string()
}

fn error(query: &str) -> QuarryError {
    match sql::plan(&catalog(), query) {
        Ok(plan) => panic!("{query} bound, but should have failed:\n{plan}"),
        Err(e) => e,
    }
}

/// Asserts the query fails with a plan error whose message contains `text`.
fn plan_error(query: &str, text: &str) {
    let e = error(query);
    assert!(
        matches!(e, QuarryError::Plan(_)),
        "{query}: expected a plan error, got {e}"
    );
    assert!(
        e.to_string().contains(text),
        "{query}: {e}\n  does not contain: {text}"
    );
}

/// Asserts the query fails as unsupported, naming `text`.
fn unsupported(query: &str, text: &str) {
    let e = error(query);
    assert!(
        matches!(e, QuarryError::Unsupported(_)),
        "{query}: expected unsupported, got {e}"
    );
    assert!(
        e.to_string().contains(text),
        "{query}: {e}\n  does not contain: {text}"
    );
}

/// The output columns' names and types.
fn columns(query: &str) -> Vec<(String, DataType)> {
    bind(query)
        .schema()
        .fields()
        .iter()
        .map(|f| (f.name.clone(), f.data_type.clone()))
        .collect()
}

fn lines(text: &[&str]) -> String {
    text.join("\n")
}

#[test]
fn a_simple_query_binds_every_clause() {
    assert_eq!(
        plan("SELECT a, b + 1 AS next FROM t WHERE f > 0.5 ORDER BY next DESC LIMIT 10 OFFSET 2"),
        lines(&[
            "Limit: skip=2 fetch=10",
            "  Sort: next DESC NULLS LAST",
            "    Projection: t.a, CAST(t.b AS Int64) + 1 AS next",
            "      Filter: t.f > 0.5",
            "        Scan: t",
        ])
    );
}

#[test]
fn without_from_there_is_one_empty_row() {
    assert_eq!(
        plan("SELECT 1 + 2"),
        lines(&["Projection: 1 + 2 AS \"1 + 2\"", "  Empty: one row"])
    );
    plan_error("SELECT *", "SELECT * with no FROM");
}

// --- Names ---

#[test]
fn names_match_without_regard_to_case() {
    // The qualifier is the table's name as the query spells it.
    assert_eq!(
        plan("SELECT A, \"MIXED_CASE\", mixed_case FROM T"),
        lines(&["Projection: T.a, T.Mixed_Case, T.Mixed_Case", "  Scan: T"])
    );
    // The output keeps the table's spelling of the name, not the query's.
    let names: Vec<String> = columns("SELECT A, mixed_case FROM t")
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert_eq!(names, vec!["a", "Mixed_Case"]);
}

#[test]
fn aliases_replace_the_table_name() {
    assert_eq!(
        plan("SELECT x.a FROM t AS x"),
        lines(&["Projection: x.a", "  Scan: t AS x"])
    );
    plan_error("SELECT t.a FROM t AS x", "table t is not in FROM");
    plan_error(
        "SELECT a FROM t, t",
        "the table name t appears more than once in FROM",
    );
    assert_eq!(
        plan("SELECT t1.a, t2.a FROM t t1, t t2")
            .lines()
            .next()
            .unwrap(),
        "Projection: t1.a, t2.a"
    );
}

#[test]
fn unknown_and_ambiguous_names_are_errors() {
    plan_error(
        "SELECT nope FROM t",
        "column nope not found; the columns are: t.a, t.b",
    );
    plan_error("SELECT t.nope FROM t", "column t.nope not found");
    plan_error("SELECT a FROM t, u", "column a is ambiguous: t.a, u.a");
    plan_error("SELECT a FROM t JOIN u ON a = a", "column a is ambiguous");
    plan_error(
        "SELECT * FROM nope",
        "table nope not found; the tables are: t, u, w",
    );
    // Qualifying resolves it.
    assert_eq!(
        plan("SELECT t.a, x FROM t, u").lines().next().unwrap(),
        "Projection: t.a, u.x"
    );
}

// --- Types ---

#[test]
fn integers_widen_and_floats_win() {
    let cols = columns("SELECT b + b, b + a, b * f, a / b, a % b, -b, f - 1 FROM t");
    let types: Vec<DataType> = cols.into_iter().map(|(_, t)| t).collect();
    use DataType::*;
    assert_eq!(
        types,
        vec![Int64, Int64, Float64, Float64, Int64, Int64, Float64]
    );
    assert_eq!(
        plan("SELECT a / b FROM t").lines().next().unwrap(),
        "Projection: CAST(t.a AS Float64) / CAST(t.b AS Float64) AS \"a / b\""
    );
    plan_error("SELECT f % 2 FROM t", "% needs integers");
}

#[test]
fn comparisons_cast_to_the_common_type() {
    assert_eq!(
        plan("SELECT a FROM t WHERE b = a").lines().nth(1).unwrap(),
        "  Filter: CAST(t.b AS Int64) = t.a"
    );
    assert_eq!(
        plan("SELECT a FROM t WHERE b < 2.5")
            .lines()
            .nth(1)
            .unwrap(),
        "  Filter: CAST(t.b AS Float64) < 2.5"
    );
    assert_eq!(
        plan("SELECT a FROM t WHERE s = NULL")
            .lines()
            .nth(1)
            .unwrap(),
        "  Filter: t.s = CAST(NULL AS Utf8)"
    );
    plan_error(
        "SELECT a FROM t WHERE s = 1",
        "cannot compare Utf8 with Int64 (1) in s = 1",
    );
    plan_error(
        "SELECT a FROM t WHERE flag = 1",
        "cannot compare Boolean with Int64",
    );
}

#[test]
fn a_string_literal_compared_with_a_date_is_a_date() {
    assert_eq!(
        plan("SELECT a FROM t WHERE d <= '1998-09-02'")
            .lines()
            .nth(1)
            .unwrap(),
        "  Filter: t.d <= DATE '1998-09-02'"
    );
    assert_eq!(
        plan("SELECT a FROM t WHERE d BETWEEN '1994-01-01' AND DATE '1995-01-01'")
            .lines()
            .nth(1)
            .unwrap(),
        "  Filter: t.d >= DATE '1994-01-01' AND t.d <= DATE '1995-01-01'"
    );
    assert_eq!(
        plan("SELECT a FROM t WHERE d IN ('2024-02-29', d)")
            .lines()
            .nth(1)
            .unwrap(),
        "  Filter: t.d IN (DATE '2024-02-29', t.d)"
    );
    plan_error(
        "SELECT a FROM t WHERE d = '1998-02-30'",
        "'1998-02-30' is compared with a date but isn't one",
    );
    plan_error("SELECT DATE '2024-13-01'", "invalid date '2024-13-01'");
    // A string column is not a literal: no implicit cast.
    plan_error(
        "SELECT a FROM t WHERE d = s",
        "cannot compare Date32 with Utf8",
    );
}

#[test]
fn date_arithmetic() {
    let types: Vec<DataType> = columns("SELECT d + 1, 1 + d, d - b, d - d FROM t")
        .into_iter()
        .map(|(_, t)| t)
        .collect();
    assert_eq!(
        types,
        vec![
            DataType::Date32,
            DataType::Date32,
            DataType::Date32,
            DataType::Int64
        ]
    );
    assert_eq!(
        plan("SELECT d - b FROM t").lines().next().unwrap(),
        "Projection: t.d - CAST(t.b AS Int64) AS \"d - b\""
    );
    plan_error(
        "SELECT 1 - d FROM t",
        "cannot apply this operator to Int64 and Date32",
    );
    plan_error(
        "SELECT d * 2 FROM t",
        "cannot apply this operator to Date32 and Int64",
    );
    plan_error(
        "SELECT d + d FROM t",
        "cannot apply this operator to Date32 and Date32",
    );
    unsupported("SELECT d + INTERVAL '1' DAY FROM t", "intervals");
}

#[test]
fn boolean_and_string_operators_check_their_operands() {
    plan_error(
        "SELECT a FROM t WHERE a AND flag",
        "AND needs a boolean, not Int64",
    );
    plan_error(
        "SELECT a FROM t WHERE NOT s",
        "NOT needs a boolean, not Utf8",
    );
    plan_error(
        "SELECT a FROM t WHERE a",
        "a filter must be a boolean, not Int64",
    );
    plan_error("SELECT s || a FROM t", "|| needs strings, not Int64");
    plan_error(
        "SELECT a FROM t WHERE a LIKE 'x'",
        "LIKE needs strings, not Int64",
    );
    assert_eq!(
        plan("SELECT a FROM t WHERE flag OR NULL")
            .lines()
            .nth(1)
            .unwrap(),
        "  Filter: t.flag OR CAST(NULL AS Boolean)"
    );
    assert_eq!(
        plan("SELECT a FROM t WHERE s NOT ILIKE 'ab%' AND s LIKE '%c'")
            .lines()
            .nth(1)
            .unwrap(),
        "  Filter: t.s NOT ILIKE 'ab%' AND t.s LIKE '%c'"
    );
    assert_eq!(columns("SELECT s || 'x' FROM t")[0].1, DataType::Utf8);
}

#[test]
fn between_and_in_desugar_or_coerce() {
    assert_eq!(
        plan("SELECT a FROM t WHERE b NOT BETWEEN 1 AND f")
            .lines()
            .nth(1)
            .unwrap(),
        "  Filter: CAST(t.b AS Float64) < CAST(1 AS Float64) OR CAST(t.b AS Float64) > t.f"
    );
    assert_eq!(
        plan("SELECT a FROM t WHERE b NOT IN (1, 2, NULL)")
            .lines()
            .nth(1)
            .unwrap(),
        "  Filter: CAST(t.b AS Int64) NOT IN (1, 2, CAST(NULL AS Int64))"
    );
    plan_error(
        "SELECT a FROM t WHERE a IN (1, 'x')",
        "IN: cannot compare Int64 with Utf8 ('x') in a IN (1, 'x')",
    );
}

#[test]
fn case_binds_to_a_searched_case() {
    assert_eq!(
        plan("SELECT CASE b WHEN 1 THEN 'one' WHEN 2 THEN 'two' END FROM t")
            .lines()
            .next()
            .unwrap(),
        "Projection: CASE WHEN CAST(t.b AS Int64) = 1 THEN 'one' WHEN CAST(t.b AS Int64) = 2 THEN 'two' \
         ELSE CAST(NULL AS Utf8) END AS \"CASE b WHEN 1 THEN 'one' WHEN 2 THEN 'two' END\""
    );
    assert_eq!(
        columns("SELECT CASE WHEN flag THEN a ELSE f END FROM t")[0].1,
        DataType::Float64
    );
    plan_error(
        "SELECT CASE WHEN a THEN 1 END FROM t",
        "CASE WHEN needs a boolean, not Int64",
    );
    plan_error(
        "SELECT CASE WHEN flag THEN 1 ELSE 'x' END FROM t",
        "CASE results: cannot compare Int64 with Utf8",
    );
}

#[test]
fn casts_name_their_arrow_type() {
    let types: Vec<DataType> = columns(
        "SELECT CAST(a AS INT), CAST(a AS BIGINT), CAST(a AS DOUBLE), CAST(a AS REAL), CAST(a AS VARCHAR), \
         CAST(s AS DATE), CAST(a AS SMALLINT), CAST(s AS BOOLEAN), a::TEXT FROM t",
    )
    .into_iter()
    .map(|(_, t)| t)
    .collect();
    use DataType::*;
    assert_eq!(
        types,
        vec![
            Int32, Int64, Float64, Float32, Utf8, Date32, Int16, Boolean, Utf8
        ]
    );
    plan_error(
        "SELECT CAST(d AS BOOLEAN) FROM t",
        "cannot cast Date32 to Boolean",
    );
    unsupported(
        "SELECT CAST(a AS DECIMAL(10, 2)) FROM t",
        "the type DECIMAL(10,2)",
    );
    unsupported("SELECT TRY_CAST(a AS INT) FROM t", "TryCast");
}

#[test]
fn literals() {
    let types: Vec<DataType> = columns("SELECT 1, 1.5, 1e3, 'x', TRUE, NULL, DATE '2024-01-01'")
        .into_iter()
        .map(|(_, t)| t)
        .collect();
    use DataType::*;
    assert_eq!(
        types,
        vec![Int64, Float64, Float64, Utf8, Boolean, Null, Date32]
    );
    // The minus is part of the literal, so the smallest Int64 fits.
    assert_eq!(
        plan("SELECT -9223372036854775808").lines().next().unwrap(),
        "Projection: -9223372036854775808 AS \"-9223372036854775808\""
    );
    plan_error(
        "SELECT 9223372036854775808",
        "the integer 9223372036854775808 is out of range",
    );
    plan_error("SELECT 1e999", "out of range for a 64-bit float");
    plan_error("SELECT \"x\" FROM t", "column x not found");
}

#[test]
fn unsupported_types_are_named() {
    // A column of an unsupported type passes through, but no operator takes it.
    assert_eq!(columns("SELECT big FROM t")[0].1, DataType::UInt64);
    unsupported("SELECT big + 1 FROM t", "arithmetic on UInt64 and Int64");
    unsupported("SELECT a FROM t WHERE big = 1", "UInt64 values in big = 1");
    unsupported("SELECT min(big) FROM t", "min over the type UInt64");
    assert_eq!(columns("SELECT count(big) FROM t")[0].1, DataType::Int64);
}

// --- Aggregation ---

#[test]
fn group_by_with_aggregates() {
    assert_eq!(
        plan("SELECT s, sum(b) AS total, count(*) FROM t WHERE flag GROUP BY s"),
        lines(&[
            // count(*) is the Aggregate's column of that name, so it needs no AS.
            "Projection: t.s, sum(CAST(t.b AS Int64)) AS total, count(*)",
            "  Aggregate: group_by=[t.s] aggregates=[sum(CAST(t.b AS Int64)), count(*)]",
            "    Filter: t.flag",
            "      Scan: t",
        ])
    );
    let types: Vec<DataType> =
        columns("SELECT count(a), sum(b), sum(f), avg(b), min(s), max(d) FROM t")
            .into_iter()
            .map(|(_, t)| t)
            .collect();
    use DataType::*;
    assert_eq!(types, vec![Int64, Int64, Float64, Float64, Utf8, Date32]);
}

#[test]
fn an_expression_equal_to_a_group_expression_is_its_column() {
    assert_eq!(
        plan("SELECT a + 1, count(*) * 2 FROM t GROUP BY a + 1"),
        lines(&[
            "Projection: t.a + 1 AS \"a + 1\", count(*) * 2 AS \"count(*) * 2\"",
            "  Aggregate: group_by=[t.a + 1] aggregates=[count(*)]",
            "    Scan: t",
        ])
    );
    // The group column's name is its text over the input; the projection refers to it by index, so the printed
    // "t.a + 1" above is the Aggregate's output column, not a recomputation.
    let LogicalPlan::Projection(p) = bind("SELECT a + 1 FROM t GROUP BY a + 1") else {
        panic!()
    };
    assert_eq!(p.exprs, vec![quarry::plan::Expr::Column(0)]);
}

#[test]
fn ungrouped_columns_are_errors() {
    plan_error(
        "SELECT a, b FROM t GROUP BY a",
        "column t.b must appear in GROUP BY or be used in an aggregate (in the select list)",
    );
    plan_error(
        "SELECT a + b FROM t GROUP BY a",
        "column t.b must appear in GROUP BY",
    );
    plan_error(
        "SELECT a, count(*) FROM t",
        "column t.a must appear in GROUP BY",
    );
    plan_error(
        "SELECT count(*) FROM t HAVING b > 1",
        "column t.b must appear in GROUP BY or be used in an aggregate (in HAVING)",
    );
    plan_error(
        "SELECT count(*) FROM t ORDER BY b",
        "column t.b must appear in GROUP BY or be used in an aggregate (in ORDER BY)",
    );
    plan_error(
        "SELECT * FROM t GROUP BY a",
        "column t.b must appear in GROUP BY",
    );
}

#[test]
fn aggregates_where_none_are_allowed() {
    plan_error(
        "SELECT a FROM t WHERE count(*) > 1",
        "aggregates are not allowed in WHERE",
    );
    plan_error(
        "SELECT count(*) FROM t GROUP BY sum(a)",
        "aggregates are not allowed in GROUP BY",
    );
    plan_error(
        "SELECT sum(count(*)) FROM t",
        "aggregates are not allowed in the argument of another aggregate",
    );
    plan_error(
        "SELECT a FROM t JOIN u ON count(*) > 0",
        "aggregates are not allowed in JOIN ... ON",
    );
    plan_error("SELECT sum(s) FROM t", "sum needs a number, not Utf8");
    plan_error("SELECT avg(d) FROM t", "avg needs a number, not Date32");
    plan_error("SELECT sum(*) FROM t", "only COUNT(*) takes *");
    plan_error("SELECT sum(a, b) FROM t", "sum takes one argument");
    unsupported("SELECT lower(s) FROM t", "the function lower");
    unsupported("SELECT sum(a) OVER () FROM t", "window functions");
    unsupported("SELECT count(*) FILTER (WHERE flag) FROM t", "FILTER");
}

#[test]
fn having_and_aggregates_shared_between_clauses() {
    // sum(b) appears in the select list, HAVING and ORDER BY: the Aggregate computes it once.
    assert_eq!(
        plan(
            "SELECT s, sum(b) FROM t GROUP BY s HAVING sum(b) > 10 AND max(f) < 1 ORDER BY sum(b)"
        ),
        lines(&[
            "Sort: sum(b) ASC NULLS LAST",
            "  Projection: t.s, sum(CAST(t.b AS Int64)) AS \"sum(b)\"",
            "    Filter: sum(CAST(t.b AS Int64)) > 10 AND max(t.f) < CAST(1 AS Float64)",
            "      Aggregate: group_by=[t.s] aggregates=[sum(CAST(t.b AS Int64)), max(t.f)]",
            "        Scan: t",
        ])
    );
    // HAVING alone makes the query an aggregate over one group.
    assert_eq!(
        plan("SELECT count(*) FROM t HAVING count(*) > 0")
            .lines()
            .nth(2)
            .unwrap(),
        "    Aggregate: group_by=[] aggregates=[count(*)]"
    );
}

#[test]
fn group_by_ordinals_and_aliases() {
    let by_expr = plan("SELECT a % 3 AS k, count(*) FROM t GROUP BY a % 3");
    assert_eq!(
        plan("SELECT a % 3 AS k, count(*) FROM t GROUP BY 1"),
        by_expr
    );
    assert_eq!(
        plan("SELECT a % 3 AS k, count(*) FROM t GROUP BY k"),
        by_expr
    );
    // A FROM column wins over an alias of the same name.
    assert_eq!(
        plan("SELECT b AS a FROM t GROUP BY a, b")
            .lines()
            .nth(1)
            .unwrap(),
        "  Aggregate: group_by=[t.a, t.b] aggregates=[]"
    );
    plan_error(
        "SELECT count(*) FROM t GROUP BY 1",
        "GROUP BY position 1 refers to an aggregate",
    );
    plan_error(
        "SELECT a FROM t GROUP BY 2",
        "GROUP BY position 2 is not in the select list (it has 1 items)",
    );
    plan_error("SELECT a FROM t GROUP BY 0", "0 is not a column position");
    // Repeated group expressions are grouped once.
    assert_eq!(
        plan("SELECT a FROM t GROUP BY a, A, 1")
            .lines()
            .nth(1)
            .unwrap(),
        "  Aggregate: group_by=[t.a] aggregates=[]"
    );
}

#[test]
fn distinct_groups_by_every_output_column() {
    assert_eq!(
        plan("SELECT DISTINCT s, b FROM t ORDER BY b"),
        lines(&[
            "Sort: t.b ASC NULLS LAST",
            "  Aggregate: group_by=[t.s, t.b] aggregates=[]",
            "    Projection: t.s, t.b",
            "      Scan: t",
        ])
    );
    plan_error(
        "SELECT DISTINCT s FROM t ORDER BY b",
        "for SELECT DISTINCT, ORDER BY expressions must appear in the select list",
    );
    assert_eq!(
        columns("SELECT count(DISTINCT s) FROM t")[0].1,
        DataType::Int64
    );
    unsupported("SELECT DISTINCT ON (a) a FROM t", "DISTINCT ON");
}

// --- ORDER BY ---

#[test]
fn order_by_output_names_ordinals_and_hidden_columns() {
    assert_eq!(
        plan("SELECT b AS x FROM t ORDER BY x"),
        plan("SELECT b AS x FROM t ORDER BY 1")
    );
    // An output name wins over the input column it shadows: here output "a" is input b.
    assert_eq!(
        plan("SELECT b AS a, a AS b FROM t ORDER BY a")
            .lines()
            .next()
            .unwrap(),
        "Sort: a ASC NULLS LAST"
    );
    assert_eq!(
        plan("SELECT s FROM t ORDER BY f DESC NULLS FIRST, a"),
        lines(&[
            "Projection: t.s",
            "  Sort: __order_0 DESC NULLS FIRST, __order_1 ASC NULLS LAST",
            "    Projection: t.s, t.f AS __order_0, t.a AS __order_1",
            "      Scan: t",
        ])
    );
    // An expression equal to a select item sorts by that item, not a hidden copy.
    assert_eq!(
        plan("SELECT a + 1 AS n FROM t ORDER BY a + 1"),
        lines(&[
            "Sort: n ASC NULLS LAST",
            "  Projection: t.a + 1 AS n",
            "    Scan: t"
        ])
    );
    plan_error(
        "SELECT a FROM t ORDER BY 2",
        "ORDER BY position 2 is not in the select list (it has 1 items)",
    );
    plan_error(
        "SELECT a AS x, b AS x FROM t ORDER BY x",
        "ORDER BY x is ambiguous",
    );
    // The same expression twice under one name is not ambiguous.
    assert_eq!(
        plan("SELECT a, a FROM t ORDER BY a")
            .lines()
            .next()
            .unwrap(),
        "Sort: t.a ASC NULLS LAST"
    );
    // Postgres's rule: an output name inside an expression is not an output name.
    plan_error("SELECT b AS x FROM t ORDER BY x + 1", "column x not found");
}

#[test]
fn order_by_an_aggregate_not_in_the_select_list() {
    assert_eq!(
        plan("SELECT s FROM t GROUP BY s ORDER BY count(*) DESC LIMIT 3"),
        lines(&[
            "Limit: skip=0 fetch=3",
            "  Projection: t.s",
            "    Sort: __order_0 DESC NULLS LAST",
            "      Projection: t.s, count(*) AS __order_0",
            "        Aggregate: group_by=[t.s] aggregates=[count(*)]",
            "          Scan: t",
        ])
    );
}

#[test]
fn limit_and_offset() {
    // MySQL's LIMIT offset, count.
    assert_eq!(
        plan("SELECT a FROM t LIMIT 2, 3").lines().next().unwrap(),
        "Limit: skip=2 fetch=3"
    );
    assert_eq!(
        plan("SELECT a FROM t OFFSET 5").lines().next().unwrap(),
        "Limit: skip=5 fetch=all"
    );
    assert_eq!(
        plan("SELECT a FROM t LIMIT 0").lines().next().unwrap(),
        "Limit: skip=0 fetch=0"
    );
    assert_eq!(
        plan("SELECT a FROM t LIMIT NULL").lines().next().unwrap(),
        "Projection: t.a"
    );
    plan_error(
        "SELECT a FROM t LIMIT -1",
        "LIMIT must be a non-negative integer",
    );
    plan_error(
        "SELECT a FROM t LIMIT 1.5",
        "LIMIT must be a non-negative integer, not 1.5",
    );
    plan_error(
        "SELECT a FROM t LIMIT a",
        "LIMIT must be a non-negative integer, not a",
    );
    plan_error(
        "SELECT a FROM t OFFSET 'x'",
        "OFFSET must be a non-negative integer",
    );
}

// --- FROM and joins ---

#[test]
fn joins() {
    assert_eq!(
        plan("SELECT t.a, u.x FROM t JOIN u ON t.a = u.a WHERE u.x LIKE 'k%'"),
        lines(&[
            "Projection: t.a, u.x",
            "  Filter: u.x LIKE 'k%'",
            "    Join: Inner on t.a = u.a",
            "      Scan: t",
            "      Scan: u",
        ])
    );
    assert_eq!(
        plan("SELECT * FROM t, u").lines().nth(1).unwrap(),
        "  Join: Cross"
    );
    assert_eq!(
        plan("SELECT * FROM t CROSS JOIN u").lines().nth(1).unwrap(),
        "  Join: Cross"
    );
    // A LEFT join's right side becomes nullable.
    let schema = bind("SELECT u.x FROM t LEFT JOIN u ON t.a = u.a AND u.x IS NOT NULL")
        .schema()
        .clone();
    assert!(schema.field(0).nullable);
    let schema = bind("SELECT t.a FROM t LEFT JOIN u ON t.a = u.a")
        .schema()
        .clone();
    assert!(!schema.field(0).nullable);
    plan_error(
        "SELECT * FROM t JOIN u ON t.a + u.a",
        "a join condition must be a boolean, not Int64",
    );
    unsupported("SELECT * FROM t JOIN u USING (a)", "USING");
    unsupported("SELECT * FROM t NATURAL JOIN u", "NATURAL");
    unsupported(
        "SELECT * FROM t RIGHT JOIN u ON t.a = u.a",
        "the join Right",
    );
    unsupported(
        "SELECT * FROM t FULL OUTER JOIN u ON t.a = u.a",
        "the join FullOuter",
    );
}

#[test]
fn wildcards_expand_in_order() {
    let names: Vec<String> = columns("SELECT u.*, t.a FROM t, u")
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert_eq!(names, vec!["a", "x", "a"]);
    assert_eq!(columns("SELECT * FROM t, u").len(), 10);
    plan_error("SELECT v.* FROM t", "table v is not in FROM");
    unsupported("SELECT * EXCLUDE (a) FROM t", "EXCLUDE");
}

// --- What isn't supported, and EXPLAIN ---

#[test]
fn unsupported_sql_is_named() {
    unsupported("SELECT * FROM (SELECT a FROM t) AS sub", "subqueries");
    unsupported("SELECT a FROM t WHERE a IN (SELECT a FROM u)", "subqueries");
    unsupported("SELECT a FROM t WHERE EXISTS (SELECT 1)", "subqueries");
    unsupported("WITH c AS (SELECT 1) SELECT * FROM c", "WITH");
    unsupported("SELECT a FROM t UNION SELECT a FROM u", "UNION");
    unsupported("SELECT a FROM t GROUP BY ROLLUP (a)", "ROLLUP");
    unsupported("VALUES (1)", "VALUES");
    unsupported("INSERT INTO t VALUES (1)", "statements other than SELECT");
    unsupported("SELECT 1; SELECT 2", "2 statements at once");
    unsupported("SELECT a FROM main.t", "without a schema");
    unsupported("SELECT x FROM t AS y (x)", "column aliases in FROM");
    assert!(matches!(
        sql::plan(&catalog(), "SELEC 1"),
        Err(QuarryError::Sql(_))
    ));
    assert!(matches!(
        sql::plan(&catalog(), ""),
        Err(QuarryError::Plan(_))
    ));
}

#[test]
fn explain_binds_the_query_without_running_it() {
    let catalog = catalog();
    match sql::bind(&catalog, "EXPLAIN SELECT a FROM t").unwrap() {
        BoundStatement::Explain(plan) => {
            assert_eq!(plan.to_string(), "Projection: t.a\n  Scan: t\n")
        }
        other => panic!("{other:?}"),
    }
    assert!(sql::plan(&catalog, "EXPLAIN SELECT a FROM t").is_err());
    unsupported("EXPLAIN ANALYZE SELECT a FROM t", "EXPLAIN ANALYZE");
}

// --- Found by the day-1 review (each failed before its fix) ---

#[test]
fn group_by_an_ambiguous_from_column_is_not_an_alias() {
    // `a` is a FROM column twice (t.a, u.a); the alias fallback is only for names that aren't FROM columns at all.
    plan_error(
        "SELECT u.x AS a FROM t, u GROUP BY a",
        "column a is ambiguous: t.a, u.a",
    );
    // Two select items of that name with different expressions are ambiguous too, as in ORDER BY.
    plan_error(
        "SELECT a AS k, b AS k FROM t GROUP BY k",
        "GROUP BY k is ambiguous",
    );
    plan_error(
        "SELECT count(*) AS k FROM t GROUP BY k",
        "GROUP BY k refers to an aggregate",
    );
}

#[test]
fn an_empty_select_list_is_an_error() {
    // sqlparser accepts these. Bound, `SELECT DISTINCT FROM t` and `SELECT FROM t HAVING TRUE` were the same
    // zero-column Aggregate, though over an empty table the first has no rows and the second one.
    plan_error("SELECT FROM t", "the select list is empty");
    plan_error("SELECT DISTINCT FROM t", "the select list is empty");
    plan_error("SELECT FROM t HAVING TRUE", "the select list is empty");
}

#[test]
fn names_match_by_unicode_lowercase_everywhere() {
    // Table names already matched this way; columns and qualifiers compared ASCII case only.
    let mut catalog = Catalog::new();
    let schema = Schema::new(vec![Field::new("Été", DataType::Int64, true)]);
    catalog
        .register(
            "Ville",
            Arc::new(MemTable::from_batch(RecordBatch::new_empty(Arc::new(
                schema,
            )))),
        )
        .unwrap();
    let plan = sql::plan(&catalog, "SELECT été, VILLE.ÉTÉ FROM ville").unwrap();
    assert_eq!(
        plan.to_string(),
        "Projection: ville.Été, ville.Été\n  Scan: ville\n"
    );
}

#[test]
fn unary_minus_keeps_a_float_type() {
    assert_eq!(
        columns("SELECT -f32, +f32 FROM w")
            .into_iter()
            .map(|(_, t)| t)
            .collect::<Vec<_>>(),
        vec![DataType::Float32, DataType::Float32]
    );
    assert_eq!(columns("SELECT -b FROM t")[0].1, DataType::Int64);
}

#[test]
fn avg_sums_in_the_arguments_own_type() {
    // avg(DISTINCT a) bound as avg(DISTINCT CAST(a AS Float64)) deduplicated the rounded floats: over {0, 2^53,
    // 2^53 + 1} it would count two values, not three. The argument now widens as SUM's does.
    assert_eq!(
        plan("SELECT avg(DISTINCT a), avg(b), avg(f) FROM t")
            .lines()
            .nth(1)
            .unwrap(),
        "  Aggregate: group_by=[] aggregates=[avg(DISTINCT t.a), avg(CAST(t.b AS Int64)), avg(t.f)]"
    );
    assert!(
        columns("SELECT avg(a) FROM t")
            .iter()
            .all(|(_, t)| *t == DataType::Float64)
    );
}

/// Runs `f` on a thread with a 2 MiB stack, Rust's default for spawned threads and for test threads.
fn on_small_stack<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::Builder::new()
        .stack_size(2 << 20)
        .spawn(f)
        .unwrap()
        .join()
        .unwrap()
}

fn or_chain(terms: usize) -> String {
    let ors: String = (1..terms).map(|i| format!(" OR b = {i}")).collect();
    format!("SELECT a FROM t WHERE b = 0{ors}")
}

#[test]
fn deep_expressions_are_refused_not_a_stack_overflow() {
    // A 5,000-term OR chain aborted the process (the binder recursed once per OR). It is now an error.
    let e = error(&or_chain(5000));
    assert!(matches!(e, QuarryError::Unsupported(_)), "{e}");
    assert!(
        e.to_string()
            .contains(&format!("nested more than {} deep", sql::MAX_EXPR_DEPTH)),
        "{e}"
    );
    // Deep parentheses never get here: sqlparser's own recursion limit refuses them as a syntax error.
    let parens = error(&format!(
        "SELECT {}a{} FROM t",
        "(".repeat(300),
        ")".repeat(300)
    ));
    assert!(matches!(parens, QuarryError::Sql(_)), "{parens}");
    // Inside an aggregate's argument the depth keeps counting.
    let ors: String = (1..300).map(|i| format!(" OR b = {i}")).collect();
    let e = error(&format!("SELECT count(b = 0{ors}) FROM t"));
    assert!(e.to_string().contains("nested more than"), "{e}");
}

#[test]
fn the_deepest_expression_allowed_binds_and_prints_on_a_small_stack() {
    // Find the longest chain that binds, and check it is close to the limit: each OR term is one level, plus the
    // comparison and its cast operand. Every Int32 `b` is cast, so the bound tree is deeper than the SQL.
    let longest = on_small_stack(|| {
        let mut n = 1;
        while sql::plan(&catalog(), &or_chain(n + 1)).is_ok() {
            n += 1;
        }
        let plan = sql::plan(&catalog(), &or_chain(n)).unwrap();
        // Printing and dropping recurse too.
        assert!(plan.to_string().len() > 1000);
        n
    });
    assert!(
        (sql::MAX_EXPR_DEPTH - 5..=sql::MAX_EXPR_DEPTH).contains(&longest),
        "{longest}"
    );
}
