//! Queries run end to end: SQL text in, rows out (DESIGN.md, "Execution"). Every query runs at several batch sizes,
//! which must give the same rows.

use std::sync::Arc;

use quarry::arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray,
};
use quarry::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use quarry::arrow::util::display::array_value_to_string;
use quarry::datasource::{BatchIter, MemTable, TableProvider, TableStatistics};
use quarry::exec::{QueryResult, collect_with_batch_size};
use quarry::{Catalog, QuarryError, Result};

/// orders(id, customer, total, qty, placed, flag): six rows with a NULL in most columns and a -0.0 total.
fn orders() -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer", DataType::Utf8, true),
        Field::new("total", DataType::Float64, true),
        Field::new("qty", DataType::Int64, true),
        Field::new("placed", DataType::Date32, true),
        Field::new("flag", DataType::Boolean, true),
    ]);
    let day = |s: &str| quarry::types::parse_date32(s).unwrap();
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
            Arc::new(StringArray::from(vec![
                "ada", "bo", "ada", "cy", "bo", "ada",
            ])),
            Arc::new(Float64Array::from(vec![
                Some(10.5),
                Some(3.0),
                None,
                Some(7.25),
                Some(-0.0),
                Some(2.5),
            ])),
            Arc::new(Int64Array::from(vec![
                Some(2),
                Some(1),
                Some(5),
                None,
                Some(3),
                Some(2),
            ])),
            Arc::new(Date32Array::from(vec![
                Some(day("2024-01-02")),
                Some(day("2024-01-03")),
                Some(day("2024-02-01")),
                Some(day("2024-02-01")),
                Some(day("2024-03-15")),
                None,
            ])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
                Some(false),
                Some(true),
            ])),
        ],
    )
    .unwrap()
}

/// One column of values.
fn column(name: &str, values: ArrayRef) -> Arc<dyn TableProvider> {
    let schema = Schema::new(vec![Field::new(name, values.data_type().clone(), true)]);
    Arc::new(MemTable::from_batch(
        RecordBatch::try_new(Arc::new(schema), vec![values]).unwrap(),
    ))
}

fn catalog() -> Catalog {
    let mut c = Catalog::new();
    c.register("orders", Arc::new(MemTable::from_batch(orders())))
        .unwrap();
    c.register(
        "floats",
        column(
            "x",
            Arc::new(Float64Array::from(vec![
                Some(-0.0),
                Some(0.0),
                Some(f64::NAN),
                Some(-f64::NAN),
                None,
                Some(1.0),
            ])),
        ),
    )
    .unwrap();
    c.register(
        "big",
        column(
            "n",
            Arc::new(Int64Array::from(vec![i64::MAX, 1, -1, i64::MAX])),
        ),
    )
    .unwrap();
    c.register(
        "wraps",
        column("n", Arc::new(Int64Array::from(vec![i64::MAX, 1, -1]))),
    )
    .unwrap();
    c.register("customers", Arc::new(MemTable::from_batch(customers())))
        .unwrap();
    c.register(
        "nobody",
        Arc::new(MemTable::from_batch(customers().slice(0, 0))),
    )
    .unwrap();
    c
}

/// customers(name, city, rank): "bo" twice, a NULL name and a NULL rank; "cy" (an order's customer) is missing.
fn customers() -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("city", DataType::Utf8, false),
        Field::new("rank", DataType::Int64, true),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(StringArray::from(vec![
                Some("ada"),
                Some("bo"),
                Some("bo"),
                Some("dee"),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                "Oslo", "Lima", "Rome", "Pune", "Nowhere",
            ])),
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(4),
            ])),
        ],
    )
    .unwrap()
}

const BATCH_SIZES: [usize; 4] = [1, 2, 3, 8192];

/// Every value of a result as text, NULL for NULLs, row by row.
fn rows_of(result: &QueryResult) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for batch in &result.batches {
        let nulls: Vec<_> = batch.columns().iter().map(|c| c.logical_nulls()).collect();
        for i in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .zip(&nulls)
                    .map(|(c, n)| {
                        if n.as_ref().is_some_and(|n| n.is_null(i)) {
                            "NULL".to_string()
                        } else {
                            array_value_to_string(c, i).unwrap()
                        }
                    })
                    .collect(),
            );
        }
    }
    rows
}

/// Runs a query over `catalog` at every batch size, as bound and as optimized, and returns its rows, after checking
/// they are the same each time (or that every run fails, with the same error).
fn run_in(catalog: &Catalog, sql: &str) -> Result<Vec<Vec<String>>> {
    let bound = quarry::sql::plan(catalog, sql)?;
    let optimized = quarry::optimizer::optimize(&bound)?;
    let mut first: Option<Result<Vec<Vec<String>>, String>> = None;
    for (label, plan) in [("bound", &bound), ("optimized", &optimized)] {
        for size in BATCH_SIZES {
            let rows = collect_with_batch_size(plan, size).map(|result| {
                assert!(
                    result.batches.iter().all(|b| b.num_rows() <= size),
                    "{sql}: a batch over {size} rows"
                );
                rows_of(&result)
            });
            let rows = rows.map_err(|e| e.to_string());
            match &first {
                None => first = Some(rows),
                Some(expected) => assert_eq!(
                    &rows, expected,
                    "{sql}, {label}, at batch size {size}\n{optimized}"
                ),
            }
        }
    }
    first.unwrap().map_err(QuarryError::Execution)
}

fn run(sql: &str) -> Vec<Vec<String>> {
    run_in(&catalog(), sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
}

fn error(sql: &str) -> String {
    match run_in(&catalog(), sql) {
        Ok(rows) => panic!("{sql} gave {rows:?}, expected an error"),
        Err(e) => e.to_string(),
    }
}

/// Rows written as `[["1", "ada"], ...]`.
fn table(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| r.iter().map(|v| v.to_string()).collect())
        .collect()
}

#[test]
fn filter_and_projection() {
    assert_eq!(
        run("SELECT id, total * 2 AS t2 FROM orders WHERE customer = 'ada' AND total IS NOT NULL"),
        table(&[&["1", "21.0"], &["6", "5.0"]])
    );
    assert_eq!(
        run("SELECT id FROM orders WHERE placed BETWEEN '2024-01-03' AND '2024-02-01'"),
        table(&[&["2"], &["3"], &["4"]])
    );
    // A NULL predicate keeps no row; so does NULL itself.
    assert_eq!(
        run("SELECT id FROM orders WHERE flag"),
        table(&[&["1"], &["4"], &["6"]])
    );
    assert!(run("SELECT id FROM orders WHERE NULL").is_empty());
    assert!(run("SELECT id FROM orders WHERE qty > 100").is_empty());
}

#[test]
fn ors_with_common_conjuncts_keep_their_rows_once_factored() {
    // Rows 3 and 4 have a NULL on each side: NULL either way.
    assert_eq!(
        run("SELECT id FROM orders WHERE (qty > 1 AND total > 3.0) OR (flag AND qty > 1)"),
        table(&[&["1"], &["6"]])
    );
    assert_eq!(
        run("SELECT id FROM orders WHERE qty > 1 OR (customer = 'bo' AND qty > 1)"),
        table(&[&["1"], &["3"], &["5"], &["6"]])
    );
    // The modulo is common to both branches but can fail, so it stays behind each branch's guard: row 2's qty - 1 is
    // zero, and neither branch reaches the modulo there.
    assert_eq!(
        run(
            "SELECT id FROM orders WHERE (qty <> 1 AND 10 % (qty - 1) = 0) OR (id > 4 AND 10 % (qty - 1) = 0)"
        ),
        table(&[&["1"], &["5"], &["6"]])
    );
}

#[test]
fn select_without_from() {
    assert_eq!(run("SELECT 1 + 1, 'a' || 'b'"), table(&[&["2", "ab"]]));
    assert!(run("SELECT 1 WHERE FALSE").is_empty());
    assert_eq!(run("SELECT count(*)"), table(&[&["1"]]));
}

#[test]
fn the_result_has_the_plans_names_and_types() {
    let result = quarry::query(
        &catalog(),
        "SELECT id AS x, total * 2, customer FROM orders",
    )
    .unwrap();
    let names: Vec<&str> = result
        .schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(names, ["x", "total * 2", "customer"]);
    let types: Vec<&DataType> = result
        .schema
        .fields()
        .iter()
        .map(|f| f.data_type())
        .collect();
    assert_eq!(
        types,
        [&DataType::Int64, &DataType::Float64, &DataType::Utf8]
    );
    assert!(!result.schema.field(0).is_nullable());
    assert_eq!(result.num_rows(), 6);
    let empty = quarry::query(&catalog(), "SELECT id FROM orders WHERE FALSE").unwrap();
    assert_eq!(empty.num_rows(), 0);
    assert_eq!(empty.to_table().unwrap(), "+----+\n| id |\n+----+\n+----+");
}

#[test]
fn order_by() {
    let ids = |sql: &str| {
        run(sql)
            .into_iter()
            .map(|r| r[0].clone())
            .collect::<Vec<_>>()
    };
    // NULLs last in both directions unless NULLS FIRST; -0.0 sorts as 0.0.
    assert_eq!(
        ids("SELECT id FROM orders ORDER BY total DESC"),
        ["1", "4", "2", "6", "5", "3"]
    );
    assert_eq!(
        ids("SELECT id FROM orders ORDER BY total NULLS FIRST"),
        ["3", "5", "6", "2", "4", "1"]
    );
    assert_eq!(
        ids("SELECT id FROM orders ORDER BY customer, id DESC"),
        ["6", "3", "1", "5", "2", "4"]
    );
    // Ties keep their input order, at every batch size.
    assert_eq!(
        ids("SELECT id FROM orders ORDER BY customer"),
        ["1", "3", "6", "2", "5", "4"]
    );
    // A key that isn't selected is computed, sorted by and dropped.
    let rows = run("SELECT id FROM orders ORDER BY qty * -1, id");
    assert_eq!(
        rows,
        table(&[&["3"], &["5"], &["1"], &["6"], &["2"], &["4"]])
    );
    assert_eq!(
        ids("SELECT id FROM orders ORDER BY NULL, 1 DESC"),
        ["6", "5", "4", "3", "2", "1"]
    );
}

#[test]
fn limit_and_offset() {
    assert_eq!(
        run("SELECT id FROM orders ORDER BY id LIMIT 2 OFFSET 3"),
        table(&[&["4"], &["5"]])
    );
    assert!(run("SELECT id FROM orders LIMIT 0").is_empty());
    assert!(run("SELECT id FROM orders OFFSET 10").is_empty());
    assert_eq!(
        run("SELECT id FROM orders LIMIT NULL OFFSET 5"),
        table(&[&["6"]])
    );
}

/// A table whose second batch is an error: a LIMIT that has its rows must not read on.
#[derive(Debug)]
struct FailsAfterOneBatch;

impl TableProvider for FailsAfterOneBatch {
    fn schema(&self) -> SchemaRef {
        orders().schema()
    }

    fn scan(&self, _projection: Option<&[usize]>, _batch_size: usize) -> Result<BatchIter> {
        let error = QuarryError::Table("read past the limit".into());
        Ok(Box::new(vec![Ok(orders()), Err(error)].into_iter()))
    }

    fn statistics(&self) -> TableStatistics {
        TableStatistics::default()
    }
}

#[test]
fn a_limit_over_a_projection_computes_only_the_rows_it_keeps() {
    // Found by the day-2 review: the projection computed whole batches before the LIMIT sliced them, so row 3's
    // division by zero failed the query at batch sizes 3 and up but not at 1 or 2. DuckDB 2.0: [0, 0] and [0].
    assert_eq!(
        run("SELECT 10 % (id - 3) FROM orders LIMIT 2"),
        table(&[&["0"], &["0"]])
    );
    assert_eq!(
        run("SELECT 10 % (id - 3) FROM orders LIMIT 1 OFFSET 1"),
        table(&[&["0"]])
    );
    let e = error("SELECT 10 % (id - 3) FROM orders LIMIT 3");
    assert!(e.contains("division by zero"), "{e}");
    // Under an ORDER BY the select list is computed for every row before the sort, so this fails at any batch size.
    // DuckDB 2.0 fails it too, but it computes some select items after its top-N (a CAST, found by the second
    // review), where Quarry fails: day 3's top-k is the place to do the same.
    let e = error("SELECT 10 % (id - 4) FROM orders ORDER BY qty * -1, id LIMIT 3");
    assert!(e.contains("division by zero"), "{e}");
}

#[test]
fn a_top_k_whose_limit_and_offset_add_past_the_largest_count_keeps_everything_after_the_offset() {
    // The top-k keeps skip + fetch rows, added saturating: wrapping round would keep none (the day-3 review's case).
    assert_eq!(
        run("SELECT id FROM orders ORDER BY id DESC LIMIT 18446744073709551615 OFFSET 4"),
        table(&[&["2"], &["1"]])
    );
    assert_eq!(
        run("SELECT id FROM orders ORDER BY id LIMIT 2 OFFSET 18446744073709551615"),
        table(&[])
    );
}

#[test]
fn limit_stops_reading() {
    let mut c = Catalog::new();
    c.register("t", Arc::new(FailsAfterOneBatch)).unwrap();
    let plan = quarry::sql::plan(&c, "SELECT id FROM t WHERE id > 1 LIMIT 3").unwrap();
    let rows = rows_of(&collect_with_batch_size(&plan, 8192).unwrap());
    assert_eq!(rows, table(&[&["2"], &["3"], &["4"]]));
    let plan = quarry::sql::plan(&c, "SELECT id FROM t LIMIT 7").unwrap();
    let e = collect_with_batch_size(&plan, 8192).unwrap_err();
    assert!(e.to_string().contains("read past the limit"), "{e}");
}

#[test]
fn group_by_with_every_aggregate() {
    assert_eq!(
        run(
            "SELECT customer, count(*), count(total), sum(qty), avg(total), min(placed), max(customer), \
             min(flag), max(flag) FROM orders GROUP BY customer ORDER BY customer"
        ),
        table(&[
            &[
                "ada",
                "3",
                "2",
                "9",
                "6.5",
                "2024-01-02",
                "ada",
                "true",
                "true"
            ],
            &[
                "bo",
                "2",
                "2",
                "4",
                "1.5",
                "2024-01-03",
                "bo",
                "false",
                "false"
            ],
            &[
                "cy",
                "1",
                "1",
                "NULL",
                "7.25",
                "2024-02-01",
                "cy",
                "true",
                "true"
            ],
        ])
    );
}

#[test]
fn aggregates_over_no_rows() {
    // No GROUP BY: one row, even over nothing. With one: no rows.
    assert_eq!(
        run(
            "SELECT count(*), count(qty), sum(qty), avg(qty), min(total), max(customer) FROM orders WHERE id > 100"
        ),
        table(&[&["0", "0", "NULL", "NULL", "NULL", "NULL"]])
    );
    assert!(
        run("SELECT customer, count(*) FROM orders WHERE id > 100 GROUP BY customer").is_empty()
    );
    assert_eq!(
        run("SELECT sum(NULL), min(NULL), count(NULL)"),
        table(&[&["NULL", "NULL", "0"]])
    );
}

#[test]
fn group_by_expressions_having_and_distinct() {
    assert_eq!(
        run("SELECT qty % 2, count(*) FROM orders GROUP BY 1 ORDER BY 1"),
        table(&[&["0", "2"], &["1", "3"], &["NULL", "1"]])
    );
    assert_eq!(
        run("SELECT customer FROM orders GROUP BY customer HAVING count(*) > 1 ORDER BY customer"),
        table(&[&["ada"], &["bo"]])
    );
    assert_eq!(
        run("SELECT DISTINCT customer FROM orders ORDER BY 1"),
        table(&[&["ada"], &["bo"], &["cy"]])
    );
    assert_eq!(
        run("SELECT count(*) FROM orders GROUP BY NULL"),
        table(&[&["6"]])
    );
    assert_eq!(
        run(
            "SELECT sum(qty) * 2 AS doubled, customer FROM orders GROUP BY customer ORDER BY doubled DESC"
        ),
        table(&[&["18", "ada"], &["8", "bo"], &["NULL", "cy"]])
    );
}

#[test]
fn distinct_aggregates() {
    assert_eq!(
        run(
            "SELECT count(DISTINCT customer), sum(DISTINCT qty), count(DISTINCT total), avg(DISTINCT qty) FROM orders"
        ),
        table(&[&["3", "11", "5", "2.75"]])
    );
    assert_eq!(
        run("SELECT customer, count(DISTINCT qty) FROM orders GROUP BY customer ORDER BY customer"),
        table(&[&["ada", "2"], &["bo", "2"], &["cy", "0"]])
    );
}

#[test]
fn floats_group_and_count_as_duckdb_does() {
    // -0.0 and 0.0 are one group, every NaN another, as DuckDB 2.0 groups them; NaN sorts after the numbers. A group's
    // key is its first row's value (here -0.0), as DuckDB prints it.
    assert_eq!(
        run("SELECT x, count(*) FROM floats GROUP BY x ORDER BY x"),
        table(&[&["-0.0", "2"], &["1.0", "1"], &["NaN", "2"], &["NULL", "1"]])
    );
    // MIN takes the last of two equal values (0.0 after -0.0), as DuckDB does.
    assert_eq!(
        run("SELECT count(DISTINCT x), min(x), max(x) FROM floats"),
        table(&[&["3", "0.0", "NaN"]])
    );
}

#[test]
fn results_are_values_from_the_input_not_their_normalized_forms() {
    // Found by the day-2 review: -0.0 came back as 0.0 from MIN/MAX, group keys and DISTINCT, because the normalized
    // bytes used to compare were decoded into the result. 1 / x shows the sign. DuckDB 2.0, line by line: -inf, -inf;
    // -inf; -0.0; -inf, -inf; inf (MIN(DISTINCT) is MIN, and among equal values the last wins); inf.
    let mut c = catalog();
    c.register(
        "z",
        column("f", Arc::new(Float64Array::from(vec![-0.0, 0.0]))),
    )
    .unwrap();
    c.register(
        "zr",
        column("f", Arc::new(Float64Array::from(vec![0.0, -0.0]))),
    )
    .unwrap();
    let one = |sql: &str| run_in(&c, sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(
        one("SELECT 1 / min(f), 1 / max(f) FROM z WHERE 1 / f < 0"),
        table(&[&["-inf", "-inf"]])
    );
    assert_eq!(one("SELECT 1 / f FROM z GROUP BY f"), table(&[&["-inf"]]));
    assert_eq!(one("SELECT DISTINCT f FROM z"), table(&[&["-0.0"]]));
    assert_eq!(
        one("SELECT 1 / min(f), 1 / max(f) FROM zr"),
        table(&[&["-inf", "-inf"]])
    );
    assert_eq!(one("SELECT 1 / min(DISTINCT f) FROM z"), table(&[&["inf"]]));
    assert_eq!(
        one("SELECT 1 / sum(DISTINCT f) FROM zr"),
        table(&[&["inf"]])
    );
}

#[test]
fn a_float_key_next_to_an_integer_key_keeps_its_first_value() {
    // Only the float key is encoded a second time, as it was; the integer key next to it must still group and come
    // out right. DuckDB 2.0 on the same rows: (1, 0.0, inf, 2), (1, nan, nan, 1), (2, -0.0, -inf, 2), (2, NULL, NULL, 1).
    let schema = Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("f", DataType::Float64, true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 1, 2, 1, 2])),
            Arc::new(Float64Array::from(vec![
                Some(0.0),
                Some(-0.0),
                Some(-0.0),
                Some(0.0),
                Some(f64::NAN),
                None,
            ])),
        ],
    )
    .unwrap();
    let mut c = Catalog::new();
    c.register("m", Arc::new(MemTable::from_batch(batch)))
        .unwrap();
    assert_eq!(
        run_in(
            &c,
            "SELECT k, f, 1 / f, count(*) FROM m GROUP BY k, f ORDER BY k, f"
        )
        .unwrap(),
        table(&[
            &["1", "0.0", "inf", "2"],
            &["1", "NaN", "NaN", "1"],
            &["2", "-0.0", "-inf", "2"],
            &["2", "NULL", "NULL", "1"],
        ])
    );
    assert_eq!(
        run_in(&c, "SELECT DISTINCT f, k FROM m ORDER BY k, f").unwrap(),
        table(&[&["0.0", "1"], &["NaN", "1"], &["-0.0", "2"], &["NULL", "2"]])
    );
}

#[test]
fn sums_add_in_128_bits_and_fail_if_the_total_does_not_fit() {
    // MAX + 1 overflows an Int64 on the way, but MAX + 1 - 1 is back in range at the end.
    assert_eq!(
        run("SELECT sum(n) FROM wraps"),
        table(&[&["9223372036854775807"]])
    );
    assert_eq!(
        run("SELECT avg(n) FROM big"),
        table(&[&["4.611686018427388e18"]])
    );
    let e = error("SELECT sum(n) FROM big");
    assert!(
        e.contains("the sum 18446744073709551614 doesn't fit in an Int64"),
        "{e}"
    );
}

#[test]
fn errors_name_the_expression() {
    let e = error("SELECT id % (qty - qty) FROM orders");
    assert!(
        e.contains("division by zero in orders.id % (orders.qty - orders.qty)"),
        "{e}"
    );
    let e = error("SELECT sum(n) FROM big");
    assert!(e.contains("overflow in sum(big.n)"), "{e}");
}

// --- joins ---

#[test]
fn inner_and_left_joins_keep_the_left_order_and_each_rows_matches_in_build_order() {
    // "bo" has two customers rows, "cy" none, and the NULL name matches nothing.
    assert_eq!(
        run("SELECT o.id, c.city FROM orders o JOIN customers c ON o.customer = c.name"),
        table(&[
            &["1", "Oslo"],
            &["2", "Lima"],
            &["2", "Rome"],
            &["3", "Oslo"],
            &["5", "Lima"],
            &["5", "Rome"],
            &["6", "Oslo"],
        ])
    );
    assert_eq!(
        run("SELECT o.id, c.city FROM orders o LEFT JOIN customers c ON c.name = o.customer"),
        table(&[
            &["1", "Oslo"],
            &["2", "Lima"],
            &["2", "Rome"],
            &["3", "Oslo"],
            &["4", "NULL"],
            &["5", "Lima"],
            &["5", "Rome"],
            &["6", "Oslo"],
        ])
    );
}

#[test]
fn the_rest_of_on_decides_which_pairs_match() {
    let on = "ON o.customer = c.name AND c.rank > 1";
    assert_eq!(
        run(&format!(
            "SELECT o.id, c.city FROM orders o JOIN customers c {on}"
        )),
        table(&[
            &["2", "Lima"],
            &["2", "Rome"],
            &["5", "Lima"],
            &["5", "Rome"]
        ])
    );
    // In a LEFT join it decides matching, not which left rows come out: every left row is there.
    assert_eq!(
        run(&format!(
            "SELECT o.id, c.city FROM orders o LEFT JOIN customers c {on}"
        )),
        table(&[
            &["1", "NULL"],
            &["2", "Lima"],
            &["2", "Rome"],
            &["3", "NULL"],
            &["4", "NULL"],
            &["5", "Lima"],
            &["5", "Rome"],
            &["6", "NULL"],
        ])
    );
    // A condition on the left alone, in a LEFT join's ON, also only decides matching.
    assert_eq!(
        run(
            "SELECT o.id, c.rank FROM orders o LEFT JOIN customers c ON o.customer = c.name AND o.id > 4"
        ),
        table(&[
            &["1", "NULL"],
            &["2", "NULL"],
            &["3", "NULL"],
            &["4", "NULL"],
            &["5", "2"],
            &["5", "3"],
            &["6", "1"],
        ])
    );
}

#[test]
fn joins_without_equalities_are_nested_loops() {
    assert_eq!(
        run("SELECT count(*) FROM orders, customers"),
        table(&[&["30"]])
    );
    assert_eq!(
        run("SELECT count(*) FROM orders CROSS JOIN customers"),
        table(&[&["30"]])
    );
    assert_eq!(
        run("SELECT o.id, c.rank FROM orders o JOIN customers c ON o.qty > c.rank WHERE o.id < 4"),
        table(&[
            &["1", "1"],
            &["3", "1"],
            &["3", "2"],
            &["3", "3"],
            &["3", "4"]
        ])
    );
    assert_eq!(
        run("SELECT o.id, c.name FROM orders o, customers c WHERE o.id = c.rank"),
        table(&[&["1", "ada"], &["2", "bo"], &["3", "bo"], &["4", "NULL"]])
    );
    // A condition that is constant, or reads one side only, is not a key either.
    assert_eq!(
        run("SELECT count(*) FROM orders o JOIN customers c ON c.rank = 2"),
        table(&[&["6"]])
    );
    assert_eq!(
        run("SELECT o.id, c.city FROM orders o LEFT JOIN customers c ON FALSE WHERE o.id < 3"),
        table(&[&["1", "NULL"], &["2", "NULL"]])
    );
}

#[test]
fn join_keys_compare_as_sql_does() {
    // NULL keys meet nothing, on either side.
    assert_eq!(
        run("SELECT o.id, c.name FROM orders o JOIN customers c ON o.qty = c.rank"),
        table(&[&["1", "bo"], &["2", "ada"], &["5", "bo"], &["6", "bo"]])
    );
    // -0.0 meets 0.0 and NaN meets NaN, as in `=`; the values that come out are the input's.
    assert_eq!(
        run("SELECT a.x, b.x FROM floats a JOIN floats b ON a.x = b.x"),
        table(&[
            &["-0.0", "-0.0"],
            &["-0.0", "0.0"],
            &["0.0", "-0.0"],
            &["0.0", "0.0"],
            &["NaN", "NaN"],
            &["NaN", "NaN"],
            &["NaN", "NaN"],
            &["NaN", "NaN"],
            &["1.0", "1.0"],
        ])
    );
    // Keys of two types meet in the type the binder casts them to: total (Float64) = rank (Int64) as Float64.
    assert_eq!(
        run("SELECT o.id, c.city FROM orders o JOIN customers c ON o.total = c.rank"),
        table(&[&["2", "Rome"]])
    );
    // Keys can be expressions, several of them, written either way round.
    assert_eq!(
        run("SELECT o.id, c.city FROM orders o JOIN customers c ON c.rank = o.id + 1"),
        table(&[&["1", "Lima"], &["2", "Rome"], &["3", "Nowhere"]])
    );
    assert_eq!(
        run(
            "SELECT o.id, c.city FROM orders o JOIN customers c ON o.customer = c.name AND c.rank = o.qty"
        ),
        table(&[&["5", "Rome"]])
    );
}

#[test]
fn the_rest_of_on_is_computed_only_for_pairs_with_equal_keys_until_it_is_pushed_down() {
    // rank 4 is the NULL name's, which no order's key meets, so the hash join alone never computes the division by
    // zero; in a nested loop every pair is a candidate, and it fails.
    let c = catalog();
    let sql = "SELECT o.id FROM orders o JOIN customers c ON o.customer = c.name AND 5 % (c.rank - 4) = 1";
    let bound = quarry::sql::plan(&c, sql).unwrap();
    for size in BATCH_SIZES {
        let rows = rows_of(&collect_with_batch_size(&bound, size).unwrap());
        assert_eq!(rows, table(&[&["2"], &["5"]]));
    }
    let e = error("SELECT o.id FROM orders o JOIN customers c ON 5 % (c.rank - 4) = 1");
    assert!(e.contains("division by zero"), "{e}");
    // The optimizer moves the condition, which reads the right side only, below the join, onto every customer: then
    // it fails, as it does on DuckDB 2.0 (which moves it too).
    let optimized = quarry::optimizer::optimize(&bound).unwrap();
    assert!(
        optimized
            .to_string()
            .contains("Filter: 5 % (c.rank - 4) = 1"),
        "{optimized}"
    );
    let e = collect_with_batch_size(&optimized, 8192).unwrap_err();
    assert!(e.to_string().contains("division by zero"), "{e}");
}

#[test]
fn joins_with_an_empty_side() {
    assert_eq!(
        run("SELECT o.id, n.city FROM orders o JOIN nobody n ON o.customer = n.name"),
        table(&[])
    );
    assert_eq!(
        run(
            "SELECT o.id, n.name, n.city, n.rank FROM orders o LEFT JOIN nobody n ON o.customer = n.name WHERE o.id < 3"
        ),
        table(&[
            &["1", "NULL", "NULL", "NULL"],
            &["2", "NULL", "NULL", "NULL"]
        ])
    );
    assert_eq!(
        run("SELECT n.name, c.city FROM nobody n LEFT JOIN customers c ON n.name = c.name"),
        table(&[])
    );
    assert_eq!(run("SELECT count(*) FROM orders, nobody"), table(&[&["0"]]));
}

#[test]
fn an_inner_join_with_nothing_to_build_never_reads_its_left_input() {
    let mut c = catalog();
    c.register("t", Arc::new(FailsAfterOneBatch)).unwrap();
    let plan = quarry::sql::plan(
        &c,
        "SELECT t.id FROM t JOIN nobody n ON t.customer = n.name",
    )
    .unwrap();
    let rows = rows_of(&collect_with_batch_size(&plan, 8192).unwrap());
    assert!(rows.is_empty());
    let plan = quarry::sql::plan(
        &c,
        "SELECT t.id FROM t LEFT JOIN nobody n ON t.customer = n.name",
    )
    .unwrap();
    let e = collect_with_batch_size(&plan, 8192).unwrap_err();
    assert!(e.to_string().contains("read past the limit"), "{e}");
}

#[test]
fn joins_compose_with_the_other_operators() {
    // Three tables, an aggregate over the join, and an ORDER BY on both sides' columns.
    assert_eq!(
        run(
            "SELECT c.city, count(*), sum(o.qty) FROM orders o JOIN customers c ON o.customer = c.name \
             GROUP BY c.city ORDER BY c.city"
        ),
        table(&[
            &["Lima", "2", "4"],
            &["Oslo", "3", "9"],
            &["Rome", "2", "4"]
        ])
    );
    assert_eq!(
        run(
            "SELECT o.id, c.city, d.city FROM orders o JOIN customers c ON o.customer = c.name \
             LEFT JOIN customers d ON d.rank = c.rank + 1 WHERE o.id <= 2 ORDER BY o.id, d.city DESC"
        ),
        table(&[
            &["1", "Oslo", "Lima"],
            &["2", "Lima", "Rome"],
            &["2", "Rome", "Nowhere"]
        ])
    );
    assert_eq!(
        run(
            "SELECT a.id, b.id FROM orders a JOIN orders b ON a.customer = b.customer AND a.id < b.id ORDER BY 1, 2"
        ),
        table(&[&["1", "3"], &["1", "6"], &["2", "5"], &["3", "6"]])
    );
}

/// orders() written to a CSV file and to a Parquet file in row groups of two rows.
fn file_catalogs(dir: &std::path::Path) -> Vec<(&'static str, Catalog)> {
    use quarry::parquet::arrow::ArrowWriter;
    use quarry::parquet::file::properties::WriterProperties;

    let csv = dir.join("orders.csv");
    std::fs::write(
        &csv,
        "id,customer,total,qty,placed,flag\n\
         1,ada,10.5,2,2024-01-02,true\n\
         2,bo,3.0,1,2024-01-03,false\n\
         3,ada,,5,2024-02-01,\n\
         4,cy,7.25,,2024-02-01,true\n\
         5,bo,-0.0,3,2024-03-15,false\n\
         6,ada,2.5,2,,true\n",
    )
    .unwrap();
    let parquet = dir.join("orders.parquet");
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .build();
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&parquet).unwrap(),
        orders().schema(),
        Some(props),
    )
    .unwrap();
    writer.write(&orders()).unwrap();
    writer.close().unwrap();
    // customers(): the NULL name is an empty CSV field, as orders' NULLs are.
    let customers_csv = dir.join("customers.csv");
    std::fs::write(
        &customers_csv,
        "name,city,rank\nada,Oslo,1\nbo,Lima,2\nbo,Rome,3\ndee,Pune,\n,Nowhere,4\n",
    )
    .unwrap();
    let customers_parquet = dir.join("customers.parquet");
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&customers_parquet).unwrap(),
        customers().schema(),
        Some(
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(2))
                .build(),
        ),
    )
    .unwrap();
    writer.write(&customers()).unwrap();
    writer.close().unwrap();
    let mut out = Vec::new();
    for (label, orders, customers) in [
        ("csv", csv, customers_csv),
        ("parquet", parquet, customers_parquet),
    ] {
        let mut c = Catalog::new();
        c.register_file("orders", orders).unwrap();
        c.register_file("customers", customers).unwrap();
        out.push((label, c));
    }
    out
}

#[test]
fn csv_and_parquet_files_give_the_same_answers_as_memory() {
    let dir = tempfile::TempDir::new().unwrap();
    let queries = [
        "SELECT * FROM orders",
        "SELECT id, total * 2 AS t2 FROM orders WHERE customer = 'ada' AND total IS NOT NULL",
        "SELECT customer, count(*), count(total), sum(qty), avg(total), min(placed), max(flag) FROM orders \
         GROUP BY customer ORDER BY customer",
        "SELECT id FROM orders ORDER BY total DESC LIMIT 3 OFFSET 1",
        "SELECT DISTINCT placed FROM orders ORDER BY placed NULLS FIRST",
        "SELECT count(*), count(DISTINCT total) FROM orders WHERE placed > '2024-01-02'",
        "SELECT qty % 2 AS parity, sum(total) FROM orders GROUP BY parity ORDER BY parity",
        // Joins over files, pushdown into Parquet scans, and row groups that pruning skips (ids 5 and 6 are one).
        "SELECT o.id, c.city FROM orders o LEFT JOIN customers c ON o.customer = c.name AND c.rank > 1",
        "SELECT c.city, count(*), sum(o.qty) FROM orders o, customers c WHERE o.customer = c.name AND o.id < 5 \
         GROUP BY c.city ORDER BY c.city",
        "SELECT o.id, o.total FROM orders o JOIN customers c ON o.qty = c.rank WHERE o.id >= 5 AND c.city <> 'Lima'",
        "SELECT count(*) FROM orders WHERE id > 4 OR placed IS NULL",
    ];
    let memory = catalog();
    let files = file_catalogs(dir.path());
    for sql in queries {
        let want = run_in(&memory, sql).unwrap();
        for (label, c) in &files {
            let got = run_in(c, sql).unwrap_or_else(|e| panic!("{label}: {sql}: {e}"));
            assert_eq!(got, want, "{label}: {sql}");
        }
    }
}

/// A table whose scan hands out Int32 values under a schema that says Int64.
#[derive(Debug)]
struct WrongType;

impl TableProvider for WrongType {
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]))
    }

    fn scan(&self, _projection: Option<&[usize]>, _batch_size: usize) -> Result<BatchIter> {
        let schema = Schema::new(vec![Field::new("n", DataType::Int32, false)]);
        let values: ArrayRef = Arc::new(quarry::arrow::array::Int32Array::from(vec![1, 2]));
        let batch = RecordBatch::try_new(Arc::new(schema), vec![values]).unwrap();
        Ok(Box::new(std::iter::once(Ok(batch))))
    }

    fn statistics(&self) -> TableStatistics {
        TableStatistics::default()
    }
}

#[test]
fn a_result_that_disagrees_with_the_plan_is_an_error() {
    // A projection rebuilds its batches against its own schema, which catches this too; a bare Scan passes the
    // table's batches through, so only the final check against the plan's schema stands in the way.
    let plan = quarry::plan::LogicalPlan::scan("t", "t", Arc::new(WrongType));
    let e = collect_with_batch_size(&plan, 8192).unwrap_err();
    assert!(e.to_string().contains("Int32"), "{e}");
    let mut c = Catalog::new();
    c.register("t", Arc::new(WrongType)).unwrap();
    let e = quarry::query(&c, "SELECT * FROM t").unwrap_err();
    assert!(e.to_string().contains("Int32"), "{e}");
}
