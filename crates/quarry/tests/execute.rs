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
    c
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

/// Runs a query over `catalog` at every batch size and returns its rows, after checking they are the same each time.
fn run_in(catalog: &Catalog, sql: &str) -> Result<Vec<Vec<String>>> {
    let plan = quarry::sql::plan(catalog, sql)?;
    let mut first: Option<Vec<Vec<String>>> = None;
    for size in BATCH_SIZES {
        let result = collect_with_batch_size(&plan, size)?;
        assert!(
            result.batches.iter().all(|b| b.num_rows() <= size),
            "{sql}: a batch over {size} rows"
        );
        let rows = rows_of(&result);
        match &first {
            None => first = Some(rows),
            Some(expected) => assert_eq!(&rows, expected, "{sql} at batch size {size}"),
        }
    }
    Ok(first.unwrap())
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
    // -0.0 and 0.0 are one group, every NaN another, as DuckDB 2.0 groups them; NaN sorts after the numbers.
    assert_eq!(
        run("SELECT x, count(*) FROM floats GROUP BY x ORDER BY x"),
        table(&[&["0.0", "2"], &["1.0", "1"], &["NaN", "2"], &["NULL", "1"]])
    );
    assert_eq!(
        run("SELECT count(DISTINCT x), min(x), max(x) FROM floats"),
        table(&[&["3", "0.0", "NaN"]])
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
    let e = error("SELECT * FROM orders AS a JOIN orders AS b ON a.id = b.id");
    assert!(e.contains("not supported: running a join"), "{e}");
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
    let mut out = Vec::new();
    for (label, path) in [("csv", csv), ("parquet", parquet)] {
        let mut c = Catalog::new();
        c.register_file("orders", path).unwrap();
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
