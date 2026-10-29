//! The `quarry` binary, run as a user would run it.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};
use std::sync::Arc;

use quarry::arrow::array::{ArrayRef, Int32Array, StringArray};
use quarry::arrow::datatypes::{DataType, Field, Schema};
use quarry::arrow::record_batch::RecordBatch;
use quarry::parquet::arrow::ArrowWriter;
use quarry::parquet::file::properties::WriterProperties;
use tempfile::TempDir;

fn quarry(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_quarry"))
        .args(args)
        .output()
        .expect("the binary runs")
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).unwrap()
}

fn stderr(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).unwrap()
}

fn sample(dir: &Path) -> String {
    let path = dir.join("orders.csv");
    fs::write(
        &path,
        "id,customer,total,placed\n1,ada,10.5,2024-01-02\n2,bo,3,2024-01-03\n3,ada,,2024-02-01\n",
    )
    .unwrap();
    path.to_str().unwrap().to_string()
}

#[test]
fn schema_lists_columns_and_types() {
    let dir = TempDir::new().unwrap();
    let out = quarry(&["schema", &sample(dir.path())]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(
        stdout(&out),
        "id: Int64\ncustomer: Utf8\ntotal: Float64\nplaced: Date32\n"
    );
}

#[test]
fn head_prints_the_first_rows() {
    let dir = TempDir::new().unwrap();
    let out = quarry(&["head", "-n", "2", &sample(dir.path())]);
    assert!(out.status.success(), "{}", stderr(&out));
    let text = stdout(&out);
    assert!(
        text.contains("| 1  | ada      | 10.5  | 2024-01-02 |"),
        "{text}"
    );
    assert!(
        text.contains("| 2  | bo       | 3.0   | 2024-01-03 |"),
        "{text}"
    );
    assert!(!text.contains("2024-02-01"), "{text}");
    // Zero rows prints the header alone.
    let out = quarry(&["head", "-n", "0", &sample(dir.path())]);
    assert!(
        stdout(&out).contains("| id | customer | total | placed |"),
        "{}",
        stdout(&out)
    );
}

#[test]
fn plan_prints_the_bound_plan() {
    let dir = TempDir::new().unwrap();
    let path = sample(dir.path());
    let sql = "SELECT customer, sum(total) AS spent FROM orders WHERE placed < '2024-02-01' GROUP BY customer ORDER BY spent DESC";
    let out = quarry(&["plan", "--table", &path, sql]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(
        stdout(&out),
        "Sort: spent DESC NULLS LAST\n\
         \x20 Projection: orders.customer, sum(orders.total) AS spent\n\
         \x20   Aggregate: group_by=[orders.customer] aggregates=[sum(orders.total)]\n\
         \x20     Filter: orders.placed < DATE '2024-02-01'\n\
         \x20       Scan: orders\n"
    );
    // name=path, and EXPLAIN.
    let out = quarry(&[
        "plan",
        "-t",
        &format!("o={path}"),
        "EXPLAIN SELECT count(*) FROM o",
    ]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(
        stdout(&out).starts_with("Projection: count(*)\n"),
        "{}",
        stdout(&out)
    );
}

#[test]
fn errors_go_to_stderr_with_status_1() {
    let dir = TempDir::new().unwrap();
    let path = sample(dir.path());
    let out = quarry(&["plan", "--table", &path, "SELECT nope FROM orders"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stdout(&out).is_empty());
    assert!(
        stderr(&out).starts_with("error: plan error: column nope not found"),
        "{}",
        stderr(&out)
    );

    let out = quarry(&[
        "schema",
        &dir.path().join("missing.parquet").to_string_lossy(),
    ]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("cannot open"), "{}", stderr(&out));

    let out = quarry(&["plan", "-t", "=x.csv", "SELECT 1"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(
        stderr(&out).contains("expected NAME=PATH"),
        "{}",
        stderr(&out)
    );
}

#[test]
fn a_bad_command_line_is_status_2() {
    assert_eq!(quarry(&["frobnicate"]).status.code(), Some(2));
    assert_eq!(
        quarry(&["head", "-n", "lots", "x.csv"]).status.code(),
        Some(2)
    );
    let version = quarry(&["--version"]);
    assert_eq!(
        stdout(&version),
        format!("quarry {}\n", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn query_prints_the_result() {
    let dir = TempDir::new().unwrap();
    let orders = sample(dir.path());
    let out = quarry(&[
        "query",
        "-t",
        &orders,
        "SELECT customer, count(*), sum(total) FROM orders GROUP BY customer ORDER BY customer",
    ]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(
        stdout(&out),
        "+----------+----------+------------+\n\
         | customer | count(*) | sum(total) |\n\
         +----------+----------+------------+\n\
         | ada      | 2        | 10.5       |\n\
         | bo       | 1        | 3.0        |\n\
         +----------+----------+------------+\n\
         2 rows\n"
    );
    let out = quarry(&[
        "query",
        "-t",
        &orders,
        "SELECT id, total FROM orders WHERE total IS NULL",
    ]);
    assert_eq!(
        stdout(&out),
        "+----+-------+\n| id | total |\n+----+-------+\n| 3  | NULL  |\n+----+-------+\n1 row\n"
    );
    let out = quarry(&["query", "-t", &orders, "SELECT id FROM orders WHERE FALSE"]);
    assert_eq!(stdout(&out), "+----+\n| id |\n+----+\n+----+\n0 rows\n");
    // EXPLAIN prints the optimized plan, the one that runs, instead of running it; `plan` prints the bound one.
    let out = quarry(&[
        "query",
        "-t",
        &orders,
        "EXPLAIN SELECT id FROM orders WHERE 1 < 2 LIMIT 1",
    ]);
    assert_eq!(
        stdout(&out),
        "Limit: skip=0 fetch=1\n  Projection: orders.id\n    Scan: orders projection=[id]\n"
    );
    let out = quarry(&[
        "plan",
        "-t",
        &orders,
        "SELECT id FROM orders WHERE 1 < 2 LIMIT 1",
    ]);
    assert_eq!(
        stdout(&out),
        "Limit: skip=0 fetch=1\n  Projection: orders.id\n    Filter: 1 < 2\n      Scan: orders\n"
    );
}

#[test]
fn query_output_writes_the_result_as_parquet() {
    let dir = TempDir::new().unwrap();
    let orders = sample(dir.path());
    let out_path = dir.path().join("out.parquet");
    let out_file = out_path.to_str().unwrap();
    // After `--`, a query may start with a SQL comment.
    let out = quarry(&[
        "query",
        "-t",
        &orders,
        "--output",
        out_file,
        "--",
        "-- totals\nSELECT id, total, total * 2 AS twice FROM orders ORDER BY id",
    ]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(stdout(&out), format!("3 rows written to {out_file}\n"));
    // The file has the result's names, types and nullability, and its NULL.
    let out = quarry(&["schema", out_file]);
    assert_eq!(
        stdout(&out),
        "id: Int64\ntotal: Float64\ntwice: Float64\nrows: 3\nrow groups: 1 (3 rows)\n"
    );
    let out = quarry(&[
        "query",
        "-t",
        out_file,
        "SELECT * FROM out WHERE total IS NULL",
    ]);
    assert_eq!(
        stdout(&out),
        "+----+-------+-------+\n| id | total | twice |\n+----+-------+-------+\n\
         | 3  | NULL  | NULL  |\n+----+-------+-------+\n1 row\n"
    );
    // No rows: the file still has the columns.
    let out = quarry(&[
        "query",
        "-t",
        &orders,
        "-o",
        out_file,
        "SELECT id FROM orders WHERE FALSE",
    ]);
    assert_eq!(stdout(&out), format!("0 rows written to {out_file}\n"));
    assert_eq!(
        stdout(&quarry(&["schema", out_file])),
        "id: Int64\nrows: 0\n"
    );
    // A file that can't be created is named in the error.
    let missing = dir.path().join("no-such-dir").join("out.parquet");
    let missing = missing.to_str().unwrap();
    let out = quarry(&[
        "query",
        "-t",
        &orders,
        "-o",
        missing,
        "SELECT id FROM orders",
    ]);
    assert_eq!(out.status.code(), Some(1));
    assert!(
        stderr(&out).starts_with(&format!("error: table error: --output {missing}: ")),
        "{}",
        stderr(&out)
    );
    // EXPLAIN has no rows to write.
    let out = quarry(&[
        "query",
        "-t",
        &orders,
        "-o",
        out_file,
        "EXPLAIN SELECT id FROM orders",
    ]);
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(
        stderr(&out),
        "error: plan error: --output takes a query's rows; EXPLAIN prints a plan\n"
    );
}

#[test]
fn query_batch_size_sets_the_batches() {
    let dir = TempDir::new().unwrap();
    let orders = sample(dir.path());
    let sql = "SELECT customer, count(*) FROM orders GROUP BY customer ORDER BY 2 DESC, 1 LIMIT 1 OFFSET 1";
    let default = stdout(&quarry(&["query", "-t", &orders, sql]));
    assert!(default.ends_with("1 row\n"), "{default}");
    for size in ["1", "2", "8192"] {
        let out = quarry(&["query", "--batch-size", size, "-t", &orders, sql]);
        assert_eq!(stdout(&out), default, "batch size {size}");
    }
    // The batches are really that size: an error under a LIMIT depends on them (DESIGN.md, "Execution"). One row a
    // batch, the LIMIT is met at id 1 and id 2's division by zero never runs; in one batch it does.
    let limited = "SELECT id FROM orders WHERE 10 % (id - 2) = 0 LIMIT 1";
    let out = quarry(&["query", "--batch-size", "1", "-t", &orders, limited]);
    assert!(
        stdout(&out).ends_with("| 1  |\n+----+\n1 row\n"),
        "{}",
        stdout(&out)
    );
    // Two rows a batch: id 2 is in the first batch, so it fails too (every size, not just 1, is passed through).
    let out = quarry(&["query", "--batch-size", "2", "-t", &orders, limited]);
    assert_eq!(out.status.code(), Some(1), "{}", stdout(&out));
    let out = quarry(&["query", "-t", &orders, limited]);
    assert_eq!(
        stderr(&out),
        "error: execution error: division by zero in 10 % (orders.id - 2)\n"
    );
    let out = quarry(&["query", "--batch-size", "0", "-t", &orders, sql]);
    assert_eq!(out.status.code(), Some(2));
    assert!(
        stderr(&out).contains("invalid value '0'"),
        "{}",
        stderr(&out)
    );
}

#[test]
fn a_query_that_fails_while_running_is_status_1() {
    let dir = TempDir::new().unwrap();
    let out = quarry(&[
        "query",
        "-t",
        &sample(dir.path()),
        "SELECT id % (id - id) FROM orders",
    ]);
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(
        stderr(&out),
        "error: execution error: division by zero in orders.id % (orders.id - orders.id)\n"
    );
    assert!(stdout(&out).is_empty());
}

#[test]
fn the_readme_examples_print_what_the_readme_says() {
    let orders = concat!(env!("CARGO_MANIFEST_DIR"), "/../../examples/orders.csv");
    let readme =
        fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../README.md")).unwrap();

    let schema = stdout(&quarry(&["schema", orders]));
    assert!(
        readme.contains(&schema),
        "README's schema output is stale:\n{schema}"
    );

    let sql = "SELECT customer, count(*) AS orders, sum(total) AS spent FROM orders WHERE status <> 'cancelled' \
               AND placed >= '2024-01-01' GROUP BY customer HAVING count(*) > 1 ORDER BY spent DESC LIMIT 3";
    let plan = stdout(&quarry(&["plan", "-t", orders, sql]));
    assert!(
        readme.contains(sql),
        "README's query differs from the test's"
    );
    assert!(readme.contains(&plan), "README's plan is stale:\n{plan}");
    let result = stdout(&quarry(&["query", "-t", orders, sql]));
    assert!(
        readme.contains(&result),
        "README's result is stale:\n{result}"
    );
    let explain = stdout(&quarry(&["query", "-t", orders, &format!("EXPLAIN {sql}")]));
    assert!(
        readme.contains(&explain),
        "README's EXPLAIN is stale:\n{explain}"
    );

    let customers = concat!(env!("CARGO_MANIFEST_DIR"), "/../../examples/customers.csv");
    let join = "SELECT c.tier, count(*) AS orders, sum(o.total) AS spent FROM orders o LEFT JOIN customers c \
                ON o.customer = c.customer WHERE o.status <> 'cancelled' GROUP BY c.tier ORDER BY c.tier";
    assert!(
        readme.contains(join),
        "README's join differs from the test's"
    );
    for sql in [join.to_string(), format!("EXPLAIN {join}")] {
        let out = stdout(&quarry(&["query", "-t", orders, "-t", customers, &sql]));
        assert!(
            readme.contains(&out),
            "README's join output is stale:\n{out}"
        );
    }

    let out = quarry(&[
        "plan",
        "-t",
        orders,
        "SELECT customer FROM orders WHERE total > 'big'",
    ]);
    assert_eq!(out.status.code(), Some(1));
    assert!(
        readme.contains(stderr(&out).trim_end()),
        "README's error is stale: {}",
        stderr(&out)
    );
}

/// Five rows in row groups of two: ids 1-5 (not null) and names with one NULL.
fn sample_parquet(dir: &Path) -> String {
    let ids: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
    let names: ArrayRef = Arc::new(StringArray::from(vec![
        Some("a"),
        None,
        Some("c"),
        Some("d"),
        Some("e"),
    ]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![ids, names]).unwrap();
    let path = dir.join("people.parquet");
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .build();
    let mut writer =
        ArrowWriter::try_new(fs::File::create(&path).unwrap(), schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    path.to_str().unwrap().to_string()
}

#[test]
fn parquet_schema_head_and_plan() {
    let dir = TempDir::new().unwrap();
    let path = sample_parquet(dir.path());
    let out = quarry(&["schema", &path]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(
        stdout(&out),
        "id: Int32 not null\nname: Utf8\nrows: 5\nrow groups: 3 (2, 2, 1 rows)\n"
    );
    // head reads across row groups and stops at n.
    let text = stdout(&quarry(&["head", "-n", "3", &path]));
    assert!(text.contains("| 3  | c    |"), "{text}");
    assert!(!text.contains("| 4  |"), "{text}");
    let plan = stdout(&quarry(&[
        "plan",
        "-t",
        &path,
        "SELECT name FROM people WHERE id % 2 = 1",
    ]));
    assert_eq!(
        plan,
        "Projection: people.name\n  Filter: CAST(people.id AS Int64) % 2 = 1\n    Scan: people\n"
    );
}
