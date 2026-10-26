//! The `quarry` binary, run as a user would run it.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

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
