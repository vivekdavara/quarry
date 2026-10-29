//! The generator's files: their schemas and row counts, that they are the same on every run, and that the rules tying
//! columns together (the crate's documentation lists them) hold on every row.

use std::collections::HashMap;
use std::path::Path;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::compute::concat_batches;
use arrow::datatypes::{Date32Type, Float64Type, Int64Type};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use quarry_tpch::{
    CURRENT_DATE, Counts, END_DATE, NATIONS, START_DATE, TABLES, generate, line_total_cents,
    retail_price_cents, schema,
};

fn read(dir: &Path, table: &str) -> RecordBatch {
    let file = std::fs::File::open(dir.join(format!("{table}.parquet"))).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let batches: Vec<RecordBatch> = reader.map(Result::unwrap).collect();
    concat_batches(&schema(table).unwrap(), &batches).unwrap()
}

fn ints(batch: &RecordBatch, column: &str) -> Vec<i64> {
    batch[column].as_primitive::<Int64Type>().values().to_vec()
}

fn floats(batch: &RecordBatch, column: &str) -> Vec<f64> {
    batch[column]
        .as_primitive::<Float64Type>()
        .values()
        .to_vec()
}

fn dates(batch: &RecordBatch, column: &str) -> Vec<i32> {
    batch[column].as_primitive::<Date32Type>().values().to_vec()
}

fn strings(batch: &RecordBatch, column: &str) -> Vec<String> {
    batch[column]
        .as_string::<i32>()
        .iter()
        .map(|s| s.unwrap().to_string())
        .collect()
}

/// A DOUBLE written from whole cents, back to cents (exact: every value is `c / 100.0` for an integer `c`).
fn to_cents(x: f64) -> i64 {
    let c = (x * 100.0).round() as i64;
    assert_eq!(c as f64 / 100.0, x, "{x} isn't a whole number of cents");
    c
}

#[test]
fn counts_follow_the_scale_factor() {
    let one = Counts::at(1.0).unwrap();
    assert_eq!(
        (one.supplier, one.customer, one.part, one.orders, one.clerks),
        (10_000, 150_000, 200_000, 1_500_000, 1_000)
    );
    let small = Counts::at(0.01).unwrap();
    assert_eq!(
        (
            small.supplier,
            small.customer,
            small.part,
            small.orders,
            small.clerks
        ),
        (100, 1_500, 2_000, 15_000, 10)
    );
    // Every table keeps at least one row, however small the factor.
    let tiny = Counts::at(1e-9).unwrap();
    assert_eq!((tiny.supplier, tiny.customer, tiny.orders), (1, 1, 1));
    for bad in [0.0, -1.0, f64::NAN, f64::INFINITY, 1000.5] {
        let error = Counts::at(bad).unwrap_err().to_string();
        assert!(error.contains("scale factor"), "{bad}: {error}");
    }
}

#[test]
fn the_dates_are_tpch_dates() {
    use arrow::array::StringArray;
    use arrow::compute::cast;
    use arrow::datatypes::DataType;
    let text = StringArray::from(vec!["1992-01-01", "1998-12-31", "1995-06-17"]);
    let days = cast(&text, &DataType::Date32).unwrap();
    assert_eq!(
        days.as_primitive::<Date32Type>().values().to_vec(),
        vec![START_DATE, END_DATE, CURRENT_DATE]
    );
}

#[test]
fn files_are_the_same_on_every_run() {
    let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let first = generate(0.002, a.path()).unwrap();
    let second = generate(0.002, b.path()).unwrap();
    for (x, y) in first.iter().zip(&second) {
        assert_eq!(
            std::fs::read(x).unwrap(),
            std::fs::read(y).unwrap(),
            "{x:?}"
        );
    }
    // Pinned, so a change to the generator is a deliberate one: the sum of every line's total in cents at this scale.
    let lines = read(a.path(), "lineitem");
    let total: i64 = floats(&lines, "l_extendedprice")
        .iter()
        .map(|x| to_cents(*x))
        .sum();
    assert_eq!((lines.num_rows(), total), (12_046, 33_801_168_106));
}

#[test]
fn the_batch_size_does_not_change_the_data() {
    // Batches of 7 rows and 3 orders cross hundreds of batch boundaries at this scale; every table must read back the
    // same, row for row, as with the default batches (65,536 rows and 16,384 orders, one batch here).
    let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    generate(0.002, a.path()).unwrap();
    quarry_tpch::generate_in_batches(0.002, b.path(), 7, 3).unwrap();
    for table in TABLES {
        assert_eq!(read(a.path(), table), read(b.path(), table), "{table}");
    }
}

#[test]
fn every_row_follows_the_rules() {
    let dir = tempfile::tempdir().unwrap();
    let paths = generate(0.005, dir.path()).unwrap();
    let names: Vec<String> = paths
        .iter()
        .map(|p| p.file_stem().unwrap().to_str().unwrap().to_string())
        .collect();
    assert_eq!(names, TABLES);
    let counts = Counts::at(0.005).unwrap();

    let region = read(dir.path(), "region");
    let nation = read(dir.path(), "nation");
    assert_eq!((region.num_rows(), nation.num_rows()), (5, 25));
    assert_eq!(ints(&nation, "n_nationkey"), (0..25).collect::<Vec<_>>());
    assert_eq!(
        ints(&nation, "n_regionkey"),
        NATIONS.iter().map(|(_, r)| *r).collect::<Vec<_>>()
    );

    for (table, rows, key, nation_key, phone) in [
        (
            "supplier",
            counts.supplier,
            "s_suppkey",
            "s_nationkey",
            "s_phone",
        ),
        (
            "customer",
            counts.customer,
            "c_custkey",
            "c_nationkey",
            "c_phone",
        ),
    ] {
        let t = read(dir.path(), table);
        assert_eq!(t.num_rows() as i64, rows, "{table}");
        assert_eq!(ints(&t, key), (1..=rows).collect::<Vec<_>>(), "{table}");
        for (n, p) in ints(&t, nation_key).iter().zip(strings(&t, phone)) {
            assert!((0..25).contains(n), "{table}: nation {n}");
            assert!(p.starts_with(&format!("{}-", n + 10)), "{table}: {p}");
        }
    }

    let part = read(dir.path(), "part");
    assert_eq!(part.num_rows() as i64, counts.part);
    let prices: HashMap<i64, i64> = ints(&part, "p_partkey")
        .into_iter()
        .zip(floats(&part, "p_retailprice").iter().map(|x| to_cents(*x)))
        .collect();
    for (k, price) in &prices {
        assert_eq!(*price, retail_price_cents(*k));
    }
    for size in ints(&part, "p_size") {
        assert!((1..=50).contains(&size));
    }

    let orders = read(dir.path(), "orders");
    let lines = read(dir.path(), "lineitem");
    assert_eq!(orders.num_rows() as i64, counts.orders);
    assert_eq!(
        ints(&orders, "o_orderkey"),
        (1..=counts.orders).collect::<Vec<_>>()
    );
    let order_dates = dates(&orders, "o_orderdate");
    for (c, d) in ints(&orders, "o_custkey").iter().zip(&order_dates) {
        assert!(
            (1..=counts.customer).contains(c) && c % 3 != 0,
            "customer {c}"
        );
        assert!((START_DATE..=END_DATE - 151).contains(d), "order date {d}");
    }

    // Each order's lines: 1-7 of them, numbered from 1, in order-key order; their dates, flags and statuses follow
    // the order's date and the current date; the order's status and total follow from them.
    let (order_keys, numbers) = (ints(&lines, "l_orderkey"), ints(&lines, "l_linenumber"));
    let (parts, suppliers) = (ints(&lines, "l_partkey"), ints(&lines, "l_suppkey"));
    let (quantity, extended) = (
        floats(&lines, "l_quantity"),
        floats(&lines, "l_extendedprice"),
    );
    let (discount, tax) = (floats(&lines, "l_discount"), floats(&lines, "l_tax"));
    let (ship, commit, receipt) = (
        dates(&lines, "l_shipdate"),
        dates(&lines, "l_commitdate"),
        dates(&lines, "l_receiptdate"),
    );
    let (flags, statuses) = (
        strings(&lines, "l_returnflag"),
        strings(&lines, "l_linestatus"),
    );
    let mut totals = vec![0i64; counts.orders as usize + 1];
    let mut shipped = vec![0usize; counts.orders as usize + 1];
    let mut per_order = vec![0usize; counts.orders as usize + 1];
    for i in 0..lines.num_rows() {
        let o = order_keys[i];
        assert!(
            i == 0 || o >= order_keys[i - 1],
            "lines out of order at {i}"
        );
        per_order[o as usize] += 1;
        assert_eq!(numbers[i], per_order[o as usize] as i64, "line {i}");
        assert!((1..=counts.supplier).contains(&suppliers[i]));
        let q = quantity[i] as i64;
        assert!((1..=50).contains(&q) && q as f64 == quantity[i]);
        let ext = to_cents(extended[i]);
        assert_eq!(ext, q * prices[&parts[i]], "line {i}");
        let (d, t) = (to_cents(discount[i]), to_cents(tax[i]));
        assert!((0..=10).contains(&d) && (0..=8).contains(&t));
        let date = order_dates[o as usize - 1];
        assert!((1..=121).contains(&(ship[i] - date)), "line {i}");
        assert!((30..=90).contains(&(commit[i] - date)), "line {i}");
        assert!((1..=30).contains(&(receipt[i] - ship[i])), "line {i}");
        let flag = flags[i].as_str();
        if receipt[i] <= CURRENT_DATE {
            assert!(flag == "R" || flag == "A", "line {i}: {flag}");
        } else {
            assert_eq!(flag, "N", "line {i}");
        }
        let status = if ship[i] > CURRENT_DATE { "O" } else { "F" };
        assert_eq!(statuses[i], status, "line {i}");
        shipped[o as usize] += (status == "F") as usize;
        totals[o as usize] += line_total_cents(ext, d, t);
    }
    let order_status = strings(&orders, "o_orderstatus");
    let order_total = floats(&orders, "o_totalprice");
    for o in 1..=counts.orders as usize {
        assert!((1..=7).contains(&per_order[o]), "order {o}");
        let status = match shipped[o] {
            0 => "O",
            s if s == per_order[o] => "F",
            _ => "P",
        };
        assert_eq!(order_status[o - 1], status, "order {o}");
        assert_eq!(to_cents(order_total[o - 1]), totals[o], "order {o}");
    }
    // Every return flag and status shows up: the date rules aren't all on one side of the current date.
    for flag in ["R", "A", "N"] {
        assert!(flags.iter().any(|f| f == flag), "{flag}");
    }
    for status in ["O", "F", "P"] {
        assert!(order_status.iter().any(|s| s == status), "{status}");
    }
    assert!(lines.columns().iter().all(|c| c.null_count() == 0));
}
