//! Row-group pruning (DESIGN.md, "Row-group pruning"): skipping is only an optimization, so a query must give the
//! same rows with it as without it. Random rows (NULLs, -0.0, NaNs, strings, dates) are written to a Parquet file in
//! row groups of 1-4 rows, and random filters run over it optimized (pruned) and bound (every row group read). The
//! tests also check that pruning happens where the statistics allow it.

use std::sync::Arc;

use proptest::prelude::*;
use quarry::Catalog;
use quarry::arrow::array::{
    ArrayRef, Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use quarry::arrow::datatypes::{DataType, Field, Schema};
use quarry::arrow::util::display::array_value_to_string;
use quarry::datasource::ParquetTable;
use quarry::exec::collect_with_batch_size;
use quarry::parquet::arrow::ArrowWriter;
use quarry::parquet::file::properties::WriterProperties;
use quarry::plan::LogicalPlan;

/// t(i BIGINT, s INT, f DOUBLE, g VARCHAR, d DATE), all nullable.
type Row = (
    Option<i64>,
    Option<i32>,
    Option<f64>,
    Option<&'static str>,
    Option<i32>,
);

const INTS: &[i64] = &[-3, 0, 1, 2, 5, 9, i64::MAX, i64::MIN];
const SMALL: &[i32] = &[-1, 0, 4, 7];
const FLOATS: &[f64] = &[-0.0, 0.0, 1.5, -2.5, f64::NAN, -f64::NAN, 9.0];
const STRINGS: &[&str] = &["", "a", "b", "B", "ab", "ä", "zz"];
const DATES: &[i32] = &[0, 19_782, -1, 19_723];

fn nullable<T: std::fmt::Debug + Clone + 'static>(
    values: &'static [T],
) -> impl Strategy<Value = Option<T>> {
    prop_oneof![1 => Just(None), 4 => proptest::sample::select(values).prop_map(Some)]
}

fn rows() -> impl Strategy<Value = Vec<Row>> {
    proptest::collection::vec(
        (
            nullable(INTS),
            nullable(SMALL),
            nullable(FLOATS),
            nullable(STRINGS),
            nullable(DATES),
        ),
        0..16,
    )
}

fn write(path: &std::path::Path, rows: &[Row], group_rows: usize) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int64, true),
        Field::new("s", DataType::Int32, true),
        Field::new("f", DataType::Float64, true),
        Field::new("g", DataType::Utf8, true),
        Field::new("d", DataType::Date32, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.0))),
        Arc::new(Int32Array::from_iter(rows.iter().map(|r| r.1))),
        Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.2))),
        Arc::new(StringArray::from_iter(rows.iter().map(|r| r.3))),
        Arc::new(Date32Array::from_iter(rows.iter().map(|r| r.4))),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(group_rows))
        .build();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// A random filter over t: comparisons of each column with a constant (either way round), IS [NOT] NULL, IN,
/// and AND/OR/NOT of those.
fn filter() -> impl Strategy<Value = String> {
    let ops = proptest::sample::select(&["=", "<>", "<", "<=", ">", ">="][..]);
    let constant = prop_oneof![
        proptest::sample::select(INTS).prop_map(|v| v.to_string()),
        proptest::sample::select(
            &[
                "-0.0",
                "0.0",
                "1.5",
                "-2.5",
                "9.0",
                "1.4",
                "CAST('NaN' AS DOUBLE)"
            ][..]
        )
        .prop_map(str::to_string),
        proptest::sample::select(STRINGS).prop_map(|s| format!("'{s}'")),
        proptest::sample::select(
            &[
                "DATE '1970-01-01'",
                "DATE '2024-02-29'",
                "DATE '1969-12-31'"
            ][..]
        )
        .prop_map(str::to_string),
        Just("NULL".to_string()),
    ];
    let column = proptest::sample::select(&["i", "s", "f", "g", "d"][..]);
    let leaf = prop_oneof![
        4 => (column.clone(), ops, constant.clone(), any::<bool>()).prop_map(|(c, op, k, flip)| {
            if flip { format!("{k} {op} {c}") } else { format!("{c} {op} {k}") }
        }),
        1 => (column.clone(), any::<bool>()).prop_map(|(c, not)| {
            format!("{c} IS {}NULL", if not { "NOT " } else { "" })
        }),
        1 => (column, proptest::collection::vec(constant, 1..3))
            .prop_map(|(c, ks)| format!("{c} IN ({})", ks.join(", "))),
    ];
    leaf.prop_recursive(2, 6, 2, |inner| {
        prop_oneof![
            (inner.clone(), inner.clone()).prop_map(|(a, b)| format!("({a}) AND ({b})")),
            (inner.clone(), inner.clone()).prop_map(|(a, b)| format!("({a}) OR ({b})")),
            inner.prop_map(|a| format!("NOT ({a})")),
        ]
    })
}

fn values(plan: &LogicalPlan) -> Result<Vec<Vec<String>>, String> {
    let result = collect_with_batch_size(plan, 3).map_err(|e| e.to_string())?;
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
        std::env::var("PROPTEST_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(500)
    ))]

    #[test]
    fn pruned_scans_give_the_same_rows(rows in rows(), group_rows in 1usize..5, filter in filter()) {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("t.parquet");
        write(&path, &rows, group_rows);
        let mut catalog = Catalog::new();
        catalog.register_file("t", &path).unwrap();
        let query = format!("SELECT * FROM t WHERE {filter}");
        let bound = match quarry::sql::plan(&catalog, &query) {
            Ok(plan) => plan,
            Err(_) => return Ok(()), // an ill-typed filter (a string compared with a date): nothing to run
        };
        let optimized = quarry::optimizer::optimize(&bound).unwrap();
        prop_assert_eq!(values(&optimized), values(&bound), "{}\n{}", query, optimized);
    }
}

fn file(rows: &[Row], group_rows: usize) -> (tempfile::TempDir, ParquetTable) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("t.parquet");
    write(&path, rows, group_rows);
    let table = ParquetTable::try_new(&path).unwrap();
    (dir, table)
}

/// The row groups the optimizer's scan filters keep, for a WHERE over t.
fn kept(table: ParquetTable, filter: &str) -> Vec<usize> {
    let table = Arc::new(table);
    let mut catalog = Catalog::new();
    catalog.register("t", table.clone()).unwrap();
    let bound = quarry::sql::plan(&catalog, &format!("SELECT i FROM t WHERE {filter}")).unwrap();
    let optimized = quarry::optimizer::optimize(&bound).unwrap();
    let mut filters = None;
    let mut find = |plan: &LogicalPlan| {
        if let LogicalPlan::Scan(s) = plan {
            filters = Some(s.filters.clone());
        }
    };
    fn walk(plan: &LogicalPlan, f: &mut impl FnMut(&LogicalPlan)) {
        f(plan);
        plan.inputs().into_iter().for_each(|i| walk(i, f));
    }
    walk(&optimized, &mut find);
    table.row_groups_matching(&filters.expect("a scan"))
}

/// Ten rows, i = 0..9, s = i % 4 as INT, f = i / 2, g = "a".."j", d = day i; the last row's f is NaN and g NULL. In
/// row groups of 2: group k holds i = 2k, 2k + 1.
fn ordered() -> Vec<Row> {
    const G: &[&str] = &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];
    (0..10)
        .map(|i| {
            let f = if i == 9 { f64::NAN } else { i as f64 / 2.0 };
            let g = if i == 9 { None } else { Some(G[i as usize]) };
            (Some(i), Some((i % 4) as i32), Some(f), g, Some(i as i32))
        })
        .collect()
}

#[test]
fn row_groups_are_skipped_where_the_statistics_allow() {
    let cases: &[(&str, &[usize])] = &[
        ("i > 6", &[3, 4]),
        ("i >= 6", &[3, 4]),
        ("7 < i", &[4]),
        ("i < 2", &[0]),
        ("i = 5", &[2]),
        ("i IN (0, 9)", &[0, 4]),
        ("i <> 5", &[0, 1, 2, 3, 4]),
        ("i BETWEEN 3 AND 4", &[1, 2]),
        ("i > 3 AND i < 6", &[2]),
        ("i < 2 OR i > 7", &[0, 4]),
        ("i > 100", &[]),
        ("i IS NULL", &[]), // parquet-rs 60 writes null counts of 0
        ("g IS NULL", &[4]),
        ("g IS NOT NULL", &[0, 1, 2, 3, 4]),
        ("CAST(i AS DOUBLE) > 7.5", &[4]),
        ("i + 1 > 9", &[0, 1, 2, 3, 4]), // an expression of the column: not known
        ("NOT (i > 6)", &[0, 1, 2, 3, 4]),
        ("g >= 'i'", &[4]),
        ("g < 'b'", &[0]),
        ("d = DATE '1970-01-04'", &[1]),
        ("s > 2", &[1, 3]), // an INT column, compared after a cast to BIGINT
        // Floats: f is 0.0 .. 4.0 and a NaN, which the statistics leave out. `<`, `<=` and `=` with a number use the
        // min; `>` can't use the max, which doesn't count the NaN.
        ("f < 1.0", &[0]),
        ("f <= 1.0", &[0, 1]),
        ("f = 2.5", &[2]),
        ("f > 3.0", &[0, 1, 2, 3, 4]),
        ("f = CAST('NaN' AS DOUBLE)", &[0, 1, 2, 3, 4]),
    ];
    for (filter, want) in cases {
        let (_dir, table) = file(&ordered(), 2);
        assert_eq!(&kept(table, filter), want, "WHERE {filter}");
    }
}

#[test]
fn a_float_row_group_of_only_nans_is_never_skipped() {
    // Its min and max are NaN, which mean nothing (NaN is the greatest value, but -NaN is the writer's min).
    let rows: Vec<Row> = [f64::NAN, -f64::NAN]
        .iter()
        .map(|&f| (Some(1), Some(1), Some(f), None, None))
        .collect();
    for filter in ["f < 1.0", "f <= 0.0", "f = 1.0"] {
        let (_dir, table) = file(&rows, 2);
        assert_eq!(kept(table, filter), vec![0], "WHERE {filter}");
    }
}

#[test]
fn explain_shows_the_filters_a_parquet_scan_is_given() {
    let (_dir, table) = file(&ordered(), 2);
    let mut catalog = Catalog::new();
    catalog.register("t", Arc::new(table)).unwrap();
    let bound = quarry::sql::plan(&catalog, "SELECT g FROM t WHERE i > 6 AND g <> 'h'").unwrap();
    assert_eq!(
        quarry::optimizer::optimize(&bound).unwrap().to_string(),
        "Projection: t.g\n  Filter: t.i > 6 AND t.g <> 'h'\n    Scan: t projection=[i, g] filters=[t.i > 6, t.g <> 'h']\n"
    );
}
