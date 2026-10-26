//! The readers and the catalog: files written here read back equal, with projections, NULLs and inferred types.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use quarry::datasource::{CsvOptions, CsvTable, MemTable, ParquetTable, TableProvider};
use quarry::{Catalog, QuarryError};
use tempfile::TempDir;

/// Ten rows: an id, a name with NULLs, a score, a day.
fn sample() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
        Field::new("day", DataType::Date32, true),
    ]));
    let names: Vec<Option<&str>> = vec![
        Some("ada"),
        None,
        Some("bo"),
        Some("cy"),
        None,
        Some("di"),
        Some("ed"),
        Some("fa"),
        Some("gu"),
        None,
    ];
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(0..10)),
        Arc::new(StringArray::from(names)),
        Arc::new(Float64Array::from_iter(
            (0..10).map(|i| if i == 3 { None } else { Some(i as f64 * 1.5) }),
        )),
        Arc::new(Date32Array::from_iter_values((0..10).map(|i| 10_000 + i))),
    ];
    RecordBatch::try_new(schema, columns).unwrap()
}

fn write_parquet(dir: &Path, batch: &RecordBatch, row_group_rows: usize) -> PathBuf {
    let path = dir.join("sample.parquet");
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group_rows))
        .build();
    let mut writer = ArrowWriter::try_new(
        fs::File::create(&path).unwrap(),
        batch.schema(),
        Some(props),
    )
    .unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    path
}

fn read_all(
    table: &dyn TableProvider,
    projection: Option<&[usize]>,
    batch_size: usize,
) -> Vec<RecordBatch> {
    table
        .scan(projection, batch_size)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn concat(schema: &SchemaRef, batches: &[RecordBatch]) -> RecordBatch {
    concat_batches(schema, batches).unwrap()
}

#[test]
fn parquet_reads_back_what_was_written() {
    let dir = TempDir::new().unwrap();
    let batch = sample();
    let table = ParquetTable::try_new(write_parquet(dir.path(), &batch, 3)).unwrap();
    assert_eq!(table.schema(), batch.schema());
    let batches = read_all(&table, None, 1024);
    assert_eq!(concat(&batch.schema(), &batches), batch);
}

#[test]
fn parquet_statistics_come_from_the_footer() {
    let dir = TempDir::new().unwrap();
    let table = ParquetTable::try_new(write_parquet(dir.path(), &sample(), 3)).unwrap();
    assert_eq!(table.num_row_groups(), 4);
    let stats = table.statistics();
    assert_eq!(stats.num_rows, Some(10));
    assert_eq!(stats.row_group_rows, vec![3, 3, 3, 1]);
}

#[test]
fn parquet_projection_keeps_the_order_asked_for() {
    let dir = TempDir::new().unwrap();
    let batch = sample();
    let table = ParquetTable::try_new(write_parquet(dir.path(), &batch, 3)).unwrap();
    // Out of file order, and with a repeat: the reader's mask yields file order once each, so this needs the reorder.
    let projection = [3, 0, 3];
    let expected = batch.project(&projection).unwrap();
    let got = concat(
        &expected.schema(),
        &read_all(&table, Some(&projection), 1024),
    );
    assert_eq!(got, expected);
}

#[test]
fn parquet_projection_in_file_order_is_not_reordered() {
    let dir = TempDir::new().unwrap();
    let batch = sample();
    let table = ParquetTable::try_new(write_parquet(dir.path(), &batch, 4)).unwrap();
    let expected = batch.project(&[1, 2]).unwrap();
    assert_eq!(
        concat(&expected.schema(), &read_all(&table, Some(&[1, 2]), 1024)),
        expected
    );
}

#[test]
fn parquet_empty_projection_still_counts_rows() {
    // COUNT(*) reads no columns; the batches must still say how many rows they hold.
    let dir = TempDir::new().unwrap();
    let table = ParquetTable::try_new(write_parquet(dir.path(), &sample(), 3)).unwrap();
    let batches = read_all(&table, Some(&[]), 1024);
    assert!(batches.iter().all(|b| b.num_columns() == 0));
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 10);
}

#[test]
fn parquet_batches_respect_the_batch_size() {
    let dir = TempDir::new().unwrap();
    let batch = sample();
    let table = ParquetTable::try_new(write_parquet(dir.path(), &batch, 10)).unwrap();
    let batches = read_all(&table, None, 4);
    assert_eq!(
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        vec![4, 4, 2]
    );
    assert_eq!(concat(&batch.schema(), &batches), batch);
}

#[test]
fn projection_out_of_range_is_an_error() {
    let dir = TempDir::new().unwrap();
    let table = ParquetTable::try_new(write_parquet(dir.path(), &sample(), 3)).unwrap();
    let err = table.scan(Some(&[0, 4]), 1024).err().unwrap();
    assert!(matches!(err, QuarryError::Table(_)), "{err}");
    assert!(
        err.to_string()
            .contains("projection index 4 is out of range for a table of 4 columns"),
        "{err}"
    );
}

#[test]
fn a_file_that_is_not_parquet_is_a_table_error() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("fake.parquet");
    fs::write(&path, "id,name\n1,a\n").unwrap();
    let err = ParquetTable::try_new(&path).unwrap_err();
    assert!(matches!(err, QuarryError::Table(_)), "{err}");
    assert!(err.to_string().contains("as Parquet"), "{err}");
    let missing = ParquetTable::try_new(dir.path().join("missing.parquet")).unwrap_err();
    assert!(missing.to_string().contains("cannot open"), "{missing}");
}

fn write_csv(dir: &Path, name: &str, text: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, text).unwrap();
    path
}

#[test]
fn csv_infers_each_type_and_reads_empty_fields_as_null() {
    let dir = TempDir::new().unwrap();
    let path = write_csv(
        dir.path(),
        "t.csv",
        "i,f,b,d,s,e\n1,1.5,true,2024-01-31,x,\n-2,,false,1998-12-01,,\n,3,TRUE,,y z,\n",
    );
    let table = CsvTable::try_new(&path).unwrap();
    let types: Vec<DataType> = table
        .schema()
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect();
    // "e" has no values at all: inference says Null, and the table reads it as strings.
    assert_eq!(
        types,
        vec![
            DataType::Int64,
            DataType::Float64,
            DataType::Boolean,
            DataType::Date32,
            DataType::Utf8,
            DataType::Utf8
        ]
    );
    let batch = concat(&table.schema(), &read_all(&table, None, 1024));
    assert_eq!(batch.num_rows(), 3);
    let i = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(i.iter().collect::<Vec<_>>(), vec![Some(1), Some(-2), None]);
    let f = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(
        f.iter().collect::<Vec<_>>(),
        vec![Some(1.5), None, Some(3.0)]
    );
    let b = batch
        .column(2)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(
        b.iter().collect::<Vec<_>>(),
        vec![Some(true), Some(false), Some(true)]
    );
    let d = batch
        .column(3)
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    assert_eq!(d.value(0), 19_753); // 2024-01-31 is 19,753 days after 1970-01-01
    assert!(d.is_null(2));
    let s = batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        s.iter().collect::<Vec<_>>(),
        vec![Some("x"), None, Some("y z")]
    );
    assert_eq!(batch.column(5).null_count(), 3);
}

#[test]
fn csv_projection_and_batch_size() {
    let dir = TempDir::new().unwrap();
    let rows: String = (0..10).map(|i| format!("{i},n{i}\n")).collect();
    let table =
        CsvTable::try_new(write_csv(dir.path(), "t.csv", &format!("id,name\n{rows}"))).unwrap();
    let batches = read_all(&table, Some(&[1, 0]), 4);
    assert_eq!(
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        vec![4, 4, 2]
    );
    let schema = batches[0].schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["name", "id"]);
    let count: usize = read_all(&table, Some(&[]), 4)
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(count, 10);
    assert_eq!(table.statistics().num_rows, None);
}

#[test]
fn csv_header_only_has_no_rows() {
    let dir = TempDir::new().unwrap();
    let table = CsvTable::try_new(write_csv(dir.path(), "t.csv", "a,b\n")).unwrap();
    assert_eq!(table.schema().fields().len(), 2);
    assert_eq!(
        read_all(&table, None, 1024)
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
}

#[test]
fn csv_empty_file_is_an_error() {
    let dir = TempDir::new().unwrap();
    let err = CsvTable::try_new(write_csv(dir.path(), "t.csv", "")).unwrap_err();
    assert!(err.to_string().contains("no header row"), "{err}");
}

#[test]
fn csv_value_past_the_inferred_rows_fails_the_scan() {
    let dir = TempDir::new().unwrap();
    let path = write_csv(dir.path(), "t.csv", "n\n1\n2\n3\nfour\n");
    let options = CsvOptions {
        infer_rows: Some(2),
        ..CsvOptions::default()
    };
    let table = CsvTable::try_new_with(&path, options).unwrap();
    assert_eq!(table.schema().field(0).data_type(), &DataType::Int64);
    let result: Result<Vec<_>, _> = table.scan(None, 1024).unwrap().collect();
    let err = result.unwrap_err();
    assert!(matches!(err, QuarryError::Arrow(_)), "{err}");
    assert!(err.to_string().contains("four"), "{err}");

    // Reading every row to infer finds it, and the column is strings.
    let options = CsvOptions {
        infer_rows: None,
        ..CsvOptions::default()
    };
    let table = CsvTable::try_new_with(&path, options).unwrap();
    assert_eq!(table.schema().field(0).data_type(), &DataType::Utf8);
}

#[test]
fn csv_with_a_given_schema_and_delimiter() {
    let dir = TempDir::new().unwrap();
    let path = write_csv(dir.path(), "t.csv", "a|b\n007|1\n");
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Float64, true),
    ]));
    let options = CsvOptions {
        delimiter: b'|',
        schema: Some(schema.clone()),
        ..CsvOptions::default()
    };
    let table = CsvTable::try_new_with(&path, options).unwrap();
    let batch = concat(&schema, &read_all(&table, None, 1024));
    let a = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(a.value(0), "007"); // a string column keeps the leading zeros an inferred integer would lose
}

#[test]
fn mem_table_splits_batches_and_checks_schemas() {
    let batch = sample();
    let table = MemTable::try_new(batch.schema(), vec![batch.clone(), batch.slice(0, 3)]).unwrap();
    let batches = read_all(&table, Some(&[0]), 4);
    assert_eq!(
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        vec![4, 4, 2, 3]
    );
    assert_eq!(table.statistics().num_rows, Some(13));

    let other = batch.project(&[0]).unwrap();
    let err = MemTable::try_new(batch.schema(), vec![batch, other]).unwrap_err();
    assert!(err.to_string().contains("batch 1 has schema"), "{err}");
}

#[test]
fn catalog_names_ignore_case_and_must_be_unique() {
    let mut catalog = Catalog::new();
    catalog
        .register("LineItem", Arc::new(MemTable::from_batch(sample())))
        .unwrap();
    assert!(catalog.table("lineitem").is_some());
    assert!(catalog.table("LINEITEM").is_some());
    assert!(catalog.table("orders").is_none());
    let err = catalog
        .register("lineitem", Arc::new(MemTable::from_batch(sample())))
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("a table named LineItem is already registered"),
        "{err}"
    );
    assert!(
        catalog
            .register("", Arc::new(MemTable::from_batch(sample())))
            .is_err()
    );
    assert_eq!(catalog.table_names(), vec!["LineItem".to_string()]);
}

#[test]
fn catalog_registers_files_by_extension() {
    let dir = TempDir::new().unwrap();
    let parquet = write_parquet(dir.path(), &sample(), 5);
    let csv = write_csv(dir.path(), "t.CSV", "a\n1\n");
    let mut catalog = Catalog::new();
    catalog.register_file("p", &parquet).unwrap();
    catalog.register_file("c", &csv).unwrap();
    assert_eq!(
        catalog.table("p").unwrap().statistics().row_group_rows,
        vec![5, 5]
    );
    assert_eq!(catalog.table("c").unwrap().schema().field(0).name(), "a");
    let err = catalog
        .register_file("x", dir.path().join("t.json"))
        .unwrap_err();
    assert!(err.to_string().contains("unknown file type"), "{err}");
}
