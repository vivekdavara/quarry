//! A table read from a CSV file with a header row.

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, StringArray};
use arrow::csv::ReaderBuilder;
use arrow::csv::reader::Format;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use super::{BatchIter, TableProvider, TableStatistics, resolve_projection};
use crate::error::{QuarryError, Result};
use crate::types::parse_date32;

/// How a CSV file is read.
#[derive(Debug, Clone)]
pub struct CsvOptions {
    /// The field separator.
    pub delimiter: u8,
    /// How many rows schema inference reads; `None` reads the whole file.
    pub infer_rows: Option<usize>,
    /// The schema to use instead of inferring one.
    pub schema: Option<SchemaRef>,
}

impl Default for CsvOptions {
    fn default() -> Self {
        CsvOptions {
            delimiter: b',',
            infer_rows: Some(1000),
            schema: None,
        }
    }
}

/// A CSV file whose first row names the columns.
///
/// The schema is inferred from the first rows (DESIGN.md, "Data sources"). A value further down that doesn't fit the
/// inferred type fails the scan that reaches it, with the line number; give a schema for files like that.
#[derive(Debug, Clone)]
pub struct CsvTable {
    path: PathBuf,
    schema: SchemaRef,
    delimiter: u8,
}

impl CsvTable {
    /// Opens a CSV file with the default options.
    pub fn try_new(path: impl AsRef<Path>) -> Result<Self> {
        Self::try_new_with(path, CsvOptions::default())
    }

    /// Opens a CSV file and infers its schema, unless the options give one.
    pub fn try_new_with(path: impl AsRef<Path>, options: CsvOptions) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let schema = match options.schema {
            Some(schema) => schema,
            None => {
                let file = open(&path)?;
                let format = Format::default()
                    .with_header(true)
                    .with_delimiter(options.delimiter);
                let (inferred, _) = format
                    .infer_schema(BufReader::new(file), options.infer_rows)
                    .map_err(|e| {
                        QuarryError::Table(format!(
                            "cannot infer the schema of {}: {e}",
                            path.display()
                        ))
                    })?;
                let inferred = only_design_types(inferred);
                Arc::new(real_dates_only(
                    &path,
                    inferred,
                    options.delimiter,
                    options.infer_rows,
                )?)
            }
        };
        if schema.fields().is_empty() {
            return Err(QuarryError::Table(format!(
                "{} has no header row",
                path.display()
            )));
        }
        Ok(CsvTable {
            path,
            schema,
            delimiter: options.delimiter,
        })
    }

    /// The file this table reads.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl TableProvider for CsvTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(&self, projection: Option<&[usize]>, batch_size: usize) -> Result<BatchIter> {
        let columns = resolve_projection(projection, self.schema.fields().len())?;
        let reader = ReaderBuilder::new(self.schema.clone())
            .with_header(true)
            .with_delimiter(self.delimiter)
            .with_batch_size(batch_size.max(1))
            .with_projection(columns)
            .build(BufReader::new(open(&self.path)?))?;
        Ok(Box::new(
            reader.map(|batch| batch.map_err(QuarryError::from)),
        ))
    }

    fn statistics(&self) -> TableStatistics {
        TableStatistics::default()
    }
}

fn open(path: &Path) -> Result<File> {
    File::open(path).map_err(|e| QuarryError::Table(format!("cannot open {}: {e}", path.display())))
}

/// Inference can give types DESIGN.md's inference doesn't list. A column with no values in the rows it read is `Null`,
/// and one of date-times is a `Timestamp`, which no expression takes; both are read as strings instead (as DuckDB
/// does for the first), so the values are kept as written and stay usable.
fn only_design_types(schema: Schema) -> Schema {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Null | DataType::Timestamp(..) | DataType::Date64 => {
                Field::new(f.name(), DataType::Utf8, true)
            }
            _ => f.as_ref().clone(),
        })
        .collect();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

/// Inference calls a column `Date32` when every value it read looks like `YYYY-MM-DD`, without checking the dates
/// exist; one `2024-02-30` among them would then fail every scan, so the file couldn't be read at all. This re-reads
/// those rows as text and makes any such column strings unless every value is a real date.
fn real_dates_only(
    path: &Path,
    schema: Schema,
    delimiter: u8,
    infer_rows: Option<usize>,
) -> Result<Schema> {
    let dates: Vec<usize> = (0..schema.fields().len())
        .filter(|&i| schema.field(i).data_type() == &DataType::Date32)
        .collect();
    if dates.is_empty() {
        return Ok(schema);
    }
    let text = Schema::new(
        schema
            .fields()
            .iter()
            .map(|f| Field::new(f.name(), DataType::Utf8, true))
            .collect::<Vec<_>>(),
    );
    let reader = ReaderBuilder::new(Arc::new(text))
        .with_header(true)
        .with_delimiter(delimiter)
        .with_projection(dates.clone())
        .build(BufReader::new(open(path)?))?;
    let mut fake = vec![false; dates.len()];
    let mut seen = 0usize;
    for batch in reader {
        let batch = batch?;
        let take = infer_rows.map_or(batch.num_rows(), |limit| batch.num_rows().min(limit - seen));
        for (k, column) in batch.columns().iter().enumerate() {
            let values = column
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("read as Utf8");
            fake[k] |= values
                .iter()
                .take(take)
                .flatten()
                .any(|v| !v.is_empty() && parse_date32(v).is_none());
        }
        seen += take;
        if infer_rows.is_some_and(|limit| seen >= limit) {
            break;
        }
    }
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| match dates.iter().position(|&d| d == i) {
            Some(k) if fake[k] => Field::new(f.name(), DataType::Utf8, true),
            _ => f.as_ref().clone(),
        })
        .collect();
    Ok(Schema::new_with_metadata(fields, schema.metadata().clone()))
}
