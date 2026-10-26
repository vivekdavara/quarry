//! A table read from a CSV file with a header row.

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::csv::ReaderBuilder;
use arrow::csv::reader::Format;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use super::{BatchIter, TableProvider, TableStatistics, resolve_projection};
use crate::error::{QuarryError, Result};

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
                Arc::new(nulls_as_strings(inferred))
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

/// Inference gives a column with no values in the rows it read the type `Null`; such a column is read as strings
/// instead, as DuckDB does, so that values further down still have somewhere to go.
fn nulls_as_strings(schema: Schema) -> Schema {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Null => Field::new(f.name(), DataType::Utf8, true),
            _ => f.as_ref().clone(),
        })
        .collect();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}
