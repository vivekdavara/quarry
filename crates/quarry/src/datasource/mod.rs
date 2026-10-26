//! Tables: anything that can describe its columns and hand out their data as Arrow record batches.

mod csv_table;
mod mem_table;
mod parquet_table;

use std::fmt::Debug;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::error::{QuarryError, Result};

pub use csv_table::{CsvOptions, CsvTable};
pub use mem_table::MemTable;
pub use parquet_table::ParquetTable;

/// The default number of rows in a batch a scan hands out.
pub const DEFAULT_BATCH_SIZE: usize = 8192;

/// A stream of record batches from a scan.
pub type BatchIter = Box<dyn Iterator<Item = Result<RecordBatch>> + Send>;

/// A table the engine can read.
pub trait TableProvider: Send + Sync + Debug {
    /// The Arrow schema of every column of the table.
    fn schema(&self) -> SchemaRef;

    /// Reads the table. With a projection, each batch holds only those columns of [`schema`](Self::schema), in the
    /// projection's order (an index may repeat); without one, every column. A batch holds at most `batch_size` rows.
    fn scan(&self, projection: Option<&[usize]>, batch_size: usize) -> Result<BatchIter>;

    /// What the table knows about itself without reading its data.
    fn statistics(&self) -> TableStatistics;
}

/// What a table knows about its size before it is read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableStatistics {
    /// The number of rows, when known without reading the data (a Parquet footer says; a CSV file doesn't).
    pub num_rows: Option<u64>,
    /// The number of rows in each Parquet row group; empty for other tables.
    pub row_group_rows: Vec<u64>,
}

/// Checks a projection against a schema of `width` columns and returns it, or the identity when there is none.
pub(crate) fn resolve_projection(projection: Option<&[usize]>, width: usize) -> Result<Vec<usize>> {
    match projection {
        None => Ok((0..width).collect()),
        Some(indices) => {
            if let Some(bad) = indices.iter().find(|&&i| i >= width) {
                return Err(QuarryError::Table(format!(
                    "projection index {bad} is out of range for a table of {width} columns"
                )));
            }
            Ok(indices.to_vec())
        }
    }
}

/// Splits a batch into slices of at most `batch_size` rows (zero-copy: each slice shares the batch's buffers).
pub(crate) fn split_batch(batch: &RecordBatch, batch_size: usize) -> Vec<RecordBatch> {
    let size = batch_size.max(1);
    let rows = batch.num_rows();
    if rows <= size {
        return vec![batch.clone()];
    }
    (0..rows)
        .step_by(size)
        .map(|start| batch.slice(start, size.min(rows - start)))
        .collect()
}
