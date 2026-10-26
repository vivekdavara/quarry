//! A table read from a Parquet file.

use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::datatypes::SchemaRef;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};

use super::{BatchIter, TableProvider, TableStatistics, resolve_projection};
use crate::error::{QuarryError, Result};

/// A Parquet file. The footer (schema and row groups) is read once, when the table is opened; each scan reopens the
/// file and decodes only the projected columns.
#[derive(Debug, Clone)]
pub struct ParquetTable {
    path: PathBuf,
    metadata: ArrowReaderMetadata,
}

impl ParquetTable {
    /// Opens a Parquet file and reads its footer.
    pub fn try_new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)
            .map_err(|e| QuarryError::Table(format!("cannot open {}: {e}", path.display())))?;
        let metadata =
            ArrowReaderMetadata::load(&file, ArrowReaderOptions::new()).map_err(|e| {
                QuarryError::Table(format!("cannot read {} as Parquet: {e}", path.display()))
            })?;
        Ok(ParquetTable { path, metadata })
    }

    /// The file this table reads.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The number of row groups in the file.
    pub fn num_row_groups(&self) -> usize {
        self.metadata.metadata().num_row_groups()
    }
}

impl TableProvider for ParquetTable {
    fn schema(&self) -> SchemaRef {
        self.metadata.schema().clone()
    }

    fn scan(&self, projection: Option<&[usize]>, batch_size: usize) -> Result<BatchIter> {
        let columns = resolve_projection(projection, self.metadata.schema().fields().len())?;
        let file = File::open(&self.path)
            .map_err(|e| QuarryError::Table(format!("cannot open {}: {e}", self.path.display())))?;
        let builder =
            ParquetRecordBatchReaderBuilder::new_with_metadata(file, self.metadata.clone())
                .with_batch_size(batch_size.max(1));

        // A ProjectionMask selects columns but yields them in the file's order, once each. Read the distinct
        // columns asked for, in file order, then put them in the projection's order (repeats included).
        let mut distinct = columns.clone();
        distinct.sort_unstable();
        distinct.dedup();
        let reorder: Vec<usize> = columns
            .iter()
            .map(|c| {
                distinct
                    .binary_search(c)
                    .expect("every projected column is in the distinct list")
            })
            .collect();
        let identity = reorder.iter().copied().eq(0..distinct.len());

        let mask = ProjectionMask::roots(builder.parquet_schema(), distinct.iter().copied());
        let reader = builder.with_projection(mask).build()?;
        Ok(Box::new(reader.map(move |batch| {
            let batch = batch?;
            Ok(if identity {
                batch
            } else {
                batch.project(&reorder)?
            })
        })))
    }

    fn statistics(&self) -> TableStatistics {
        let metadata = self.metadata.metadata();
        let row_group_rows: Vec<u64> = metadata
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows().max(0) as u64)
            .collect();
        TableStatistics {
            num_rows: Some(row_group_rows.iter().sum()),
            row_group_rows,
        }
    }
}
