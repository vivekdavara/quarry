//! A table held in memory, for tests and small inputs.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use super::{BatchIter, TableProvider, TableStatistics, resolve_projection, split_batch};
use crate::error::{QuarryError, Result};

/// Record batches in memory, all with one schema.
#[derive(Debug, Clone)]
pub struct MemTable {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl MemTable {
    /// A table of these batches; each must have exactly `schema`.
    pub fn try_new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<Self> {
        if let Some(bad) = batches.iter().position(|b| b.schema() != schema) {
            return Err(QuarryError::Table(format!(
                "batch {bad} has schema {:?}, not the table's {:?}",
                batches[bad].schema(),
                schema
            )));
        }
        Ok(MemTable { schema, batches })
    }

    /// A table of one batch, with that batch's schema.
    pub fn from_batch(batch: RecordBatch) -> Self {
        MemTable {
            schema: batch.schema(),
            batches: vec![batch],
        }
    }
}

impl TableProvider for MemTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(&self, projection: Option<&[usize]>, batch_size: usize) -> Result<BatchIter> {
        let columns = resolve_projection(projection, self.schema.fields().len())?;
        let mut out = Vec::new();
        for batch in &self.batches {
            let projected = batch.project(&columns)?;
            out.extend(split_batch(&projected, batch_size).into_iter().map(Ok));
        }
        Ok(Box::new(out.into_iter()))
    }

    fn statistics(&self) -> TableStatistics {
        let rows = self.batches.iter().map(|b| b.num_rows() as u64).sum();
        TableStatistics {
            num_rows: Some(rows),
            row_group_rows: Vec::new(),
        }
    }
}
