//! Execution: a logical plan run over Arrow record batches (DESIGN.md, "Execution").
//!
//! Each plan node becomes an iterator of record batches that pulls from its input's (the Volcano model, a batch at a
//! time). Scan, filter, projection and limit stream; sort and aggregate read their whole input first. This interprets
//! the logical plan directly; day 3 puts a physical plan in between (hash join, top-k sort, row-group pruning).

mod aggregate;
mod expr;

use std::sync::Arc;

use arrow::array::{AsArray, RecordBatch, RecordBatchOptions, UInt32Array};
use arrow::compute::{
    SortColumn, SortOptions, concat_batches, filter_record_batch, lexsort_to_indices,
};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::util::display::FormatOptions;
use arrow::util::pretty::pretty_format_batches_with_options;

use crate::datasource::{BatchIter, DEFAULT_BATCH_SIZE, split_batch};
use crate::error::{Result, exec_err, unsupported};
use crate::plan::{Filter, LogicalPlan, PlanSchema, Projection, Sort};

pub use expr::evaluate;
pub(crate) use expr::{normalize_floats, take_rows};

/// A query's whole result.
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// The result's columns: the plan's output names, types and nullability.
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

impl QueryResult {
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    /// The result as a text table, NULLs shown as `NULL`, with a header even when there are no rows.
    pub fn to_table(&self) -> Result<String> {
        let options = FormatOptions::default().with_null("NULL");
        let empty = [RecordBatch::new_empty(self.schema.clone())];
        let batches = if self.batches.is_empty() {
            &empty[..]
        } else {
            &self.batches[..]
        };
        Ok(pretty_format_batches_with_options(batches, &options)?.to_string())
    }
}

/// Runs a plan to completion with the default batch size.
pub fn collect(plan: &LogicalPlan) -> Result<QueryResult> {
    collect_with_batch_size(plan, DEFAULT_BATCH_SIZE)
}

/// Runs a plan to completion, with scans (and sorts and aggregates) handing out batches of at most `batch_size`
/// rows. The answer doesn't depend on the batch size; the tests check that.
pub fn collect_with_batch_size(plan: &LogicalPlan, batch_size: usize) -> Result<QueryResult> {
    let schema = Arc::new(plan.schema().to_arrow());
    let batches = execute(plan, batch_size)?.collect::<Result<Vec<_>>>()?;
    Ok(QueryResult { schema, batches })
}

/// Runs a plan, streaming its result. Each batch has the plan's output schema (which the batch's arrays are checked
/// against: a type or nullability the plan got wrong is an error here, not a wrong answer).
pub fn execute(plan: &LogicalPlan, batch_size: usize) -> Result<BatchIter> {
    let schema = Arc::new(plan.schema().to_arrow());
    let input = stream(plan, batch_size.max(1))?;
    Ok(Box::new(input.map(move |batch| {
        let batch = batch?;
        Ok(RecordBatch::try_new_with_options(
            schema.clone(),
            batch.columns().to_vec(),
            &rows(batch.num_rows()),
        )?)
    })))
}

/// Options for a batch of `n` rows (which a batch with no columns needs).
fn rows(n: usize) -> RecordBatchOptions {
    RecordBatchOptions::new().with_row_count(Some(n))
}

/// The node's output as a stream of batches. Batches of a node's own output carry whatever Arrow schema is at hand;
/// plan expressions refer to columns by index, and `execute` puts the plan's names on the result.
fn stream(plan: &LogicalPlan, batch_size: usize) -> Result<BatchIter> {
    match plan {
        LogicalPlan::Scan(s) => s.provider.scan(s.projection.as_deref(), batch_size),
        LogicalPlan::Empty(e) => {
            let batches = if e.one_row {
                vec![Ok(RecordBatch::try_new_with_options(
                    Arc::new(Schema::empty()),
                    vec![],
                    &rows(1),
                )?)]
            } else {
                vec![]
            };
            Ok(Box::new(batches.into_iter()))
        }
        LogicalPlan::Filter(f) => filter(f, batch_size),
        LogicalPlan::Projection(p) => project(p, stream(&p.input, batch_size)?),
        LogicalPlan::Aggregate(a) => {
            aggregate::aggregate(a, stream(&a.input, batch_size)?, batch_size)
        }
        LogicalPlan::Sort(s) => sort(s, batch_size),
        LogicalPlan::Limit(l) => limited(&l.input, l.skip, l.fetch, batch_size),
        LogicalPlan::Join(_) => Err(unsupported!(
            "running a join (the plan binds; day 3 adds the join operators)"
        )),
    }
}

fn filter(f: &Filter, batch_size: usize) -> Result<BatchIter> {
    let input = stream(&f.input, batch_size)?;
    let schema: Arc<PlanSchema> = f.input.schema().clone();
    let predicate = f.predicate.clone();
    let filtered = input.map(move |batch| {
        let batch = batch?;
        let mask = evaluate(&predicate, &schema, &batch)?;
        // `WHERE NULL` has the NULL type: no row is TRUE.
        if *mask.data_type() == DataType::Null {
            return Ok(batch.slice(0, 0));
        }
        let Some(mask) = mask.as_boolean_opt() else {
            return Err(exec_err!(
                "a filter evaluated to {}, not a boolean",
                mask.data_type()
            ));
        };
        Ok(filter_record_batch(&batch, mask)?)
    });
    Ok(Box::new(
        filtered.filter(|b| !matches!(b, Ok(b) if b.num_rows() == 0)),
    ))
}

/// `LIMIT` over `plan`. A projection makes one row of each input row, so the limit is applied below it (below a chain
/// of them): the projection then computes only the rows the limit keeps, and `SELECT 10 % (id - 3) FROM t LIMIT 2`
/// never computes row 3's division by zero, whatever the batch size.
fn limited(
    plan: &LogicalPlan,
    skip: usize,
    fetch: Option<usize>,
    batch_size: usize,
) -> Result<BatchIter> {
    match plan {
        LogicalPlan::Projection(p) => project(p, limited(&p.input, skip, fetch, batch_size)?),
        other => Ok(Box::new(LimitStream {
            input: stream(other, batch_size)?,
            skip,
            fetch,
        })),
    }
}

/// Evaluates a projection's expressions over each batch of its input.
fn project(p: &Projection, input: BatchIter) -> Result<BatchIter> {
    let in_schema = p.input.schema().clone();
    let out_schema = Arc::new(p.schema.to_arrow());
    let exprs = p.exprs.clone();
    Ok(Box::new(input.map(move |batch| {
        let batch = batch?;
        let columns = exprs
            .iter()
            .map(|e| evaluate(e, &in_schema, &batch))
            .collect::<Result<Vec<_>>>()?;
        Ok(RecordBatch::try_new_with_options(
            out_schema.clone(),
            columns,
            &rows(batch.num_rows()),
        )?)
    })))
}

/// Sorts the whole input. Ties keep their input order (the row number is the last key), so a result never depends
/// on the sort algorithm.
fn sort(s: &Sort, batch_size: usize) -> Result<BatchIter> {
    let batches = stream(&s.input, batch_size)?.collect::<Result<Vec<_>>>()?;
    let Some(first) = batches.first() else {
        return Ok(Box::new(std::iter::empty()));
    };
    let all = concat_batches(&first.schema(), &batches)?;
    let schema = s.input.schema();
    let mut columns = Vec::with_capacity(s.keys.len() + 1);
    for key in &s.keys {
        let values = normalize_floats(&evaluate(&key.expr, schema, &all)?);
        // A NULL-typed key (ORDER BY NULL) is the same for every row.
        if *values.data_type() == DataType::Null {
            continue;
        }
        columns.push(SortColumn {
            values,
            options: Some(SortOptions {
                descending: !key.ascending,
                nulls_first: key.nulls_first,
            }),
        });
    }
    let position = 0..u32::try_from(all.num_rows())
        .map_err(|_| unsupported!("sorting more than 2^32 rows"))?;
    columns.push(SortColumn {
        values: Arc::new(UInt32Array::from_iter_values(position)),
        options: None,
    });
    let order = lexsort_to_indices(&columns, None)?;
    let sorted = take_rows(&all, order.values())?;
    Ok(Box::new(
        split_batch(&sorted, batch_size).into_iter().map(Ok),
    ))
}

/// `LIMIT fetch OFFSET skip` over a stream: it stops pulling from its input once it has `fetch` rows.
struct LimitStream {
    input: BatchIter,
    skip: usize,
    fetch: Option<usize>,
}

impl Iterator for LimitStream {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.fetch == Some(0) {
                return None;
            }
            let batch = match self.input.next()? {
                Ok(b) => b,
                Err(e) => return Some(Err(e)),
            };
            let n = batch.num_rows();
            if self.skip >= n {
                self.skip -= n;
                continue;
            }
            let start = std::mem::take(&mut self.skip);
            let mut len = n - start;
            if let Some(fetch) = &mut self.fetch {
                len = len.min(*fetch);
                *fetch -= len;
            }
            return Some(Ok(batch.slice(start, len)));
        }
    }
}

impl std::fmt::Debug for LimitStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LimitStream")
            .field("skip", &self.skip)
            .field("fetch", &self.fetch)
            .finish_non_exhaustive()
    }
}
