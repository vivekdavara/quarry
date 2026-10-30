//! The Aggregate node: rows grouped by hashing their group keys in Arrow's row format, and one accumulator per
//! aggregate per group (DESIGN.md, "Execution").
//!
//! Arrow's row format encodes a row's key columns as bytes that compare and hash as the values do, for any types, so
//! a `HashMap` from those bytes to a group number is the whole grouping. Floats are normalized for that (-0.0 is 0.0,
//! one NaN), because the format orders floats by IEEE 754's total order, which tells those apart; but what comes out
//! is a value from the input, as in DuckDB: a group's key is its first row's (-0.0 stays -0.0), MIN and MAX are the
//! winning value itself.

use std::collections::{HashMap, HashSet};

use ahash::RandomState;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, Int64Array, RecordBatch, UInt32Array, new_null_array,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use arrow::row::{RowConverter, SortField};

use super::{evaluate, normalize_floats, rows};
use crate::datasource::{BatchIter, split_batch};
use crate::error::{Result, exec_err};
use crate::plan::{Aggregate, AggregateFunction, Expr, PlanSchema};
use crate::types::is_float;

/// Reads the whole input, then hands out one row per group: the group keys, then the aggregates. Without a GROUP BY
/// there is exactly one group, even over no rows (`SELECT count(*) FROM t WHERE FALSE` is 0); with one, no rows make
/// no groups.
pub(super) fn aggregate(
    node: &Aggregate,
    input: BatchIter,
    batch_size: usize,
) -> Result<BatchIter> {
    let schema = node.input.schema();
    let mut groups = Groups::new(&node.group_by, schema)?;
    let mut accumulators = node
        .aggregates
        .iter()
        .map(|a| accumulator(a, schema))
        .collect::<Result<Vec<_>>>()?;
    for batch in input {
        let batch = batch?;
        let ids = groups.assign(&node.group_by, schema, &batch)?;
        for (acc, agg) in accumulators.iter_mut().zip(&node.aggregates) {
            let Expr::Aggregate { arg, .. } = agg else {
                unreachable!("LogicalPlan::aggregate accepts only aggregates")
            };
            let values = match arg {
                Some(arg) => Some(evaluate(arg, schema, &batch)?),
                None => None,
            };
            acc.update(&ids, values.as_ref(), groups.len())?;
        }
    }
    let num_groups = groups.len();
    let mut columns = groups.keys()?;
    for acc in &mut accumulators {
        columns.push(acc.finish(num_groups)?);
    }
    let out = RecordBatch::try_new_with_options(
        Arc::new(node.schema.to_arrow()),
        columns,
        &rows(num_groups),
    )?;
    if out.num_rows() == 0 {
        return Ok(Box::new(std::iter::empty()));
    }
    Ok(Box::new(split_batch(&out, batch_size).into_iter().map(Ok)))
}

/// The groups seen so far: each distinct key and its group number, in order of first appearance.
struct Groups {
    /// `None` without a GROUP BY: every row is in group 0, which exists from the start.
    converter: Option<RowConverter>,
    /// Group numbers by the row-format bytes of the normalized key.
    numbers: HashMap<Vec<u8>, usize, RandomState>,
    /// Each group's key as its first row had it (not normalized), in row format, for the output.
    keys: Vec<Vec<u8>>,
}

impl Groups {
    fn new(group_by: &[Expr], schema: &PlanSchema) -> Result<Groups> {
        let converter = if group_by.is_empty() {
            None
        } else {
            let fields = group_by
                .iter()
                .map(|g| SortField::new(g.data_type(schema)))
                .collect();
            Some(RowConverter::new(fields)?)
        };
        Ok(Groups {
            converter,
            numbers: HashMap::default(),
            keys: Vec::new(),
        })
    }

    fn len(&self) -> usize {
        if self.converter.is_none() {
            1
        } else {
            self.keys.len()
        }
    }

    /// The group number of each row of the batch, making new groups as needed.
    fn assign(
        &mut self,
        group_by: &[Expr],
        schema: &PlanSchema,
        batch: &RecordBatch,
    ) -> Result<Vec<usize>> {
        let Some(converter) = &self.converter else {
            return Ok(vec![0; batch.num_rows()]);
        };
        let columns = group_by
            .iter()
            .map(|g| evaluate(g, schema, batch))
            .collect::<Result<Vec<_>>>()?;
        let normalized: Vec<ArrayRef> = columns.iter().map(normalize_floats).collect();
        let encoded = converter.convert_columns(&normalized)?;
        // Only float keys change when normalized; otherwise the encoded rows are the keys as they are.
        let originals = match columns.iter().any(|c| is_float(c.data_type())) {
            true => Some(converter.convert_columns(&columns)?),
            false => None,
        };
        let mut ids = Vec::with_capacity(batch.num_rows());
        for (i, row) in encoded.iter().enumerate() {
            let bytes: &[u8] = row.as_ref();
            let id = match self.numbers.get(bytes) {
                Some(&id) => id,
                None => {
                    let id = self.keys.len();
                    let original = match &originals {
                        Some(o) => o.row(i).as_ref().to_vec(),
                        None => bytes.to_vec(),
                    };
                    self.keys.push(original);
                    self.numbers.insert(bytes.to_vec(), id);
                    id
                }
            };
            ids.push(id);
        }
        Ok(ids)
    }

    /// The group key columns, one row per group.
    fn keys(&self) -> Result<Vec<ArrayRef>> {
        let Some(converter) = &self.converter else {
            return Ok(vec![]);
        };
        let parser = converter.parser();
        Ok(converter.convert_rows(self.keys.iter().map(|k| parser.parse(k)))?)
    }
}

/// One aggregate's state for every group.
trait Accumulator: Send {
    /// Adds a batch: row `i` is in group `groups[i]`, and `values` is the argument's column (`None` for COUNT(*)).
    /// `num_groups` is how many groups exist now (new ones may have appeared in this batch).
    fn update(
        &mut self,
        groups: &[usize],
        values: Option<&ArrayRef>,
        num_groups: usize,
    ) -> Result<()>;

    /// One value per group.
    fn finish(&mut self, num_groups: usize) -> Result<ArrayRef>;
}

/// The rows of a column that aren't NULL (logically: a `NullArray` has no null buffer but is all NULL).
fn valid(values: &ArrayRef) -> impl Fn(usize) -> bool + '_ {
    let nulls = values.logical_nulls();
    move |i| nulls.as_ref().is_none_or(|n| n.is_valid(i))
}

fn accumulator(agg: &Expr, schema: &PlanSchema) -> Result<Box<dyn Accumulator>> {
    let Expr::Aggregate {
        func,
        arg,
        distinct,
    } = agg
    else {
        unreachable!("LogicalPlan::aggregate accepts only aggregates")
    };
    let name = agg.display(schema).to_string();
    let arg_type = arg.as_ref().map(|a| a.data_type(schema));
    let inner: Box<dyn Accumulator> = match (func, &arg_type) {
        (AggregateFunction::Count, _) => Box::new(Count { counts: vec![] }),
        (AggregateFunction::Sum | AggregateFunction::Avg, Some(DataType::Float64)) => {
            Box::new(FloatSum {
                sums: vec![],
                counts: vec![],
                avg: *func == AggregateFunction::Avg,
            })
        }
        // The binder widens SUM's and AVG's arguments: integers (and NULL) arrive as Int64.
        (AggregateFunction::Sum | AggregateFunction::Avg, Some(DataType::Int64)) => {
            Box::new(IntSum {
                sums: vec![],
                counts: vec![],
                avg: *func == AggregateFunction::Avg,
                name,
            })
        }
        (AggregateFunction::Min | AggregateFunction::Max, Some(t)) => {
            Box::new(MinMax::new(t, *func == AggregateFunction::Max)?)
        }
        (f, t) => return Err(exec_err!("no accumulator for {} over {t:?}", f.name())),
    };
    // MIN(DISTINCT x) is MIN(x); DuckDB computes it so too.
    let min_max = matches!(func, AggregateFunction::Min | AggregateFunction::Max);
    if *distinct && !min_max {
        let t = arg_type.expect("DISTINCT has an argument");
        return Ok(Box::new(Distinct {
            converter: RowConverter::new(vec![SortField::new(t)])?,
            seen: HashSet::default(),
            inner,
        }));
    }
    Ok(inner)
}

/// COUNT(*) counts rows, COUNT(x) the rows where x isn't NULL.
struct Count {
    counts: Vec<i64>,
}

impl Accumulator for Count {
    fn update(
        &mut self,
        groups: &[usize],
        values: Option<&ArrayRef>,
        num_groups: usize,
    ) -> Result<()> {
        self.counts.resize(num_groups, 0);
        match values {
            None => groups.iter().for_each(|&g| self.counts[g] += 1),
            Some(values) => {
                let valid = valid(values);
                for (i, &g) in groups.iter().enumerate() {
                    if valid(i) {
                        self.counts[g] += 1;
                    }
                }
            }
        }
        Ok(())
    }

    fn finish(&mut self, num_groups: usize) -> Result<ArrayRef> {
        self.counts.resize(num_groups, 0);
        Ok(Arc::new(Int64Array::from(std::mem::take(&mut self.counts))))
    }
}

/// SUM or AVG of Int64 values, added up in an i128 so no partial sum overflows. A SUM that doesn't fit an Int64 at the
/// end is an error (DuckDB returns a 128-bit HUGEINT there; Quarry's SUM is an Int64). AVG divides at the end, as a
/// float.
struct IntSum {
    sums: Vec<i128>,
    counts: Vec<i64>,
    avg: bool,
    name: String,
}

impl Accumulator for IntSum {
    fn update(
        &mut self,
        groups: &[usize],
        values: Option<&ArrayRef>,
        num_groups: usize,
    ) -> Result<()> {
        self.sums.resize(num_groups, 0);
        self.counts.resize(num_groups, 0);
        let values = values.expect("SUM has an argument");
        let ints = values.as_primitive::<Int64Type>();
        for (i, &g) in groups.iter().enumerate() {
            if ints.is_valid(i) {
                self.sums[g] += i128::from(ints.value(i));
                self.counts[g] += 1;
            }
        }
        Ok(())
    }

    fn finish(&mut self, num_groups: usize) -> Result<ArrayRef> {
        self.sums.resize(num_groups, 0);
        self.counts.resize(num_groups, 0);
        let present = self.counts.iter().map(|&c| c > 0);
        if self.avg {
            let avgs: Float64Array = self
                .sums
                .iter()
                .zip(present)
                .zip(&self.counts)
                .map(|((&s, p), &c)| p.then(|| s as f64 / c as f64))
                .collect();
            return Ok(Arc::new(avgs));
        }
        let sums = self
            .sums
            .iter()
            .zip(present)
            .map(|(&s, p)| match p {
                false => Ok(None),
                true => i64::try_from(s).map(Some).map_err(|_| {
                    exec_err!(
                        "overflow in {}: the sum {s} doesn't fit in an Int64",
                        self.name
                    )
                }),
            })
            .collect::<Result<Int64Array>>()?;
        Ok(Arc::new(sums))
    }
}

/// SUM or AVG of Float64 values.
struct FloatSum {
    sums: Vec<f64>,
    counts: Vec<i64>,
    avg: bool,
}

impl Accumulator for FloatSum {
    fn update(
        &mut self,
        groups: &[usize],
        values: Option<&ArrayRef>,
        num_groups: usize,
    ) -> Result<()> {
        self.sums.resize(num_groups, 0.0);
        self.counts.resize(num_groups, 0);
        let values = values.expect("SUM has an argument");
        let floats = values.as_primitive::<Float64Type>();
        if floats.nulls().is_none() {
            add_in_runs(&mut self.sums, &mut self.counts, groups, floats.values());
            return Ok(());
        }
        for (i, &g) in groups.iter().enumerate() {
            if floats.is_valid(i) {
                self.sums[g] += floats.value(i);
                self.counts[g] += 1;
            }
        }
        Ok(())
    }

    fn finish(&mut self, num_groups: usize) -> Result<ArrayRef> {
        self.sums.resize(num_groups, 0.0);
        self.counts.resize(num_groups, 0);
        let out: Float64Array = self
            .sums
            .iter()
            .zip(&self.counts)
            .map(|(&s, &c)| match (c, self.avg) {
                (0, _) => None,
                (c, true) => Some(s / c as f64),
                (_, false) => Some(s),
            })
            .collect();
        Ok(Arc::new(out))
    }
}

/// Adds `values[i]` to `sums[groups[i]]` and counts it, for values with no NULLs, keeping a run of rows in one group
/// in a register. Adding a row to the sum in memory waits for the previous row's store to the same group (TPC-H Q1's
/// four groups come in runs, so most rows did); a run's running sum doesn't. Each group's values are still added one
/// at a time in row order, so the sums are the same bits as one row at a time.
fn add_in_runs(sums: &mut [f64], counts: &mut [i64], groups: &[usize], values: &[f64]) {
    let mut rows = groups.iter().zip(values);
    let Some((&first, &v)) = rows.next() else {
        return;
    };
    let (mut group, mut sum, mut n) = (first, sums[first] + v, 1);
    for (&g, &v) in rows {
        if g == group {
            sum += v;
            n += 1;
        } else {
            sums[group] = sum;
            counts[group] += n;
            (group, sum, n) = (g, sums[g] + v, 1);
        }
    }
    sums[group] = sum;
    counts[group] += n;
}

/// MIN or MAX of any type the row format encodes: the values are compared as their row-format bytes, which order as
/// the values do (floats normalized first, so NaN is the greatest, as in DuckDB). Per group it keeps the best value's
/// normalized bytes, to compare, and its bytes as it was, to decode back to the argument's type at the end. Among
/// equal values (-0.0 and 0.0) the last one wins, as in DuckDB.
struct MinMax {
    data_type: DataType,
    /// `None` for the NULL type, whose MIN and MAX are NULL.
    converter: Option<RowConverter>,
    /// (normalized, original) bytes.
    best: Vec<Option<(Vec<u8>, Vec<u8>)>>,
    max: bool,
}

impl MinMax {
    fn new(data_type: &DataType, max: bool) -> Result<MinMax> {
        let converter = match data_type {
            DataType::Null => None,
            t => Some(RowConverter::new(vec![SortField::new(t.clone())])?),
        };
        Ok(MinMax {
            data_type: data_type.clone(),
            converter,
            best: vec![],
            max,
        })
    }
}

impl Accumulator for MinMax {
    fn update(
        &mut self,
        groups: &[usize],
        values: Option<&ArrayRef>,
        num_groups: usize,
    ) -> Result<()> {
        self.best.resize(num_groups, None);
        let Some(converter) = &self.converter else {
            return Ok(());
        };
        let values = values.expect("MIN has an argument");
        let valid = valid(values);
        let normalized = normalize_floats(values);
        let encoded = converter.convert_columns(std::slice::from_ref(&normalized))?;
        let originals = match is_float(values.data_type()) {
            true => Some(converter.convert_columns(std::slice::from_ref(values))?),
            false => None,
        };
        for (i, &g) in groups.iter().enumerate() {
            if !valid(i) {
                continue;
            }
            let row = encoded.row(i);
            let row: &[u8] = row.as_ref();
            let better = match &self.best[g] {
                None => true,
                Some((best, _)) if self.max => row >= best.as_slice(),
                Some((best, _)) => row <= best.as_slice(),
            };
            if better {
                let original = match &originals {
                    Some(o) => o.row(i).as_ref().to_vec(),
                    None => row.to_vec(),
                };
                self.best[g] = Some((row.to_vec(), original));
            }
        }
        Ok(())
    }

    fn finish(&mut self, num_groups: usize) -> Result<ArrayRef> {
        self.best.resize(num_groups, None);
        let Some(converter) = &self.converter else {
            return Ok(new_null_array(&self.data_type, num_groups));
        };
        // Decode the groups that have a value, then spread them out with NULLs for the groups that don't.
        let parser = converter.parser();
        let found = converter.convert_rows(
            self.best
                .iter()
                .flatten()
                .map(|(_, original)| parser.parse(original)),
        )?;
        let mut next = 0u32;
        let positions: UInt32Array = self
            .best
            .iter()
            .map(|b| {
                b.as_ref().map(|_| {
                    next += 1;
                    next - 1
                })
            })
            .collect();
        Ok(take(&found[0], &positions, None)?)
    }
}

/// `agg(DISTINCT x)`: passes each group's first occurrence of each value on to the aggregate, as it was, by remembering
/// the (group, value) pairs it has seen (values in row format, floats normalized: -0.0 and 0.0 are one value).
struct Distinct {
    converter: RowConverter,
    seen: HashSet<(usize, Vec<u8>), RandomState>,
    inner: Box<dyn Accumulator>,
}

impl Accumulator for Distinct {
    fn update(
        &mut self,
        groups: &[usize],
        values: Option<&ArrayRef>,
        num_groups: usize,
    ) -> Result<()> {
        let values = values.expect("DISTINCT has an argument");
        let valid = valid(values);
        let encoded = self
            .converter
            .convert_columns(&[normalize_floats(values)])?;
        let mut keep = Vec::new();
        let mut kept_groups = Vec::new();
        for (i, &g) in groups.iter().enumerate() {
            if valid(i) && self.seen.insert((g, encoded.row(i).as_ref().to_vec())) {
                keep.push(i as u32);
                kept_groups.push(g);
            }
        }
        let kept = take(values, &UInt32Array::from(keep), None)?;
        self.inner.update(&kept_groups, Some(&kept), num_groups)
    }

    fn finish(&mut self, num_groups: usize) -> Result<ArrayRef> {
        self.inner.finish(num_groups)
    }
}
