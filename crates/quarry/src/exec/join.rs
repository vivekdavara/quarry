//! The Join node: a hash join on the equalities in `ON`, or a nested-loop join when there are none (DESIGN.md,
//! "Joins").
//!
//! The right input is the build side: it is read whole, and with equality keys its rows are grouped by key in a hash
//! table (keys in Arrow's row format with floats normalized, as the aggregate groups them, so -0.0 meets 0.0 and NaN
//! meets NaN; a NULL key meets nothing). The left input streams: each of its rows looks up its candidates (the build
//! rows with an equal key, or every build row in a nested loop), the rest of `ON` is evaluated on the candidate
//! pairs, and the pairs where it is TRUE come out. A LEFT join's left row without such a pair comes out once, with
//! NULLs on the right, in its place. The output is in the left input's order, each left row's matches in the build
//! side's order, whatever the batch size.

use std::collections::{HashMap, VecDeque};

use ahash::RandomState;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, RecordBatch, UInt32Array};
use arrow::compute::{concat_batches, filter_record_batch, take};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::row::{RowConverter, Rows, SortField};

use super::{evaluate, normalize_floats, rows, stream, take_rows};
use crate::datasource::{BatchIter, split_batch};
use crate::error::{Result, exec_err};
use crate::plan::{BinaryOp, Expr, Join, JoinKind, PlanSchemaRef};

/// `ON` split into what the hash table matches and what is evaluated on the pairs it finds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct JoinCondition {
    /// Over the left input's columns.
    pub left_keys: Vec<Expr>,
    /// Over the right input's columns (numbered from 0), one for each left key, of the same type.
    pub right_keys: Vec<Expr>,
    /// The other conjuncts, in their order, over the joined schema.
    pub residual: Option<Expr>,
}

/// Splits `ON` into equality keys and the rest. A conjunct `l = r` is a key when `l` reads only left columns and `r`
/// only right columns (or the other way round), each at least one, and both have the same type; every other
/// conjunct is evaluated on the pairs the keys find, in its order.
pub(crate) fn split_condition(on: Option<&Expr>, left_width: usize) -> JoinCondition {
    let mut condition = JoinCondition {
        left_keys: vec![],
        right_keys: vec![],
        residual: None,
    };
    let Some(on) = on else {
        return condition;
    };
    let side = |e: &Expr| {
        let columns = e.columns();
        match (columns.first(), columns.last()) {
            (Some(_), Some(&last)) if last < left_width => Some(JoinSide::Left),
            (Some(&first), Some(_)) if first >= left_width => Some(JoinSide::Right),
            _ => None,
        }
    };
    let mut rest = Vec::new();
    for c in on.conjuncts() {
        if let Expr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        } = c
        {
            let pair = match (side(left), side(right)) {
                (Some(JoinSide::Left), Some(JoinSide::Right)) => Some((left, right)),
                (Some(JoinSide::Right), Some(JoinSide::Left)) => Some((right, left)),
                _ => None,
            };
            if let Some((l, r)) = pair {
                condition.left_keys.push((**l).clone());
                condition
                    .right_keys
                    .push(r.map_columns(&|i| i - left_width));
                continue;
            }
        }
        rest.push(c.clone());
    }
    condition.residual = Expr::and_all(rest);
    condition
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinSide {
    Left,
    Right,
}

/// Runs a join: reads the right input whole, then streams the left through it.
pub(super) fn join(node: &Join, batch_size: usize) -> Result<BatchIter> {
    let left_schema = node.left.schema().clone();
    let right_schema = node.right.schema().clone();
    let mut condition = split_condition(node.on.as_ref(), left_schema.len());
    // Keys of different types (the binder casts both sides of `=` to one type, so this is a safeguard) are compared
    // by the residual instead.
    let same_types = condition
        .left_keys
        .iter()
        .zip(&condition.right_keys)
        .all(|(l, r)| l.data_type(&left_schema) == r.data_type(&right_schema));
    if !same_types {
        condition = JoinCondition {
            left_keys: vec![],
            right_keys: vec![],
            residual: node.on.clone(),
        };
    }

    // One converter encodes both sides' keys, so equal keys are equal bytes.
    let converter = RowConverter::new(
        condition
            .left_keys
            .iter()
            .map(|k| SortField::new(k.data_type(&left_schema)))
            .collect(),
    )?;
    let build = Build::new(
        &node.right,
        &right_schema,
        &condition.right_keys,
        &converter,
        batch_size,
    )?;
    if build.batch.num_rows() == 0 && node.kind != JoinKind::Left {
        // Nothing can match, so the left input isn't read at all.
        return Ok(Box::new(std::iter::empty()));
    }
    Ok(Box::new(JoinStream {
        left: stream(&node.left, batch_size)?,
        left_schema,
        converter,
        left_keys: condition.left_keys,
        residual: condition.residual,
        joined_schema: node.schema.clone(),
        out_schema: Arc::new(node.schema.to_arrow()),
        outer: node.kind == JoinKind::Left,
        build,
        batch_size,
        probe: None,
        ready: VecDeque::new(),
    }))
}

/// The build side: every row of the right input, and with keys, those rows grouped by key.
struct Build {
    batch: RecordBatch,
    /// `None` for a nested loop (no keys): every row is a candidate for every left row.
    table: Option<KeyTable>,
}

/// The build rows grouped by key: group `g`'s rows are `rows[starts[g]..starts[g + 1]]`, in input order.
struct KeyTable {
    groups: HashMap<Vec<u8>, u32, RandomState>,
    starts: Vec<u32>,
    rows: Vec<u32>,
}

impl Build {
    fn new(
        plan: &crate::plan::LogicalPlan,
        schema: &PlanSchemaRef,
        keys: &[Expr],
        converter: &RowConverter,
        batch_size: usize,
    ) -> Result<Build> {
        let batches = stream(plan, batch_size)?.collect::<Result<Vec<_>>>()?;
        let arrow_schema = Arc::new(schema.to_arrow());
        let num_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        if num_rows >= u32::MAX as usize {
            return Err(exec_err!(
                "a join's right input has {num_rows} rows; at most 2^32 - 1 are supported"
            ));
        }
        let batch = if arrow_schema.fields().is_empty() {
            RecordBatch::try_new_with_options(arrow_schema, vec![], &rows(num_rows))?
        } else {
            let columns = concat_batches(&arrow_schema, &batches)?.columns().to_vec();
            RecordBatch::try_new_with_options(arrow_schema, columns, &rows(num_rows))?
        };
        if keys.is_empty() {
            return Ok(Build { batch, table: None });
        }
        let (encoded, valid) = encode_keys(keys, schema, &batch, converter)?;
        let mut groups: HashMap<Vec<u8>, u32, RandomState> = HashMap::default();
        let mut group_of = Vec::with_capacity(num_rows);
        let mut sizes: Vec<u32> = Vec::new();
        for (i, row) in encoded.iter().enumerate() {
            if !valid[i] {
                group_of.push(u32::MAX);
                continue;
            }
            let bytes = row.as_ref();
            let g = match groups.get(bytes) {
                Some(&g) => g,
                None => {
                    let g = sizes.len() as u32;
                    groups.insert(bytes.to_vec(), g);
                    sizes.push(0);
                    g
                }
            };
            sizes[g as usize] += 1;
            group_of.push(g);
        }
        // A counting sort of the row numbers by group, which keeps each group's rows in input order.
        let mut starts = Vec::with_capacity(sizes.len() + 1);
        let mut total = 0u32;
        starts.push(0);
        for size in &sizes {
            total += size;
            starts.push(total);
        }
        let mut next = starts.clone();
        let mut rows_by_group = vec![0u32; total as usize];
        for (row, &g) in group_of.iter().enumerate() {
            if g != u32::MAX {
                rows_by_group[next[g as usize] as usize] = row as u32;
                next[g as usize] += 1;
            }
        }
        Ok(Build {
            batch,
            table: Some(KeyTable {
                groups,
                starts,
                rows: rows_by_group,
            }),
        })
    }
}

/// The keys of every row of `batch` in row format (floats normalized), and whether each row's keys are all
/// non-NULL (a NULL key equals nothing).
fn encode_keys(
    keys: &[Expr],
    schema: &PlanSchemaRef,
    batch: &RecordBatch,
    converter: &RowConverter,
) -> Result<(Rows, Vec<bool>)> {
    let columns = keys
        .iter()
        .map(|k| evaluate(k, schema, batch).map(|c| normalize_floats(&c)))
        .collect::<Result<Vec<ArrayRef>>>()?;
    let encoded = converter.convert_columns(&columns)?;
    Ok((encoded, non_null_rows(&columns, batch.num_rows())))
}

fn non_null_rows(columns: &[ArrayRef], num_rows: usize) -> Vec<bool> {
    let mut valid = vec![true; num_rows];
    for c in columns {
        if *c.data_type() == DataType::Null {
            valid.fill(false);
        } else if let Some(nulls) = c.logical_nulls() {
            for (v, ok) in valid.iter_mut().zip(nulls.iter()) {
                *v &= ok;
            }
        }
    }
    valid
}

/// One left batch being joined: each row's candidates, and how far the pairs have got.
struct Probe {
    batch: RecordBatch,
    /// For each left row, its candidates: a range of the key table's `rows`, or of the build rows themselves in a
    /// nested loop.
    candidates: Vec<(u32, u32)>,
    /// The next pair to make: left row `row`, its candidate number `offset`.
    row: usize,
    offset: u32,
    /// For a LEFT join, whether each left row has had a pair where `ON` is TRUE.
    matched: Vec<bool>,
}

struct JoinStream {
    left: BatchIter,
    left_schema: PlanSchemaRef,
    converter: RowConverter,
    left_keys: Vec<Expr>,
    residual: Option<Expr>,
    joined_schema: PlanSchemaRef,
    out_schema: SchemaRef,
    outer: bool,
    build: Build,
    batch_size: usize,
    probe: Option<Probe>,
    /// Output already made, at most `batch_size` rows a batch.
    ready: VecDeque<RecordBatch>,
}

impl JoinStream {
    /// Looks up the candidates of each row of a left batch.
    fn start(&self, batch: RecordBatch) -> Result<Probe> {
        let n = batch.num_rows();
        let candidates = match &self.build.table {
            None => vec![(0, self.build.batch.num_rows() as u32); n],
            Some(table) => {
                let (encoded, valid) =
                    encode_keys(&self.left_keys, &self.left_schema, &batch, &self.converter)?;
                encoded
                    .iter()
                    .zip(valid)
                    .map(|(row, ok)| match table.groups.get(row.as_ref()) {
                        Some(&g) if ok => (table.starts[g as usize], table.starts[g as usize + 1]),
                        _ => (0, 0),
                    })
                    .collect()
            }
        };
        Ok(Probe {
            batch,
            candidates,
            row: 0,
            offset: 0,
            matched: vec![false; if self.outer { n } else { 0 }],
        })
    }

    /// Joins the next stretch of the current left batch: at most `batch_size` candidate pairs (and left rows without
    /// candidates). Returns false once the batch is done.
    fn step(&mut self) -> Result<bool> {
        let Some(probe) = &mut self.probe else {
            return Ok(false);
        };
        let first_row = probe.row;
        if first_row == probe.candidates.len() {
            return Ok(false);
        }
        let mut left_rows = Vec::new();
        let mut build_rows = Vec::new();
        let mut budget = self.batch_size;
        while probe.row < probe.candidates.len() && budget > 0 {
            let (start, end) = probe.candidates[probe.row];
            let from = start + probe.offset;
            let take_n = ((end - from) as usize).min(budget) as u32;
            for c in from..from + take_n {
                left_rows.push(probe.row as u32);
                build_rows.push(match &self.build.table {
                    Some(table) => table.rows[c as usize],
                    None => c,
                });
            }
            budget -= (take_n as usize).max(1);
            if from + take_n == end {
                probe.row += 1;
                probe.offset = 0;
            } else {
                probe.offset += take_n;
            }
        }
        // Left rows before `probe.row` have had all their pairs (the row it stopped in may have more to come).
        let done_rows = first_row..probe.row;

        let pairs = self
            .build
            .pairs(&self.out_schema, &probe.batch, &left_rows, &build_rows)?;
        let keep = match &self.residual {
            None => None,
            Some(residual) => Some(true_rows(&evaluate(
                residual,
                &self.joined_schema,
                &pairs,
            )?)?),
        };
        if !self.outer {
            let out = match keep {
                None => pairs,
                Some(keep) => filter_record_batch(&pairs, &BooleanArray::from(keep))?,
            };
            self.ready.extend(split_batch(&out, self.batch_size));
            return Ok(true);
        }

        // A LEFT join: the pairs that hold, and in place of each finished left row without one, the row with NULLs.
        let mut out_left = Vec::with_capacity(left_rows.len());
        let mut out_build: Vec<Option<u32>> = Vec::with_capacity(left_rows.len());
        let mut pair = 0;
        for row in first_row..=probe.row.min(probe.candidates.len().saturating_sub(1)) {
            while pair < left_rows.len() && left_rows[pair] as usize == row {
                if keep.as_ref().is_none_or(|k| k[pair]) {
                    probe.matched[row] = true;
                    out_left.push(row as u32);
                    out_build.push(Some(build_rows[pair]));
                }
                pair += 1;
            }
            if done_rows.contains(&row) && !probe.matched[row] {
                out_left.push(row as u32);
                out_build.push(None);
            }
        }
        let out =
            self.build
                .pairs_or_nulls(&self.out_schema, &probe.batch, &out_left, &out_build)?;
        self.ready.extend(split_batch(&out, self.batch_size));
        Ok(true)
    }
}

impl Build {
    /// The joined rows `(left[i], build[i])`.
    fn pairs(
        &self,
        schema: &SchemaRef,
        left: &RecordBatch,
        left_rows: &[u32],
        build_rows: &[u32],
    ) -> Result<RecordBatch> {
        let l = take_rows(left, left_rows)?;
        let b = take_rows(&self.batch, build_rows)?;
        combine(schema, &l, b.columns(), left_rows.len())
    }

    /// The joined rows `(left[i], build[i])`, NULLs on the right where `build[i]` is `None`.
    fn pairs_or_nulls(
        &self,
        schema: &SchemaRef,
        left: &RecordBatch,
        left_rows: &[u32],
        build_rows: &[Option<u32>],
    ) -> Result<RecordBatch> {
        let l = take_rows(left, left_rows)?;
        let indices = UInt32Array::from(build_rows.to_vec());
        let b = self
            .batch
            .columns()
            .iter()
            .map(|c| take(c, &indices, None))
            .collect::<Result<Vec<_>, _>>()?;
        combine(schema, &l, &b, left_rows.len())
    }
}

fn combine(
    schema: &SchemaRef,
    left: &RecordBatch,
    right: &[ArrayRef],
    n: usize,
) -> Result<RecordBatch> {
    let mut columns = left.columns().to_vec();
    columns.extend_from_slice(right);
    Ok(RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &rows(n),
    )?)
}

/// Which rows of a condition's result are TRUE (not FALSE, not NULL).
fn true_rows(mask: &ArrayRef) -> Result<Vec<bool>> {
    if *mask.data_type() == DataType::Null {
        return Ok(vec![false; mask.len()]);
    }
    let Some(mask) = mask.as_boolean_opt() else {
        return Err(exec_err!(
            "a join condition evaluated to {}, not a boolean",
            mask.data_type()
        ));
    };
    Ok((0..mask.len())
        .map(|i| mask.is_valid(i) && mask.value(i))
        .collect())
}

impl Iterator for JoinStream {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(batch) = self.ready.pop_front() {
                if batch.num_rows() > 0 {
                    return Some(Ok(batch));
                }
                continue;
            }
            match self.step() {
                Err(e) => return Some(Err(e)),
                Ok(true) => continue,
                Ok(false) => {}
            }
            let batch = match self.left.next()? {
                Ok(b) => b,
                Err(e) => return Some(Err(e)),
            };
            match self.start(batch) {
                Ok(probe) => self.probe = Some(probe),
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::ScalarValue;

    fn col(i: usize) -> Expr {
        Expr::Column(i)
    }

    fn eq(l: Expr, r: Expr) -> Expr {
        Expr::binary(BinaryOp::Eq, l, r)
    }

    fn and(l: Expr, r: Expr) -> Expr {
        Expr::binary(BinaryOp::And, l, r)
    }

    #[test]
    fn keys_are_equalities_between_one_side_and_the_other() {
        // Left columns 0-1, right columns 2-3.
        let plus_one = Expr::binary(BinaryOp::Plus, col(0), Expr::Literal(ScalarValue::Int64(1)));
        let on = and(
            and(eq(col(0), col(2)), eq(col(3), plus_one.clone())),
            and(
                eq(col(0), col(1)),
                eq(col(2), Expr::Literal(ScalarValue::Int64(5))),
            ),
        );
        let c = split_condition(Some(&on), 2);
        // `r = l` is turned round, and the right side's columns are numbered from 0.
        assert_eq!(c.left_keys, vec![col(0), plus_one]);
        assert_eq!(c.right_keys, vec![col(0), col(1)]);
        // Both sides of `l0 = l1` are on the left, `r0 = 5` reads one side: the residual, in order.
        assert_eq!(
            c.residual,
            Some(and(
                eq(col(0), col(1)),
                eq(col(2), Expr::Literal(ScalarValue::Int64(5)))
            ))
        );
    }

    #[test]
    fn an_equality_mixing_sides_or_a_non_equality_is_residual() {
        // l0 + r0 = r1 reads both sides on its left: not a key. l0 < r0: not an equality.
        let mixed = eq(Expr::binary(BinaryOp::Plus, col(0), col(2)), col(3));
        let less = Expr::binary(BinaryOp::Lt, col(0), col(2));
        let on = and(mixed.clone(), less.clone());
        let c = split_condition(Some(&on), 2);
        assert!(c.left_keys.is_empty() && c.right_keys.is_empty());
        assert_eq!(c.residual, Some(and(mixed, less)));
        // An OR of equalities is one conjunct, so not a key either.
        let or = Expr::binary(BinaryOp::Or, eq(col(0), col(2)), eq(col(1), col(3)));
        let c = split_condition(Some(&or), 2);
        assert!(c.left_keys.is_empty());
        assert_eq!(c.residual, Some(or));
        // No ON: no keys, no residual (a cross join).
        assert_eq!(
            split_condition(None, 2),
            JoinCondition {
                left_keys: vec![],
                right_keys: vec![],
                residual: None
            }
        );
    }
}
