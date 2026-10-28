//! Row-group pruning: which of a Parquet file's row groups can hold a row that a scan's filters keep, judged from the
//! footer's statistics alone (each column's min, max and null count per row group).
//!
//! Each filter is rewritten into a condition on the statistics: `x > 5` can hold in a row group only if `max(x) > 5`,
//! `x = 5` only if `min(x) <= 5 AND max(x) >= 5`, `x IS NULL` only if its null count is above 0. The rewritten
//! condition is evaluated by the engine's own evaluator over a batch with one row per row group, so it compares
//! exactly as the filter will (strings by bytes, -0.0 as 0.0), and a row group is skipped only where it is FALSE. A
//! missing statistic is NULL, which keeps the row group; so does any filter the rewrite doesn't know, and any error.
//! Skipping is only ever an optimization: the filter still runs on every row that is read.
//!
//! Floats need care: Parquet writers leave NaN out of min and max (NaN is the greatest value in SQL here, so a row
//! group whose max is 1.0 may still hold a NaN that `x > 5.0` keeps), and a row group of only NaNs has NaN for both.
//! So for a float column only the min is used (`<` and `<=` against any constant, `=` against a DOUBLE literal that
//! isn't NaN), and NaN statistics count as missing.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Int64Array, RecordBatch};
use arrow::compute::{cast, nullif};
use arrow::datatypes::{DataType, Float32Type, Float64Type, Schema};
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::file::metadata::ParquetMetaData;

use crate::exec::evaluate;
use crate::plan::{BinaryOp, Expr, PlanField, PlanSchema, ScalarValue};
use crate::types::is_float;

/// The row groups, in file order, that can hold a row for which every one of `filters` (over the table's columns) is
/// TRUE.
pub(crate) fn row_groups(
    filters: &[Expr],
    schema: &Schema,
    metadata: &ParquetMetaData,
) -> Vec<usize> {
    let all: Vec<usize> = (0..metadata.num_row_groups()).collect();
    if filters.is_empty() || all.is_empty() {
        return all;
    }
    let rows = metadata.row_groups().iter().map(|rg| rg.num_rows());
    let mut stats = Statistics {
        schema,
        metadata,
        columns: HashMap::new(),
        fields: vec![PlanField::new(None, "rows", DataType::Int64, false)],
        arrays: vec![Arc::new(Int64Array::from_iter_values(rows))],
    };
    let rewritten: Vec<Expr> = filters.iter().filter_map(|f| stats.rewrite(f)).collect();
    let Some(condition) = Expr::and_all(rewritten) else {
        return all;
    };
    let plan_schema = PlanSchema::new(stats.fields);
    let Ok(batch) = RecordBatch::try_new(Arc::new(plan_schema.to_arrow()), stats.arrays) else {
        return all;
    };
    let Ok(result) = evaluate(&condition, &plan_schema, &batch) else {
        return all;
    };
    let Some(result) = result.as_boolean_opt() else {
        return all; // NULL-typed: no row group is known to be FALSE
    };
    all.into_iter()
        .filter(|&i| !(result.is_valid(i) && !result.value(i)))
        .collect()
}

/// The statistics columns built so far, one row per row group: column 0 the row count, then for each table column `i`
/// a filter uses, its min, max and null count at `columns[i]`, `columns[i] + 1` and `columns[i] + 2`.
struct Statistics<'a> {
    schema: &'a Schema,
    metadata: &'a ParquetMetaData,
    columns: HashMap<usize, Option<usize>>,
    fields: Vec<PlanField>,
    arrays: Vec<ArrayRef>,
}

impl Statistics<'_> {
    /// Where table column `i`'s statistics are, reading them on first use; `None` if they can't be read.
    fn column(&mut self, i: usize) -> Option<usize> {
        if let Some(&at) = self.columns.get(&i) {
            return at;
        }
        let at = self.read(i);
        self.columns.insert(i, at);
        at
    }

    fn read(&mut self, i: usize) -> Option<usize> {
        let field = self.schema.fields().get(i)?;
        let converter = StatisticsConverter::try_new(
            field.name(),
            self.schema,
            self.metadata.file_metadata().schema_descr(),
        )
        .ok()?
        .with_missing_null_counts_as_zero(false);
        let groups = self.metadata.row_groups();
        let min = converter.row_group_mins(groups.iter()).ok()?;
        let max = converter.row_group_maxes(groups.iter()).ok()?;
        let nulls = converter.row_group_null_counts(groups.iter()).ok()?;
        if min.data_type() != field.data_type() || max.data_type() != field.data_type() {
            return None;
        }
        let nulls = cast(&nulls, &DataType::Int64).ok()?;
        let at = self.fields.len();
        let name = field.name();
        for (what, data_type) in [
            ("min", field.data_type()),
            ("max", field.data_type()),
            ("nulls", &DataType::Int64),
        ] {
            self.fields.push(PlanField::new(
                None,
                &format!("{what}({name})"),
                data_type.clone(),
                true,
            ));
        }
        self.arrays.push(without_nans(&min)?);
        self.arrays.push(without_nans(&max)?);
        self.arrays.push(nulls);
        Some(at)
    }

    /// A condition on the statistics that is FALSE only for row groups where `filter` can't be TRUE on any row;
    /// `None` if there is no such condition this knows.
    fn rewrite(&mut self, filter: &Expr) -> Option<Expr> {
        match filter {
            Expr::Binary {
                op: BinaryOp::And,
                left,
                right,
            } => match (self.rewrite(left), self.rewrite(right)) {
                (Some(l), Some(r)) => Some(Expr::binary(BinaryOp::And, l, r)),
                (Some(one), None) | (None, Some(one)) => Some(one),
                (None, None) => None,
            },
            Expr::Binary {
                op: BinaryOp::Or,
                left,
                right,
            } => Some(Expr::binary(
                BinaryOp::Or,
                self.rewrite(left)?,
                self.rewrite(right)?,
            )),
            Expr::Binary { op, left, right } if op.is_comparison() => {
                if is_constant(right) {
                    self.compare(left, *op, right)
                } else if is_constant(left) {
                    self.compare(right, flip(*op), left)
                } else {
                    None
                }
            }
            Expr::IsNull(e) => {
                let (column, _) = self.target(e)?;
                Some(Expr::binary(
                    BinaryOp::Gt,
                    Expr::Column(column + 2),
                    Expr::Literal(ScalarValue::Int64(0)),
                ))
            }
            Expr::IsNotNull(e) => {
                let (column, _) = self.target(e)?;
                Some(Expr::binary(
                    BinaryOp::Lt,
                    Expr::Column(column + 2),
                    Expr::Column(0),
                ))
            }
            Expr::Like {
                expr,
                pattern,
                negated: false,
                case_insensitive: false,
            } => self.like_prefix(expr, pattern),
            Expr::InList {
                expr,
                list,
                negated: false,
            } if list.iter().all(is_constant) => {
                let mut any = None;
                for item in list {
                    let eq = self.compare(expr, BinaryOp::Eq, item)?;
                    any = Some(match any {
                        None => eq,
                        Some(prev) => Expr::binary(BinaryOp::Or, prev, eq),
                    });
                }
                any
            }
            _ => None,
        }
    }

    /// `x op value`, where `x` is a column or a widening cast of one.
    fn compare(&mut self, x: &Expr, op: BinaryOp, value: &Expr) -> Option<Expr> {
        let (column, wrap) = self.target(x)?;
        let float = is_float(&self.fields[column].data_type);
        let number = matches!(value, Expr::Literal(ScalarValue::Float64(f)) if !f.is_nan());
        let min = || wrap(Expr::Column(column));
        let max = || wrap(Expr::Column(column + 1));
        let v = || value.clone();
        Some(match op {
            BinaryOp::Lt => Expr::binary(BinaryOp::Lt, min(), v()),
            BinaryOp::LtEq => Expr::binary(BinaryOp::LtEq, min(), v()),
            BinaryOp::Eq if !float || number => Expr::binary(
                BinaryOp::And,
                Expr::binary(BinaryOp::LtEq, min(), v()),
                Expr::binary(BinaryOp::GtEq, max(), v()),
            ),
            _ if float => return None,
            BinaryOp::Gt => Expr::binary(BinaryOp::Gt, max(), v()),
            BinaryOp::GtEq => Expr::binary(BinaryOp::GtEq, max(), v()),
            BinaryOp::NotEq => Expr::Not(Box::new(Expr::binary(
                BinaryOp::And,
                Expr::binary(BinaryOp::Eq, min(), v()),
                Expr::binary(BinaryOp::Eq, max(), v()),
            ))),
            _ => return None,
        })
    }

    /// `x LIKE 'abc%...'`: every value it keeps starts with the pattern's prefix before its first wildcard, so it is at
    /// least that prefix, and (for an ASCII prefix) below the prefix with its last byte one higher. A pattern that
    /// starts with a wildcard has no prefix and says nothing.
    fn like_prefix(&mut self, x: &Expr, pattern: &Expr) -> Option<Expr> {
        let Expr::Literal(ScalarValue::Utf8(pattern)) = pattern else {
            return None;
        };
        let prefix: String = pattern
            .chars()
            .take_while(|c| *c != '%' && *c != '_')
            .collect();
        if prefix.is_empty() || !matches!(x, Expr::Column(_)) {
            return None;
        }
        let (column, _) = self.target(x)?;
        if self.fields[column].data_type != DataType::Utf8 {
            return None;
        }
        let text = |s: String| Expr::Literal(ScalarValue::Utf8(s));
        let at_least = Expr::binary(
            BinaryOp::GtEq,
            Expr::Column(column + 1),
            text(prefix.clone()),
        );
        let last = *prefix.as_bytes().last().expect("not empty");
        if !prefix.is_ascii() || last == 0x7F {
            return Some(at_least);
        }
        let mut upper = prefix.into_bytes();
        *upper.last_mut().expect("not empty") += 1;
        let upper = String::from_utf8(upper).expect("ASCII stays UTF-8");
        Some(Expr::binary(
            BinaryOp::And,
            at_least,
            Expr::binary(BinaryOp::Lt, Expr::Column(column), text(upper)),
        ))
    }

    /// The statistics of the column `e` reads, if `e` is a column or a cast of one that keeps order (an integer to
    /// a wider integer or a float, a REAL to a DOUBLE), and how to put a statistic through the same cast.
    fn target(&mut self, e: &Expr) -> Option<(usize, impl Fn(Expr) -> Expr + use<>)> {
        let (column, to) = match e {
            Expr::Column(i) => (*i, None),
            Expr::Cast { expr, data_type } => match **expr {
                Expr::Column(i) => {
                    let from = self.schema.fields().get(i)?.data_type();
                    let keeps_order = match data_type {
                        DataType::Int64 => from.is_integer(),
                        DataType::Float64 => from.is_integer() || is_float(from),
                        _ => false,
                    };
                    if !keeps_order {
                        return None;
                    }
                    (i, Some(data_type.clone()))
                }
                _ => return None,
            },
            _ => return None,
        };
        let at = self.column(column)?;
        Some((at, move |stat: Expr| match &to {
            Some(t) => stat.cast(t),
            None => stat,
        }))
    }
}

fn is_constant(e: &Expr) -> bool {
    e.columns().is_empty() && !e.contains_aggregate()
}

/// `a op b` as `b op' a`.
fn flip(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        other => other,
    }
}

/// A float statistic with NaNs made NULL (unknown); other types as they are.
fn without_nans(a: &ArrayRef) -> Option<ArrayRef> {
    let nans: BooleanArray = match a.data_type() {
        DataType::Float64 => a
            .as_primitive::<Float64Type>()
            .iter()
            .map(|v| Some(v.is_some_and(f64::is_nan)))
            .collect(),
        DataType::Float32 => a
            .as_primitive::<Float32Type>()
            .iter()
            .map(|v| Some(v.is_some_and(f32::is_nan)))
            .collect(),
        _ => return Some(a.clone()),
    };
    nullif(a, &nans).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    use parquet::file::metadata::RowGroupMetaData;
    use parquet::file::properties::WriterProperties;
    use parquet::file::statistics::Statistics as ParquetStatistics;

    #[test]
    fn a_missing_null_count_is_unknown_not_zero() {
        // x = [NULL, 1] in one row group, written by parquet-rs (which records the null count), then the footer
        // rebuilt without null counts, as another writer may leave it. `x IS NULL` must keep the row group: reading a
        // missing count as 0 (the converter's default) would skip the group holding the NULL.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![None, Some(1)]))],
        )
        .unwrap();
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("x.parquet");
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            schema.clone(),
            Some(WriterProperties::builder().build()),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let loaded = ArrowReaderMetadata::load(
            &std::fs::File::open(&path).unwrap(),
            ArrowReaderOptions::new(),
        )
        .unwrap();
        let metadata = loaded.metadata();
        let is_null = Expr::IsNull(Box::new(Expr::Column(0)));
        assert_eq!(
            row_groups(std::slice::from_ref(&is_null), &schema, metadata),
            vec![0]
        );

        let without_counts: Vec<RowGroupMetaData> = metadata
            .row_groups()
            .iter()
            .map(|rg| {
                let columns = rg
                    .columns()
                    .iter()
                    .map(|c| {
                        let stats = ParquetStatistics::int64(Some(1), Some(1), None, None, false);
                        c.clone()
                            .into_builder()
                            .set_statistics(stats)
                            .build()
                            .unwrap()
                    })
                    .collect();
                rg.clone()
                    .into_builder()
                    .set_column_metadata(columns)
                    .build()
                    .unwrap()
            })
            .collect();
        let stripped = ParquetMetaData::new(metadata.file_metadata().clone(), without_counts);
        assert_eq!(row_groups(&[is_null], &schema, &stripped), vec![0]);
        // The min and max still prune: x > 5 can't hold where min = max = 1.
        let above = Expr::binary(
            BinaryOp::Gt,
            Expr::Column(0),
            Expr::Literal(ScalarValue::Int64(5)),
        );
        assert_eq!(
            row_groups(&[above], &schema, &stripped),
            Vec::<usize>::new()
        );
    }
}
