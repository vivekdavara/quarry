//! Vectorized expression evaluation (DESIGN.md, "Execution"): a bound [`Expr`] over a [`RecordBatch`], a whole column
//! at a time with Arrow's compute kernels.
//!
//! A literal stays a one-row scalar until an operator needs a full column, so `l_quantity < 24` compares a column with
//! a constant without building a column of 24s, and an expression of literals alone is computed once per batch.
//!
//! `AND`, `OR` and `CASE` evaluate their later operands only on the rows that still need them, as DuckDB does: in
//! `b <> 0 AND a % b = 1` the modulo never sees a zero, and a `CASE` branch never runs on a row it doesn't take.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Date32Array, Datum, Float64Array, Int64Array,
    NullArray, RecordBatch, RecordBatchOptions, StringArray, UInt32Array, new_empty_array,
    new_null_array,
};
use arrow::compute::kernels::{arity, boolean, cmp, comparison, concat_elements, numeric};
use arrow::compute::{CastOptions, cast_with_options, interleave, take};
use arrow::datatypes::{DataType, Float32Type, Float64Type};
use arrow::error::ArrowError;

use crate::error::{QuarryError, Result, exec_err};
use crate::plan::{BinaryOp, Expr, PlanSchema, ScalarValue};
use crate::types::is_integer;

/// Evaluates `expr` over `batch`, whose columns are `schema`'s (the input of the plan node holding the expression;
/// it names the columns in error messages). The result has one value per row of the batch.
///
/// An expression is computed for rows: over no rows it is never evaluated, so `9223372036854775807 + 1` fails only
/// when there is a row to compute it for, as it would row by row.
pub fn evaluate(expr: &Expr, schema: &PlanSchema, batch: &RecordBatch) -> Result<ArrayRef> {
    if batch.num_rows() == 0 {
        return Ok(new_empty_array(&expr.data_type(schema)));
    }
    Ok(Evaluator { schema }
        .eval(expr, batch)?
        .into_array(batch.num_rows())?)
}

/// An evaluated expression: a column with a value per row, or one value (a one-row array) for every row.
#[derive(Debug, Clone)]
enum Value {
    Array(ArrayRef),
    Scalar(ArrayRef),
}

impl Datum for Value {
    fn get(&self) -> (&dyn Array, bool) {
        match self {
            Value::Array(a) => (a.as_ref(), false),
            Value::Scalar(a) => (a.as_ref(), true),
        }
    }
}

impl Value {
    /// A kernel's result: a scalar when every input was one.
    fn from_result(array: ArrayRef, scalar: bool) -> Value {
        if scalar {
            Value::Scalar(array)
        } else {
            Value::Array(array)
        }
    }

    fn is_scalar(&self) -> bool {
        matches!(self, Value::Scalar(_))
    }

    /// The values: the column, or the scalar's one-row array.
    fn values(&self) -> &ArrayRef {
        match self {
            Value::Array(a) | Value::Scalar(a) => a,
        }
    }

    fn data_type(&self) -> &DataType {
        self.values().data_type()
    }

    /// A column of `rows` values (a scalar repeated).
    fn into_array(self, rows: usize) -> Result<ArrayRef, ArrowError> {
        match self {
            Value::Array(a) => Ok(a),
            Value::Scalar(a) => take(&a, &UInt32Array::from(vec![0u32; rows]), None),
        }
    }

    /// Applies a kernel to the values, keeping a scalar a scalar.
    fn map(
        self,
        f: impl FnOnce(&ArrayRef) -> Result<ArrayRef, ArrowError>,
    ) -> Result<Value, ArrowError> {
        match self {
            Value::Array(a) => Ok(Value::Array(f(&a)?)),
            Value::Scalar(a) => Ok(Value::Scalar(f(&a)?)),
        }
    }
}

/// A literal as a one-row array.
fn literal_array(v: &ScalarValue) -> ArrayRef {
    match v {
        ScalarValue::Null => Arc::new(NullArray::new(1)),
        ScalarValue::Boolean(b) => Arc::new(BooleanArray::from(vec![*b])),
        ScalarValue::Int64(x) => Arc::new(Int64Array::from(vec![*x])),
        ScalarValue::Float64(x) => Arc::new(Float64Array::from(vec![*x])),
        ScalarValue::Utf8(s) => Arc::new(StringArray::from(vec![s.as_str()])),
        ScalarValue::Date32(d) => Arc::new(Date32Array::from(vec![*d])),
    }
}

/// The array as a `T`, or an error naming the type it has (the binder's types make this an engine bug, never a
/// panic).
fn downcast<T: 'static>(a: &ArrayRef) -> Result<&T, ArrowError> {
    a.as_any().downcast_ref::<T>().ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "expected {}, got {}",
            std::any::type_name::<T>(),
            a.data_type()
        ))
    })
}

/// Float values as SQL compares them: -0.0 is 0.0, and every NaN is one NaN. (Arrow's kernels order floats by IEEE
/// 754's total order, which tells those apart.) NaN stays greater than every other number, as in DuckDB.
pub(crate) fn normalize_floats(a: &ArrayRef) -> ArrayRef {
    match a.data_type() {
        DataType::Float64 => Arc::new(
            a.as_primitive::<Float64Type>()
                .unary::<_, Float64Type>(|x| if x.is_nan() { f64::NAN } else { x + 0.0 }),
        ),
        DataType::Float32 => Arc::new(
            a.as_primitive::<Float32Type>()
                .unary::<_, Float32Type>(|x| if x.is_nan() { f32::NAN } else { x + 0.0 }),
        ),
        _ => a.clone(),
    }
}

/// The rows `rows` of a batch, as a batch (which may have no columns: the row count is kept).
pub(crate) fn take_rows(batch: &RecordBatch, rows: &[u32]) -> Result<RecordBatch, ArrowError> {
    let indices = UInt32Array::from(rows.to_vec());
    let columns = batch
        .columns()
        .iter()
        .map(|c| take(c, &indices, None))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows.len())),
    )
}

/// A column of `rows` values: `values[j]` at row `at[j]`, NULL elsewhere.
fn scatter(rows: usize, at: &[u32], values: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let nulls = new_null_array(values.data_type(), 1);
    let mut picks = vec![(1, 0); rows];
    for (j, &row) in at.iter().enumerate() {
        picks[row as usize] = (0, j);
    }
    interleave(&[values.as_ref(), nulls.as_ref()], &picks)
}

/// The rows of a boolean column that are TRUE (not FALSE, not NULL), and the rest.
fn split_true(cond: &ArrayRef, rows: &[u32]) -> Result<(Vec<u32>, Vec<u32>), ArrowError> {
    if *cond.data_type() == DataType::Null {
        return Ok((Vec::new(), rows.to_vec()));
    }
    let b = downcast::<BooleanArray>(cond)?;
    let (mut hit, mut miss) = (Vec::new(), Vec::new());
    for (i, &row) in rows.iter().enumerate() {
        if b.is_valid(i) && b.value(i) {
            hit.push(row);
        } else {
            miss.push(row);
        }
    }
    Ok((hit, miss))
}

/// Cast options that fail on a value that doesn't convert, instead of making it NULL: the plan's nullability
/// assumes a cast never turns a value into NULL, and DuckDB's CAST fails too.
fn strict() -> CastOptions<'static> {
    CastOptions {
        safe: false,
        ..Default::default()
    }
}

/// `CAST(v AS to)`. A float cast to an integer rounds half to even first, as DuckDB does (Arrow truncates).
fn cast_value(v: Value, to: &DataType) -> Result<Value, ArrowError> {
    if v.data_type() == to {
        return Ok(v);
    }
    v.map(|a| {
        let a: ArrayRef = match a.data_type() {
            DataType::Float64 if is_integer(to) => Arc::new(
                a.as_primitive::<Float64Type>()
                    .unary::<_, Float64Type>(f64::round_ties_even),
            ),
            DataType::Float32 if is_integer(to) => Arc::new(
                a.as_primitive::<Float32Type>()
                    .unary::<_, Float32Type>(f32::round_ties_even),
            ),
            _ => a.clone(),
        };
        cast_with_options(&a, to, &strict())
    })
}

/// Two boolean values combined by three-valued `AND` or `OR`.
fn kleene(and: bool, l: Value, r: Value, rows: usize) -> Result<Value, ArrowError> {
    let scalar = l.is_scalar() && r.is_scalar();
    let n = if scalar { 1 } else { rows };
    let (l, r) = (l.into_array(n)?, r.into_array(n)?);
    let (l, r) = (downcast::<BooleanArray>(&l)?, downcast::<BooleanArray>(&r)?);
    let out = if and {
        boolean::and_kleene(l, r)?
    } else {
        boolean::or_kleene(l, r)?
    };
    Ok(Value::from_result(Arc::new(out), scalar))
}

/// A comparison: NULL when either side is the NULL type, floats normalized first.
fn compare(op: BinaryOp, l: Value, r: Value, rows: usize) -> Result<Value, ArrowError> {
    let scalar = l.is_scalar() && r.is_scalar();
    if *l.data_type() == DataType::Null || *r.data_type() == DataType::Null {
        let n = if scalar { 1 } else { rows };
        return Ok(Value::from_result(
            new_null_array(&DataType::Boolean, n),
            scalar,
        ));
    }
    let l = l.map(|a| Ok(normalize_floats(a)))?;
    let r = r.map(|a| Ok(normalize_floats(a)))?;
    let out = match op {
        BinaryOp::Eq => cmp::eq(&l, &r)?,
        BinaryOp::NotEq => cmp::neq(&l, &r)?,
        BinaryOp::Lt => cmp::lt(&l, &r)?,
        BinaryOp::LtEq => cmp::lt_eq(&l, &r)?,
        BinaryOp::Gt => cmp::gt(&l, &r)?,
        BinaryOp::GtEq => cmp::gt_eq(&l, &r)?,
        other => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "{other:?} is not a comparison"
            )));
        }
    };
    Ok(Value::from_result(Arc::new(out), scalar))
}

/// `a % b` on integers: an error on a zero divisor (as in DuckDB) and on `i64::MIN % -1` (which overflows in the
/// division a remainder comes from; DuckDB fails there too, Arrow's kernel returns 0).
fn modulo(l: &ArrayRef, r: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let out: Int64Array = arity::try_binary(
        downcast::<Int64Array>(l)?,
        downcast::<Int64Array>(r)?,
        |a, b| {
            if b == 0 {
                Err(ArrowError::DivideByZero)
            } else {
                a.checked_rem(b)
                    .ok_or_else(|| ArrowError::ArithmeticOverflow(format!("{a} % {b}")))
            }
        },
    )?;
    Ok(Arc::new(out))
}

/// A date plus or minus whole days, or the days between two dates.
fn date_arithmetic(op: BinaryOp, l: &Value, r: &Value) -> Result<ArrayRef, ArrowError> {
    let days = |v: &Value| -> Result<Value, ArrowError> {
        if *v.data_type() == DataType::Date32 {
            v.clone().map(|a| {
                cast_with_options(
                    &cast_with_options(a, &DataType::Int32, &strict())?,
                    &DataType::Int64,
                    &strict(),
                )
            })
        } else {
            Ok(v.clone())
        }
    };
    let (a, b) = (days(l)?, days(r)?);
    let out = if op == BinaryOp::Plus {
        numeric::add(&a, &b)?
    } else {
        numeric::sub(&a, &b)?
    };
    if *l.data_type() == DataType::Date32 && *r.data_type() == DataType::Date32 {
        return Ok(out);
    }
    let days = cast_with_options(&out, &DataType::Int32, &strict())
        .map_err(|_| ArrowError::ArithmeticOverflow("the date is out of range".to_string()))?;
    cast_with_options(&days, &DataType::Date32, &strict())
}

/// DuckDB's LIKE has no escape character and Arrow's treats `\` as one; doubling each backslash makes it literal.
fn literal_backslashes(pattern: Value) -> Result<Value, ArrowError> {
    pattern.map(|a| {
        let s = downcast::<StringArray>(a)?;
        if !s.iter().flatten().any(|p| p.contains('\\')) {
            return Ok(a.clone());
        }
        Ok(Arc::new(
            s.iter()
                .map(|p| p.map(|p| p.replace('\\', r"\\")))
                .collect::<StringArray>(),
        ))
    })
}

struct Evaluator<'a> {
    schema: &'a PlanSchema,
}

impl Evaluator<'_> {
    /// A kernel's failure, as an error naming the expression that failed.
    fn error(&self, expr: &Expr, e: ArrowError) -> QuarryError {
        let shown = expr.display(self.schema);
        match e {
            ArrowError::DivideByZero => exec_err!("division by zero in {shown}"),
            ArrowError::ArithmeticOverflow(msg) => exec_err!("overflow in {shown} ({msg})"),
            ArrowError::CastError(msg) | ArrowError::ParseError(msg) => {
                exec_err!("{msg} in {shown}")
            }
            other => QuarryError::Arrow(other),
        }
    }

    fn eval(&self, expr: &Expr, batch: &RecordBatch) -> Result<Value> {
        let fail = |e: ArrowError| self.error(expr, e);
        let rows = batch.num_rows();
        match expr {
            Expr::Column(i) => Ok(Value::Array(batch.column(*i).clone())),
            Expr::Literal(v) => Ok(Value::Scalar(literal_array(v))),
            Expr::Binary {
                op: op @ (BinaryOp::And | BinaryOp::Or),
                left,
                right,
            } => self.and_or(*op == BinaryOp::And, left, right, batch),
            Expr::Binary { op, left, right } => {
                let l = self.eval(left, batch)?;
                let r = self.eval(right, batch)?;
                self.binary(*op, l, r, rows).map_err(fail)
            }
            Expr::Not(e) => self
                .eval(e, batch)?
                .map(|a| Ok(Arc::new(boolean::not(downcast::<BooleanArray>(a)?)?)))
                .map_err(fail),
            Expr::Negative(e) => self.eval(e, batch)?.map(|a| numeric::neg(a)).map_err(fail),
            Expr::IsNull(e) => self
                .eval(e, batch)?
                .map(|a| Ok(Arc::new(boolean::is_null(a)?)))
                .map_err(fail),
            Expr::IsNotNull(e) => self
                .eval(e, batch)?
                .map(|a| Ok(Arc::new(boolean::is_not_null(a)?)))
                .map_err(fail),
            Expr::Like {
                expr: value,
                pattern,
                negated,
                case_insensitive,
            } => {
                let v = self.eval(value, batch)?;
                let p = literal_backslashes(self.eval(pattern, batch)?).map_err(fail)?;
                let scalar = v.is_scalar() && p.is_scalar();
                let out = match (negated, case_insensitive) {
                    (false, false) => comparison::like(&v, &p),
                    (true, false) => comparison::nlike(&v, &p),
                    (false, true) => comparison::ilike(&v, &p),
                    (true, true) => comparison::nilike(&v, &p),
                }
                .map_err(fail)?;
                Ok(Value::from_result(Arc::new(out), scalar))
            }
            Expr::InList {
                expr: value,
                list,
                negated,
            } => {
                // x IN (a, b) is x = a OR x = b, in three-valued logic: TRUE on a match, else NULL if anything was
                // NULL, else FALSE.
                let v = self.eval(value, batch)?;
                let mut found = Value::Scalar(literal_array(&ScalarValue::Boolean(false)));
                for item in list {
                    let item = self.eval(item, batch)?;
                    let eq = compare(BinaryOp::Eq, v.clone(), item, rows).map_err(fail)?;
                    found = kleene(false, found, eq, rows).map_err(fail)?;
                }
                if *negated {
                    found
                        .map(|a| Ok(Arc::new(boolean::not(downcast::<BooleanArray>(a)?)?)))
                        .map_err(fail)
                } else {
                    Ok(found)
                }
            }
            Expr::Case {
                when_then,
                else_expr,
            } => self.case(when_then, else_expr, batch),
            Expr::Cast {
                expr: inner,
                data_type,
            } => cast_value(self.eval(inner, batch)?, data_type).map_err(fail),
            Expr::Aggregate { .. } => Err(exec_err!(
                "an aggregate is only computed by an Aggregate node: {}",
                expr.display(self.schema)
            )),
        }
    }

    /// The arithmetic operators and `||` (comparisons too, for completeness).
    fn binary(&self, op: BinaryOp, l: Value, r: Value, rows: usize) -> Result<Value, ArrowError> {
        if op.is_comparison() {
            return compare(op, l, r, rows);
        }
        let scalar = l.is_scalar() && r.is_scalar();
        let n = if scalar { 1 } else { rows };
        let dates = *l.data_type() == DataType::Date32 || *r.data_type() == DataType::Date32;
        let out = match op {
            BinaryOp::Plus | BinaryOp::Minus if dates => date_arithmetic(op, &l, &r)?,
            BinaryOp::Plus => numeric::add(&l, &r)?,
            BinaryOp::Minus => numeric::sub(&l, &r)?,
            BinaryOp::Multiply => numeric::mul(&l, &r)?,
            // Both sides are Float64 (the binder casts them), so x / 0 is ±inf and 0 / 0 is NaN, as in DuckDB.
            BinaryOp::Divide => numeric::div(&l, &r)?,
            BinaryOp::Modulo => modulo(&l.into_array(n)?, &r.into_array(n)?)?,
            BinaryOp::Concat => {
                let (l, r) = (l.into_array(n)?, r.into_array(n)?);
                Arc::new(concat_elements::concat_elements_utf8(
                    downcast::<StringArray>(&l)?,
                    downcast::<StringArray>(&r)?,
                )?)
            }
            other => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "{other:?} is not an arithmetic operator"
                )));
            }
        };
        Ok(Value::from_result(out, scalar))
    }

    /// `AND` (`and`) or `OR`: the right side is evaluated only on the rows the left doesn't decide (FALSE decides an
    /// AND, TRUE an OR).
    fn and_or(&self, and: bool, left: &Expr, right: &Expr, batch: &RecordBatch) -> Result<Value> {
        let fail = |e: ArrowError| self.error(left, e);
        let rows = batch.num_rows();
        let decisive = !and;
        let l = self.eval(left, batch)?;
        let b = downcast::<BooleanArray>(l.values()).map_err(fail)?;
        if l.is_scalar() {
            if b.is_valid(0) && b.value(0) == decisive {
                return Ok(l);
            }
            let r = self.eval(right, batch)?;
            return kleene(and, l, r, rows).map_err(fail);
        }
        let undecided: Vec<u32> = (0..rows)
            .filter(|&i| !(b.is_valid(i) && b.value(i) == decisive))
            .map(|i| i as u32)
            .collect();
        if undecided.is_empty() {
            return Ok(l);
        }
        let r = if undecided.len() == rows {
            self.eval(right, batch)?
        } else {
            let sub = take_rows(batch, &undecided).map_err(fail)?;
            let values = self
                .eval(right, &sub)?
                .into_array(undecided.len())
                .map_err(fail)?;
            Value::Array(scatter(rows, &undecided, &values).map_err(fail)?)
        };
        kleene(and, l, r, rows).map_err(fail)
    }

    /// The rows `rows` of the batch: the batch itself when that is all of them.
    fn subset(&self, batch: &RecordBatch, rows: &[u32]) -> Result<RecordBatch, ArrowError> {
        if rows.len() == batch.num_rows() {
            Ok(batch.clone())
        } else {
            take_rows(batch, rows)
        }
    }

    /// A searched CASE: each WHEN is evaluated on the rows no earlier WHEN took, each THEN only on its own rows.
    fn case(
        &self,
        when_then: &[(Expr, Expr)],
        else_expr: &Expr,
        batch: &RecordBatch,
    ) -> Result<Value> {
        let rows = batch.num_rows();
        let fail = |e: ArrowError| self.error(else_expr, e);
        if rows == 0 {
            return Ok(Value::Array(new_empty_array(
                &else_expr.data_type(self.schema),
            )));
        }
        let mut remaining: Vec<u32> = (0..rows as u32).collect();
        // Each branch's values for its rows, and for each row of the result, (branch, position in its values).
        let mut pieces: Vec<ArrayRef> = Vec::new();
        let mut picks = vec![(0, 0); rows];
        let mut record = |at: &[u32], values: ArrayRef| {
            for (j, &row) in at.iter().enumerate() {
                picks[row as usize] = (pieces.len(), j);
            }
            pieces.push(values);
        };
        for (when, then) in when_then {
            if remaining.is_empty() {
                break;
            }
            let sub = self.subset(batch, &remaining).map_err(fail)?;
            let cond = self
                .eval(when, &sub)?
                .into_array(remaining.len())
                .map_err(fail)?;
            let (hit, miss) = split_true(&cond, &remaining).map_err(fail)?;
            if !hit.is_empty() {
                let sub = self.subset(batch, &hit).map_err(fail)?;
                let values = self.eval(then, &sub)?.into_array(hit.len()).map_err(fail)?;
                record(&hit, values);
            }
            remaining = miss;
        }
        if !remaining.is_empty() {
            let sub = self.subset(batch, &remaining).map_err(fail)?;
            let values = self
                .eval(else_expr, &sub)?
                .into_array(remaining.len())
                .map_err(fail)?;
            record(&remaining, values);
        }
        if let [only] = pieces.as_slice() {
            return Ok(Value::Array(only.clone()));
        }
        let sources: Vec<&dyn Array> = pieces.iter().map(|p| p.as_ref()).collect();
        Ok(Value::Array(interleave(&sources, &picks).map_err(fail)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Catalog;
    use crate::datasource::MemTable;
    use crate::plan::LogicalPlan;
    use arrow::array::Float32Array;
    use arrow::datatypes::{Field, Schema};
    use arrow::util::display::array_value_to_string;

    /// t(a BIGINT, b BIGINT, f DOUBLE, s VARCHAR, d DATE), four rows.
    fn batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("f", DataType::Float64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("d", DataType::Date32, true),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(7),
                    Some(-7),
                    None,
                    Some(i64::MAX),
                ])),
                Arc::new(Int64Array::from(vec![Some(2), Some(0), Some(3), Some(1)])),
                Arc::new(Float64Array::from(vec![
                    Some(-0.0),
                    Some(f64::NAN),
                    Some(2.5),
                    None,
                ])),
                Arc::new(StringArray::from(vec![
                    Some(r"a\b"),
                    Some("x_y"),
                    None,
                    Some("ÄB"),
                ])),
                Arc::new(Date32Array::from(vec![
                    Some(19_753), // 2024-01-31
                    Some(0),
                    None,
                    Some(19_782), // 2024-02-29
                ])),
            ],
        )
        .unwrap()
    }

    /// Binds `SELECT <sql> FROM t` and evaluates the one expression over the batch.
    fn eval(sql: &str) -> Result<ArrayRef> {
        let b = batch();
        let mut catalog = Catalog::new();
        catalog
            .register("t", Arc::new(MemTable::from_batch(b.clone())))
            .unwrap();
        let plan = crate::sql::plan(&catalog, &format!("SELECT {sql} FROM t")).unwrap();
        let LogicalPlan::Projection(p) = plan else {
            panic!("expected a projection: {plan}")
        };
        evaluate(&p.exprs[0], p.input.schema(), &b)
    }

    /// The values as strings, NULL for nulls (logical ones: a `NullArray` has no null buffer).
    fn show(sql: &str) -> Vec<String> {
        let a = eval(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        let nulls = a.logical_nulls();
        (0..a.len())
            .map(|i| {
                if nulls.as_ref().is_some_and(|n| n.is_null(i)) {
                    "NULL".to_string()
                } else {
                    array_value_to_string(&a, i).unwrap()
                }
            })
            .collect()
    }

    fn error(sql: &str) -> String {
        match eval(sql) {
            Ok(a) => panic!("{sql} gave {a:?}, expected an error"),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn columns_and_literals() {
        assert_eq!(show("a"), ["7", "-7", "NULL", "9223372036854775807"]);
        assert_eq!(show("1"), ["1", "1", "1", "1"]);
        assert_eq!(show("NULL"), ["NULL", "NULL", "NULL", "NULL"]);
        assert_eq!(show("1 + 2 * 3"), ["7", "7", "7", "7"]);
    }

    // The expected values below are DuckDB 2.0.0's for the same expressions (checked by hand on day 2).

    #[test]
    fn integer_arithmetic_fails_on_overflow() {
        assert_eq!(show("a - b"), ["5", "-7", "NULL", "9223372036854775806"]);
        assert_eq!(show("-a"), ["-7", "7", "NULL", "-9223372036854775807"]);
        let e = error("a + b");
        assert!(
            e.starts_with("execution error: overflow in t.a + t.b"),
            "{e}"
        );
        let e = error("a * 2");
        assert!(e.contains("overflow in t.a * 2"), "{e}");
        let e = error("-(a + 1)");
        assert!(e.contains("overflow in t.a + 1"), "{e}");
    }

    #[test]
    fn division_is_ieee_and_modulo_by_zero_fails() {
        assert_eq!(
            show("a / b"),
            ["3.5", "-inf", "NULL", "9.223372036854776e18"]
        );
        assert_eq!(show("0 / 0"), ["NaN", "NaN", "NaN", "NaN"]);
        assert_eq!(
            error("a % b"),
            "execution error: division by zero in t.a % t.b"
        );
        assert_eq!(show("a % 3"), ["1", "-1", "NULL", "1"]);
        let e = error("(-9223372036854775808) % -1");
        assert!(e.contains("overflow"), "{e}");
    }

    #[test]
    fn nulls_hide_the_values_under_them() {
        // Row 2's `a` is NULL over a zero in the value buffer; the modulo must not look at it.
        assert_eq!(show("b % a"), ["2", "0", "NULL", "1"]);
        assert_eq!(show("a % CAST(NULL AS BIGINT)"), ["NULL"; 4]);
    }

    #[test]
    fn and_or_skip_the_rows_the_left_side_decides() {
        assert_eq!(
            show("b <> 0 AND a % b = 1"),
            ["true", "false", "NULL", "false"]
        );
        assert_eq!(
            show("b = 0 OR a % b = 1"),
            ["true", "true", "NULL", "false"]
        );
        // A scalar left side that decides: the right side is never evaluated.
        assert_eq!(show("FALSE AND a % 0 = 1"), ["false"; 4]);
        assert_eq!(show("TRUE OR a % 0 = 1"), ["true"; 4]);
        // Three-valued logic.
        assert_eq!(show("NULL AND FALSE"), ["false"; 4]);
        assert_eq!(show("NULL OR FALSE"), ["NULL"; 4]);
        assert_eq!(show("NULL OR TRUE"), ["true"; 4]);
        assert_eq!(show("a > 0 AND NULL"), ["NULL", "false", "NULL", "NULL"]);
        let e = error("b = 0 AND a % b = 1");
        assert!(e.contains("division by zero"), "{e}");
    }

    #[test]
    fn case_runs_each_branch_on_its_own_rows() {
        assert_eq!(
            show("CASE WHEN b = 0 THEN 0 ELSE a % b END"),
            ["1", "0", "NULL", "0"]
        );
        assert_eq!(
            show("CASE WHEN a > 0 THEN 'pos' WHEN a < 0 THEN 'neg' END"),
            ["pos", "neg", "NULL", "pos"]
        );
        assert_eq!(
            show("CASE b WHEN 2 THEN 'two' ELSE 'other' END"),
            ["two", "other", "other", "other"]
        );
        assert_eq!(show("CASE WHEN NULL THEN 1 ELSE 2 END"), ["2"; 4]);
        // A later WHEN sees only the rows the earlier ones didn't take: row 1's b is 0.
        assert_eq!(
            show("CASE WHEN b = 0 THEN 'zero' WHEN a % b = 1 THEN 'odd' END"),
            ["odd", "zero", "NULL", "NULL"]
        );
        // A NULL condition isn't TRUE, whatever value bit lies under the NULL (row 2's a is NULL over a 0, so
        // `a < 1` has its value bit set).
        assert_eq!(
            show("CASE WHEN a < 1 THEN 'lt' ELSE 'other' END"),
            ["other", "lt", "other", "other"]
        );
        // A cast that fails on every non-NULL string runs only where its branch is taken.
        assert_eq!(
            show("CASE WHEN s IS NULL THEN CAST(s AS BIGINT) ELSE -1 END"),
            ["-1", "-1", "NULL", "-1"]
        );
        assert!(error("CAST(s AS BIGINT)").contains("in CAST(t.s AS Int64)"));
    }

    #[test]
    fn comparisons_treat_negative_zero_and_nan_as_duckdb_does() {
        assert_eq!(show("f = 0.0"), ["true", "false", "false", "NULL"]);
        assert_eq!(show("f = 0 / 0"), ["false", "true", "false", "NULL"]);
        assert_eq!(show("f > 1e308"), ["false", "true", "false", "NULL"]);
        assert_eq!(show("a < 0"), ["false", "true", "NULL", "false"]);
        assert_eq!(show("NULL = NULL"), ["NULL"; 4]);
        assert_eq!(show("d >= '2024-01-01'"), ["true", "false", "NULL", "true"]);
        // Strings compare by their UTF-8 bytes, as in DuckDB: 'Ä' (0xC3 0x84) is after 'b'.
        assert_eq!(show("s < 'b'"), ["true", "false", "NULL", "false"]);
    }

    #[test]
    fn in_lists_use_three_valued_logic() {
        assert_eq!(show("a IN (7, 9)"), ["true", "false", "NULL", "false"]);
        assert_eq!(show("a IN (7, NULL)"), ["true", "NULL", "NULL", "NULL"]);
        assert_eq!(
            show("a NOT IN (7, NULL)"),
            ["false", "NULL", "NULL", "NULL"]
        );
        assert_eq!(show("f IN (0.0)"), ["true", "false", "false", "NULL"]);
    }

    #[test]
    fn like_has_no_escape_character() {
        assert_eq!(show(r"s LIKE 'a\b'"), ["true", "false", "NULL", "false"]);
        assert_eq!(show(r"s LIKE 'x\_y'"), ["false", "false", "NULL", "false"]);
        assert_eq!(show("s LIKE 'x_y'"), ["false", "true", "NULL", "false"]);
        assert_eq!(show("s NOT LIKE '%b'"), ["false", "true", "NULL", "true"]);
        assert_eq!(show("s ILIKE 'äb'"), ["false", "false", "NULL", "true"]);
        assert_eq!(show("s LIKE 'äb'"), ["false", "false", "NULL", "false"]);
    }

    #[test]
    fn casts_round_floats_and_fail_instead_of_making_nulls() {
        // Half to even, as DuckDB's DOUBLE to BIGINT: 2.5 -> 2, 3.5 -> 4, -2.5 -> -2, 2.7 -> 3.
        assert_eq!(show("CAST(2.5 AS BIGINT)"), ["2"; 4]);
        assert_eq!(show("CAST(3.5 AS BIGINT)"), ["4"; 4]);
        assert_eq!(show("CAST(-2.5 AS BIGINT)"), ["-2"; 4]);
        assert_eq!(show("CAST(2.7 AS INT)"), ["3"; 4]);
        assert_eq!(show("CAST(CAST(-2.5 AS REAL) AS SMALLINT)"), ["-2"; 4]);
        assert_eq!(show("CAST(CAST(2.7 AS REAL) AS INT)"), ["3"; 4]);
        assert_eq!(show("CAST(CAST(3.5 AS REAL) AS BIGINT)"), ["4"; 4]);
        assert_eq!(
            show("CAST(a AS VARCHAR)"),
            ["7", "-7", "NULL", "9223372036854775807"]
        );
        let e = error("CAST(a AS INT)");
        assert!(e.contains("in CAST(t.a AS Int32)"), "{e}");
        let e = error("CAST(f AS BIGINT)");
        assert!(e.contains("in CAST(t.f AS Int64)"), "NaN doesn't cast: {e}");
        let e = error("CAST(1e19 AS BIGINT)");
        assert!(e.contains("in CAST(1e19 AS Int64)"), "{e}");
    }

    #[test]
    fn dates_add_days_and_subtract_to_days() {
        assert_eq!(
            show("d + 1"),
            ["2024-02-01", "1970-01-02", "NULL", "2024-03-01"]
        );
        assert_eq!(
            show("1 + d"),
            ["2024-02-01", "1970-01-02", "NULL", "2024-03-01"]
        );
        assert_eq!(
            show("d - 1"),
            ["2024-01-30", "1969-12-31", "NULL", "2024-02-28"]
        );
        assert_eq!(
            show("d - DATE '2024-01-01'"),
            ["30", "-19723", "NULL", "59"]
        );
        let e = error("d + 2147483647");
        assert!(e.contains("the date is out of range"), "{e}");
    }

    #[test]
    fn strings_concatenate_and_null_propagates() {
        assert_eq!(show("s || '!'"), [r"a\b!", "x_y!", "NULL", "ÄB!"]);
        assert_eq!(show("'a' || NULL"), ["NULL"; 4]);
    }

    #[test]
    fn is_null_and_not() {
        assert_eq!(show("a IS NULL"), ["false", "false", "true", "false"]);
        assert_eq!(show("NULL IS NOT NULL"), ["false"; 4]);
        assert_eq!(show("NOT (a > 0)"), ["false", "true", "NULL", "false"]);
    }

    #[test]
    fn float32_normalizes_too() {
        let a: ArrayRef = Arc::new(Float32Array::from(vec![-0.0f32, f32::NAN]));
        let n = normalize_floats(&a);
        let n = n.as_primitive::<Float32Type>();
        assert_eq!(n.value(0).to_bits(), 0.0f32.to_bits());
        assert!(n.value(1).is_nan());
    }
}
