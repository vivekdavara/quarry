//! Constant folding: a subexpression that reads no column is computed once, when the query is planned, and replaced
//! by its value. A filter that folds to TRUE goes away; one that folds to FALSE or NULL makes no rows, and its input
//! isn't read.
//!
//! A constant that fails (`1 % 0`, `CAST('x' AS INT)`) is left as it is: computed per batch, it fails only when a row
//! reaches it, as it would without the rule (`SELECT 1 % 0 FROM t WHERE FALSE` is no rows, not an error; so is
//! `CASE WHEN x > 0 THEN 1 ELSE 1 % 0 END` where every `x` is positive). Its operands are still folded.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, RecordBatch, RecordBatchOptions};
use arrow::datatypes::{DataType, Date32Type, Float64Type, Int64Type, Schema};

use crate::exec::evaluate;
use crate::plan::{
    Aggregate, BinaryOp, Empty, Expr, Filter, Join, LogicalPlan, PlanSchema, Projection,
    ScalarValue, Sort, SortKey,
};

pub(super) fn fold_plan(plan: &LogicalPlan) -> LogicalPlan {
    let inputs: Vec<LogicalPlan> = plan.inputs().into_iter().map(fold_plan).collect();
    let with_inputs = plan.with_new_inputs(inputs);
    match &with_inputs {
        LogicalPlan::Scan(_) | LogicalPlan::Empty(_) | LogicalPlan::Limit(_) => with_inputs,
        LogicalPlan::Filter(f) => {
            let predicate = fold(&f.predicate, f.input.schema());
            match truth(&predicate) {
                Some(true) => (*f.input).clone(),
                Some(false) => LogicalPlan::Empty(Empty {
                    one_row: false,
                    schema: f.input.schema().clone(),
                }),
                None => LogicalPlan::Filter(Filter {
                    input: f.input.clone(),
                    predicate,
                }),
            }
        }
        LogicalPlan::Projection(p) => LogicalPlan::Projection(Projection {
            input: p.input.clone(),
            exprs: p.exprs.iter().map(|e| fold(e, p.input.schema())).collect(),
            schema: p.schema.clone(),
        }),
        LogicalPlan::Aggregate(a) => LogicalPlan::Aggregate(Aggregate {
            input: a.input.clone(),
            group_by: a
                .group_by
                .iter()
                .map(|e| fold(e, a.input.schema()))
                .collect(),
            aggregates: a
                .aggregates
                .iter()
                .map(|e| fold(e, a.input.schema()))
                .collect(),
            schema: a.schema.clone(),
        }),
        LogicalPlan::Join(j) => LogicalPlan::Join(Join {
            left: j.left.clone(),
            right: j.right.clone(),
            kind: j.kind,
            on: j.on.as_ref().map(|on| fold(on, &j.schema)),
            schema: j.schema.clone(),
        }),
        LogicalPlan::Sort(s) => LogicalPlan::Sort(Sort {
            input: s.input.clone(),
            keys: s
                .keys
                .iter()
                .map(|k| SortKey {
                    expr: fold(&k.expr, s.input.schema()),
                    ..k.clone()
                })
                .collect(),
        }),
    }
}

/// The expression with each constant subexpression that computes replaced by its value, largest first, and then
/// `AND`s and `OR`s with a constant operand simplified.
pub(crate) fn fold(expr: &Expr, schema: &PlanSchema) -> Expr {
    simplify(&expr.transform(&mut |e| fold_constant(e, schema)), schema)
}

/// `AND` and `OR` with a TRUE or FALSE operand, where the result is known without changing what is computed. `AND` and
/// `OR` are lazy (the right operand runs only on rows the left leaves undecided), so `FALSE AND x` is FALSE without
/// ever computing `x`, and `TRUE AND x` and `x AND TRUE` are `x` (computed on every row either way). `x AND FALSE`
/// stays: `x` is computed on every row, and may fail.
fn simplify(expr: &Expr, schema: &PlanSchema) -> Expr {
    expr.transform(&mut |e| match e {
        Expr::Binary {
            op: op @ (BinaryOp::And | BinaryOp::Or),
            left,
            right,
        } => {
            let (left, right) = (simplify(left, schema), simplify(right, schema));
            let boolean = |e: &Expr| e.data_type(schema) == DataType::Boolean;
            let constant = |e: &Expr| match e {
                Expr::Literal(ScalarValue::Boolean(b)) => Some(*b),
                _ => None,
            };
            // The value that decides the operator (FALSE for AND, TRUE for OR), and the one that doesn't.
            let decides = *op == BinaryOp::Or;
            Some(match (constant(&left), constant(&right)) {
                (Some(l), _) if l == decides => left,
                (Some(_), _) if boolean(&right) => right,
                (_, Some(r)) if r != decides && boolean(&left) => left,
                _ => Expr::binary(*op, left, right),
            })
        }
        _ => None,
    })
}

fn fold_constant(e: &Expr, schema: &PlanSchema) -> Option<Expr> {
    if is_literal(e) || e.contains_aggregate() || !e.columns().is_empty() {
        return None;
    }
    let one_row = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .ok()?;
    let value = evaluate(e, schema, &one_row).ok()?;
    let folded = literal(&value)?;
    // A value the literals can't hold the type of (an INT, a REAL) stays computed.
    (folded.data_type(schema) == e.data_type(schema)).then_some(folded)
}

/// A literal, or a typed NULL (`CAST(NULL AS type)`, as the binder writes one).
fn is_literal(e: &Expr) -> bool {
    match e {
        Expr::Literal(_) => true,
        Expr::Cast { expr, .. } => matches!(**expr, Expr::Literal(ScalarValue::Null)),
        _ => false,
    }
}

/// The one value of a one-row column as an expression, if a literal can hold it.
fn literal(value: &ArrayRef) -> Option<Expr> {
    if value.len() != 1 {
        return None;
    }
    let data_type = value.data_type();
    if *data_type == DataType::Null {
        return Some(Expr::Literal(ScalarValue::Null));
    }
    if value.is_null(0) {
        return Some(Expr::Literal(ScalarValue::Null).cast(data_type));
    }
    let scalar = match data_type {
        DataType::Boolean => ScalarValue::Boolean(value.as_boolean().value(0)),
        DataType::Int64 => ScalarValue::Int64(value.as_primitive::<Int64Type>().value(0)),
        DataType::Float64 => ScalarValue::Float64(value.as_primitive::<Float64Type>().value(0)),
        DataType::Date32 => ScalarValue::Date32(value.as_primitive::<Date32Type>().value(0)),
        DataType::Utf8 => ScalarValue::Utf8(value.as_string::<i32>().value(0).to_string()),
        _ => return None,
    };
    Some(Expr::Literal(scalar))
}

/// What a folded filter keeps: every row (TRUE), none (FALSE or NULL), or it depends on the row.
fn truth(predicate: &Expr) -> Option<bool> {
    match predicate {
        Expr::Literal(ScalarValue::Boolean(b)) => Some(*b),
        e if is_literal(e) => Some(false), // a NULL
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lit(v: i64) -> Expr {
        Expr::Literal(ScalarValue::Int64(v))
    }

    #[test]
    fn folds_constants_and_leaves_failing_ones_to_fail_when_a_row_needs_them() {
        let schema = PlanSchema::new(vec![crate::plan::PlanField::new(
            Some("t"),
            "x",
            DataType::Int64,
            true,
        )]);
        let sum = Expr::binary(BinaryOp::Plus, lit(1), lit(2));
        assert_eq!(fold(&sum, &schema), lit(3));
        // x + (1 + 2): the constant operand folds, the column doesn't.
        let with_column = Expr::binary(BinaryOp::Plus, Expr::Column(0), sum.clone());
        assert_eq!(
            fold(&with_column, &schema),
            Expr::binary(BinaryOp::Plus, Expr::Column(0), lit(3))
        );
        // (1 + 2) % (2 - 2) fails, so it stays, with its operands folded.
        let failing = Expr::binary(
            BinaryOp::Modulo,
            sum,
            Expr::binary(BinaryOp::Minus, lit(2), lit(2)),
        );
        assert_eq!(
            fold(&failing, &schema),
            Expr::binary(BinaryOp::Modulo, lit(3), lit(0))
        );
        // A NULL result keeps its type.
        let null_sum = Expr::binary(
            BinaryOp::Plus,
            lit(1),
            Expr::Literal(ScalarValue::Null).cast(&DataType::Int64),
        );
        assert_eq!(
            fold(&null_sum, &schema),
            Expr::Literal(ScalarValue::Null).cast(&DataType::Int64)
        );
        // A value no literal can hold (an INT) stays computed.
        let int32 = lit(7).cast(&DataType::Int32);
        assert_eq!(fold(&int32, &schema), int32);
    }
}
