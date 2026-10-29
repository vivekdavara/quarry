//! Factoring an `OR`'s common conjuncts: `(a AND b) OR (a AND c)` is `a AND (b OR c)`, and `a OR (a AND b)` is `a`.
//! Both hold in SQL's three-valued logic (it is distributive, like two-valued logic), so they give the same rows. The
//! point is what comes next: a factored `a` is a conjunct of its own, which predicate pushdown can move below a join or
//! make a join key. TPC-H's Q19 writes its join key inside each of three `OR` branches; without this rule it is a
//! nested loop over every pair of rows.
//!
//! Only conjuncts that can't fail are factored: comparisons, `IN`, `IS [NOT] NULL`, `NOT`, `AND` and `OR` over columns
//! and literals. `AND` and `OR` are lazy, so in `(x <> 0 AND 10 % x = 1) OR (y AND 10 % x = 1)` the modulo never sees
//! a zero `x`; factored out, it would be computed on every row. A factored conjunct that can't fail computes the same
//! values on more rows, and everything left in the `OR` is computed on fewer, so the rewritten condition can fail only
//! where the original did.

use crate::plan::{BinaryOp, Expr, Filter, Join, LogicalPlan};

pub(super) fn factor_plan(plan: &LogicalPlan) -> LogicalPlan {
    let inputs: Vec<LogicalPlan> = plan.inputs().into_iter().map(factor_plan).collect();
    let plan = plan.with_new_inputs(inputs);
    match &plan {
        LogicalPlan::Filter(f) => LogicalPlan::Filter(Filter {
            input: f.input.clone(),
            predicate: factor(&f.predicate),
        }),
        LogicalPlan::Join(j) => LogicalPlan::Join(Join {
            on: j.on.as_ref().map(factor),
            ..j.clone()
        }),
        _ => plan,
    }
}

/// The condition with each of its conjuncts that is an `OR` factored.
fn factor(condition: &Expr) -> Expr {
    let conjuncts = condition.conjuncts().into_iter().flat_map(factor_or);
    Expr::and_all(conjuncts).expect("a condition has a conjunct")
}

/// An `OR` whose branches all have conjuncts in common that can't fail, as those conjuncts followed by the `OR` of
/// what is left of each branch (or by nothing, when some branch has nothing left); anything else as it is.
fn factor_or(conjunct: &Expr) -> Vec<Expr> {
    let branches: Vec<Vec<&Expr>> = conjunct
        .disjuncts()
        .into_iter()
        .map(Expr::conjuncts)
        .collect();
    let mut common: Vec<&Expr> = Vec::new();
    if branches.len() > 1 {
        for &c in &branches[0] {
            if cannot_fail(c)
                && !common.contains(&c)
                && branches[1..].iter().all(|b| b.contains(&c))
            {
                common.push(c);
            }
        }
    }
    if common.is_empty() {
        return vec![conjunct.clone()];
    }
    let rest: Option<Vec<Expr>> = branches
        .iter()
        .map(|b| {
            Expr::and_all(
                b.iter()
                    .filter(|c| !common.contains(c))
                    .map(|c| (*c).clone()),
            )
        })
        .collect();
    let mut out: Vec<Expr> = common.into_iter().cloned().collect();
    // A branch with nothing left is TRUE wherever the common conjuncts are, and so is the OR: `a OR (a AND b)` is `a`
    // (where `a` is NULL, so is `a AND b` or it is FALSE, and the OR is NULL either way).
    if let Some(rest) = rest {
        out.extend(
            rest.into_iter()
                .reduce(|all, b| Expr::binary(BinaryOp::Or, all, b)),
        );
    }
    out
}

/// Whether computing the expression can't fail on any row: comparisons and boolean operators over columns and
/// literals. Arithmetic (overflow, `%` by zero), casts and `LIKE` patterns are left out.
fn cannot_fail(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) | Expr::Literal(_) => true,
        Expr::Binary { op, left, right } => {
            (op.is_comparison() || matches!(op, BinaryOp::And | BinaryOp::Or))
                && cannot_fail(left)
                && cannot_fail(right)
        }
        Expr::Not(e) | Expr::IsNull(e) | Expr::IsNotNull(e) => cannot_fail(e),
        Expr::InList { expr, list, .. } => cannot_fail(expr) && list.iter().all(cannot_fail),
        _ => false,
    }
}
