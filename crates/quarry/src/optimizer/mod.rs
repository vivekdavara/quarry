//! The optimizer: rules that rewrite a bound logical plan into one that gives the same rows with less work (DESIGN.md,
//! "The optimizer").
//!
//! The rules run once each, in this order:
//!
//! 1. constant folding (`fold.rs`): constant subexpressions are computed once; a filter that is always TRUE goes away, one
//!    that is never TRUE makes no rows.
//! 2. predicate pushdown (`predicates.rs`): conditions move below joins to the side whose columns they read, and the
//!    conditions that read both sides move into the join, so a comma join with an equality is a hash join.
//! 3. projection pushdown (`columns.rs`): each scan reads only the columns used above it. It runs last because it
//!    renumbers every column above a scan.
//!
//! Each rule keeps the plan's output columns (names, types, order), and the tests run every query with and without
//! the optimizer and require the same rows.

mod columns;
mod fold;
mod predicates;

use crate::error::Result;
use crate::plan::LogicalPlan;

/// The plan after every rule.
pub fn optimize(plan: &LogicalPlan) -> Result<LogicalPlan> {
    let plan = fold::fold_plan(plan);
    let plan = predicates::push_predicates(&plan)?;
    columns::prune_columns(&plan)
}
