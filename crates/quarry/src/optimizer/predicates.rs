//! Predicate pushdown: each condition moves down to the lowest place it can be computed, so rows are dropped before
//! a join pairs them, and a comma join with an equality in `WHERE` becomes a hash join.
//!
//! A filter over an inner or cross join joins its `ON` (for an inner join the two mean the same; a cross join with an
//! `ON` is an inner join). Then the `ON`'s conjuncts (the operands of its chain of `AND`s) go where their columns are:
//! one reading only the left input's columns becomes a filter on the left input, one reading only the right's a filter
//! on the right, and the rest stay in the `ON`. Over a `LEFT` join, a filter's conjuncts on the left go
//! down to the left (a left row the filter drops would have made only rows it drops), the rest stay; of its `ON`,
//! conjuncts on the right go down to the right (a build row they reject could never have matched), and conjuncts on
//! the left stay, since they decide only which left rows find a match. Conjuncts that go to the same place keep their
//! order.
//!
//! A condition that fails on some row (`5 % (c.rank - 4) = 1`) is then computed for rows the original order wouldn't
//! have reached: a key equality earlier in the `ON` no longer guards it. DuckDB 2.0 moves conditions the same way and
//! fails the same queries (checked by hand).

use std::sync::Arc;

use crate::error::Result;
use crate::plan::{Expr, Filter, Join, JoinKind, LogicalPlan};

pub(super) fn push_predicates(plan: &LogicalPlan) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Filter(f) => match f.input.as_ref() {
            LogicalPlan::Join(j) => {
                let (join, above) = filter_into_join(j, &f.predicate)?;
                let join = push_predicates(&join)?;
                Ok(filter(join, above))
            }
            LogicalPlan::Filter(inner) => {
                // A filter on a filter is one filter, the lower one's conjuncts first.
                let merged = Expr::and_all([inner.predicate.clone(), f.predicate.clone()])
                    .expect("two conjuncts");
                push_predicates(&filter((*inner.input).clone(), Some(merged)))
            }
            _ => {
                let input = push_predicates(&f.input)?;
                Ok(filter(input, Some(f.predicate.clone())))
            }
        },
        LogicalPlan::Join(j) => {
            let join = push_join_condition(j)?;
            let LogicalPlan::Join(j) = &join else {
                unreachable!("push_join_condition makes a join")
            };
            let left = push_predicates(&j.left)?;
            let right = push_predicates(&j.right)?;
            Ok(join.with_new_inputs(vec![left, right]))
        }
        other => {
            let inputs = other
                .inputs()
                .into_iter()
                .map(push_predicates)
                .collect::<Result<Vec<_>>>()?;
            Ok(other.with_new_inputs(inputs))
        }
    }
}

/// `input` under a filter of `predicate`, or `input` itself without one.
fn filter(input: LogicalPlan, predicate: Option<Expr>) -> LogicalPlan {
    match predicate {
        None => input,
        Some(predicate) => LogicalPlan::Filter(Filter {
            input: Arc::new(input),
            predicate,
        }),
    }
}

/// Where a conjunct over a join's columns can be computed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Place {
    Left,
    Right,
    Both,
}

fn place(conjunct: &Expr, left_width: usize) -> Place {
    let columns = conjunct.columns();
    match (columns.first(), columns.last()) {
        (Some(_), Some(&last)) if last < left_width => Place::Left,
        (Some(&first), Some(_)) if first >= left_width => Place::Right,
        _ => Place::Both, // both sides, or no column at all
    }
}

/// The conjuncts sorted into left, right and the rest, each in order; the right's renumbered to the right input.
fn split(predicate: Option<&Expr>, left_width: usize) -> (Vec<Expr>, Vec<Expr>, Vec<Expr>) {
    let (mut left, mut right, mut both) = (vec![], vec![], vec![]);
    for c in predicate.map(Expr::conjuncts).unwrap_or_default() {
        match place(c, left_width) {
            Place::Left => left.push(c.clone()),
            Place::Right => right.push(c.map_columns(&|i| i - left_width)),
            Place::Both => both.push(c.clone()),
        }
    }
    (left, right, both)
}

/// A filter's predicate over a join, moved into it where it can be. Returns the join and what stays above it.
fn filter_into_join(j: &Join, predicate: &Expr) -> Result<(LogicalPlan, Option<Expr>)> {
    let width = j.left.schema().len();
    match j.kind {
        JoinKind::Inner | JoinKind::Cross => {
            // For an inner join WHERE and ON are the same, so the whole predicate joins the ON, after ON's own
            // conjuncts; `push_join_condition` then moves each conjunct that reads one side only down to it.
            let on = Expr::and_all(
                j.on.iter()
                    .cloned()
                    .chain(predicate.conjuncts().into_iter().cloned()),
            );
            let join =
                LogicalPlan::join((*j.left).clone(), (*j.right).clone(), JoinKind::Inner, on)?;
            Ok((join, None))
        }
        JoinKind::Left => {
            // Only the left's conjuncts go down; the others stay above, in order.
            let (left, _, _) = split(Some(predicate), width);
            let above = Expr::and_all(
                predicate
                    .conjuncts()
                    .into_iter()
                    .filter(|c| place(c, width) != Place::Left)
                    .cloned(),
            );
            let join = LogicalPlan::join(
                filter((*j.left).clone(), Expr::and_all(left)),
                (*j.right).clone(),
                j.kind,
                j.on.clone(),
            )?;
            Ok((join, above))
        }
    }
}

/// A join's `ON` conjuncts that read one side only, moved down to that side where it is allowed.
fn push_join_condition(j: &Join) -> Result<LogicalPlan> {
    let width = j.left.schema().len();
    let (left, right, both) = split(j.on.as_ref(), width);
    let (left, on) = match j.kind {
        JoinKind::Inner | JoinKind::Cross => (left, Expr::and_all(both)),
        // A LEFT join's conditions on the left decide matching, so they stay, in their place among the others.
        JoinKind::Left => {
            let stay = j.on.as_ref().map(Expr::conjuncts).unwrap_or_default();
            let stay = stay
                .into_iter()
                .filter(|c| place(c, width) != Place::Right)
                .cloned();
            (vec![], Expr::and_all(stay))
        }
    };
    LogicalPlan::join(
        filter((*j.left).clone(), Expr::and_all(left)),
        filter((*j.right).clone(), Expr::and_all(right)),
        j.kind,
        on,
    )
}
