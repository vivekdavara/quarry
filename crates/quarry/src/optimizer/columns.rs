//! Projection pushdown: each scan reads only the columns something above it uses.
//!
//! The rule walks down from the root with the set of a node's output columns its parent needs. Projections and
//! aggregates need the columns their expressions read; filters, sorts and joins add the columns of their own
//! expressions to what passes through them; a scan reads what reaches it. Since bound expressions refer to columns by
//! position, every node above a narrowed one has its expressions renumbered. Projections and aggregates keep every
//! output (so the result's columns never change), and an unused projection item is still computed; only scans drop
//! columns.

use crate::error::Result;
use crate::plan::{Expr, Filter, LogicalPlan, PlanSchema, Projection, Scan, Sort, SortKey};

/// The plan with every scan narrowed to the columns used above it. The output columns are the same.
pub(super) fn prune_columns(plan: &LogicalPlan) -> Result<LogicalPlan> {
    let all: Vec<usize> = (0..plan.schema().len()).collect();
    let (pruned, map) = prune(plan, &all)?;
    debug_assert!(map.iter().enumerate().all(|(i, &m)| m == Some(i)));
    Ok(pruned)
}

/// Where each of a node's old output columns is in its new output (`None`: no longer there).
type ColumnMap = Vec<Option<usize>>;

fn remap(expr: &Expr, map: &ColumnMap) -> Expr {
    expr.map_columns(&|i| map[i].expect("a column a parent needs is kept"))
}

/// The columns, ascending and each once, of `required` and the expressions'.
fn with_columns<'a>(required: &[usize], exprs: impl IntoIterator<Item = &'a Expr>) -> Vec<usize> {
    let mut all = required.to_vec();
    for e in exprs {
        all.extend(e.columns());
    }
    all.sort_unstable();
    all.dedup();
    all
}

/// Rewrites `plan` so its output has at least the columns `required` (ascending) and returns where each old output
/// column went.
fn prune(plan: &LogicalPlan, required: &[usize]) -> Result<(LogicalPlan, ColumnMap)> {
    match plan {
        LogicalPlan::Scan(s) => {
            let table: Vec<usize> = match &s.projection {
                Some(p) => p.clone(),
                None => (0..s.schema.len()).collect(),
            };
            let mut map = vec![None; s.schema.len()];
            for (new, &old) in required.iter().enumerate() {
                map[old] = Some(new);
            }
            let schema = PlanSchema::new(
                required
                    .iter()
                    .map(|&i| s.schema.field(i).clone())
                    .collect(),
            );
            let scan = Scan {
                projection: Some(required.iter().map(|&i| table[i]).collect()),
                schema: schema.into(),
                ..s.clone()
            };
            Ok((LogicalPlan::Scan(scan), map))
        }
        LogicalPlan::Empty(_) => Ok((plan.clone(), identity(plan))),
        LogicalPlan::Filter(f) => {
            let needed = with_columns(required, [&f.predicate]);
            let (input, map) = prune(&f.input, &needed)?;
            let predicate = remap(&f.predicate, &map);
            Ok((
                LogicalPlan::Filter(Filter {
                    input: input.into(),
                    predicate,
                }),
                map,
            ))
        }
        LogicalPlan::Sort(s) => {
            let needed = with_columns(required, s.keys.iter().map(|k| &k.expr));
            let (input, map) = prune(&s.input, &needed)?;
            let keys = s
                .keys
                .iter()
                .map(|k| SortKey {
                    expr: remap(&k.expr, &map),
                    ..k.clone()
                })
                .collect();
            Ok((
                LogicalPlan::Sort(Sort {
                    input: input.into(),
                    keys,
                }),
                map,
            ))
        }
        LogicalPlan::Limit(_) => {
            let (input, map) = prune(plan.inputs()[0], required)?;
            Ok((plan.with_new_inputs(vec![input]), map))
        }
        LogicalPlan::Projection(p) => {
            let needed = with_columns(&[], &p.exprs);
            let (input, map) = prune(&p.input, &needed)?;
            let projection = Projection {
                input: input.into(),
                exprs: p.exprs.iter().map(|e| remap(e, &map)).collect(),
                schema: p.schema.clone(),
            };
            Ok((LogicalPlan::Projection(projection), identity(plan)))
        }
        LogicalPlan::Aggregate(a) => {
            let needed = with_columns(&[], a.group_by.iter().chain(&a.aggregates));
            let (input, map) = prune(&a.input, &needed)?;
            let aggregate = LogicalPlan::Aggregate(crate::plan::Aggregate {
                input: input.into(),
                group_by: a.group_by.iter().map(|e| remap(e, &map)).collect(),
                aggregates: a.aggregates.iter().map(|e| remap(e, &map)).collect(),
                schema: a.schema.clone(),
            });
            Ok((aggregate, identity(plan)))
        }
        LogicalPlan::Join(j) => {
            let width = j.left.schema().len();
            let needed = with_columns(required, &j.on);
            let (left_needed, right_needed): (Vec<usize>, Vec<usize>) =
                needed.iter().partition(|&&i| i < width);
            let right_needed: Vec<usize> = right_needed.iter().map(|i| i - width).collect();
            let (left, left_map) = prune(&j.left, &left_needed)?;
            let (right, right_map) = prune(&j.right, &right_needed)?;
            let left_width = left.schema().len();
            let map: ColumnMap = left_map
                .iter()
                .copied()
                .chain(right_map.iter().map(|m| m.map(|i| i + left_width)))
                .collect();
            let on = j.on.as_ref().map(|on| remap(on, &map));
            Ok((LogicalPlan::join(left, right, j.kind, on)?, map))
        }
    }
}

fn identity(plan: &LogicalPlan) -> ColumnMap {
    (0..plan.schema().len()).map(Some).collect()
}
