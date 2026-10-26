//! Binding statements: a SELECT's clauses, in SQL's logical order, into a `LogicalPlan` (DESIGN.md, "Binding a
//! SELECT").

use sqlparser::ast::{
    self, Distinct, GroupByExpr, JoinConstraint, JoinOperator, LimitClause, OrderByKind,
    OrderBySort, Query, Select, SelectItem, SelectItemQualifiedWildcardKind, SetExpr, Statement,
    TableFactor, TableWithJoins, Value, WildcardAdditionalOptions,
};

use super::BoundStatement;
use super::expr::{Scope, bind_expr, has_column, same_name};
use crate::Catalog;
use crate::error::{Result, plan_err, unsupported};
use crate::plan::{Expr, JoinKind, LogicalPlan, PlanSchema, SortKey};

/// Binds statements against a catalog.
#[derive(Debug, Clone, Copy)]
pub struct Binder<'a> {
    catalog: &'a Catalog,
}

/// A select-list item, bound over the FROM scope (it may hold aggregates).
struct Item {
    expr: Expr,
    name: String,
}

/// An ORDER BY key before the projection exists: an output column, or an expression over the FROM scope.
enum OrderTarget {
    Output(usize),
    Expr(Expr),
}

impl<'a> Binder<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        Binder { catalog }
    }

    pub fn bind_statement(&self, statement: &Statement) -> Result<BoundStatement> {
        match statement {
            Statement::Query(query) => Ok(BoundStatement::Query(self.bind_query(query)?)),
            Statement::Explain {
                statement,
                analyze,
                verbose,
                ..
            } => {
                if *analyze || *verbose {
                    return Err(unsupported!("EXPLAIN ANALYZE and EXPLAIN VERBOSE"));
                }
                match statement.as_ref() {
                    Statement::Query(query) => Ok(BoundStatement::Explain(self.bind_query(query)?)),
                    _ => Err(unsupported!("EXPLAIN of anything but a query")),
                }
            }
            _ => Err(unsupported!(
                "statements other than SELECT and EXPLAIN SELECT"
            )),
        }
    }

    pub fn bind_query(&self, query: &Query) -> Result<LogicalPlan> {
        if query.with.is_some() {
            return Err(unsupported!("WITH"));
        }
        if query.fetch.is_some() {
            return Err(unsupported!("FETCH (use LIMIT)"));
        }
        if !query.locks.is_empty()
            || query.for_clause.is_some()
            || query.settings.is_some()
            || query.format_clause.is_some()
            || !query.pipe_operators.is_empty()
        {
            return Err(unsupported!("FOR, SETTINGS, FORMAT and pipe operators"));
        }
        let select = match query.body.as_ref() {
            SetExpr::Select(select) => select,
            SetExpr::SetOperation { op, .. } => return Err(unsupported!("{op}")),
            SetExpr::Values(_) => return Err(unsupported!("VALUES")),
            SetExpr::Query(_) => {
                return Err(unsupported!("a parenthesized query as the body of a query"));
            }
            _ => return Err(unsupported!("statements other than SELECT")),
        };
        let (skip, fetch) = bind_limit(query.limit_clause.as_ref())?;
        let order_by = match &query.order_by {
            None => &[][..],
            Some(order_by) => match &order_by.kind {
                OrderByKind::Expressions(exprs) => exprs.as_slice(),
                OrderByKind::All(_) => return Err(unsupported!("ORDER BY ALL")),
            },
        };
        if query
            .order_by
            .as_ref()
            .is_some_and(|o| o.interpolate.is_some())
        {
            return Err(unsupported!("INTERPOLATE"));
        }
        let plan = self.bind_select(select, order_by)?;
        Ok(if skip > 0 || fetch.is_some() {
            LogicalPlan::limit(plan, skip, fetch)
        } else {
            plan
        })
    }

    fn bind_select(&self, select: &Select, order_by: &[ast::OrderByExpr]) -> Result<LogicalPlan> {
        check_select_clauses(select)?;

        // 1. FROM, 2. WHERE.
        let mut plan = self.bind_from(&select.from)?;
        if let Some(selection) = &select.selection {
            let predicate =
                bind_expr(selection, Scope::without_aggregates(plan.schema(), "WHERE"))?;
            plan = LogicalPlan::filter(plan, predicate)?;
        }
        let from_schema = plan.schema().clone();
        let scope = Scope::new(&from_schema);

        // 5 (bound early, the other clauses refer to it). The select list over the FROM scope.
        let mut items = Vec::new();
        for item in &select.projection {
            bind_select_item(item, scope, &mut items)?;
        }
        // sqlparser accepts `SELECT FROM t` and `SELECT DISTINCT FROM t`; a result needs at least one column.
        if items.is_empty() {
            return Err(plan_err!("the select list is empty"));
        }
        let distinct = match &select.distinct {
            None | Some(Distinct::All) => false,
            Some(Distinct::Distinct) => true,
            Some(Distinct::On(_)) => return Err(unsupported!("DISTINCT ON")),
        };

        // 7 (classified early, its aggregates count for step 3). ORDER BY keys: output columns or expressions.
        let mut order_targets = Vec::with_capacity(order_by.len());
        for key in order_by {
            order_targets.push(bind_order_target(&key.expr, &items, scope)?);
        }

        // 3. Aggregation.
        let group_by_asts = match &select.group_by {
            GroupByExpr::Expressions(exprs, modifiers) if modifiers.is_empty() => exprs.as_slice(),
            GroupByExpr::Expressions(..) => {
                return Err(unsupported!("GROUP BY ... WITH ROLLUP/CUBE/TOTALS"));
            }
            GroupByExpr::All(_) => return Err(unsupported!("GROUP BY ALL")),
        };
        let having = match &select.having {
            Some(h) => Some(bind_expr(h, scope)?),
            None => None,
        };
        let aggregating = !group_by_asts.is_empty()
            || having.is_some()
            || items.iter().any(|i| i.expr.contains_aggregate())
            || order_targets
                .iter()
                .any(|t| matches!(t, OrderTarget::Expr(e) if e.contains_aggregate()));

        if aggregating {
            let mut group_by: Vec<Expr> = Vec::with_capacity(group_by_asts.len());
            for g in group_by_asts {
                let bound = bind_group_by(g, &items, &from_schema)?;
                if !group_by.contains(&bound) {
                    group_by.push(bound);
                }
            }
            // Each distinct aggregate once, in order of first appearance: select list, HAVING, ORDER BY.
            let mut aggregates: Vec<Expr> = Vec::new();
            let mut collect = |e: &Expr| {
                e.visit(&mut |sub| {
                    if matches!(sub, Expr::Aggregate { .. }) && !aggregates.contains(sub) {
                        aggregates.push(sub.clone());
                    }
                })
            };
            items.iter().for_each(|i| collect(&i.expr));
            having.iter().for_each(&mut collect);
            order_targets.iter().for_each(|t| {
                if let OrderTarget::Expr(e) = t {
                    collect(e)
                }
            });

            plan = LogicalPlan::aggregate(plan, group_by.clone(), aggregates.clone())?;
            let over_aggregate = |e: &Expr, clause: &str| {
                rewrite_over_aggregate(e, &group_by, &aggregates, &from_schema, clause)
            };
            for item in &mut items {
                item.expr = over_aggregate(&item.expr, "the select list")?;
            }
            for target in &mut order_targets {
                if let OrderTarget::Expr(e) = target {
                    *e = over_aggregate(e, "ORDER BY")?;
                }
            }
            // 4. HAVING.
            if let Some(having) = having {
                let predicate = over_aggregate(&having, "HAVING")?;
                plan = LogicalPlan::filter(plan, predicate)?;
            }
        }

        // 5. SELECT, with any ORDER BY expressions not in the select list as hidden columns after the visible ones.
        let visible = items.len();
        let mut exprs: Vec<(Expr, String)> = items.into_iter().map(|i| (i.expr, i.name)).collect();
        let mut sort_columns = Vec::with_capacity(order_targets.len());
        for target in order_targets {
            sort_columns.push(match target {
                OrderTarget::Output(k) => k,
                OrderTarget::Expr(e) => match exprs.iter().position(|(x, _)| *x == e) {
                    Some(k) => k,
                    None if distinct => {
                        return Err(plan_err!(
                            "for SELECT DISTINCT, ORDER BY expressions must appear in the select list: {}",
                            e.display(plan.schema())
                        ));
                    }
                    None => {
                        exprs.push((e, format!("__order_{}", exprs.len() - visible)));
                        exprs.len() - 1
                    }
                },
            });
        }
        let hidden = exprs.len() > visible;
        let names: Vec<String> = exprs[..visible].iter().map(|(_, n)| n.clone()).collect();
        plan = LogicalPlan::projection(plan, exprs);

        // 6. DISTINCT: group by every output column.
        if distinct {
            let columns = (0..visible).map(Expr::Column).collect();
            plan = LogicalPlan::aggregate(plan, columns, vec![])?;
        }

        // 7. ORDER BY.
        if !sort_columns.is_empty() {
            let keys = sort_columns
                .into_iter()
                .zip(order_by)
                .map(|(column, key)| sort_key(column, key))
                .collect::<Result<Vec<_>>>()?;
            plan = LogicalPlan::sort(plan, keys);
        }
        if hidden {
            let visible_columns = names
                .into_iter()
                .enumerate()
                .map(|(i, name)| (Expr::Column(i), name))
                .collect();
            plan = LogicalPlan::projection(plan, visible_columns);
        }
        Ok(plan)
    }

    /// FROM: a scan per table, joined left to right; no FROM is one empty row.
    fn bind_from(&self, from: &[TableWithJoins]) -> Result<LogicalPlan> {
        let mut qualifiers: Vec<String> = Vec::new();
        let mut plan: Option<LogicalPlan> = None;
        for table in from {
            let mut current = self.bind_table(&table.relation, &mut qualifiers)?;
            for join in &table.joins {
                if join.global {
                    return Err(unsupported!("GLOBAL JOIN"));
                }
                let right = self.bind_table(&join.relation, &mut qualifiers)?;
                let (kind, constraint) = match &join.join_operator {
                    JoinOperator::Join(c) | JoinOperator::Inner(c) => (JoinKind::Inner, c),
                    JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => (JoinKind::Left, c),
                    JoinOperator::CrossJoin(c) => (JoinKind::Cross, c),
                    other => return Err(unsupported!("the join {other:?}")),
                };
                current = match (kind, constraint) {
                    (JoinKind::Cross, JoinConstraint::None) => {
                        LogicalPlan::join(current, right, JoinKind::Cross, None)?
                    }
                    (JoinKind::Cross, _) => return Err(plan_err!("CROSS JOIN takes no ON")),
                    (_, JoinConstraint::On(on)) => {
                        let schema = current.schema().join(right.schema());
                        let on = bind_expr(on, Scope::without_aggregates(&schema, "JOIN ... ON"))?;
                        LogicalPlan::join(current, right, kind, Some(on))?
                    }
                    (_, JoinConstraint::Using(_)) => return Err(unsupported!("JOIN ... USING")),
                    (_, JoinConstraint::Natural) => return Err(unsupported!("NATURAL JOIN")),
                    (_, JoinConstraint::None) => {
                        return Err(plan_err!("JOIN needs an ON condition"));
                    }
                };
            }
            plan = Some(match plan {
                None => current,
                Some(left) => LogicalPlan::join(left, current, JoinKind::Cross, None)?,
            });
        }
        Ok(plan.unwrap_or_else(|| LogicalPlan::empty(true)))
    }

    fn bind_table(
        &self,
        factor: &TableFactor,
        qualifiers: &mut Vec<String>,
    ) -> Result<LogicalPlan> {
        let (name, alias) = match factor {
            TableFactor::Table {
                name,
                alias,
                args: None,
                with_hints,
                version: None,
                with_ordinality: false,
                partitions,
                json_path: None,
                sample: None,
                index_hints,
            } if with_hints.is_empty() && partitions.is_empty() && index_hints.is_empty() => {
                (name, alias)
            }
            TableFactor::Table { .. } => {
                return Err(unsupported!("table options or table functions in FROM"));
            }
            TableFactor::Derived { .. } => return Err(unsupported!("subqueries")),
            other => return Err(unsupported!("{other} in FROM")),
        };
        let table_name = match name.0.as_slice() {
            [ast::ObjectNamePart::Identifier(ident)] => ident.value.clone(),
            _ => {
                return Err(unsupported!(
                    "the table name {name}: give it without a schema"
                ));
            }
        };
        let provider = self.catalog.table(&table_name).ok_or_else(|| {
            let known = self.catalog.table_names();
            if known.is_empty() {
                plan_err!("table {table_name} not found; no tables are registered")
            } else {
                plan_err!(
                    "table {table_name} not found; the tables are: {}",
                    known.join(", ")
                )
            }
        })?;
        let qualifier = match alias {
            None => table_name.clone(),
            Some(alias) if alias.columns.is_empty() => alias.name.value.clone(),
            Some(_) => return Err(unsupported!("column aliases in FROM")),
        };
        if qualifiers.iter().any(|q| same_name(q, &qualifier)) {
            return Err(plan_err!(
                "the table name {qualifier} appears more than once in FROM; give each an alias"
            ));
        }
        qualifiers.push(qualifier.clone());
        Ok(LogicalPlan::scan(&table_name, &qualifier, provider))
    }
}

/// Clauses of SELECT that parse but that Quarry doesn't implement.
fn check_select_clauses(select: &Select) -> Result<()> {
    let unsupported_clause = if select.top.is_some() {
        Some("TOP")
    } else if select.into.is_some() {
        Some("SELECT INTO")
    } else if select.exclude.is_some()
        || select.select_modifiers.is_some()
        || !select.optimizer_hints.is_empty()
    {
        Some("SELECT modifiers")
    } else if !select.lateral_views.is_empty() {
        Some("LATERAL VIEW")
    } else if select.prewhere.is_some() {
        Some("PREWHERE")
    } else if !select.connect_by.is_empty() {
        Some("CONNECT BY")
    } else if !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
    {
        Some("CLUSTER BY, DISTRIBUTE BY and SORT BY")
    } else if !select.named_window.is_empty() || select.qualify.is_some() {
        Some("WINDOW and QUALIFY")
    } else if select.value_table_mode.is_some() {
        Some("SELECT AS STRUCT/VALUE")
    } else {
        None
    };
    match unsupported_clause {
        Some(clause) => Err(unsupported!("{clause}")),
        None => Ok(()),
    }
}

fn check_wildcard_options(options: &WildcardAdditionalOptions) -> Result<()> {
    if options.opt_ilike.is_some()
        || options.opt_exclude.is_some()
        || options.opt_except.is_some()
        || options.opt_replace.is_some()
        || options.opt_rename.is_some()
        || options.opt_alias.is_some()
    {
        return Err(unsupported!(
            "* with ILIKE, EXCLUDE, EXCEPT, REPLACE, RENAME or AS"
        ));
    }
    Ok(())
}

fn bind_select_item(item: &SelectItem, scope: Scope<'_>, items: &mut Vec<Item>) -> Result<()> {
    let schema = scope.schema;
    match item {
        SelectItem::UnnamedExpr(ast) => {
            let expr = bind_expr(ast, scope)?;
            // A bare column keeps the table's spelling of its name; anything else is named by its SQL text.
            let name = match (&expr, ast) {
                (Expr::Column(i), ast::Expr::Identifier(_) | ast::Expr::CompoundIdentifier(_)) => {
                    schema.field(*i).name.clone()
                }
                _ => ast.to_string(),
            };
            items.push(Item { expr, name });
        }
        SelectItem::ExprWithAlias { expr, alias } => {
            items.push(Item {
                expr: bind_expr(expr, scope)?,
                name: alias.value.clone(),
            });
        }
        SelectItem::ExprWithAliases { .. } => {
            return Err(unsupported!("more than one alias for an item"));
        }
        SelectItem::Wildcard(options) => {
            check_wildcard_options(options)?;
            if schema.is_empty() {
                return Err(plan_err!("SELECT * with no FROM"));
            }
            items.extend((0..schema.len()).map(|i| Item {
                expr: Expr::Column(i),
                name: schema.field(i).name.clone(),
            }));
        }
        SelectItem::QualifiedWildcard(kind, options) => {
            check_wildcard_options(options)?;
            let table = match kind {
                SelectItemQualifiedWildcardKind::ObjectName(name) => match name.0.as_slice() {
                    [ast::ObjectNamePart::Identifier(ident)] => ident.value.clone(),
                    _ => return Err(unsupported!("{name}.*")),
                },
                SelectItemQualifiedWildcardKind::Expr(e) => return Err(unsupported!("{e}.*")),
            };
            let columns: Vec<usize> = (0..schema.len())
                .filter(|&i| {
                    schema
                        .field(i)
                        .qualifier
                        .as_deref()
                        .is_some_and(|q| same_name(q, &table))
                })
                .collect();
            if columns.is_empty() {
                return Err(plan_err!(
                    "table {table} is not in FROM (or is known by an alias there)"
                ));
            }
            items.extend(columns.into_iter().map(|i| Item {
                expr: Expr::Column(i),
                name: schema.field(i).name.clone(),
            }));
        }
    }
    Ok(())
}

/// A positive integer literal, as written in `ORDER BY 2` or `GROUP BY 1`.
fn ordinal(ast: &ast::Expr) -> Option<Result<usize>> {
    match ast {
        ast::Expr::Value(v) => match &v.value {
            Value::Number(text, _) => Some(
                text.parse::<usize>()
                    .ok()
                    .filter(|&k| k >= 1)
                    .ok_or_else(|| plan_err!("{text} is not a column position")),
            ),
            _ => None,
        },
        _ => None,
    }
}

fn bind_order_target(ast: &ast::Expr, items: &[Item], scope: Scope<'_>) -> Result<OrderTarget> {
    if let Some(k) = ordinal(ast) {
        let k = k?;
        return match k <= items.len() {
            true => Ok(OrderTarget::Output(k - 1)),
            false => Err(plan_err!(
                "ORDER BY position {k} is not in the select list (it has {} items)",
                items.len()
            )),
        };
    }
    if let ast::Expr::Identifier(ident) = ast {
        let matches: Vec<usize> = (0..items.len())
            .filter(|&i| same_name(&items[i].name, &ident.value))
            .collect();
        if let Some(&first) = matches.first() {
            // Two outputs with this name are fine if they are the same expression (SELECT a, a ... ORDER BY a).
            if matches.iter().any(|&i| items[i].expr != items[first].expr) {
                return Err(plan_err!(
                    "ORDER BY {} is ambiguous: the select list has more than one",
                    ident.value
                ));
            }
            return Ok(OrderTarget::Output(first));
        }
    }
    Ok(OrderTarget::Expr(bind_expr(ast, scope)?))
}

fn bind_group_by(ast: &ast::Expr, items: &[Item], from: &PlanSchema) -> Result<Expr> {
    if let Some(k) = ordinal(ast) {
        let k = k?;
        let item = items.get(k - 1).ok_or_else(|| {
            plan_err!(
                "GROUP BY position {k} is not in the select list (it has {} items)",
                items.len()
            )
        })?;
        if item.expr.contains_aggregate() {
            return Err(plan_err!(
                "GROUP BY position {k} refers to an aggregate: {}",
                item.expr.display(from)
            ));
        }
        return Ok(item.expr.clone());
    }
    // A bare name that isn't a FROM column at all may be a select-list alias. (A FROM column that is ambiguous is
    // still a FROM column: binding it below reports the ambiguity.)
    if let ast::Expr::Identifier(ident) = ast
        && !has_column(from, &ident.value)
    {
        let matches: Vec<&Item> = items
            .iter()
            .filter(|i| same_name(&i.name, &ident.value))
            .collect();
        if let Some(item) = matches.first() {
            if matches.iter().any(|m| m.expr != item.expr) {
                return Err(plan_err!(
                    "GROUP BY {} is ambiguous: the select list has more than one",
                    ident.value
                ));
            }
            if item.expr.contains_aggregate() {
                return Err(plan_err!("GROUP BY {} refers to an aggregate", ident.value));
            }
            return Ok(item.expr.clone());
        }
    }
    bind_expr(ast, Scope::without_aggregates(from, "GROUP BY"))
}

/// Rewrites an expression over the FROM scope into one over the Aggregate's output (group columns, then aggregates):
/// a subexpression equal to a group expression becomes its column, an aggregate its column, and any other column is
/// an error.
fn rewrite_over_aggregate(
    expr: &Expr,
    group_by: &[Expr],
    aggregates: &[Expr],
    from: &PlanSchema,
    clause: &str,
) -> Result<Expr> {
    let mut error = None;
    let rewritten = expr.transform(&mut |e| {
        if let Some(g) = group_by.iter().position(|g| g == e) {
            return Some(Expr::Column(g));
        }
        if let Some(a) = aggregates.iter().position(|a| a == e) {
            return Some(Expr::Column(group_by.len() + a));
        }
        if let Expr::Column(c) = e {
            error.get_or_insert_with(|| {
                plan_err!(
                    "column {} must appear in GROUP BY or be used in an aggregate (in {clause})",
                    from.field(*c).qualified_name()
                )
            });
            return Some(e.clone());
        }
        None
    });
    match error {
        Some(e) => Err(e),
        None => Ok(rewritten),
    }
}

fn sort_key(column: usize, key: &ast::OrderByExpr) -> Result<SortKey> {
    if key.with_fill.is_some() {
        return Err(unsupported!("WITH FILL"));
    }
    let ascending = match &key.options.sort {
        None | Some(OrderBySort::Asc) => true,
        Some(OrderBySort::Desc) => false,
        Some(OrderBySort::Using(_)) => return Err(unsupported!("ORDER BY ... USING")),
    };
    // NULLs sort last in both directions unless asked otherwise, as in DuckDB.
    let nulls_first = key.options.nulls_first.unwrap_or(false);
    Ok(SortKey {
        expr: Expr::Column(column),
        ascending,
        nulls_first,
    })
}

/// LIMIT and OFFSET as (skip, fetch); each must be a non-negative integer literal.
fn bind_limit(clause: Option<&LimitClause>) -> Result<(usize, Option<usize>)> {
    let count = |e: &ast::Expr, what: &str| -> Result<usize> {
        match e {
            ast::Expr::Value(v) => match &v.value {
                Value::Number(text, _) => text
                    .parse::<usize>()
                    .map_err(|_| plan_err!("{what} must be a non-negative integer, not {text}")),
                _ => Err(plan_err!("{what} must be a non-negative integer, not {e}")),
            },
            _ => Err(plan_err!("{what} must be a non-negative integer, not {e}")),
        }
    };
    match clause {
        None => Ok((0, None)),
        Some(LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) => {
            if !limit_by.is_empty() {
                return Err(unsupported!("LIMIT ... BY"));
            }
            let fetch = match limit {
                Some(ast::Expr::Value(v)) if matches!(v.value, Value::Null) => None, // LIMIT NULL is no limit
                Some(e) => Some(count(e, "LIMIT")?),
                None => None,
            };
            let skip = match offset {
                Some(o) => count(&o.value, "OFFSET")?,
                None => 0,
            };
            Ok((skip, fetch))
        }
        Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
            Ok((count(offset, "OFFSET")?, Some(count(limit, "LIMIT")?)))
        }
    }
}
