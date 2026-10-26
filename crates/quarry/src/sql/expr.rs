//! Binding expressions: names to column indices, literals to values, operators to typed `Expr`s with explicit casts
//! (DESIGN.md, "Types" and "Names").

use arrow::compute::can_cast_types;
use arrow::datatypes::DataType;
use sqlparser::ast::{
    self, BinaryOperator, CastKind, DuplicateTreatment, FunctionArg, FunctionArgExpr,
    FunctionArguments, Ident, UnaryOperator, Value,
};

use crate::error::{Result, plan_err, unsupported};
use crate::plan::{AggregateFunction, BinaryOp, Expr, PlanSchema, ScalarValue};
use crate::types::{
    common_type, is_float, is_integer, is_numeric, is_string, is_supported, parse_date32,
    widen_numeric,
};

/// Where an expression is being bound: the columns it can name, and whether it may hold an aggregate (and if not,
/// the clause to name in the error).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Scope<'a> {
    pub schema: &'a PlanSchema,
    /// `None` when aggregates are allowed; otherwise the clause, for "aggregates are not allowed in WHERE".
    pub no_aggregates: Option<&'static str>,
    /// How many expressions enclose the one being bound.
    depth: usize,
}

/// The deepest an expression may nest (`a = 0 OR a = 1 OR ...` is one level per OR). Binding, printing and dropping an
/// expression all recurse once per level; 128 levels fit a 2 MiB thread stack in a debug build (a test checks it),
/// and no query of the TPC-H kind comes near.
pub const MAX_EXPR_DEPTH: usize = 128;

impl<'a> Scope<'a> {
    pub fn new(schema: &'a PlanSchema) -> Self {
        Scope {
            schema,
            no_aggregates: None,
            depth: 0,
        }
    }

    pub fn without_aggregates(schema: &'a PlanSchema, clause: &'static str) -> Self {
        Scope {
            schema,
            no_aggregates: Some(clause),
            depth: 0,
        }
    }

    /// The scope for an expression's operands, or an error past the depth limit.
    fn nested(self) -> Result<Self> {
        if self.depth >= MAX_EXPR_DEPTH {
            return Err(unsupported!(
                "expressions nested more than {MAX_EXPR_DEPTH} deep (a long chain of AND/OR is one level per operator; \
                 an IN list is one level)"
            ));
        }
        Ok(Scope {
            depth: self.depth + 1,
            ..self
        })
    }
}

/// Whether two identifiers name the same thing: equal without regard to case, by Unicode's lowercase mapping (the
/// catalog's rule for table names too).
pub(crate) fn same_name(a: &str, b: &str) -> bool {
    a == b || a.to_lowercase() == b.to_lowercase()
}

/// Whether some column in the schema has this name (any qualifier).
pub(crate) fn has_column(schema: &PlanSchema, name: &str) -> bool {
    schema.fields().iter().any(|f| same_name(&f.name, name))
}

/// The column a name refers to: case-insensitive, with an optional table qualifier.
pub(crate) fn resolve_column(
    schema: &PlanSchema,
    qualifier: Option<&str>,
    name: &str,
) -> Result<usize> {
    if let Some(q) = qualifier {
        let known = schema
            .fields()
            .iter()
            .any(|f| f.qualifier.as_deref().is_some_and(|fq| same_name(fq, q)));
        if !known {
            return Err(plan_err!(
                "table {q} is not in FROM (or is known by an alias there)"
            ));
        }
    }
    let matches: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| {
            same_name(&f.name, name)
                && qualifier
                    .is_none_or(|q| f.qualifier.as_deref().is_some_and(|fq| same_name(fq, q)))
        })
        .map(|(i, _)| i)
        .collect();
    let written = match qualifier {
        Some(q) => format!("{q}.{name}"),
        None => name.to_string(),
    };
    match matches.as_slice() {
        [i] => Ok(*i),
        [] => {
            let columns: Vec<String> = schema.fields().iter().map(|f| f.qualified_name()).collect();
            Err(plan_err!(
                "column {written} not found; the columns are: {}",
                columns.join(", ")
            ))
        }
        many => {
            let names: Vec<String> = many
                .iter()
                .map(|&i| schema.field(i).qualified_name())
                .collect();
            Err(plan_err!(
                "column {written} is ambiguous: {}",
                names.join(", ")
            ))
        }
    }
}

/// Binds one AST expression in a scope.
pub(crate) fn bind_expr(ast: &ast::Expr, scope: Scope<'_>) -> Result<Expr> {
    use ast::Expr as A;
    let scope = scope.nested()?;
    let schema = scope.schema;
    match ast {
        A::Identifier(ident) => Ok(Expr::Column(resolve_column(schema, None, &ident.value)?)),
        A::CompoundIdentifier(parts) => match parts.as_slice() {
            [table, column] => Ok(Expr::Column(resolve_column(
                schema,
                Some(&table.value),
                &column.value,
            )?)),
            _ => Err(unsupported!("the name {ast}: only column or table.column")),
        },
        A::Value(v) => literal(&v.value, false),
        A::TypedString(ts) => match (&ts.data_type, &ts.value.value) {
            (ast::DataType::Date, Value::SingleQuotedString(s)) => match parse_date32(s) {
                Some(days) => Ok(Expr::Literal(ScalarValue::Date32(days))),
                None => Err(plan_err!("invalid date '{s}' (expected YYYY-MM-DD)")),
            },
            _ => Err(unsupported!("the typed literal {ast}")),
        },
        A::Nested(inner) => bind_expr(inner, scope),
        A::UnaryOp { op, expr } => {
            // A minus before a number is part of the literal, so -9223372036854775808 fits in an Int64.
            if let (UnaryOperator::Minus, A::Value(v)) = (op, expr.as_ref())
                && let Value::Number(_, _) = &v.value
            {
                return literal(&v.value, true);
            }
            let operand = bind_expr(expr, scope)?;
            let t = operand.data_type(schema);
            match op {
                UnaryOperator::Not => Ok(Expr::Not(Box::new(boolean_operand(
                    "NOT", operand, schema,
                )?))),
                UnaryOperator::Minus | UnaryOperator::Plus
                    if is_numeric(&t) || t == DataType::Null =>
                {
                    // Integers widen to Int64 (so -(-128) fits); a float keeps its type.
                    let target = if is_float(&t) {
                        t.clone()
                    } else {
                        DataType::Int64
                    };
                    let operand = cast_to(operand, &t, &target);
                    Ok(if *op == UnaryOperator::Minus {
                        Expr::Negative(Box::new(operand))
                    } else {
                        operand
                    })
                }
                UnaryOperator::Minus | UnaryOperator::Plus => Err(plan_err!(
                    "cannot apply unary {op} to {t}: {}",
                    operand.display(schema)
                )),
                _ => Err(unsupported!("the operator {op}")),
            }
        }
        A::BinaryOp { left, op, right } => {
            let l = bind_expr(left, scope)?;
            let r = bind_expr(right, scope)?;
            bind_binary(op, l, r, schema, ast)
        }
        A::IsNull(e) => Ok(Expr::IsNull(Box::new(bind_expr(e, scope)?))),
        A::IsNotNull(e) => Ok(Expr::IsNotNull(Box::new(bind_expr(e, scope)?))),
        A::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let mut all = coerce_all(
                vec![
                    bind_expr(expr, scope)?,
                    bind_expr(low, scope)?,
                    bind_expr(high, scope)?,
                ],
                schema,
                "BETWEEN",
                ast,
            )?;
            let (high, low, value) = (all.pop().unwrap(), all.pop().unwrap(), all.pop().unwrap());
            Ok(if *negated {
                Expr::binary(
                    BinaryOp::Or,
                    Expr::binary(BinaryOp::Lt, value.clone(), low),
                    Expr::binary(BinaryOp::Gt, value, high),
                )
            } else {
                Expr::binary(
                    BinaryOp::And,
                    Expr::binary(BinaryOp::GtEq, value.clone(), low),
                    Expr::binary(BinaryOp::LtEq, value, high),
                )
            })
        }
        A::InList {
            expr,
            list,
            negated,
        } => {
            let mut all = vec![bind_expr(expr, scope)?];
            for item in list {
                all.push(bind_expr(item, scope)?);
            }
            let mut all = coerce_all(all, schema, "IN", ast)?;
            let value = all.remove(0);
            Ok(Expr::InList {
                expr: Box::new(value),
                list: all,
                negated: *negated,
            })
        }
        A::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        }
        | A::ILike {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => {
            let case_insensitive = matches!(ast, A::ILike { .. });
            if *any {
                return Err(unsupported!("LIKE ANY"));
            }
            if escape_char.is_some() {
                return Err(unsupported!("LIKE ... ESCAPE"));
            }
            let what = if case_insensitive { "ILIKE" } else { "LIKE" };
            let value = string_operand(what, bind_expr(expr, scope)?, schema)?;
            let pattern = string_operand(what, bind_expr(pattern, scope)?, schema)?;
            Ok(Expr::Like {
                expr: Box::new(value),
                pattern: Box::new(pattern),
                negated: *negated,
                case_insensitive,
            })
        }
        A::Case {
            operand,
            conditions,
            else_result,
            ..
        } => bind_case(
            operand.as_deref(),
            conditions,
            else_result.as_deref(),
            scope,
            ast,
        ),
        A::Cast {
            kind,
            expr,
            data_type,
            format,
        } => {
            if *kind != CastKind::Cast && *kind != CastKind::DoubleColon {
                return Err(unsupported!("{kind:?}"));
            }
            if format.is_some() {
                return Err(unsupported!("CAST ... FORMAT"));
            }
            let target = sql_type(data_type)?;
            let value = bind_expr(expr, scope)?;
            let from = value.data_type(schema);
            if !can_cast_types(&from, &target) {
                return Err(plan_err!(
                    "cannot cast {from} to {target}: {}",
                    value.display(schema)
                ));
            }
            Ok(value.cast(&target))
        }
        A::Function(function) => bind_function(function, scope),
        A::Subquery(_) | A::InSubquery { .. } | A::Exists { .. } => Err(unsupported!("subqueries")),
        A::Interval(_) => Err(unsupported!("intervals")),
        A::GroupingSets(_) | A::Cube(_) | A::Rollup(_) => {
            Err(unsupported!("GROUPING SETS, CUBE and ROLLUP"))
        }
        A::Wildcard(_) | A::QualifiedWildcard(..) => Err(plan_err!(
            "* is only allowed in the select list and COUNT(*)"
        )),
        other => Err(unsupported!("the expression {other}")),
    }
}

/// A literal value. `negative` puts a minus in front of a number's text before it is parsed.
fn literal(value: &Value, negative: bool) -> Result<Expr> {
    let scalar = match value {
        Value::Number(text, _) => {
            let text = if negative {
                format!("-{text}")
            } else {
                text.clone()
            };
            if text.contains(['.', 'e', 'E']) {
                match text.parse::<f64>() {
                    Ok(v) if v.is_finite() => ScalarValue::Float64(v),
                    _ => {
                        return Err(plan_err!(
                            "the number {text} is out of range for a 64-bit float"
                        ));
                    }
                }
            } else {
                match text.parse::<i64>() {
                    Ok(v) => ScalarValue::Int64(v),
                    Err(_) => {
                        return Err(plan_err!(
                            "the integer {text} is out of range for a 64-bit integer"
                        ));
                    }
                }
            }
        }
        Value::SingleQuotedString(s) => ScalarValue::Utf8(s.clone()),
        Value::Boolean(b) => ScalarValue::Boolean(*b),
        Value::Null => ScalarValue::Null,
        Value::DoubleQuotedString(s) => {
            return Err(plan_err!(
                "\"{s}\" is a name, not a string; strings take single quotes"
            ));
        }
        other => return Err(unsupported!("the literal {other}")),
    };
    Ok(Expr::Literal(scalar))
}

/// The Arrow type a SQL type name in a CAST means (DESIGN.md's table of types).
fn sql_type(t: &ast::DataType) -> Result<DataType> {
    use ast::DataType as T;
    Ok(match t {
        T::Boolean | T::Bool => DataType::Boolean,
        T::SmallInt(None) | T::Int2(None) | T::Int16 => DataType::Int16,
        T::Int(None) | T::Integer(None) | T::Int4(None) | T::Int32 => DataType::Int32,
        T::BigInt(None) | T::Int8(None) | T::Int64 => DataType::Int64,
        T::Real | T::Float4 | T::Float32 => DataType::Float32,
        T::Double(ast::ExactNumberInfo::None)
        | T::DoublePrecision
        | T::Float(ast::ExactNumberInfo::None)
        | T::Float8
        | T::Float64 => DataType::Float64,
        T::Varchar(None) | T::Text | T::String(None) => DataType::Utf8,
        T::Date | T::Date32 => DataType::Date32,
        // The plan prints casts with Arrow's names; Int64, Float64 and the rest parse as SQL types already, and
        // Utf8 parses as a custom type. Accepting it makes a printed plan's expressions bind again.
        T::Custom(name, args)
            if args.is_empty() && name.to_string().eq_ignore_ascii_case("utf8") =>
        {
            DataType::Utf8
        }
        other => return Err(unsupported!("the type {other}")),
    })
}

fn cast_to(e: Expr, from: &DataType, to: &DataType) -> Expr {
    if from == to { e } else { e.cast(to) }
}

/// Makes every expression the same type: the common type of all (DESIGN.md's rules 1–4), after rule 5 turns string
/// literals into dates when any of the expressions is a date. Returns them cast to it.
fn coerce_all(
    exprs: Vec<Expr>,
    schema: &PlanSchema,
    what: &str,
    context: &ast::Expr,
) -> Result<Vec<Expr>> {
    let prefix = if what.is_empty() {
        String::new()
    } else {
        format!("{what}: ")
    };
    let has_date = exprs
        .iter()
        .any(|e| e.data_type(schema) == DataType::Date32);
    let mut exprs: Vec<Expr> = if has_date {
        exprs
            .into_iter()
            .map(|e| match e {
                Expr::Literal(ScalarValue::Utf8(s)) => match parse_date32(&s) {
                    Some(days) => Ok(Expr::Literal(ScalarValue::Date32(days))),
                    None => Err(plan_err!(
                        "{prefix}'{s}' is compared with a date but isn't one (expected YYYY-MM-DD) in {context}"
                    )),
                },
                e => Ok(e),
            })
            .collect::<Result<_>>()?
    } else {
        exprs
    };
    let types: Vec<DataType> = exprs.iter().map(|e| e.data_type(schema)).collect();
    if let Some(bad) = types.iter().find(|t| !is_supported(t)) {
        return Err(unsupported!("{prefix}{bad} values in {context}"));
    }
    let mut target = DataType::Null;
    for (e, t) in exprs.iter().zip(&types) {
        target = common_type(&target, t).ok_or_else(|| {
            plan_err!(
                "{prefix}cannot compare {target} with {t} ({}) in {context}",
                e.display(schema)
            )
        })?;
    }
    for (e, t) in exprs.iter_mut().zip(&types) {
        if *t != target {
            *e = std::mem::replace(e, Expr::Literal(ScalarValue::Null)).cast(&target);
        }
    }
    Ok(exprs)
}

fn boolean_operand(what: &str, e: Expr, schema: &PlanSchema) -> Result<Expr> {
    match e.data_type(schema) {
        DataType::Boolean => Ok(e),
        DataType::Null => Ok(e.cast(&DataType::Boolean)),
        other => Err(plan_err!(
            "{what} needs a boolean, not {other}: {}",
            e.display(schema)
        )),
    }
}

fn string_operand(what: &str, e: Expr, schema: &PlanSchema) -> Result<Expr> {
    match e.data_type(schema) {
        DataType::Utf8 => Ok(e),
        t if is_string(&t) || t == DataType::Null => Ok(e.cast(&DataType::Utf8)),
        other => Err(plan_err!(
            "{what} needs strings, not {other}: {}",
            e.display(schema)
        )),
    }
}

/// Binds `l op r`; `context` is the whole expression as written, for error messages.
fn bind_binary(
    op: &BinaryOperator,
    l: Expr,
    r: Expr,
    schema: &PlanSchema,
    context: &ast::Expr,
) -> Result<Expr> {
    let op = match op {
        BinaryOperator::Plus => BinaryOp::Plus,
        BinaryOperator::Minus => BinaryOp::Minus,
        BinaryOperator::Multiply => BinaryOp::Multiply,
        BinaryOperator::Divide => BinaryOp::Divide,
        BinaryOperator::Modulo => BinaryOp::Modulo,
        BinaryOperator::Eq => BinaryOp::Eq,
        BinaryOperator::NotEq => BinaryOp::NotEq,
        BinaryOperator::Lt => BinaryOp::Lt,
        BinaryOperator::LtEq => BinaryOp::LtEq,
        BinaryOperator::Gt => BinaryOp::Gt,
        BinaryOperator::GtEq => BinaryOp::GtEq,
        BinaryOperator::And => BinaryOp::And,
        BinaryOperator::Or => BinaryOp::Or,
        BinaryOperator::StringConcat => BinaryOp::Concat,
        other => return Err(unsupported!("the operator {other}")),
    };
    match op {
        BinaryOp::And | BinaryOp::Or => {
            let name = if op == BinaryOp::And { "AND" } else { "OR" };
            Ok(Expr::binary(
                op,
                boolean_operand(name, l, schema)?,
                boolean_operand(name, r, schema)?,
            ))
        }
        BinaryOp::Concat => Ok(Expr::binary(
            op,
            string_operand("||", l, schema)?,
            string_operand("||", r, schema)?,
        )),
        op if op.is_comparison() => {
            let mut both = coerce_all(vec![l, r], schema, "", context)?;
            let r = both.pop().unwrap();
            let l = both.pop().unwrap();
            Ok(Expr::binary(op, l, r))
        }
        _ => bind_arithmetic(op, l, r, schema),
    }
}

fn bind_arithmetic(op: BinaryOp, l: Expr, r: Expr, schema: &PlanSchema) -> Result<Expr> {
    let (lt, rt) = (l.data_type(schema), r.data_type(schema));
    let number = |t: &DataType| is_numeric(t) || *t == DataType::Null;
    match (op, &lt, &rt) {
        // Date arithmetic: a date plus or minus whole days, and the days between two dates.
        (BinaryOp::Plus | BinaryOp::Minus, DataType::Date32, t) if is_integer(t) => {
            Ok(Expr::binary(op, l, cast_to(r, t, &DataType::Int64)))
        }
        (BinaryOp::Plus, t, DataType::Date32) if is_integer(t) => {
            Ok(Expr::binary(op, cast_to(l, t, &DataType::Int64), r))
        }
        (BinaryOp::Minus, DataType::Date32, DataType::Date32) => Ok(Expr::binary(op, l, r)),
        (_, a, b) if number(a) && number(b) => {
            let common = common_type(a, b).expect("two numbers have a common type");
            let target = match op {
                BinaryOp::Divide => DataType::Float64,
                BinaryOp::Modulo
                    if !(is_integer(a) || *a == DataType::Null)
                        || !(is_integer(b) || *b == DataType::Null) =>
                {
                    return Err(plan_err!(
                        "% needs integers, not {a} and {b}: {} % {}",
                        l.display(schema),
                        r.display(schema)
                    ));
                }
                _ => widen_numeric(&common),
            };
            Ok(Expr::binary(
                op,
                cast_to(l, a, &target),
                cast_to(r, b, &target),
            ))
        }
        _ => {
            let e = Expr::binary(op, l.clone(), r.clone());
            if !is_supported(&lt) || !is_supported(&rt) {
                return Err(unsupported!("arithmetic on {lt} and {rt}"));
            }
            Err(plan_err!(
                "cannot apply this operator to {lt} and {rt}: {}",
                e.display(schema)
            ))
        }
    }
}

fn bind_case(
    operand: Option<&ast::Expr>,
    conditions: &[ast::CaseWhen],
    else_result: Option<&ast::Expr>,
    scope: Scope<'_>,
    context: &ast::Expr,
) -> Result<Expr> {
    let schema = scope.schema;
    let mut whens = Vec::with_capacity(conditions.len());
    match operand {
        // CASE x WHEN a THEN ...: x and every a share a type, and each WHEN becomes x = a.
        Some(operand) => {
            let mut all = vec![bind_expr(operand, scope)?];
            for c in conditions {
                all.push(bind_expr(&c.condition, scope)?);
            }
            let mut all = coerce_all(all, schema, "CASE", context)?;
            let value = all.remove(0);
            whens.extend(
                all.into_iter()
                    .map(|a| Expr::binary(BinaryOp::Eq, value.clone(), a)),
            );
        }
        None => {
            for c in conditions {
                whens.push(boolean_operand(
                    "CASE WHEN",
                    bind_expr(&c.condition, scope)?,
                    schema,
                )?);
            }
        }
    }
    let mut results = Vec::with_capacity(conditions.len() + 1);
    for c in conditions {
        results.push(bind_expr(&c.result, scope)?);
    }
    results.push(match else_result {
        Some(e) => bind_expr(e, scope)?,
        None => Expr::Literal(ScalarValue::Null),
    });
    let mut results = coerce_all(results, schema, "CASE results", context)?;
    let else_expr = results.pop().unwrap();
    Ok(Expr::Case {
        when_then: whens.into_iter().zip(results).collect(),
        else_expr: Box::new(else_expr),
    })
}

fn bind_function(function: &ast::Function, scope: Scope<'_>) -> Result<Expr> {
    let schema = scope.schema;
    let name = match function.name.0.as_slice() {
        [ast::ObjectNamePart::Identifier(Ident { value, .. })] => value.clone(),
        _ => return Err(unsupported!("the function {}", function.name)),
    };
    let Some(func) = AggregateFunction::from_name(&name) else {
        return Err(unsupported!("the function {name}"));
    };
    if function.over.is_some() {
        return Err(unsupported!("window functions"));
    }
    if function.filter.is_some()
        || function.null_treatment.is_some()
        || !function.within_group.is_empty()
    {
        return Err(unsupported!(
            "FILTER, WITHIN GROUP or IGNORE NULLS on {name}"
        ));
    }
    if !matches!(function.parameters, FunctionArguments::None) {
        return Err(unsupported!("parameters on {name}"));
    }
    if let Some(clause) = scope.no_aggregates {
        return Err(plan_err!(
            "aggregates are not allowed in {clause}: {name}(...)"
        ));
    }
    let list = match &function.args {
        FunctionArguments::List(list) => list,
        _ => return Err(plan_err!("{name} takes one argument")),
    };
    if !list.clauses.is_empty() {
        return Err(unsupported!("ORDER BY or LIMIT inside {name}(...)"));
    }
    let distinct = matches!(list.duplicate_treatment, Some(DuplicateTreatment::Distinct));
    // Inside an aggregate's argument another aggregate is an error, with this message.
    let inner = Scope {
        no_aggregates: Some("the argument of another aggregate"),
        ..scope
    };
    let arg = match list.args.as_slice() {
        [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
            if func == AggregateFunction::Count && !distinct =>
        {
            None
        }
        [
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard | FunctionArgExpr::QualifiedWildcard(_)),
        ] => {
            return Err(plan_err!("only COUNT(*) takes *"));
        }
        [FunctionArg::Unnamed(FunctionArgExpr::Expr(e))] => Some(bind_expr(e, inner)?),
        [FunctionArg::Unnamed(FunctionArgExpr::WildcardWithOptions(_))] => {
            return Err(unsupported!("* with options in {name}"));
        }
        _ => return Err(plan_err!("{name} takes one argument")),
    };
    let arg = match arg {
        None => None,
        Some(arg) => {
            let t = arg.data_type(schema);
            let arg = match func {
                AggregateFunction::Count => arg,
                AggregateFunction::Sum | AggregateFunction::Avg => {
                    if !(is_numeric(&t) || t == DataType::Null) {
                        return Err(plan_err!(
                            "{name} needs a number, not {t}: {}",
                            arg.display(schema)
                        ));
                    }
                    // Integers widen to Int64 and floats to Float64, so the executor adds values of one type. AVG
                    // sums the same way and divides as a float at the end: casting each value to Float64 first would
                    // lose integers above 2^53, and AVG(DISTINCT) would deduplicate the rounded floats.
                    cast_to(arg, &t, &widen_numeric(&t))
                }
                AggregateFunction::Min | AggregateFunction::Max => {
                    if !is_supported(&t) {
                        return Err(unsupported!("{name} over the type {t}"));
                    }
                    arg
                }
            };
            Some(Box::new(arg))
        }
    };
    Ok(Expr::Aggregate {
        func,
        arg,
        distinct,
    })
}
