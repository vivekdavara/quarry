//! The logical plan: a tree of relational operators over bound expressions (DESIGN.md, "The logical plan").

mod expr;

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};

use crate::datasource::TableProvider;
use crate::error::{Result, plan_err};

pub use expr::{AggregateFunction, BinaryOp, Expr, ExprDisplay, ScalarValue};

/// A column of a plan node's output.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlanField {
    /// The table the column came from (its name or alias in `FROM`), if it came straight from one.
    pub qualifier: Option<String>,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl PlanField {
    pub fn new(qualifier: Option<&str>, name: &str, data_type: DataType, nullable: bool) -> Self {
        PlanField {
            qualifier: qualifier.map(str::to_string),
            name: name.to_string(),
            data_type,
            nullable,
        }
    }

    /// `qualifier.name`, or `name` without a qualifier.
    pub fn qualified_name(&self) -> String {
        match &self.qualifier {
            Some(q) => format!("{q}.{}", self.name),
            None => self.name.clone(),
        }
    }
}

/// The output columns of a plan node, in order. Names may repeat; plan expressions refer to columns by index.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct PlanSchema {
    fields: Vec<PlanField>,
}

pub type PlanSchemaRef = Arc<PlanSchema>;

impl PlanSchema {
    pub fn new(fields: Vec<PlanField>) -> Self {
        PlanSchema { fields }
    }

    pub fn empty() -> Self {
        PlanSchema::default()
    }

    /// A table's columns, each qualified by the name the query knows the table by.
    pub fn from_arrow(qualifier: &str, schema: &Schema) -> Self {
        let fields = schema
            .fields()
            .iter()
            .map(|f| {
                PlanField::new(
                    Some(qualifier),
                    f.name(),
                    f.data_type().clone(),
                    f.is_nullable(),
                )
            })
            .collect();
        PlanSchema { fields }
    }

    pub fn fields(&self) -> &[PlanField] {
        &self.fields
    }

    pub fn field(&self, i: usize) -> &PlanField {
        &self.fields[i]
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// This schema's columns followed by `other`'s.
    pub fn join(&self, other: &PlanSchema) -> PlanSchema {
        PlanSchema {
            fields: self
                .fields
                .iter()
                .chain(other.fields.iter())
                .cloned()
                .collect(),
        }
    }

    /// The Arrow schema of a result with these columns (unqualified names: a result has no tables).
    pub fn to_arrow(&self) -> Schema {
        Schema::new(
            self.fields
                .iter()
                .map(|f| Field::new(&f.name, f.data_type.clone(), f.nullable))
                .collect::<Vec<_>>(),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinKind {
    Inner,
    Left,
    Cross,
}

/// One key of an ORDER BY.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SortKey {
    pub expr: Expr,
    pub ascending: bool,
    pub nulls_first: bool,
}

/// A node of the logical plan. Every node's output schema is computed when it is built.
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan(Scan),
    Empty(Empty),
    Filter(Filter),
    Projection(Projection),
    Aggregate(Aggregate),
    Join(Join),
    Sort(Sort),
    Limit(Limit),
}

#[derive(Debug, Clone)]
pub struct Scan {
    /// The table's name as the query wrote it.
    pub table_name: String,
    /// The name the query knows the table by: its alias, or its name.
    pub qualifier: String,
    pub provider: Arc<dyn TableProvider>,
    /// The table's columns to read, if not all (set by day 3's projection pushdown).
    pub projection: Option<Vec<usize>>,
    pub schema: PlanSchemaRef,
}

#[derive(Debug, Clone)]
pub struct Empty {
    /// One row with no columns (`SELECT 1` without `FROM`), or no rows.
    pub one_row: bool,
    pub schema: PlanSchemaRef,
}

#[derive(Debug, Clone)]
pub struct Filter {
    pub input: Arc<LogicalPlan>,
    pub predicate: Expr,
}

#[derive(Debug, Clone)]
pub struct Projection {
    pub input: Arc<LogicalPlan>,
    pub exprs: Vec<Expr>,
    pub schema: PlanSchemaRef,
}

#[derive(Debug, Clone)]
pub struct Aggregate {
    pub input: Arc<LogicalPlan>,
    pub group_by: Vec<Expr>,
    /// Each an `Expr::Aggregate`.
    pub aggregates: Vec<Expr>,
    pub schema: PlanSchemaRef,
}

#[derive(Debug, Clone)]
pub struct Join {
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,
    pub kind: JoinKind,
    /// Over the joined schema (left's columns, then right's); `None` for a cross join.
    pub on: Option<Expr>,
    pub schema: PlanSchemaRef,
}

#[derive(Debug, Clone)]
pub struct Sort {
    pub input: Arc<LogicalPlan>,
    pub keys: Vec<SortKey>,
}

#[derive(Debug, Clone)]
pub struct Limit {
    pub input: Arc<LogicalPlan>,
    pub skip: usize,
    pub fetch: Option<usize>,
}

fn check_boolean(what: &str, expr: &Expr, input: &PlanSchema) -> Result<()> {
    match expr.data_type(input) {
        DataType::Boolean | DataType::Null => Ok(()),
        other => Err(plan_err!(
            "{what} must be a boolean, not {other}: {}",
            expr.display(input)
        )),
    }
}

impl LogicalPlan {
    pub fn scan(
        table_name: &str,
        qualifier: &str,
        provider: Arc<dyn TableProvider>,
    ) -> LogicalPlan {
        let schema = Arc::new(PlanSchema::from_arrow(qualifier, &provider.schema()));
        LogicalPlan::Scan(Scan {
            table_name: table_name.to_string(),
            qualifier: qualifier.to_string(),
            provider,
            projection: None,
            schema,
        })
    }

    pub fn empty(one_row: bool) -> LogicalPlan {
        LogicalPlan::Empty(Empty {
            one_row,
            schema: Arc::new(PlanSchema::empty()),
        })
    }

    pub fn filter(input: LogicalPlan, predicate: Expr) -> Result<LogicalPlan> {
        check_boolean("a filter", &predicate, input.schema())?;
        Ok(LogicalPlan::Filter(Filter {
            input: Arc::new(input),
            predicate,
        }))
    }

    /// A projection of `(expression, output name)` pairs. A bare column keeps its qualifier.
    pub fn projection(input: LogicalPlan, exprs: Vec<(Expr, String)>) -> LogicalPlan {
        let in_schema = input.schema();
        let fields = exprs
            .iter()
            .map(|(e, name)| {
                let qualifier = match e {
                    Expr::Column(i) if in_schema.field(*i).name == *name => {
                        in_schema.field(*i).qualifier.as_deref()
                    }
                    _ => None,
                };
                PlanField::new(
                    qualifier,
                    name,
                    e.data_type(in_schema),
                    e.nullable(in_schema),
                )
            })
            .collect();
        let schema = Arc::new(PlanSchema::new(fields));
        LogicalPlan::Projection(Projection {
            input: Arc::new(input),
            exprs: exprs.into_iter().map(|(e, _)| e).collect(),
            schema,
        })
    }

    /// Groups by `group_by` and computes `aggregates`; the output is the group columns, then the aggregates. A
    /// group expression that is a bare column keeps that column's name and qualifier; the rest are named by their
    /// text.
    pub fn aggregate(
        input: LogicalPlan,
        group_by: Vec<Expr>,
        aggregates: Vec<Expr>,
    ) -> Result<LogicalPlan> {
        let in_schema = input.schema();
        let mut fields = Vec::with_capacity(group_by.len() + aggregates.len());
        for g in &group_by {
            if g.contains_aggregate() {
                return Err(plan_err!(
                    "aggregates are not allowed in GROUP BY: {}",
                    g.display(in_schema)
                ));
            }
            fields.push(match g {
                Expr::Column(i) => in_schema.field(*i).clone(),
                _ => PlanField::new(
                    None,
                    &g.display(in_schema).to_string(),
                    g.data_type(in_schema),
                    g.nullable(in_schema),
                ),
            });
        }
        for a in &aggregates {
            let Expr::Aggregate { arg, .. } = a else {
                return Err(plan_err!("not an aggregate: {}", a.display(in_schema)));
            };
            if arg.as_ref().is_some_and(|arg| arg.contains_aggregate()) {
                return Err(plan_err!(
                    "aggregate calls cannot be nested: {}",
                    a.display(in_schema)
                ));
            }
            let name = a.display(in_schema).to_string();
            fields.push(PlanField::new(
                None,
                &name,
                a.data_type(in_schema),
                a.nullable(in_schema),
            ));
        }
        let schema = Arc::new(PlanSchema::new(fields));
        Ok(LogicalPlan::Aggregate(Aggregate {
            input: Arc::new(input),
            group_by,
            aggregates,
            schema,
        }))
    }

    pub fn join(
        left: LogicalPlan,
        right: LogicalPlan,
        kind: JoinKind,
        on: Option<Expr>,
    ) -> Result<LogicalPlan> {
        let mut right_fields = right.schema().fields().to_vec();
        if kind == JoinKind::Left {
            right_fields.iter_mut().for_each(|f| f.nullable = true);
        }
        let schema = Arc::new(left.schema().join(&PlanSchema::new(right_fields)));
        if let Some(on) = &on {
            check_boolean("a join condition", on, &schema)?;
            if on.contains_aggregate() {
                return Err(plan_err!(
                    "aggregates are not allowed in JOIN ... ON: {}",
                    on.display(&schema)
                ));
            }
        }
        Ok(LogicalPlan::Join(Join {
            left: Arc::new(left),
            right: Arc::new(right),
            kind,
            on,
            schema,
        }))
    }

    pub fn sort(input: LogicalPlan, keys: Vec<SortKey>) -> LogicalPlan {
        LogicalPlan::Sort(Sort {
            input: Arc::new(input),
            keys,
        })
    }

    pub fn limit(input: LogicalPlan, skip: usize, fetch: Option<usize>) -> LogicalPlan {
        LogicalPlan::Limit(Limit {
            input: Arc::new(input),
            skip,
            fetch,
        })
    }

    /// The node's output columns.
    pub fn schema(&self) -> &PlanSchemaRef {
        match self {
            LogicalPlan::Scan(s) => &s.schema,
            LogicalPlan::Empty(e) => &e.schema,
            LogicalPlan::Filter(f) => f.input.schema(),
            LogicalPlan::Projection(p) => &p.schema,
            LogicalPlan::Aggregate(a) => &a.schema,
            LogicalPlan::Join(j) => &j.schema,
            LogicalPlan::Sort(s) => s.input.schema(),
            LogicalPlan::Limit(l) => l.input.schema(),
        }
    }

    /// The node's inputs, left to right.
    pub fn inputs(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::Scan(_) | LogicalPlan::Empty(_) => vec![],
            LogicalPlan::Filter(f) => vec![&f.input],
            LogicalPlan::Projection(p) => vec![&p.input],
            LogicalPlan::Aggregate(a) => vec![&a.input],
            LogicalPlan::Join(j) => vec![&j.left, &j.right],
            LogicalPlan::Sort(s) => vec![&s.input],
            LogicalPlan::Limit(l) => vec![&l.input],
        }
    }

    /// The node's own line in the printed plan, without its inputs.
    fn describe(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalPlan::Scan(s) => {
                write!(f, "Scan: {}", s.table_name)?;
                if s.qualifier != s.table_name {
                    write!(f, " AS {}", s.qualifier)?;
                }
                if let Some(projection) = &s.projection {
                    let table = s.provider.schema();
                    let names: Vec<&str> = projection
                        .iter()
                        .map(|&i| table.field(i).name().as_str())
                        .collect();
                    write!(f, " projection=[{}]", names.join(", "))?;
                }
                Ok(())
            }
            LogicalPlan::Empty(e) => write!(
                f,
                "Empty: {}",
                if e.one_row { "one row" } else { "no rows" }
            ),
            LogicalPlan::Filter(flt) => {
                write!(f, "Filter: {}", flt.predicate.display(flt.input.schema()))
            }
            LogicalPlan::Projection(p) => {
                write!(f, "Projection: ")?;
                let input = p.input.schema();
                for (i, (e, field)) in p.exprs.iter().zip(p.schema.fields()).enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", e.display(input))?;
                    let keeps_name =
                        matches!(e, Expr::Column(c) if input.field(*c).name == field.name);
                    if !keeps_name {
                        write!(f, " AS {}", quote_name(&field.name))?;
                    }
                }
                Ok(())
            }
            LogicalPlan::Aggregate(a) => {
                let input = a.input.schema();
                let list = |exprs: &[Expr]| {
                    exprs
                        .iter()
                        .map(|e| e.display(input).to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                write!(
                    f,
                    "Aggregate: group_by=[{}] aggregates=[{}]",
                    list(&a.group_by),
                    list(&a.aggregates)
                )
            }
            LogicalPlan::Join(j) => match (&j.kind, &j.on) {
                (JoinKind::Cross, _) | (_, None) => write!(f, "Join: {:?}", j.kind),
                (kind, Some(on)) => write!(f, "Join: {kind:?} on {}", on.display(&j.schema)),
            },
            LogicalPlan::Sort(s) => {
                write!(f, "Sort: ")?;
                for (i, key) in s.keys.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    let direction = if key.ascending { "ASC" } else { "DESC" };
                    let nulls = if key.nulls_first { "FIRST" } else { "LAST" };
                    write!(
                        f,
                        "{} {direction} NULLS {nulls}",
                        key.expr.display(s.input.schema())
                    )?;
                }
                Ok(())
            }
            LogicalPlan::Limit(l) => match l.fetch {
                Some(n) => write!(f, "Limit: skip={} fetch={n}", l.skip),
                None => write!(f, "Limit: skip={} fetch=all", l.skip),
            },
        }
    }

    fn fmt_indent(&self, f: &mut fmt::Formatter<'_>, depth: usize) -> fmt::Result {
        write!(f, "{:width$}", "", width = depth * 2)?;
        self.describe(f)?;
        writeln!(f)?;
        self.inputs()
            .iter()
            .try_for_each(|input| input.fmt_indent(f, depth + 1))
    }
}

/// The plan as an indented tree, the root first, one node a line (each line ends with a newline).
impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_indent(f, 0)
    }
}

/// A name as it would be written in SQL: bare when it is a plain lowercase identifier, quoted otherwise.
fn quote_name(name: &str) -> String {
    let plain = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if plain {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}
