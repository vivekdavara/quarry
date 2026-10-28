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
    ///
    /// A table's column (one with a qualifier) prints each part as SQL would need it, quoted when it isn't a plain
    /// identifier: `w."x + 1"` is a column, `w.x + 1` an expression. A computed column (an aggregate's output, a
    /// select-list name) has no qualifier and prints its name as it is, which for an aggregate is its own text.
    pub fn qualified_name(&self) -> String {
        match &self.qualifier {
            Some(q) => format!("{}.{}", quote_name(q), quote_name(&self.name)),
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
    /// The table's columns to read, if not all (set by the optimizer's projection pushdown).
    pub projection: Option<Vec<usize>>,
    /// Conditions over the table's columns (numbered as in the table, not the projection) that the provider may use
    /// to skip data no row of which they keep, such as Parquet row groups (set by the optimizer's predicate
    /// pushdown). A filter above the scan still checks every row.
    pub filters: Vec<Expr>,
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
            filters: vec![],
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

    /// The node with its inputs replaced (left to right, as [`inputs`](Self::inputs) lists them), keeping its own
    /// expressions and output schema. For the optimizer's rules: each new input must have the old one's output
    /// columns, in order, with their types.
    pub fn with_new_inputs(&self, inputs: Vec<LogicalPlan>) -> LogicalPlan {
        assert_eq!(inputs.len(), self.inputs().len(), "one new input per input");
        let mut inputs = inputs.into_iter().map(Arc::new);
        let mut next = || inputs.next().expect("counted above");
        match self {
            LogicalPlan::Scan(_) | LogicalPlan::Empty(_) => self.clone(),
            LogicalPlan::Filter(f) => LogicalPlan::Filter(Filter {
                input: next(),
                predicate: f.predicate.clone(),
            }),
            LogicalPlan::Projection(p) => LogicalPlan::Projection(Projection {
                input: next(),
                exprs: p.exprs.clone(),
                schema: p.schema.clone(),
            }),
            LogicalPlan::Aggregate(a) => LogicalPlan::Aggregate(Aggregate {
                input: next(),
                group_by: a.group_by.clone(),
                aggregates: a.aggregates.clone(),
                schema: a.schema.clone(),
            }),
            LogicalPlan::Join(j) => LogicalPlan::Join(Join {
                left: next(),
                right: next(),
                kind: j.kind,
                on: j.on.clone(),
                schema: j.schema.clone(),
            }),
            LogicalPlan::Sort(s) => LogicalPlan::Sort(Sort {
                input: next(),
                keys: s.keys.clone(),
            }),
            LogicalPlan::Limit(l) => LogicalPlan::Limit(Limit {
                input: next(),
                skip: l.skip,
                fetch: l.fetch,
            }),
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
                if !s.filters.is_empty() {
                    let table = PlanSchema::from_arrow(&s.qualifier, &s.provider.schema());
                    let filters: Vec<String> = s
                        .filters
                        .iter()
                        .map(|e| e.display(&table).to_string())
                        .collect();
                    write!(f, " filters=[{}]", filters.join(", "))?;
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

/// Words that can't be a bare identifier without changing what SQL means: sqlparser's reserved lists, and the keywords
/// that start or join an expression.
fn is_reserved(name: &str) -> bool {
    use sqlparser::keywords::{
        RESERVED_FOR_COLUMN_ALIAS, RESERVED_FOR_IDENTIFIER, RESERVED_FOR_TABLE_ALIAS,
    };
    const EXPRESSION_WORDS: &[&str] = &[
        "NULL", "TRUE", "FALSE", "NOT", "AND", "OR", "IS", "IN", "LIKE", "ILIKE", "BETWEEN",
        "CASE", "WHEN", "THEN", "ELSE", "END", "CAST", "AS", "DISTINCT", "ALL", "ANY", "SOME",
        "DATE",
    ];
    let upper = name.to_uppercase();
    EXPRESSION_WORDS.contains(&upper.as_str())
        || RESERVED_FOR_COLUMN_ALIAS
            .iter()
            .chain(RESERVED_FOR_TABLE_ALIAS)
            .chain(RESERVED_FOR_IDENTIFIER)
            .any(|k| format!("{k:?}") == upper)
}

/// A name as it would be written in SQL: bare when it is an identifier (a letter or `_`, then letters, digits or `_`,
/// in any case and any script, since names match without regard to case) that isn't reserved; quoted otherwise, with
/// any `"` doubled.
pub(crate) fn quote_name(name: &str) -> String {
    let plain = name
        .chars()
        .next()
        .is_some_and(|c| c.is_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_alphanumeric() || c == '_')
        && !is_reserved(name);
    if plain {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::MemTable;
    use arrow::array::RecordBatch;

    fn table() -> Arc<dyn TableProvider> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, true),
        ]);
        Arc::new(MemTable::from_batch(RecordBatch::new_empty(Arc::new(
            schema,
        ))))
    }

    fn count(arg: Option<Expr>) -> Expr {
        Expr::Aggregate {
            func: AggregateFunction::Count,
            arg: arg.map(Box::new),
            distinct: false,
        }
    }

    #[test]
    fn constructors_refuse_what_the_binder_never_builds() {
        let scan = || LogicalPlan::scan("t", "t", table());
        let err = LogicalPlan::filter(scan(), Expr::Column(0)).unwrap_err();
        assert!(
            err.to_string()
                .contains("a filter must be a boolean, not Int64: t.a"),
            "{err}"
        );
        let err = LogicalPlan::aggregate(scan(), vec![], vec![Expr::Column(0)]).unwrap_err();
        assert!(err.to_string().contains("not an aggregate: t.a"), "{err}");
        let nested = Expr::Aggregate {
            func: AggregateFunction::Sum,
            arg: Some(Box::new(count(None))),
            distinct: false,
        };
        let err = LogicalPlan::aggregate(scan(), vec![], vec![nested]).unwrap_err();
        assert!(
            err.to_string().contains("aggregate calls cannot be nested"),
            "{err}"
        );
        let err = LogicalPlan::aggregate(scan(), vec![count(None)], vec![]).unwrap_err();
        assert!(
            err.to_string()
                .contains("aggregates are not allowed in GROUP BY"),
            "{err}"
        );
        let on = count(Some(Expr::Column(0)));
        let err = LogicalPlan::join(scan(), scan(), JoinKind::Inner, Some(on)).unwrap_err();
        assert!(err.to_string().contains("must be a boolean"), "{err}");
    }

    #[test]
    fn output_schemas() {
        let scan = LogicalPlan::scan("t", "x", table());
        assert_eq!(scan.schema().field(1).qualified_name(), "x.b");
        // Group columns keep a bare column's field; other group expressions and aggregates are named by their text.
        let plus = Expr::binary(
            BinaryOp::Plus,
            Expr::Column(0),
            Expr::Literal(ScalarValue::Int64(1)),
        );
        let agg =
            LogicalPlan::aggregate(scan.clone(), vec![Expr::Column(1), plus], vec![count(None)])
                .unwrap();
        let names: Vec<String> = agg
            .schema()
            .fields()
            .iter()
            .map(PlanField::qualified_name)
            .collect();
        assert_eq!(names, vec!["x.b", "x.a + 1", "count(*)"]);
        assert!(!agg.schema().field(2).nullable);
        // A LEFT join's right side is nullable; the left keeps its own.
        let left = LogicalPlan::join(
            scan.clone(),
            LogicalPlan::scan("t", "y", table()),
            JoinKind::Left,
            None,
        )
        .unwrap();
        let nullable: Vec<bool> = left.schema().fields().iter().map(|f| f.nullable).collect();
        assert_eq!(nullable, vec![false, true, true, true, true, true]);
        assert_eq!(left.schema().to_arrow().field(3).name(), "a"); // a result's names are unqualified
    }

    #[test]
    fn printing_nodes_the_binder_does_not_make_yet() {
        let LogicalPlan::Scan(mut scan) = LogicalPlan::scan("lineitem", "l", table()) else {
            unreachable!()
        };
        scan.projection = Some(vec![2, 0]); // day 3's projection pushdown sets this
        let plan = LogicalPlan::limit(LogicalPlan::Scan(scan), 0, None);
        assert_eq!(
            plan.to_string(),
            "Limit: skip=0 fetch=all\n  Scan: lineitem AS l projection=[c, a]\n"
        );
        assert_eq!(LogicalPlan::empty(false).to_string(), "Empty: no rows\n");
        let projection = LogicalPlan::projection(
            LogicalPlan::scan("t", "t", table()),
            vec![
                (Expr::Column(1), "weird \"name\"".to_string()),
                (Expr::Column(0), "a".to_string()),
            ],
        );
        assert_eq!(
            projection.to_string().lines().next().unwrap(),
            "Projection: t.b AS \"weird \"\"name\"\"\", t.a"
        );
        // A table column whose name isn't an identifier is quoted where it is used: a digit first, a space, a keyword.
        let schema = Schema::new(vec![
            Field::new("9lives", DataType::Int64, true),
            Field::new("two words", DataType::Int64, true),
            Field::new("select", DataType::Int64, true),
        ]);
        let odd: Arc<dyn TableProvider> = Arc::new(MemTable::from_batch(RecordBatch::new_empty(
            Arc::new(schema),
        )));
        let projection = LogicalPlan::projection(
            LogicalPlan::scan("t", "t", odd),
            vec![
                (Expr::Column(0), "9lives".to_string()),
                (Expr::Column(1), "two words".to_string()),
                (Expr::Column(2), "select".to_string()),
            ],
        );
        assert_eq!(
            projection.to_string().lines().next().unwrap(),
            "Projection: t.\"9lives\", t.\"two words\", t.\"select\""
        );
    }
}
