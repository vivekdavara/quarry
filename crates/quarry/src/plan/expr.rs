//! Bound expressions: what the binder produces and the executor evaluates.

use std::fmt;
use std::hash::{Hash, Hasher};

use arrow::datatypes::DataType;

use super::PlanSchema;
use crate::types::{format_date32, widen_numeric};

/// A constant. A typed NULL is `Cast(Literal(Null), type)`.
#[derive(Debug, Clone)]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Int64(i64),
    Float64(f64),
    Utf8(String),
    /// Days since 1970-01-01.
    Date32(i32),
}

impl ScalarValue {
    pub fn data_type(&self) -> DataType {
        match self {
            ScalarValue::Null => DataType::Null,
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Utf8(_) => DataType::Utf8,
            ScalarValue::Date32(_) => DataType::Date32,
        }
    }
}

// Expressions are compared and hashed to match a SELECT item with a GROUP BY expression, so a float literal is
// compared by its bits: 1.0 equals 1.0, and NaN equals NaN (the same expression, whatever it evaluates to).
impl PartialEq for ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        use ScalarValue::*;
        match (self, other) {
            (Null, Null) => true,
            (Boolean(a), Boolean(b)) => a == b,
            (Int64(a), Int64(b)) => a == b,
            (Float64(a), Float64(b)) => a.to_bits() == b.to_bits(),
            (Utf8(a), Utf8(b)) => a == b,
            (Date32(a), Date32(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for ScalarValue {}

impl Hash for ScalarValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            ScalarValue::Null => {}
            ScalarValue::Boolean(v) => v.hash(state),
            ScalarValue::Int64(v) => v.hash(state),
            ScalarValue::Float64(v) => v.to_bits().hash(state),
            ScalarValue::Utf8(v) => v.hash(state),
            ScalarValue::Date32(v) => v.hash(state),
        }
    }
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScalarValue::Null => write!(f, "NULL"),
            ScalarValue::Boolean(true) => write!(f, "TRUE"),
            ScalarValue::Boolean(false) => write!(f, "FALSE"),
            ScalarValue::Int64(v) => write!(f, "{v}"),
            // `{:?}` keeps a point or an exponent (1.0, 1e300), so the text reads back as a float.
            ScalarValue::Float64(v) => write!(f, "{v:?}"),
            ScalarValue::Utf8(v) => write!(f, "'{}'", v.replace('\'', "''")),
            ScalarValue::Date32(v) => write!(f, "DATE '{}'", format_date32(*v)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOp {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Concat,
}

impl BinaryOp {
    pub fn is_comparison(self) -> bool {
        matches!(
            self,
            BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq
        )
    }

    pub fn is_arithmetic(self) -> bool {
        matches!(
            self,
            BinaryOp::Plus
                | BinaryOp::Minus
                | BinaryOp::Multiply
                | BinaryOp::Divide
                | BinaryOp::Modulo
        )
    }

    fn symbol(self) -> &'static str {
        match self {
            BinaryOp::Plus => "+",
            BinaryOp::Minus => "-",
            BinaryOp::Multiply => "*",
            BinaryOp::Divide => "/",
            BinaryOp::Modulo => "%",
            BinaryOp::Eq => "=",
            BinaryOp::NotEq => "<>",
            BinaryOp::Lt => "<",
            BinaryOp::LtEq => "<=",
            BinaryOp::Gt => ">",
            BinaryOp::GtEq => ">=",
            BinaryOp::And => "AND",
            BinaryOp::Or => "OR",
            BinaryOp::Concat => "||",
        }
    }

    /// Binding strength, in sqlparser's order (its `prec_value`), so a printed expression parses back to the same
    /// tree: OR < AND < NOT < IS < LIKE < comparisons, IN, BETWEEN < + - < * / % || < unary minus.
    fn precedence(self) -> u8 {
        match self {
            BinaryOp::Or => 1,
            BinaryOp::And => 2,
            op if op.is_comparison() => 6,
            BinaryOp::Plus | BinaryOp::Minus => 8,
            _ => 9, // * / % and || (sqlparser gives || the precedence of *)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggregateFunction {
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_lowercase().as_str() {
            "count" => Some(AggregateFunction::Count),
            "sum" => Some(AggregateFunction::Sum),
            "avg" => Some(AggregateFunction::Avg),
            "min" => Some(AggregateFunction::Min),
            "max" => Some(AggregateFunction::Max),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            AggregateFunction::Count => "count",
            AggregateFunction::Sum => "sum",
            AggregateFunction::Avg => "avg",
            AggregateFunction::Min => "min",
            AggregateFunction::Max => "max",
        }
    }

    /// The result type for an argument of this type (the binder has checked the argument is allowed).
    pub fn return_type(self, arg: Option<&DataType>) -> DataType {
        match (self, arg) {
            (AggregateFunction::Count, _) => DataType::Int64,
            (AggregateFunction::Avg, _) => DataType::Float64,
            (AggregateFunction::Sum, Some(t)) => widen_numeric(t),
            (AggregateFunction::Min | AggregateFunction::Max, Some(t)) => t.clone(),
            (_, None) => DataType::Null,
        }
    }
}

/// A bound expression. Columns are indices into the input schema of the plan node holding the expression, and the
/// binder has already cast operands to the types each operator takes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Expr {
    Column(usize),
    Literal(ScalarValue),
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Not(Box<Expr>),
    Negative(Box<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
        case_insensitive: bool,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// A searched CASE (`CASE x WHEN a` is bound as `CASE WHEN x = a`); no ELSE is `ELSE NULL`, made explicit.
    Case {
        when_then: Vec<(Expr, Expr)>,
        else_expr: Box<Expr>,
    },
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },
    /// Only in an `Aggregate` node's aggregate list. `arg` is `None` for `COUNT(*)`.
    Aggregate {
        func: AggregateFunction,
        arg: Option<Box<Expr>>,
        distinct: bool,
    },
}

impl Expr {
    pub fn binary(op: BinaryOp, left: Expr, right: Expr) -> Expr {
        Expr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    pub fn cast(self, data_type: &DataType) -> Expr {
        Expr::Cast {
            expr: Box::new(self),
            data_type: data_type.clone(),
        }
    }

    /// The type of the expression's values over `input`.
    pub fn data_type(&self, input: &PlanSchema) -> DataType {
        match self {
            Expr::Column(i) => input.field(*i).data_type.clone(),
            Expr::Literal(v) => v.data_type(),
            Expr::Binary { op, left, right } => match op {
                op if op.is_comparison() => DataType::Boolean,
                BinaryOp::And | BinaryOp::Or => DataType::Boolean,
                BinaryOp::Concat => DataType::Utf8,
                BinaryOp::Divide => DataType::Float64,
                _ => match (left.data_type(input), right.data_type(input)) {
                    (DataType::Date32, DataType::Date32) => DataType::Int64,
                    (DataType::Date32, _) | (_, DataType::Date32) => DataType::Date32,
                    (l, _) => l,
                },
            },
            Expr::Not(_)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::Like { .. }
            | Expr::InList { .. } => DataType::Boolean,
            Expr::Negative(e) => e.data_type(input),
            Expr::Case {
                when_then,
                else_expr,
            } => when_then.first().map_or_else(
                || else_expr.data_type(input),
                |(_, then)| then.data_type(input),
            ),
            Expr::Cast { data_type, .. } => data_type.clone(),
            Expr::Aggregate { func, arg, .. } => {
                func.return_type(arg.as_ref().map(|a| a.data_type(input)).as_ref())
            }
        }
    }

    /// Whether the expression can be NULL over `input`.
    pub fn nullable(&self, input: &PlanSchema) -> bool {
        match self {
            Expr::Column(i) => input.field(*i).nullable,
            Expr::Literal(v) => matches!(v, ScalarValue::Null),
            Expr::IsNull(_) | Expr::IsNotNull(_) => false,
            // Division and modulo by zero are NULL (day 2 decides the executor's rule; the type allows it either way).
            Expr::Binary {
                op: BinaryOp::Divide | BinaryOp::Modulo,
                ..
            } => true,
            Expr::Binary { left, right, .. } => left.nullable(input) || right.nullable(input),
            Expr::Not(e) | Expr::Negative(e) | Expr::Cast { expr: e, .. } => e.nullable(input),
            Expr::Like { expr, pattern, .. } => expr.nullable(input) || pattern.nullable(input),
            Expr::InList { expr, list, .. } => {
                expr.nullable(input) || list.iter().any(|e| e.nullable(input))
            }
            Expr::Case {
                when_then,
                else_expr,
            } => {
                else_expr.nullable(input) || when_then.iter().any(|(_, then)| then.nullable(input))
            }
            Expr::Aggregate {
                func: AggregateFunction::Count,
                ..
            } => false,
            // SUM, AVG, MIN and MAX of no rows (or only NULLs) are NULL.
            Expr::Aggregate { .. } => true,
        }
    }

    /// Whether an aggregate appears anywhere in the expression.
    pub fn contains_aggregate(&self) -> bool {
        let mut found = false;
        self.visit(&mut |e| found |= matches!(e, Expr::Aggregate { .. }));
        found
    }

    /// Calls `f` on this expression and every subexpression, parents before children.
    pub fn visit(&self, f: &mut impl FnMut(&Expr)) {
        f(self);
        match self {
            Expr::Column(_) | Expr::Literal(_) => {}
            Expr::Binary { left, right, .. } => {
                left.visit(f);
                right.visit(f);
            }
            Expr::Not(e)
            | Expr::Negative(e)
            | Expr::IsNull(e)
            | Expr::IsNotNull(e)
            | Expr::Cast { expr: e, .. } => e.visit(f),
            Expr::Like { expr, pattern, .. } => {
                expr.visit(f);
                pattern.visit(f);
            }
            Expr::InList { expr, list, .. } => {
                expr.visit(f);
                list.iter().for_each(|e| e.visit(f));
            }
            Expr::Case {
                when_then,
                else_expr,
            } => {
                for (when, then) in when_then {
                    when.visit(f);
                    then.visit(f);
                }
                else_expr.visit(f);
            }
            Expr::Aggregate { arg, .. } => {
                if let Some(arg) = arg {
                    arg.visit(f);
                }
            }
        }
    }

    /// Rebuilds the expression from the top down: where `f` returns `Some`, that replaces the subexpression (and its
    /// children aren't visited); elsewhere the children are rewritten.
    pub fn transform(&self, f: &mut impl FnMut(&Expr) -> Option<Expr>) -> Expr {
        if let Some(replaced) = f(self) {
            return replaced;
        }
        let mut t = |e: &Expr| Box::new(e.transform(f));
        match self {
            Expr::Column(_) | Expr::Literal(_) => self.clone(),
            Expr::Binary { op, left, right } => Expr::Binary {
                op: *op,
                left: t(left),
                right: t(right),
            },
            Expr::Not(e) => Expr::Not(t(e)),
            Expr::Negative(e) => Expr::Negative(t(e)),
            Expr::IsNull(e) => Expr::IsNull(t(e)),
            Expr::IsNotNull(e) => Expr::IsNotNull(t(e)),
            Expr::Cast { expr, data_type } => Expr::Cast {
                expr: t(expr),
                data_type: data_type.clone(),
            },
            Expr::Like {
                expr,
                pattern,
                negated,
                case_insensitive,
            } => Expr::Like {
                expr: t(expr),
                pattern: t(pattern),
                negated: *negated,
                case_insensitive: *case_insensitive,
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => Expr::InList {
                expr: t(expr),
                list: list.iter().map(|e| *t(e)).collect(),
                negated: *negated,
            },
            Expr::Case {
                when_then,
                else_expr,
            } => Expr::Case {
                when_then: when_then.iter().map(|(w, th)| (*t(w), *t(th))).collect(),
                else_expr: t(else_expr),
            },
            Expr::Aggregate {
                func,
                arg,
                distinct,
            } => Expr::Aggregate {
                func: *func,
                arg: arg.as_ref().map(|a| t(a)),
                distinct: *distinct,
            },
        }
    }

    /// The input columns the expression reads, in ascending order, each once.
    pub fn columns(&self) -> Vec<usize> {
        let mut columns = Vec::new();
        self.visit(&mut |e| {
            if let Expr::Column(i) = e {
                columns.push(*i);
            }
        });
        columns.sort_unstable();
        columns.dedup();
        columns
    }

    /// The expression with each column `i` replaced by column `f(i)` (for moving it to another input's schema).
    pub fn map_columns(&self, f: &impl Fn(usize) -> usize) -> Expr {
        self.transform(&mut |e| match e {
            Expr::Column(i) => Some(Expr::Column(f(*i))),
            _ => None,
        })
    }

    /// The operands of a chain of `AND`s, left to right (`a AND (b AND c)` is `[a, b, c]`); any other expression is
    /// its own only conjunct.
    pub fn conjuncts(&self) -> Vec<&Expr> {
        match self {
            Expr::Binary {
                op: BinaryOp::And,
                left,
                right,
            } => {
                let mut all = left.conjuncts();
                all.extend(right.conjuncts());
                all
            }
            other => vec![other],
        }
    }

    /// The operands of a chain of `OR`s, left to right, as [`Expr::conjuncts`] does for `AND`.
    pub fn disjuncts(&self) -> Vec<&Expr> {
        match self {
            Expr::Binary {
                op: BinaryOp::Or,
                left,
                right,
            } => {
                let mut all = left.disjuncts();
                all.extend(right.disjuncts());
                all
            }
            other => vec![other],
        }
    }

    /// The conjuncts joined by `AND`, left-deep and in order (so `AND`'s left-to-right laziness keeps their order);
    /// `None` for none.
    pub fn and_all(conjuncts: impl IntoIterator<Item = Expr>) -> Option<Expr> {
        conjuncts
            .into_iter()
            .reduce(|all, c| Expr::binary(BinaryOp::And, all, c))
    }

    /// The expression printed with its columns' names from `input`.
    pub fn display<'a>(&'a self, input: &'a PlanSchema) -> ExprDisplay<'a> {
        ExprDisplay { expr: self, input }
    }

    fn precedence(&self) -> u8 {
        match self {
            Expr::Binary { op, .. } => op.precedence(),
            Expr::Not(_) => 3,
            Expr::IsNull(_) | Expr::IsNotNull(_) => 4,
            Expr::Like { .. } => 5,
            Expr::InList { .. } => 6,
            Expr::Negative(_) => 10,
            _ => 11, // atoms: columns, literals, CASE, CAST, function calls
        }
    }
}

/// The precedence of the comparison operators (and IN).
const COMPARISON: u8 = 6;

/// An expression with the schema that names its columns, for printing.
#[derive(Debug, Clone, Copy)]
pub struct ExprDisplay<'a> {
    expr: &'a Expr,
    input: &'a PlanSchema,
}

impl ExprDisplay<'_> {
    fn child<'b>(&'b self, expr: &'b Expr) -> ExprDisplay<'b> {
        ExprDisplay {
            expr,
            input: self.input,
        }
    }

    /// Writes a child, in parentheses when it binds less tightly than its parent (or equally, on the right side of a
    /// binary operator: `a - (b - c)`).
    fn operand(
        &self,
        f: &mut fmt::Formatter<'_>,
        child: &Expr,
        parent: u8,
        right: bool,
    ) -> fmt::Result {
        let p = child.precedence();
        if p < parent || (right && p == parent) {
            write!(f, "({})", self.child(child))
        } else {
            write!(f, "{}", self.child(child))
        }
    }
}

impl fmt::Display for ExprDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prec = self.expr.precedence();
        match self.expr {
            Expr::Column(i) => write!(f, "{}", self.input.field(*i).qualified_name()),
            Expr::Literal(v) => write!(f, "{v}"),
            Expr::Binary { op, left, right } => {
                self.operand(f, left, prec, false)?;
                write!(f, " {} ", op.symbol())?;
                self.operand(f, right, prec, true)
            }
            Expr::Not(e) => {
                write!(f, "NOT ")?;
                self.operand(f, e, prec, false)
            }
            Expr::Negative(e) => {
                // A number right after the minus would read back as one negative literal (`-5` is the literal -5,
                // not the negation of 5), and a minus after it would print `--`, which starts a SQL comment; both
                // get parentheses: `-(5)`, `-(-5)`, `-(-t.a)`.
                write!(f, "-")?;
                let needs_parentheses = matches!(**e, Expr::Negative(_))
                    || matches!(
                        &**e,
                        Expr::Literal(ScalarValue::Int64(_) | ScalarValue::Float64(_))
                    );
                if needs_parentheses {
                    write!(f, "({})", self.child(e))
                } else {
                    self.operand(f, e, prec, false)
                }
            }
            // `(a = 1) IS NULL`, not `a = 1 IS NULL`: sqlparser reads both alike, people don't.
            Expr::IsNull(e) => {
                self.operand(f, e, COMPARISON + 1, false)?;
                write!(f, " IS NULL")
            }
            Expr::IsNotNull(e) => {
                self.operand(f, e, COMPARISON + 1, false)?;
                write!(f, " IS NOT NULL")
            }
            Expr::Like {
                expr,
                pattern,
                negated,
                case_insensitive,
            } => {
                self.operand(f, expr, prec, true)?;
                let not = if *negated { " NOT" } else { "" };
                let like = if *case_insensitive { "ILIKE" } else { "LIKE" };
                write!(f, "{not} {like} ")?;
                self.operand(f, pattern, prec, true)
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                self.operand(f, expr, prec, true)?;
                write!(f, "{} IN (", if *negated { " NOT" } else { "" })?;
                for (i, item) in list.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", self.child(item))?;
                }
                write!(f, ")")
            }
            Expr::Case {
                when_then,
                else_expr,
            } => {
                write!(f, "CASE")?;
                for (when, then) in when_then {
                    write!(f, " WHEN {} THEN {}", self.child(when), self.child(then))?;
                }
                write!(f, " ELSE {} END", self.child(else_expr))
            }
            Expr::Cast { expr, data_type } => {
                write!(f, "CAST({} AS {data_type})", self.child(expr))
            }
            Expr::Aggregate {
                func,
                arg,
                distinct,
            } => {
                write!(f, "{}(", func.name())?;
                if *distinct {
                    write!(f, "DISTINCT ")?;
                }
                match arg {
                    Some(arg) => write!(f, "{})", self.child(arg)),
                    None => write!(f, "*)"),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::PlanField;

    fn schema() -> PlanSchema {
        PlanSchema::new(vec![
            PlanField::new(Some("t"), "a", DataType::Int64, true),
            PlanField::new(Some("t"), "b", DataType::Int64, false),
            PlanField::new(None, "s", DataType::Utf8, true),
        ])
    }

    fn col(i: usize) -> Expr {
        Expr::Column(i)
    }

    fn lit(v: i64) -> Expr {
        Expr::Literal(ScalarValue::Int64(v))
    }

    fn show(e: &Expr) -> String {
        e.display(&schema()).to_string()
    }

    #[test]
    fn parentheses_only_where_precedence_needs_them() {
        use BinaryOp::*;
        let sum = Expr::binary(Plus, col(0), col(1));
        assert_eq!(
            show(&Expr::binary(Multiply, sum.clone(), lit(2))),
            "(t.a + t.b) * 2"
        );
        assert_eq!(
            show(&Expr::binary(
                Plus,
                Expr::binary(Multiply, col(0), lit(2)),
                col(1)
            )),
            "t.a * 2 + t.b"
        );
        // Left-associative: a - b - c needs none, a - (b - c) needs them.
        assert_eq!(
            show(&Expr::binary(
                Minus,
                Expr::binary(Minus, col(0), col(1)),
                lit(1)
            )),
            "t.a - t.b - 1"
        );
        assert_eq!(
            show(&Expr::binary(
                Minus,
                col(0),
                Expr::binary(Minus, col(1), lit(1))
            )),
            "t.a - (t.b - 1)"
        );
        let or = Expr::binary(
            Or,
            Expr::binary(Eq, col(0), lit(1)),
            Expr::binary(Eq, col(1), lit(2)),
        );
        assert_eq!(
            show(&Expr::binary(
                And,
                or.clone(),
                Expr::IsNull(Box::new(col(2)))
            )),
            "(t.a = 1 OR t.b = 2) AND s IS NULL"
        );
        assert_eq!(show(&Expr::Not(Box::new(or))), "NOT (t.a = 1 OR t.b = 2)");
        assert_eq!(
            show(&Expr::IsNull(Box::new(Expr::binary(Eq, col(0), lit(1))))),
            "(t.a = 1) IS NULL"
        );
    }

    #[test]
    fn minus_signs_never_print_as_a_comment() {
        assert_eq!(show(&Expr::Negative(Box::new(lit(-5)))), "-(-5)");
        // Found by the round-trip property: `-0` reads back as the literal 0, not the negation of 0.
        assert_eq!(show(&Expr::Negative(Box::new(lit(0)))), "-(0)");
        assert_eq!(
            show(&Expr::Negative(Box::new(Expr::Literal(
                ScalarValue::Float64(0.5)
            )))),
            "-(0.5)"
        );
        assert_eq!(
            show(&Expr::Negative(Box::new(Expr::Negative(Box::new(col(0)))))),
            "-(-t.a)"
        );
        assert_eq!(show(&Expr::Negative(Box::new(col(0)))), "-t.a");
        assert_eq!(
            show(&Expr::Negative(Box::new(Expr::binary(
                BinaryOp::Plus,
                col(0),
                lit(1)
            )))),
            "-(t.a + 1)"
        );
        assert_eq!(
            show(&Expr::binary(BinaryOp::Minus, col(0), lit(-1))),
            "t.a - -1"
        );
    }

    #[test]
    fn literals_print_as_sql() {
        let show_lit = |v: ScalarValue| v.to_string();
        assert_eq!(show_lit(ScalarValue::Float64(1.0)), "1.0");
        assert_eq!(show_lit(ScalarValue::Float64(1e300)), "1e300");
        assert_eq!(show_lit(ScalarValue::Utf8("it's".into())), "'it''s'");
        assert_eq!(show_lit(ScalarValue::Date32(10_471)), "DATE '1998-09-02'");
        assert_eq!(show_lit(ScalarValue::Boolean(false)), "FALSE");
        assert_eq!(show_lit(ScalarValue::Null), "NULL");
    }

    #[test]
    fn the_rest_print_as_sql() {
        let like = Expr::Like {
            expr: Box::new(col(2)),
            pattern: Box::new(Expr::Literal(ScalarValue::Utf8("a%".into()))),
            negated: true,
            case_insensitive: true,
        };
        assert_eq!(show(&like), "s NOT ILIKE 'a%'");
        let in_list = Expr::InList {
            expr: Box::new(col(0)),
            list: vec![lit(1), lit(2)],
            negated: false,
        };
        assert_eq!(show(&in_list), "t.a IN (1, 2)");
        let case = Expr::Case {
            when_then: vec![(Expr::binary(BinaryOp::Gt, col(0), lit(0)), lit(1))],
            else_expr: Box::new(lit(0)),
        };
        assert_eq!(show(&case), "CASE WHEN t.a > 0 THEN 1 ELSE 0 END");
        assert_eq!(
            show(&col(1).cast(&DataType::Float64)),
            "CAST(t.b AS Float64)"
        );
        let count = Expr::Aggregate {
            func: AggregateFunction::Count,
            arg: None,
            distinct: false,
        };
        assert_eq!(show(&count), "count(*)");
        let sum = Expr::Aggregate {
            func: AggregateFunction::Sum,
            arg: Some(Box::new(col(0))),
            distinct: true,
        };
        assert_eq!(show(&sum), "sum(DISTINCT t.a)");
    }

    #[test]
    fn types_and_nullability() {
        let s = schema();
        assert_eq!(
            Expr::binary(BinaryOp::Divide, col(0), col(1)).data_type(&s),
            DataType::Float64
        );
        assert_eq!(
            Expr::binary(BinaryOp::Lt, col(0), col(1)).data_type(&s),
            DataType::Boolean
        );
        assert!(!col(1).nullable(&s));
        assert!(col(0).nullable(&s));
        assert!(!Expr::IsNull(Box::new(col(0))).nullable(&s));
        assert!(Expr::binary(BinaryOp::Divide, col(1), col(1)).nullable(&s));
        let sum = Expr::Aggregate {
            func: AggregateFunction::Sum,
            arg: Some(Box::new(col(1))),
            distinct: false,
        };
        assert!(sum.nullable(&s), "the sum of no rows is NULL");
        let count = Expr::Aggregate {
            func: AggregateFunction::Count,
            arg: Some(Box::new(col(0))),
            distinct: false,
        };
        assert!(!count.nullable(&s));
        assert_eq!(count.data_type(&s), DataType::Int64);
    }

    #[test]
    fn float_literals_compare_by_bits() {
        assert_eq!(
            ScalarValue::Float64(f64::NAN),
            ScalarValue::Float64(f64::NAN)
        );
        assert_ne!(ScalarValue::Float64(0.0), ScalarValue::Float64(-0.0));
        assert_ne!(ScalarValue::Float64(1.0), ScalarValue::Int64(1));
    }

    #[test]
    fn transform_replaces_from_the_top() {
        let e = Expr::binary(
            BinaryOp::Plus,
            col(0),
            Expr::binary(BinaryOp::Multiply, col(0), lit(2)),
        );
        let replaced = e.transform(&mut |x| (*x == col(0)).then(|| col(1)));
        assert_eq!(show(&replaced), "t.b + t.b * 2");
        let mut columns = 0;
        e.visit(&mut |x| columns += usize::from(matches!(x, Expr::Column(_))));
        assert_eq!(columns, 2);
    }
}
