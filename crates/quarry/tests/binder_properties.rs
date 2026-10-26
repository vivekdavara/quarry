//! Random expressions from a grammar of the supported SQL, bound as `SELECT <expr> FROM t`.
//!
//! The binder must never panic. Each expression's type and nullability are worked out a second time, here, by a
//! direct reading of DESIGN.md's rules (the `model` below, which shares no code with the binder): an expression the
//! model types must bind, to exactly that type and nullability, and one it rejects must fail with an error. Every
//! bound expression must also have the explicit casts DESIGN.md promises: both sides of an operator the same type.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::can_cast_types;
use arrow::datatypes::{DataType, Field, Schema};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use quarry::datasource::MemTable;
use quarry::plan::{BinaryOp, Expr, LogicalPlan, PlanSchema};
use quarry::{Catalog, QuarryError, sql};

/// The columns of `t`: (name, type, nullable).
const COLUMNS: &[(&str, DataType, bool)] = &[
    ("a", DataType::Int64, false),
    ("b", DataType::Int32, true),
    ("f", DataType::Float64, true),
    ("s", DataType::Utf8, true),
    ("d", DataType::Date32, true),
    ("flag", DataType::Boolean, true),
];

fn catalog() -> Catalog {
    let fields: Vec<Field> = COLUMNS
        .iter()
        .map(|(n, t, null)| Field::new(*n, t.clone(), *null))
        .collect();
    let mut catalog = Catalog::new();
    let batch = RecordBatch::new_empty(Arc::new(Schema::new(fields)));
    catalog
        .register("t", Arc::new(MemTable::from_batch(batch)))
        .unwrap();
    catalog
}

/// A generated expression: the grammar, before it is printed as SQL.
#[derive(Debug, Clone)]
enum G {
    Column(usize),
    Int(i64),
    Float(f64),
    Str(&'static str),
    Bool(bool),
    Null,
    Date(&'static str),
    Binary(&'static str, Box<G>, Box<G>),
    Not(Box<G>),
    Neg(Box<G>),
    IsNull(Box<G>, bool),
    Between(Box<G>, Box<G>, Box<G>, bool),
    In(Box<G>, Vec<G>, bool),
    Case(Vec<(G, G)>, Option<Box<G>>),
    Cast(Box<G>, &'static str),
    Like(Box<G>, Box<G>),
}

/// String literals: plain ones, dates (one impossible), an empty one, one with a quote.
const STRINGS: &[&str] = &["abc", "1998-09-02", "2024-02-30", "", "it's"];
const OPERATORS: &[&str] = &[
    "+", "-", "*", "/", "%", "=", "<>", "<", ">=", "AND", "OR", "||",
];
/// SQL type names and the Arrow types DESIGN.md says they mean.
const CASTS: &[(&str, DataType)] = &[
    ("BIGINT", DataType::Int64),
    ("INT", DataType::Int32),
    ("DOUBLE", DataType::Float64),
    ("VARCHAR", DataType::Utf8),
    ("DATE", DataType::Date32),
    ("BOOLEAN", DataType::Boolean),
];

fn leaf() -> impl Strategy<Value = G> {
    prop_oneof![
        4 => (0..COLUMNS.len()).prop_map(G::Column),
        2 => prop_oneof![Just(0i64), Just(1), Just(-7), Just(i64::MAX), Just(i64::MIN)].prop_map(G::Int),
        1 => prop_oneof![Just(0.5f64), Just(-2.25), Just(1e300)].prop_map(G::Float),
        2 => prop::sample::select(STRINGS).prop_map(G::Str),
        1 => any::<bool>().prop_map(G::Bool),
        1 => Just(G::Null),
        1 => Just(G::Date("2000-02-29")),
    ]
}

fn expr() -> impl Strategy<Value = G> {
    leaf().prop_recursive(4, 48, 4, |inner| {
        let b = |g: G| Box::new(g);
        prop_oneof![
            6 => (prop::sample::select(OPERATORS), inner.clone(), inner.clone()).prop_map(move |(op, l, r)| G::Binary(op, b(l), b(r))),
            1 => inner.clone().prop_map(move |e| G::Not(b(e))),
            1 => inner.clone().prop_map(move |e| G::Neg(b(e))),
            1 => (inner.clone(), any::<bool>()).prop_map(move |(e, not)| G::IsNull(b(e), not)),
            1 => (inner.clone(), inner.clone(), inner.clone(), any::<bool>()).prop_map(move |(e, l, h, not)| G::Between(b(e), b(l), b(h), not)),
            1 => (inner.clone(), prop::collection::vec(inner.clone(), 1..4), any::<bool>()).prop_map(move |(e, list, not)| G::In(b(e), list, not)),
            1 => (prop::collection::vec((inner.clone(), inner.clone()), 1..3), prop::option::of(inner.clone())).prop_map(move |(wt, e)| G::Case(wt, e.map(b))),
            1 => (inner.clone(), prop::sample::select(CASTS.iter().map(|(n, _)| *n).collect::<Vec<_>>())).prop_map(move |(e, t)| G::Cast(b(e), t)),
            1 => (inner.clone(), inner).prop_map(move |(e, p)| G::Like(b(e), b(p))),
        ]
    })
}

/// The SQL text, fully parenthesized so the grammar's tree is the parser's.
fn render(g: &G) -> String {
    match g {
        G::Column(i) => COLUMNS[*i].0.to_string(),
        G::Int(v) if *v < 0 => format!("({v})"),
        G::Int(v) => v.to_string(),
        G::Float(v) if *v < 0.0 => format!("({v:?})"),
        G::Float(v) => format!("{v:?}"),
        G::Str(s) => format!("'{}'", s.replace('\'', "''")),
        G::Bool(v) => v.to_string().to_uppercase(),
        G::Null => "NULL".into(),
        G::Date(s) => format!("DATE '{s}'"),
        G::Binary(op, l, r) => format!("({} {op} {})", render(l), render(r)),
        G::Not(e) => format!("(NOT {})", render(e)),
        G::Neg(e) => format!("(- {})", render(e)),
        G::IsNull(e, not) => format!("({} IS {}NULL)", render(e), if *not { "NOT " } else { "" }),
        G::Between(e, l, h, not) => {
            format!(
                "({} {}BETWEEN {} AND {})",
                render(e),
                if *not { "NOT " } else { "" },
                render(l),
                render(h)
            )
        }
        G::In(e, list, not) => {
            let items: Vec<String> = list.iter().map(render).collect();
            format!(
                "({} {}IN ({}))",
                render(e),
                if *not { "NOT " } else { "" },
                items.join(", ")
            )
        }
        G::Case(wt, e) => {
            let mut s = "(CASE".to_string();
            for (w, t) in wt {
                s += &format!(" WHEN {} THEN {}", render(w), render(t));
            }
            if let Some(e) = e {
                s += &format!(" ELSE {}", render(e));
            }
            s + " END)"
        }
        G::Cast(e, t) => format!("CAST({} AS {t})", render(e)),
        G::Like(e, p) => format!("({} LIKE {})", render(e), render(p)),
    }
}

/// The model: DESIGN.md's rules, read directly. `None` means the binder must refuse the expression.
mod model {
    use super::*;

    fn integer(t: &DataType) -> bool {
        matches!(t, DataType::Int32 | DataType::Int64)
    }

    fn number(t: &DataType) -> bool {
        integer(t) || *t == DataType::Float64 || *t == DataType::Null
    }

    fn string(t: &DataType) -> bool {
        matches!(t, DataType::Utf8 | DataType::Null)
    }

    fn boolean(t: &DataType) -> bool {
        matches!(t, DataType::Boolean | DataType::Null)
    }

    fn widen(t: &DataType) -> DataType {
        if *t == DataType::Float64 {
            DataType::Float64
        } else {
            DataType::Int64
        }
    }

    /// Rules 1-4.
    fn common2(a: &DataType, b: &DataType) -> Option<DataType> {
        match (a, b) {
            _ if a == b => Some(a.clone()),
            (DataType::Null, t) | (t, DataType::Null) => Some(t.clone()),
            _ if integer(a) && integer(b) => Some(DataType::Int64),
            _ if number(a) && number(b) => Some(DataType::Float64),
            _ => None,
        }
    }

    /// Rule 5 too: with a date among them, each string literal must be a date and is one.
    fn common(items: &[&G]) -> Option<DataType> {
        let types: Vec<DataType> = items.iter().map(|g| ty(g)).collect::<Option<_>>()?;
        let has_date = types.contains(&DataType::Date32);
        let mut target = DataType::Null;
        for (g, t) in items.iter().zip(&types) {
            let t = match g {
                G::Str(s) if has_date => {
                    quarry::types::parse_date32(s)?;
                    DataType::Date32
                }
                _ => t.clone(),
            };
            target = common2(&target, &t)?;
        }
        Some(target)
    }

    pub fn ty(g: &G) -> Option<DataType> {
        Some(match g {
            G::Column(i) => COLUMNS[*i].1.clone(),
            G::Int(_) => DataType::Int64,
            G::Float(_) => DataType::Float64,
            G::Str(_) => DataType::Utf8,
            G::Bool(_) => DataType::Boolean,
            G::Null => DataType::Null,
            G::Date(_) => DataType::Date32,
            G::Binary(op, l, r) => {
                let (lt, rt) = (ty(l)?, ty(r)?);
                match *op {
                    "AND" | "OR" if boolean(&lt) && boolean(&rt) => DataType::Boolean,
                    "||" if string(&lt) && string(&rt) => DataType::Utf8,
                    "=" | "<>" | "<" | ">=" => {
                        common(&[l, r])?;
                        DataType::Boolean
                    }
                    "+" | "-" if lt == DataType::Date32 && integer(&rt) => DataType::Date32,
                    "+" if integer(&lt) && rt == DataType::Date32 => DataType::Date32,
                    "-" if lt == DataType::Date32 && rt == DataType::Date32 => DataType::Int64,
                    "/" if number(&lt) && number(&rt) => DataType::Float64,
                    "%" if (integer(&lt) || lt == DataType::Null)
                        && (integer(&rt) || rt == DataType::Null) =>
                    {
                        DataType::Int64
                    }
                    "+" | "-" | "*" if number(&lt) && number(&rt) => widen(&common2(&lt, &rt)?),
                    _ => return None,
                }
            }
            G::Not(e) if boolean(&ty(e)?) => DataType::Boolean,
            G::Neg(e) if number(&ty(e)?) => widen(&ty(e)?),
            G::IsNull(e, _) => {
                ty(e)?;
                DataType::Boolean
            }
            G::Between(e, l, h, _) => {
                common(&[e, l, h])?;
                DataType::Boolean
            }
            G::In(e, list, _) => {
                let mut all: Vec<&G> = vec![e];
                all.extend(list.iter());
                common(&all)?;
                DataType::Boolean
            }
            G::Case(wt, e) => {
                for (w, _) in wt {
                    if !boolean(&ty(w)?) {
                        return None;
                    }
                }
                let mut results: Vec<&G> = wt.iter().map(|(_, t)| t).collect();
                let null = G::Null;
                results.push(e.as_deref().unwrap_or(&null));
                common(&results)?
            }
            G::Cast(e, name) => {
                let to = CASTS.iter().find(|(n, _)| n == name).unwrap().1.clone();
                if !can_cast_types(&ty(e)?, &to) {
                    return None;
                }
                to
            }
            G::Like(e, p) if string(&ty(e)?) && string(&ty(p)?) => DataType::Boolean,
            _ => return None,
        })
    }

    /// Nullability, for an expression `ty` accepts.
    pub fn nullable(g: &G) -> bool {
        match g {
            G::Column(i) => COLUMNS[*i].2,
            G::Null => true,
            G::Int(_) | G::Float(_) | G::Str(_) | G::Bool(_) | G::Date(_) => false,
            G::Binary("/" | "%", ..) => true, // division by zero
            G::Binary(_, l, r) => nullable(l) || nullable(r),
            G::Not(e) | G::Neg(e) | G::Cast(e, _) => nullable(e),
            G::IsNull(..) => false,
            G::Between(e, l, h, _) => nullable(e) || nullable(l) || nullable(h),
            G::In(e, list, _) => nullable(e) || list.iter().any(nullable),
            G::Case(wt, e) => {
                e.as_deref().is_none_or(nullable) || wt.iter().any(|(_, t)| nullable(t))
            }
            G::Like(e, p) => nullable(e) || nullable(p),
        }
    }
}

/// DESIGN.md's promise: execution never meets two different types on one operator.
fn check_operands(e: &Expr, schema: &PlanSchema) -> Result<(), String> {
    let t = |x: &Expr| x.data_type(schema);
    let mut problem = None;
    e.visit(&mut |x| {
        if problem.is_some() {
            return;
        }
        let ok = match x {
            Expr::Binary {
                op: BinaryOp::And | BinaryOp::Or,
                left,
                right,
            } => t(left) == DataType::Boolean && t(right) == DataType::Boolean,
            Expr::Binary {
                op: BinaryOp::Concat,
                left,
                right,
            } => t(left) == DataType::Utf8 && t(right) == DataType::Utf8,
            Expr::Binary {
                op: BinaryOp::Plus | BinaryOp::Minus,
                left,
                right,
            } if t(left) == DataType::Date32 || t(right) == DataType::Date32 => {
                let (l, r) = (t(left), t(right));
                (l == DataType::Date32 && (r == DataType::Int64 || r == DataType::Date32))
                    || (l == DataType::Int64 && r == DataType::Date32)
            }
            // Division is float division: the executor must not see integers there.
            Expr::Binary {
                op: BinaryOp::Divide,
                left,
                right,
            } => t(left) == DataType::Float64 && t(right) == DataType::Float64,
            Expr::Binary { left, right, .. } => t(left) == t(right),
            Expr::Not(inner) => t(inner) == DataType::Boolean,
            Expr::Negative(inner) => matches!(t(inner), DataType::Int64 | DataType::Float64),
            Expr::InList { expr, list, .. } => list.iter().all(|i| t(i) == t(expr)),
            Expr::Case {
                when_then,
                else_expr,
            } => when_then
                .iter()
                .all(|(w, th)| t(w) == DataType::Boolean && t(th) == t(else_expr)),
            Expr::Like { expr, pattern, .. } => {
                t(expr) == DataType::Utf8 && t(pattern) == DataType::Utf8
            }
            _ => true,
        };
        if !ok {
            problem = Some(format!(
                "operands of different types: {}",
                x.display(schema)
            ));
        }
    });
    problem.map_or(Ok(()), Err)
}

/// The kinds of value the typed generator aims for.
#[derive(Debug, Clone, Copy)]
enum Class {
    Int,
    Float,
    Str,
    Date,
    Bool,
}

/// Expressions that are well typed by construction, of the given class, nested up to `depth` operators deep: the
/// random grammar above mostly makes ill-typed trees, and these reach the deep casts it rarely does.
fn typed(class: Class, depth: u32) -> BoxedStrategy<G> {
    let b = |g: G| Box::new(g);
    let leaf: BoxedStrategy<G> = match class {
        Class::Int => prop_oneof![
            Just(G::Column(0)),
            Just(G::Column(1)),
            Just(G::Int(3)),
            Just(G::Int(-7))
        ]
        .boxed(),
        Class::Float => prop_oneof![
            Just(G::Column(2)),
            Just(G::Float(0.5)),
            Just(G::Float(-2.25))
        ]
        .boxed(),
        Class::Str => prop_oneof![
            Just(G::Column(3)),
            Just(G::Str("abc")),
            Just(G::Str("it's"))
        ]
        .boxed(),
        Class::Date => prop_oneof![Just(G::Column(4)), Just(G::Date("2000-02-29"))].boxed(),
        Class::Bool => prop_oneof![Just(G::Column(5)), Just(G::Bool(true))].boxed(),
    };
    if depth == 0 {
        return leaf;
    }
    let sub = |c: Class| typed(c, depth - 1);
    let number = prop_oneof![sub(Class::Int), sub(Class::Float)];
    let bin = move |op: &'static str, l: G, r: G| G::Binary(op, b(l), b(r));
    let case = move |w: G, t: G, e: G| G::Case(vec![(w, t)], Some(b(e)));
    let recursive: BoxedStrategy<G> = match class {
        Class::Int => prop_oneof![
            (
                prop::sample::select(vec!["+", "-", "*", "%"]),
                sub(Class::Int),
                sub(Class::Int)
            )
                .prop_map(move |(op, l, r)| bin(op, l, r)),
            sub(Class::Int).prop_map(move |e| G::Neg(b(e))),
            (sub(Class::Date), sub(Class::Date)).prop_map(move |(l, r)| bin("-", l, r)),
            (sub(Class::Bool), sub(Class::Int), sub(Class::Int))
                .prop_map(move |(w, t, e)| case(w, t, e)),
            sub(Class::Float).prop_map(move |e| G::Cast(b(e), "BIGINT")),
        ]
        .boxed(),
        Class::Float => prop_oneof![
            (
                prop::sample::select(vec!["+", "-", "*"]),
                sub(Class::Float),
                number.clone()
            )
                .prop_map(move |(op, l, r)| bin(op, l, r)),
            (number.clone(), number.clone()).prop_map(move |(l, r)| bin("/", l, r)),
            sub(Class::Float).prop_map(move |e| G::Neg(b(e))),
            (sub(Class::Bool), sub(Class::Int), sub(Class::Float))
                .prop_map(move |(w, t, e)| case(w, t, e)),
            sub(Class::Int).prop_map(move |e| G::Cast(b(e), "DOUBLE")),
        ]
        .boxed(),
        Class::Str => prop_oneof![
            (sub(Class::Str), sub(Class::Str)).prop_map(move |(l, r)| bin("||", l, r)),
            (sub(Class::Bool), sub(Class::Str), sub(Class::Str))
                .prop_map(move |(w, t, e)| case(w, t, e)),
            sub(Class::Int).prop_map(move |e| G::Cast(b(e), "VARCHAR")),
        ]
        .boxed(),
        Class::Date => prop_oneof![
            (
                prop::sample::select(vec!["+", "-"]),
                sub(Class::Date),
                sub(Class::Int)
            )
                .prop_map(move |(op, l, r)| bin(op, l, r)),
            (sub(Class::Int), sub(Class::Date)).prop_map(move |(l, r)| bin("+", l, r)),
            (sub(Class::Bool), sub(Class::Date), sub(Class::Date))
                .prop_map(move |(w, t, e)| case(w, t, e)),
        ]
        .boxed(),
        Class::Bool => {
            let compare = prop::sample::select(vec!["=", "<>", "<", ">="]);
            prop_oneof![
                (compare.clone(), number.clone(), number.clone())
                    .prop_map(move |(op, l, r)| bin(op, l, r)),
                (compare.clone(), sub(Class::Str), sub(Class::Str))
                    .prop_map(move |(op, l, r)| bin(op, l, r)),
                (
                    compare,
                    sub(Class::Date),
                    prop::sample::select(vec!["1998-09-02", "2024-01-31"])
                )
                    .prop_map(move |(op, l, r)| bin(op, l, G::Str(r))),
                (
                    prop::sample::select(vec!["AND", "OR"]),
                    sub(Class::Bool),
                    sub(Class::Bool)
                )
                    .prop_map(move |(op, l, r)| bin(op, l, r)),
                sub(Class::Bool).prop_map(move |e| G::Not(b(e))),
                (number.clone(), any::<bool>()).prop_map(move |(e, not)| G::IsNull(b(e), not)),
                (
                    number.clone(),
                    number.clone(),
                    number.clone(),
                    any::<bool>()
                )
                    .prop_map(move |(e, l, h, not)| G::Between(
                        b(e),
                        b(l),
                        b(h),
                        not
                    )),
                (
                    sub(Class::Int),
                    prop::collection::vec(number, 1..3),
                    any::<bool>()
                )
                    .prop_map(move |(e, list, not)| G::In(b(e), list, not)),
                (sub(Class::Str), sub(Class::Str)).prop_map(move |(e, p)| G::Like(b(e), b(p))),
            ]
            .boxed()
        }
    };
    prop_oneof![1 => leaf, 3 => recursive].boxed()
}

fn any_typed() -> impl Strategy<Value = G> {
    prop_oneof![
        typed(Class::Int, 3),
        typed(Class::Float, 3),
        typed(Class::Str, 3),
        typed(Class::Date, 3),
        typed(Class::Bool, 3),
    ]
}

/// Binds `SELECT <g> FROM t` and holds the result to the model.
fn check(g: &G) -> Result<(), TestCaseError> {
    let text = format!("SELECT {} FROM t", render(g));
    let result = sql::plan(&catalog(), &text);
    match (model::ty(g), result) {
        (Some(expected), Ok(plan)) => {
            let LogicalPlan::Projection(p) = &plan else {
                panic!("{text}: not a projection:\n{plan}")
            };
            let input = p.input.schema();
            prop_assert_eq!(
                &p.schema.field(0).data_type,
                &expected,
                "{}\n{}",
                text,
                plan
            );
            prop_assert_eq!(
                p.schema.field(0).nullable,
                model::nullable(g),
                "nullability of {}\n{}",
                text,
                plan
            );
            if let Err(problem) = check_operands(&p.exprs[0], input) {
                prop_assert!(false, "{}: {}", text, problem);
            }
        }
        (Some(expected), Err(e)) => prop_assert!(
            false,
            "{}: the model says {}, the binder says {}",
            text,
            expected,
            e
        ),
        (None, Ok(plan)) => prop_assert!(
            false,
            "{}: the model refuses it, the binder bound:\n{}",
            text,
            plan
        ),
        (None, Err(QuarryError::Plan(_) | QuarryError::Unsupported(_))) => {}
        (None, Err(e)) => prop_assert!(false, "{}: unexpected kind of error {}", text, e),
    }
    Ok(())
}

/// 3,000 cases a property by default; `PROPTEST_CASES=100000 cargo test --release` searches longer.
fn cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3000)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(cases()))]

    #[test]
    fn the_binder_agrees_with_the_model(g in expr()) {
        check(&g)?;
    }

    #[test]
    fn well_typed_expressions_bind_to_the_model_type(g in any_typed()) {
        prop_assert!(model::ty(&g).is_some(), "the typed generator made an ill-typed tree: {}", render(&g));
        check(&g)?;
    }
}

#[test]
fn the_grammar_reaches_both_outcomes() {
    // A guard on the random grammar: if it stopped making typable trees (or ill-typed ones), its property would be
    // testing one side only. Count both over a fixed seed.
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;
    let mut runner = TestRunner::deterministic();
    let strategy = expr();
    let (mut typed, mut refused) = (0, 0);
    for _ in 0..2000 {
        let g = strategy.new_tree(&mut runner).unwrap().current();
        if model::ty(&g).is_some() {
            typed += 1
        } else {
            refused += 1
        }
    }
    // 322 typed and 1,678 refused when this was written: the random grammar is mostly for the refusals, and the
    // typed generator covers the other side.
    assert!(
        typed >= 200 && refused >= 1000,
        "typed {typed}, refused {refused}"
    );
}
