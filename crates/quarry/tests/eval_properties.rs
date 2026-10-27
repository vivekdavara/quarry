//! Random well-typed expressions, evaluated two ways over random rows: by the engine (`exec::evaluate`, a column at a
//! time with Arrow's kernels) and by the reference interpreter below, one row at a time over plain Rust values. The
//! interpreter shares no code with the engine; it is a direct reading of DESIGN.md's rules ("Types", "Execution").
//! The two must agree on every value, or both fail. The engine must also give the same answer over any split of the
//! rows into batches.

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Date32Array, Float64Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow::datatypes::{
    DataType, Date32Type, Field, Float64Type, Int16Type, Int32Type, Int64Type, Schema,
};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use quarry::datasource::MemTable;
use quarry::exec::evaluate;
use quarry::plan::{BinaryOp, Expr, LogicalPlan, ScalarValue};
use quarry::types::format_date32;
use quarry::{Catalog, sql};

/// A value, as the reference interpreter sees it.
#[derive(Debug, Clone)]
enum V {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Date(i32),
}

impl PartialEq for V {
    /// Floats compare by their bits (so -0.0 and 0.0 differ: both evaluators must produce the same one), NaNs equal.
    fn eq(&self, other: &V) -> bool {
        match (self, other) {
            (V::Null, V::Null) => true,
            (V::Bool(a), V::Bool(b)) => a == b,
            (V::Int(a), V::Int(b)) => a == b,
            (V::Float(a), V::Float(b)) => (a.is_nan() && b.is_nan()) || a.to_bits() == b.to_bits(),
            (V::Str(a), V::Str(b)) => a == b,
            (V::Date(a), V::Date(b)) => a == b,
            _ => false,
        }
    }
}

/// The reference interpreter: one bound expression over one row.
mod reference {
    use super::*;

    type R = Result<V, String>;

    pub fn eval(e: &Expr, row: &[V]) -> R {
        match e {
            Expr::Column(i) => Ok(row[*i].clone()),
            Expr::Literal(v) => Ok(match v {
                ScalarValue::Null => V::Null,
                ScalarValue::Boolean(b) => V::Bool(*b),
                ScalarValue::Int64(x) => V::Int(*x),
                ScalarValue::Float64(x) => V::Float(*x),
                ScalarValue::Utf8(s) => V::Str(s.clone()),
                ScalarValue::Date32(d) => V::Date(*d),
            }),
            // AND and OR look at the right side only when the left doesn't decide.
            Expr::Binary {
                op: BinaryOp::And,
                left,
                right,
            } => match eval(left, row)? {
                V::Bool(false) => Ok(V::Bool(false)),
                l => match (l, eval(right, row)?) {
                    (_, V::Bool(false)) => Ok(V::Bool(false)),
                    (V::Bool(true), r) => Ok(r),
                    _ => Ok(V::Null),
                },
            },
            Expr::Binary {
                op: BinaryOp::Or,
                left,
                right,
            } => match eval(left, row)? {
                V::Bool(true) => Ok(V::Bool(true)),
                l => match (l, eval(right, row)?) {
                    (_, V::Bool(true)) => Ok(V::Bool(true)),
                    (V::Bool(false), r) => Ok(r),
                    _ => Ok(V::Null),
                },
            },
            Expr::Binary { op, left, right } => {
                let (l, r) = (eval(left, row)?, eval(right, row)?);
                if matches!(l, V::Null) || matches!(r, V::Null) {
                    return Ok(V::Null);
                }
                if op.is_comparison() {
                    let o = compare(&l, &r)?;
                    return Ok(V::Bool(match op {
                        BinaryOp::Eq => o == Ordering::Equal,
                        BinaryOp::NotEq => o != Ordering::Equal,
                        BinaryOp::Lt => o == Ordering::Less,
                        BinaryOp::LtEq => o != Ordering::Greater,
                        BinaryOp::Gt => o == Ordering::Greater,
                        _ => o != Ordering::Less,
                    }));
                }
                arithmetic(*op, l, r)
            }
            Expr::Not(e) => match eval(e, row)? {
                V::Bool(b) => Ok(V::Bool(!b)),
                V::Null => Ok(V::Null),
                v => Err(format!("NOT {v:?}")),
            },
            Expr::Negative(e) => match eval(e, row)? {
                V::Int(x) => x.checked_neg().map(V::Int).ok_or("overflow".into()),
                V::Float(x) => Ok(V::Float(-x)),
                V::Null => Ok(V::Null),
                v => Err(format!("-{v:?}")),
            },
            Expr::IsNull(e) => Ok(V::Bool(matches!(eval(e, row)?, V::Null))),
            Expr::IsNotNull(e) => Ok(V::Bool(!matches!(eval(e, row)?, V::Null))),
            Expr::Like {
                expr,
                pattern,
                negated,
                case_insensitive,
            } => match (eval(expr, row)?, eval(pattern, row)?) {
                (V::Str(s), V::Str(p)) => {
                    let m = if *case_insensitive {
                        like(&s.to_lowercase(), &p.to_lowercase())
                    } else {
                        like(&s, &p)
                    };
                    Ok(V::Bool(m != *negated))
                }
                _ => Ok(V::Null),
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let v = eval(expr, row)?;
                let mut found = V::Bool(false);
                for item in list {
                    let item = eval(item, row)?;
                    let eq = if matches!(v, V::Null) || matches!(item, V::Null) {
                        V::Null
                    } else {
                        V::Bool(compare(&v, &item)? == Ordering::Equal)
                    };
                    found = match (found, eq) {
                        (V::Bool(true), _) | (_, V::Bool(true)) => V::Bool(true),
                        (V::Bool(false), V::Bool(false)) => V::Bool(false),
                        _ => V::Null,
                    };
                }
                match (found, negated) {
                    (V::Bool(b), true) => Ok(V::Bool(!b)),
                    (f, _) => Ok(f),
                }
            }
            Expr::Case {
                when_then,
                else_expr,
            } => {
                for (when, then) in when_then {
                    if eval(when, row)? == V::Bool(true) {
                        return eval(then, row);
                    }
                }
                eval(else_expr, row)
            }
            Expr::Cast { expr, data_type } => cast(eval(expr, row)?, data_type),
            Expr::Aggregate { .. } => Err("an aggregate in a scalar expression".into()),
        }
    }

    /// SQL's order: numbers with -0.0 = 0.0 and NaN = NaN above every other number, strings by bytes.
    fn compare(l: &V, r: &V) -> Result<Ordering, String> {
        Ok(match (l, r) {
            (V::Int(a), V::Int(b)) => a.cmp(b),
            (V::Float(a), V::Float(b)) => match (a.is_nan(), b.is_nan()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                (false, false) => a.partial_cmp(b).unwrap(),
            },
            (V::Str(a), V::Str(b)) => a.as_bytes().cmp(b.as_bytes()),
            (V::Date(a), V::Date(b)) => a.cmp(b),
            (V::Bool(a), V::Bool(b)) => a.cmp(b),
            _ => return Err(format!("comparing {l:?} with {r:?}")),
        })
    }

    fn arithmetic(op: BinaryOp, l: V, r: V) -> R {
        let overflow = || "overflow".to_string();
        let date = |days: Option<i64>| {
            days.and_then(|d| i32::try_from(d).ok())
                .map(V::Date)
                .ok_or_else(overflow)
        };
        match (op, l, r) {
            (BinaryOp::Plus, V::Int(a), V::Int(b)) => {
                a.checked_add(b).map(V::Int).ok_or_else(overflow)
            }
            (BinaryOp::Minus, V::Int(a), V::Int(b)) => {
                a.checked_sub(b).map(V::Int).ok_or_else(overflow)
            }
            (BinaryOp::Multiply, V::Int(a), V::Int(b)) => {
                a.checked_mul(b).map(V::Int).ok_or_else(overflow)
            }
            (BinaryOp::Modulo, V::Int(_), V::Int(0)) => Err("division by zero".into()),
            (BinaryOp::Modulo, V::Int(a), V::Int(b)) => {
                a.checked_rem(b).map(V::Int).ok_or_else(overflow)
            }
            (BinaryOp::Plus, V::Float(a), V::Float(b)) => Ok(V::Float(a + b)),
            (BinaryOp::Minus, V::Float(a), V::Float(b)) => Ok(V::Float(a - b)),
            (BinaryOp::Multiply, V::Float(a), V::Float(b)) => Ok(V::Float(a * b)),
            (BinaryOp::Divide, V::Float(a), V::Float(b)) => Ok(V::Float(a / b)),
            (BinaryOp::Plus, V::Date(d), V::Int(n)) | (BinaryOp::Plus, V::Int(n), V::Date(d)) => {
                date(i64::from(d).checked_add(n))
            }
            (BinaryOp::Minus, V::Date(d), V::Int(n)) => date(i64::from(d).checked_sub(n)),
            (BinaryOp::Minus, V::Date(a), V::Date(b)) => Ok(V::Int(i64::from(a) - i64::from(b))),
            (BinaryOp::Concat, V::Str(a), V::Str(b)) => Ok(V::Str(a + &b)),
            (op, l, r) => Err(format!("{op:?} on {l:?} and {r:?}")),
        }
    }

    fn cast(v: V, to: &DataType) -> R {
        let failed = || format!("cannot cast {v:?} to {to}");
        let int_range = |t: &DataType| match t {
            DataType::Int16 => Some((i64::from(i16::MIN), i64::from(i16::MAX))),
            DataType::Int32 => Some((i64::from(i32::MIN), i64::from(i32::MAX))),
            DataType::Int64 => Some((i64::MIN, i64::MAX)),
            _ => None,
        };
        match (&v, to) {
            (V::Null, _) => Ok(V::Null),
            (V::Int(x), t) if int_range(t).is_some() => {
                let (lo, hi) = int_range(t).unwrap();
                if (lo..=hi).contains(x) {
                    Ok(V::Int(*x))
                } else {
                    Err(failed())
                }
            }
            (V::Float(x), t) if int_range(t).is_some() => {
                // Half to even, as DuckDB.
                let (lo, hi) = int_range(t).unwrap();
                let r = x.round_ties_even();
                if r >= lo as f64 && r < (hi as f64) + 1.0 && !r.is_nan() {
                    Ok(V::Int(r as i64))
                } else {
                    Err(failed())
                }
            }
            (V::Int(x), DataType::Float64) => Ok(V::Float(*x as f64)),
            (V::Float(_), DataType::Float64) => Ok(v),
            (V::Int(x), DataType::Utf8) => Ok(V::Str(x.to_string())),
            (V::Bool(b), DataType::Utf8) => Ok(V::Str(b.to_string())),
            (V::Date(d), DataType::Utf8) => Ok(V::Str(format_date32(*d))),
            (V::Str(_), DataType::Utf8) => Ok(v),
            (V::Str(s), DataType::Int64) => s.parse().map(V::Int).map_err(|_| failed()),
            (V::Str(s), DataType::Float64) => s.parse().map(V::Float).map_err(|_| failed()),
            (V::Bool(_), DataType::Boolean) | (V::Date(_), DataType::Date32) => Ok(v),
            _ => Err(format!("the reference doesn't cast {v:?} to {to}")),
        }
    }

    /// LIKE with `%` and `_` and no escape character, over characters (newlines included).
    pub fn like(s: &str, p: &str) -> bool {
        let (s, p): (Vec<char>, Vec<char>) = (s.chars().collect(), p.chars().collect());
        // m[i][j]: the first i characters of s match the first j of p.
        let mut m = vec![vec![false; p.len() + 1]; s.len() + 1];
        m[0][0] = true;
        for j in 1..=p.len() {
            m[0][j] = p[j - 1] == '%' && m[0][j - 1];
        }
        for i in 1..=s.len() {
            for j in 1..=p.len() {
                m[i][j] = match p[j - 1] {
                    '%' => m[i][j - 1] || m[i - 1][j],
                    '_' => m[i - 1][j - 1],
                    c => m[i - 1][j - 1] && s[i - 1] == c,
                };
            }
        }
        m[s.len()][p.len()]
    }
}

// The table: t(a BIGINT, b BIGINT, f DOUBLE, s VARCHAR, d DATE, p BOOLEAN), every column nullable.

const INTS: &[i64] = &[0, 1, -1, 2, 7, -7, 3, i64::MAX, i64::MIN, 4_000_000_000];
const FLOATS: &[f64] = &[
    0.0,
    -0.0,
    1.5,
    -2.5,
    0.5,
    1e308,
    f64::NAN,
    f64::INFINITY,
    2.0,
];
const STRINGS: &[&str] = &[
    "",
    "a",
    "ab",
    "A%",
    "a_b",
    "x\ny",
    "Äb",
    "äB",
    "12",
    "-3",
    "1.5",
    "2024-02-29",
    r"a\b",
    "_",
    r"\_",
];
const DATES: &[i32] = &[19_782, 0, -1, 2_932_896, -719_528, 19_723];

fn nullable<T: std::fmt::Debug + Clone + 'static>(
    values: &'static [T],
) -> impl Strategy<Value = Option<T>> {
    prop_oneof![1 => Just(None), 6 => proptest::sample::select(values).prop_map(Some)]
}

type Row = (
    Option<i64>,
    Option<i64>,
    Option<f64>,
    Option<&'static str>,
    Option<i32>,
    Option<bool>,
);

fn rows() -> impl Strategy<Value = Vec<Row>> {
    let row = (
        nullable(INTS),
        nullable(INTS),
        nullable(FLOATS),
        nullable(STRINGS),
        nullable(DATES),
        prop_oneof![1 => Just(None), 3 => any::<bool>().prop_map(Some)],
    );
    proptest::collection::vec(row, 0..24)
}

fn batch(rows: &[Row]) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
        Field::new("f", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("d", DataType::Date32, true),
        Field::new("p", DataType::Boolean, true),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(rows.iter().map(|r| r.0).collect::<Int64Array>()),
        Arc::new(rows.iter().map(|r| r.1).collect::<Int64Array>()),
        Arc::new(rows.iter().map(|r| r.2).collect::<Float64Array>()),
        Arc::new(rows.iter().map(|r| r.3).collect::<StringArray>()),
        Arc::new(rows.iter().map(|r| r.4).collect::<Date32Array>()),
        Arc::new(rows.iter().map(|r| r.5).collect::<BooleanArray>()),
    ];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

fn row_values(r: &Row) -> Vec<V> {
    fn opt<T>(v: Option<T>, f: impl FnOnce(T) -> V) -> V {
        v.map_or(V::Null, f)
    }
    vec![
        opt(r.0, V::Int),
        opt(r.1, V::Int),
        opt(r.2, V::Float),
        opt(r.3, |s| V::Str(s.to_string())),
        opt(r.4, V::Date),
        opt(r.5, V::Bool),
    ]
}

/// An engine result as reference values.
fn values(a: &ArrayRef) -> Vec<V> {
    let nulls = a.logical_nulls();
    (0..a.len())
        .map(|i| {
            if nulls.as_ref().is_some_and(|n| n.is_null(i)) {
                return V::Null;
            }
            match a.data_type() {
                DataType::Boolean => V::Bool(a.as_boolean().value(i)),
                DataType::Int64 => V::Int(a.as_primitive::<Int64Type>().value(i)),
                DataType::Int32 => V::Int(i64::from(a.as_primitive::<Int32Type>().value(i))),
                DataType::Int16 => V::Int(i64::from(a.as_primitive::<Int16Type>().value(i))),
                DataType::Float64 => V::Float(a.as_primitive::<Float64Type>().value(i)),
                DataType::Utf8 => V::Str(a.as_string::<i32>().value(i).to_string()),
                DataType::Date32 => V::Date(a.as_primitive::<Date32Type>().value(i)),
                t => panic!("no reference value for {t}"),
            }
        })
        .collect()
}

// A generator of well-typed SQL, by the type it must produce.

#[derive(Debug, Clone, Copy)]
enum T {
    Int,
    Float,
    Str,
    Date,
    Bool,
}

fn leaf(t: T) -> BoxedStrategy<String> {
    let pick = |xs: &'static [&'static str]| {
        proptest::sample::select(xs)
            .prop_map(str::to_string)
            .boxed()
    };
    match t {
        T::Int => pick(&[
            "a",
            "b",
            "0",
            "1",
            "-1",
            "7",
            "9223372036854775807",
            "(-9223372036854775808)",
            "NULL",
        ]),
        T::Float => pick(&["f", "0.0", "-0.0", "1.5", "-2.5", "1e308"]),
        T::Str => pick(&[
            "s", "''", "'a'", "'a_b'", "'12'", "'%b'", "'A%'", "'_'", "'ä%'", r"'a\b'", r"'%\'",
            r"'\_'", r"'%\%'",
        ]),
        T::Date => pick(&["d", "DATE '2024-02-29'", "DATE '1970-01-01'"]),
        T::Bool => pick(&["p", "TRUE", "FALSE", "NULL"]),
    }
}

/// `template` with each `{}` replaced by an expression of the next type in `types`.
fn fill(template: &'static str, types: &'static [T], depth: u32) -> BoxedStrategy<String> {
    let parts: Vec<BoxedStrategy<String>> =
        types.iter().map(|&t| typed_sql(t, depth - 1)).collect();
    parts
        .prop_map(move |values| {
            let mut out = template.to_string();
            for v in values {
                out = out.replacen("{}", &v, 1);
            }
            out
        })
        .boxed()
}

fn typed_sql(t: T, depth: u32) -> BoxedStrategy<String> {
    if depth == 0 {
        return leaf(t);
    }
    use T::*;
    let compound = match t {
        Int => prop_oneof![
            fill("({} + {})", &[Int, Int], depth),
            fill("({} - {})", &[Int, Int], depth),
            fill("({} * {})", &[Int, Int], depth),
            fill("({} % {})", &[Int, Int], depth),
            fill("(- {})", &[Int], depth),
            fill("CAST({} AS BIGINT)", &[Float], depth),
            fill("CAST({} AS INT)", &[Int], depth),
            fill("CAST({} AS SMALLINT)", &[Float], depth),
            fill("CAST({} AS BIGINT)", &[Str], depth),
            fill("({} - {})", &[Date, Date], depth),
            fill("CASE WHEN {} THEN {} ELSE {} END", &[Bool, Int, Int], depth),
            fill(
                "CASE WHEN {} THEN {} WHEN {} THEN {} END",
                &[Bool, Int, Bool, Int],
                depth
            ),
            fill(
                "CASE {} WHEN {} THEN {} ELSE {} END",
                &[Int, Int, Int, Int],
                depth
            ),
        ]
        .boxed(),
        Float => prop_oneof![
            fill("({} + {})", &[Float, Float], depth),
            fill("({} * {})", &[Float, Int], depth),
            fill("({} - {})", &[Int, Float], depth),
            fill("({} / {})", &[Int, Int], depth),
            fill("({} / {})", &[Float, Float], depth),
            fill("(- {})", &[Float], depth),
            fill("CAST({} AS DOUBLE)", &[Int], depth),
            fill("CAST({} AS DOUBLE)", &[Str], depth),
            fill(
                "CASE WHEN {} THEN {} ELSE {} END",
                &[Bool, Float, Int],
                depth
            ),
        ]
        .boxed(),
        Str => prop_oneof![
            fill("({} || {})", &[Str, Str], depth),
            fill("CAST({} AS VARCHAR)", &[Int], depth),
            fill("CAST({} AS VARCHAR)", &[Bool], depth),
            Just("CAST(d AS VARCHAR)".to_string()).boxed(),
            fill("CASE WHEN {} THEN {} END", &[Bool, Str], depth),
        ]
        .boxed(),
        Date => prop_oneof![
            fill("({} + {})", &[Date, Int], depth),
            fill("({} - {})", &[Date, Int], depth),
            fill("({} + {})", &[Int, Date], depth),
        ]
        .boxed(),
        Bool => prop_oneof![
            fill("({} = {})", &[Int, Int], depth),
            fill("({} < {})", &[Float, Float], depth),
            fill("({} >= {})", &[Float, Int], depth),
            fill("({} <> {})", &[Str, Str], depth),
            fill("({} <= {})", &[Date, Date], depth),
            fill("({} > {})", &[Bool, Bool], depth),
            fill("({} AND {})", &[Bool, Bool], depth),
            fill("({} OR {})", &[Bool, Bool], depth),
            fill("(NOT {})", &[Bool], depth),
            fill("({} IS NULL)", &[Int], depth),
            fill("({} IS NOT NULL)", &[Str], depth),
            fill("({} LIKE {})", &[Str, Str], depth),
            fill("({} NOT ILIKE {})", &[Str, Str], depth),
            fill("({} IN ({}, {}, NULL))", &[Int, Int, Int], depth),
            fill("({} NOT IN ({}, {}))", &[Float, Float, Int], depth),
            fill("({} BETWEEN {} AND {})", &[Int, Int, Int], depth),
            fill(
                "CASE WHEN {} THEN {} ELSE {} END",
                &[Bool, Bool, Bool],
                depth
            ),
        ]
        .boxed(),
    };
    prop_oneof![1 => leaf(t), 3 => compound].boxed()
}

fn any_expr() -> impl Strategy<Value = String> {
    prop_oneof![
        typed_sql(T::Int, 3),
        typed_sql(T::Float, 3),
        typed_sql(T::Str, 2),
        typed_sql(T::Date, 2),
        typed_sql(T::Bool, 3),
    ]
}

/// Binds `SELECT <sql> FROM t` (`None` if the binder refuses it: a string compared with a date that isn't one, say).
fn bind(sql: &str, rows: &[Row]) -> Option<(Expr, Arc<quarry::plan::PlanSchema>)> {
    let mut catalog = Catalog::new();
    catalog
        .register("t", Arc::new(MemTable::from_batch(batch(rows))))
        .unwrap();
    match sql::plan(&catalog, &format!("SELECT {sql} FROM t")).ok()? {
        LogicalPlan::Projection(p) => Some((p.exprs[0].clone(), p.input.schema().clone())),
        other => panic!("expected a projection, got {other}"),
    }
}

fn check(sql: &str, rows: &[Row], split: usize) -> Result<(), TestCaseError> {
    let Some((expr, schema)) = bind(sql, rows) else {
        return Ok(());
    };
    let b = batch(rows);
    let engine = evaluate(&expr, &schema, &b);
    // Row by row: the first row that fails fails the whole column, as it does in the engine.
    let expected: Result<Vec<V>, String> = rows
        .iter()
        .map(|r| reference::eval(&expr, &row_values(r)))
        .collect();
    match (&engine, &expected) {
        (Ok(a), Ok(want)) => {
            prop_assert_eq!(a.data_type(), &expr.data_type(&schema), "{}", sql);
            let got = values(a);
            prop_assert!(
                &got == want,
                "{sql}\nrows {rows:?}\nengine    {got:?}\nreference {want:?}"
            );
        }
        (Err(_), Err(_)) => {}
        (Ok(a), Err(e)) => {
            prop_assert!(
                false,
                "{sql}\nrows {rows:?}\nengine {:?}\nreference fails: {e}",
                values(a)
            )
        }
        (Err(e), Ok(want)) => prop_assert!(
            false,
            "{sql}\nrows {rows:?}\nengine fails: {e}\nreference {want:?}"
        ),
    }
    // Two batches (sliced, so the arrays have offsets) give the same values, or fail when the whole does.
    let split = split.min(b.num_rows());
    let parts = [b.slice(0, split), b.slice(split, b.num_rows() - split)];
    let mut joined = Vec::new();
    for part in &parts {
        match evaluate(&expr, &schema, part) {
            Ok(a) => joined.extend(values(&a)),
            Err(e) => {
                prop_assert!(
                    engine.is_err(),
                    "{sql}: a slice fails ({e}) but the whole batch doesn't"
                );
                return Ok(());
            }
        }
    }
    if let Ok(whole) = &engine {
        prop_assert!(
            joined == values(whole),
            "{sql}: slices differ from the whole batch"
        );
    } else {
        prop_assert!(false, "{sql}: the whole batch fails but no slice does");
    }
    Ok(())
}

/// 2,000 cases by default; `PROPTEST_CASES=100000 cargo test --release` searches longer.
fn cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(cases()))]

    #[test]
    fn the_engine_agrees_with_the_reference(sql in any_expr(), rows in rows(), split in 0usize..24) {
        check(&sql, &rows, split)?;
    }
}

#[test]
fn the_reference_like_matches_by_characters() {
    use reference::like;
    assert!(like("x\ny", "x_y"));
    assert!(like("Äb", "_b"));
    assert!(like(r"a\b", r"a\b"));
    assert!(!like("a_b", r"a\_b"));
    assert!(like("", "%"));
    assert!(!like("", "_"));
    assert!(like("abc", "a%c"));
}

#[test]
fn the_generator_reaches_errors_and_values() {
    // Some well-typed expressions fail (overflow, % 0, bad casts) on these rows and some don't; both must occur, or
    // the property above checks only one side.
    let rows: Vec<Row> = vec![
        (
            Some(i64::MAX),
            Some(0),
            Some(1.5),
            Some("12"),
            Some(0),
            Some(true),
        ),
        (
            Some(7),
            Some(2),
            Some(f64::NAN),
            Some("a"),
            Some(19_782),
            None,
        ),
    ];
    let failing = [
        "(a + 1)",
        "(a % b)",
        "CAST(s AS BIGINT)",
        "CAST(f AS BIGINT)",
    ];
    for sql in failing {
        let (e, s) = bind(sql, &rows).unwrap();
        assert!(evaluate(&e, &s, &batch(&rows)).is_err(), "{sql}");
    }
    let fine = [
        "(b = 0 OR (a % b) = 1)",
        "CASE WHEN (b <> 0) THEN (a % b) END",
        "(f / 0.0)",
    ];
    for sql in fine {
        let (e, s) = bind(sql, &rows).unwrap();
        assert!(evaluate(&e, &s, &batch(&rows)).is_ok(), "{sql}");
    }
}
