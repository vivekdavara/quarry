# Walkthrough

Questions an interviewer is likely to ask about Quarry, with short answers grounded in the code. Day 1 covers the
readers, the logical plan and the binder; later days add sections.

## The big picture

**What is Quarry, in one sentence?**
A SQL engine for analytical queries over Parquet and CSV files: SQL text → `sqlparser` AST → a bound, typed
`LogicalPlan` → (from day 2) vectorized operators over Arrow record batches.

**Why Arrow?**
Arrow is a columnar in-memory format with compute kernels (comparison, arithmetic, filtering, casting) that work on
a whole column at a time, and the `parquet` crate decodes straight into it. A query engine's operators then pass
`RecordBatch`es (a schema plus one array per column) and evaluate expressions column by column, which is what
makes analytical engines fast. Quarry writes the engine and uses Arrow as the memory format and the kernels.

**Why not write your own SQL parser?**
SQL's grammar is large (`sqlparser` 0.63's `Expr` enum alone has 64 variants) and parsing it well is a solved problem. The parts
that decide what a query *means* (which `a`, which type, which aggregate, what `ORDER BY 2` refers to) come after
parsing, in the binder, and those are written here: `crates/quarry/src/sql/`.

## Binding

**What does the binder do that the parser doesn't?**
The parser only knows syntax: `a + b` is a `BinaryOp` of two identifiers. The binder (`sql/binder.rs`,
`sql/expr.rs`) knows the tables: it resolves `a` to column 0 of `t`, finds that `a` is `Int64` and `b` is `Int32`,
inserts `CAST(t.b AS Int64)`, gives the sum the type `Int64`, and refuses `a + s` when `s` is a string. Every
error a user can make is found there, before any data is read.

**Why do bound expressions hold column indices instead of names?**
Names are ambiguous and indices aren't. `SELECT t.id, u.id FROM t, u` has two columns named `id`; after binding they
are `Column(0)` and `Column(2)` of the join's output, and nothing downstream looks a name up again
(`plan/expr.rs`, `Expr::Column(usize)`). Each plan node's schema still carries names and qualifiers, for printing
and for the result's column names.

**How do you resolve a column name?**
`resolve_column` in `sql/expr.rs`: compare case-insensitively against every field in scope (and, for `t.a`, only
fields whose qualifier is `t`). Exactly one match is the answer; none lists the columns there are; more than one
lists the candidates ("column a is ambiguous: t.a, u.a"). A table is known by its alias when it has one, so `FROM t
AS x` makes `t.a` an error, as in PostgreSQL.

**Why are casts explicit in the plan?**
So the executor never decides a type. `b = a` with `b: Int32` and `a: Int64` binds to `CAST(t.b AS Int64) = t.a`;
the day-2 evaluator then only ever compares two arrays of one type, which is what Arrow's kernels want, and the
printed plan shows exactly what will run. The rules are in DESIGN.md ("The common type") and `types.rs`
(`common_type`); `coerce_all` in `sql/expr.rs` applies them to comparisons, `BETWEEN`, `IN` and `CASE`.

**How does `l_shipdate <= '1998-09-02'` work if a string can't be compared with a date?**
Rule 5: when one side is a `Date32` and the other is a string *literal*, the binder parses the literal as a date
at bind time (`coerce_all`), so the plan says `DATE '1998-09-02'`. A string *column* gets no such treatment, and a
literal that isn't a real date (`'1998-02-30'`) is an error. Dates are parsed strictly by `types::parse_date32`
(exactly `YYYY-MM-DD`, real calendar days), tested over every day from 1600 to 2400 and against Arrow's cast.

**Why is `/` always float division?**
DuckDB's rule (`5 / 2` is 2.5), and DuckDB is the oracle for day 4's differential tests; PostgreSQL truncates.
Picking one and writing it down matters more than which: the binder casts both sides to `Float64`, so the
executor can't get it wrong. The property test checks that every division's operands are `Float64`.

## Aggregation and ordering

**How do you check `GROUP BY`? Why is `SELECT a + 1 ... GROUP BY a + 1` fine but `SELECT a + b ... GROUP BY a`
not?**
Every select, `HAVING` and `ORDER BY` expression is bound over the FROM scope first, aggregates included. Then the
Aggregate node is built (its output is the group expressions, then the distinct aggregates) and each expression
is rewritten over that output by `rewrite_over_aggregate` in `sql/binder.rs`: top down, a subexpression *equal* to
a group expression becomes that group column, an aggregate becomes its column, and any column left over is an
error. `a + 1` equals the group expression, so it becomes `Column(0)`; in `a + b`, `a` matches but `b` is left over
("column t.b must appear in GROUP BY or be used in an aggregate"). Equality is structural on bound expressions,
which is why literals compare by bits (`ScalarValue`'s `PartialEq`).

**If `sum(x)` appears in the select list, `HAVING` and `ORDER BY`, is it computed three times?**
No. The binder collects aggregates from all three clauses and keeps each distinct one once, in order of first
appearance; the rewrite turns every occurrence into the same column. The test
`having_and_aggregates_shared_between_clauses` pins it.

**How does `ORDER BY` work when the key isn't in the select list?**
Each key is an ordinal (`ORDER BY 2`), a bare name equal to an output column, or an expression over the input. An
expression equal to a select item sorts by that item; any other is appended to the projection as a hidden column
(`__order_0`), the Sort uses it, and a final Projection drops it. With `DISTINCT` that's an error, because the
hidden column would change which rows are distinct. Bare output names win over input columns (`SELECT b AS a, a
AS b ... ORDER BY a` sorts by input `b`), and an output name inside an expression doesn't count, both as in
PostgreSQL.

**Why is `DISTINCT` an Aggregate?**
`SELECT DISTINCT x, y` is `GROUP BY x, y` with no aggregates, so one operator (day 3's hash aggregate) does both.

## The readers

**What does a scan return, and what was subtle about Parquet projections?**
`TableProvider::scan(projection, batch_size)` returns an iterator of `RecordBatch`es holding the projected columns,
in the projection's order. The `parquet` crate's `ProjectionMask` selects columns but yields them in *file* order,
once each, so a projection `[3, 0, 3]` comes back as `[0, 3]`. `ParquetTable::scan` reads the distinct columns and
reorders them (`datasource/parquet_table.rs`); the test `parquet_projection_keeps_the_order_asked_for` fails
without it.

**Why does an empty projection matter?**
`COUNT(*)` needs no columns but does need row counts. Arrow allows a `RecordBatch` with zero columns and an explicit
row count; the tests check every reader yields those (`parquet_empty_projection_still_counts_rows`).

**How does CSV schema inference work, and how can it go wrong?**
`arrow-csv` reads the first 1,000 rows and picks, per column, the narrowest of boolean, integer, float, date or
timestamp that every value fits, else string. Quarry then corrects three things, each tested: a column with no
values comes back as `Null`, and one of date-times as `Timestamp`, and both become strings (`only_design_types`);
and a "date" column is only checked against the pattern, so one `2024-02-30` would make every scan fail: the
sampled rows are re-read as text and the column becomes strings unless each value is a real date
(`real_dates_only`, found by the review). A value after the sample that doesn't fit still fails the scan that
reaches it, so `CsvOptions { infer_rows: None, .. }` reads the whole file to infer.

## Testing

**How do you test a binder when nothing executes yet?**
Three ways. `tests/binder.rs` binds a query per DESIGN.md rule and compares the printed plan, which shows every
cast and every column, or the error and its message. `tests/binder_properties.rs` generates thousands of random
expressions and checks the binder against a model of the type rules written separately in the test. And the plan's
printing is itself tested (`plan/expr.rs`), so the plan strings are a reliable view of the tree.

**What does the property test check, and how do you know it's strong enough?**
For `SELECT <expr> FROM t`: no panics; an expression the model types binds to exactly that type and nullability;
one the model refuses fails with an error; every operator's operands have one type. To check the test itself, I
mutated the binder and ran it: dropping the casts and a wrong nullability were caught, but integer division
*wasn't* at first, because the check only required both operands to match each other. Division's operands must now
be `Float64`, and that mutant fails it.

**Why two generators?**
A random grammar over every operator mostly makes ill-typed trees (322 typable of 2,000 at the fixed seed), which
tests the refusals well and the deep casts badly. The type-directed generator builds well-typed trees three
operators deep, where casts nest inside `CASE`s inside arithmetic.

## Limits and next steps

**What doesn't it support, and what happens if you try?**
Subqueries, `WITH`, set operations, window functions, `RIGHT`/`FULL` joins, intervals and scalar functions other
than `CAST`. Each is a `QuarryError::Unsupported` naming what it was, never a panic or a wrong plan
(`unsupported_sql_is_named`).

**What comes next?**
Day 2 executes the plan: a physical operator per node and expression evaluation with Arrow kernels. Day 3 adds the
hash join, predicate pushdown into Parquet row groups (the footer is already read once per table; day 3 reads its
per-column min and max) and optimizer rules.
Day 4 compares every result with DuckDB's on TPC-H-style data.
