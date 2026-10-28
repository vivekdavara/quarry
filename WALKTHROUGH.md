# Walkthrough

Questions an interviewer is likely to ask about Quarry, with short answers grounded in the code. Day 1 covers the
readers, the logical plan and the binder; day 2 execution; day 3 joins, the optimizer, row-group pruning and the top-k;
later days add sections.

## The big picture

**What is Quarry, in one sentence?**
A SQL engine for analytical queries over Parquet and CSV files: SQL text → `sqlparser` AST → a bound, typed
`LogicalPlan` → vectorized operators over Arrow record batches.

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

## Execution (day 2)

**How does a plan run?**
`exec::execute` (`exec/mod.rs`) turns each plan node into an iterator of `RecordBatch`es that pulls from its input's
iterator: the Volcano model, but a batch of up to 8,192 rows at a time instead of a row. A Scan is the table's own
iterator; Filter, Projection and Limit map each batch as it comes; Sort and Aggregate have to see everything, so they
drain their input first and then hand out their result in batches. Because each node only pulls, `LIMIT 10` over a
billion-row file stops reading after the batch that completes its ten rows (`limit_stops_reading` proves it with a
table whose second batch is an error).

**Why batches rather than rows?**
Per-row interpretation pays a dispatch (which operator? which type?) for every value. With batches, an expression is
dispatched once per batch and the work inside is a tight loop over a typed array (Arrow's kernels), which the
compiler vectorizes. Batches are also small enough to stay in cache, which whole columns wouldn't be.

**What does "vectorized evaluation" mean in the code?**
`exec::evaluate` (`exec/expr.rs`) takes a bound `Expr` and a batch and returns a whole column. `a + b` is one call to
Arrow's `numeric::add` on two arrays. A literal is a one-row `Value::Scalar`, and Arrow's kernels accept a column and
a scalar (the `Datum` trait), so `l_quantity < 24` never builds a column of 24s, and `1 + 2` is computed once per
batch.

**How does `WHERE b <> 0 AND a % b = 1` avoid dividing by zero, if both sides are computed a column at a time?**
It wouldn't, if both were. `AND` evaluates its left side first; the rows where that is `FALSE` are decided. Only the
other rows are taken out of the batch (`take_rows`), the right side is evaluated on them, and the results are put
back in place with NULLs at the decided rows (`scatter`, via Arrow's `interleave`), which three-valued `AND` then
ignores. `OR` does the same with `TRUE`, and `CASE` evaluates each `THEN` only on the rows whose `WHEN` took them.
DuckDB behaves the same way, which I checked; PostgreSQL documents the same guarantee for `CASE`.

**Why does `%` by zero fail but `/` by zero return infinity?**
To match DuckDB 2.0, the oracle for day 4: `/` is always float division in Quarry (the binder casts both sides to
Float64), and IEEE floats have an answer for x/0. Integer `%` has none, so it is an error, as is integer overflow.
DESIGN.md's "Execution" table lists every such rule; I checked each one on DuckDB before writing it.

**Why does a cast that fails raise an error instead of returning NULL?**
Two reasons. DuckDB's `CAST` fails too (`TRY_CAST` is the one that returns NULL). And the plan's nullability analysis
assumes a cast doesn't create NULLs: `CAST(id AS INT)` of a non-null column is typed non-null. Arrow's cast has a
"safe" mode that makes NULLs; Quarry uses the strict one. One more difference handled by hand: Arrow truncates a
float cast to an integer and DuckDB rounds half to even, so Quarry rounds first.

**How does GROUP BY work?**
`exec/aggregate.rs`. Each batch's group-key columns are encoded with Arrow's row format (`RowConverter`): every row
becomes a byte string that compares and hashes as the values do, whatever the types and however many keys. A
`HashMap<Vec<u8>, usize>` maps those bytes to a group number, so grouping is one lookup per row. Each aggregate has an
accumulator with a slot per group (`Count`, `IntSum`, `FloatSum`, `MinMax`, `Distinct`); after the input is drained,
the group keys are decoded back into columns and each accumulator produces its column.

**Why are floats "normalized" before grouping and comparing?**
Arrow orders floats by IEEE 754's total order, which puts -0.0 before 0.0 and distinguishes NaNs by their bits. SQL
(and DuckDB) says -0.0 = 0.0 and NaN = NaN. So before comparing, grouping, deduplicating or sorting, Quarry maps
-0.0 to 0.0 and every NaN to one NaN (`normalize_floats`); the property tests caught every mutant that skipped it.

**Why sum integers in an i128?**
So a partial sum can't overflow: `[i64::MAX, 1, -1]` sums to `i64::MAX` even though the first two overflow an i64
on the way. Only the final total must fit in an Int64; if it doesn't, the query fails (DuckDB returns a 128-bit
integer there instead; Quarry's SUM type is Int64). AVG keeps the same i128 and divides at the end, so integers above
2^53 aren't rounded before they're added.

**What do MIN and MAX keep for each group?**
The row-format bytes of the best value so far. Comparing bytes works for every type the row format encodes
(strings, dates, booleans, any integer width), so one accumulator covers them all, and the bytes are decoded back to
the argument's type at the end.

**`SELECT count(*) FROM t WHERE FALSE` returns a row. Why, and why doesn't the same query with GROUP BY?**
SQL's rule: without GROUP BY, an aggregate query is one group, even over no rows, so the answer is one row with
`count(*) = 0` and `sum(x)` NULL. With GROUP BY, groups come from rows; no rows make no groups. `Groups::len` is 1
from the start when there is no GROUP BY.

**What does the final schema check catch?**
`execute` rebuilds every result batch against the plan's output schema, and Arrow refuses a batch whose array types
differ from the schema or whose non-nullable column holds a NULL. So if the binder's type or nullability analysis
were wrong, the query would fail loudly instead of returning a wrong answer.

**How do you know the vectorized evaluator computes the right thing?**
`tests/eval_properties.rs` generates well-typed SQL expressions (every operator, `CASE`, `IN`, `LIKE`, casts, date
arithmetic) over random rows full of edge values (i64::MIN and MAX, -0.0, NaN, infinity, backslashes, newlines) and
evaluates each twice: with the engine, and with an interpreter in the test that applies DESIGN.md's rules one row at
a time over plain Rust values, sharing no code with the engine. They must agree on every value, bit for bit, or both
fail; and two slices of the batch must give the same as the whole. `tests/aggregate_properties.rs` does the same for
random GROUP BY queries against a grouping written in plain Rust. Then I mutated the engine (no -0.0 normalization,
AND evaluating every row, CASE evaluating untaken branches, truncating casts, `\` as a LIKE escape, `i64::MIN % -1`
as 0; six mutations of the aggregates) and each made a property fail. One survived at first: the LIKE backslash
mutant, because the generator rarely paired a backslash string with a backslash pattern; adding `_`, `\_` and
`%\%` to its data fixed that.

**Why does an expression over an empty batch return nothing, even `9223372036854775807 + 1`?**
Because an expression is computed for rows. Row by row, a failing constant fails only if a row exists to compute it
for; the property test's reference works that way, and the engine first disagreed on empty tables (it computed the
scalar once and failed). Now `evaluate` returns an empty column for an empty batch without evaluating.

## Joins and the optimizer (day 3)

**How does a join run?**
`exec/join.rs`. `ON` is split into its conjuncts (`Expr::conjuncts`). A conjunct `l = r` where `l` reads only the
left input's columns and `r` only the right's (either way round) is a key; the rest is the residual. The right input
is the build side: read whole, its keys evaluated, encoded in Arrow's row format and grouped by a `HashMap<bytes,
group>`, then a counting sort lays each group's rows out contiguously in input order (`KeyTable { starts, rows }`).
The left input streams: each left row's key looks up its group, those build rows are its candidates, and the residual
is evaluated on the candidate pairs, at most `batch_size` pairs at a time (`JoinStream::step`). With no key at all
(`CROSS JOIN`, `ON o.qty > c.rank`) every build row is a candidate: a nested loop through the same code.

**How do NULLs and floats behave in join keys?**
As `=` does. A row with a NULL in any key column goes in no group and looks nothing up (`non_null_rows`), because
`NULL = NULL` is NULL, not TRUE. Floats are normalized before encoding (`normalize_floats`: -0.0 to 0.0, every NaN to
one NaN), the same rule GROUP BY uses, so -0.0 meets 0.0 and NaN meets NaN; the values that come out are the input's
own. One `RowConverter` encodes both sides, so equal keys are equal bytes.

**What order do join results come in?**
The left input's order, each left row's matches in the build side's order, and a LEFT join's unmatched left row in
its own place, with NULLs on the right. That holds at every batch size, which is why `execute.rs` can compare joins
row for row at batch sizes 1, 2, 3 and 8192, and `join_properties.rs` can compare with a plain nested loop in order.
The LEFT case is the subtle one: a left row whose candidates are split across chunks isn't known to be unmatched
until its last chunk, so `step` emits the NULL row only for rows whose candidates are all done (`done_rows`).

**Why is the right side always the build side?**
Simplicity and the order guarantee. Building the smaller side is faster, but for an inner join it would make the
output follow the other input's order. The statistics exist (`TableStatistics::num_rows`); swapping is a day-4 option.

**What does the optimizer do?**
`optimizer::optimize` runs three rules once each: constant folding, predicate pushdown, projection pushdown (DESIGN.md,
"The optimizer"). Each keeps the plan's output columns exactly, which is what lets `execute.rs` run every query bound
and optimized and require the same rows or the same error.

**How does constant folding avoid turning a lazy error into an eager one?**
`fold_constant` evaluates a column-free subexpression over a one-row, zero-column batch; if that fails (`7 % 0`), the
subexpression is left in the plan to fail only when a row reaches it, so `CASE WHEN a > 0 THEN 1 ELSE 7 % 0 END` over
positive `a`s still works. Its operands still fold (`7 % (2 - 2)` becomes `7 % 0`). The AND/OR simplifications are
the ones laziness makes exact: `FALSE AND x` never computed `x`, so it is FALSE; `TRUE AND x` and `x AND TRUE` computed
`x` on every row anyway, so they are `x`; `x AND FALSE` stays, because `x` might fail.

**How does predicate pushdown turn a comma join into a hash join?**
`FROM t, u WHERE t.a = u.a AND u.y > 3` binds as a filter over a cross join. For an inner or cross join, WHERE and ON
mean the same, so `filter_into_join` moves the whole predicate into the ON (a cross join with an ON becomes inner);
`push_join_condition` then sends each conjunct that reads one side only down to that side as a filter. What's left
in the ON (`t.a = u.a`) is a key, so the join is a hash join.

**Why is a LEFT join treated differently?**
Because moving a condition must not change which left rows come out. A WHERE conjunct on the left side can go below
(a left row it drops only makes rows it drops), but one on the right must stay above: it sees the NULLs of unmatched
rows (`WHERE u.x IS NULL` is the anti-join idiom). In the ON it's the reverse: a conjunct on the right can filter the
build side (a build row it rejects can't match), but one on the left stays, since it decides matching, not existence.

**Doesn't pushdown change which rows a failing condition sees?**
Yes, and that's documented. `ON o.customer = c.name AND 5 % (c.rank - 4) = 1` never divides by zero in the hash join
alone (the residual runs only on key matches), but pushed down it filters every customer and fails. I checked DuckDB
2.0: it fails the same query, for the same reason. The test
`the_rest_of_on_is_computed_only_for_pairs_with_equal_keys_until_it_is_pushed_down` pins both behaviours.

**How does projection pushdown work with index-based columns?**
`columns.rs` walks down with the set of output columns the parent needs; each node adds its own expressions' columns;
a scan reads exactly the union. Every node returns a `ColumnMap` (old output position to new), and its parent rewrites
its expressions through it (`Expr::map_columns`). Projections and aggregates keep all their outputs, so the renumbering
stops at them and the result's columns never change. `SELECT count(*) FROM t` ends with `projection=[]`: the Parquet
reader decodes no column and only counts rows.

**How does row-group pruning work, and why is it safe?**
`datasource/pruning.rs` rewrites each filter the scan was given into a condition on statistics (`x > 5` becomes
`max(x) > 5`, `x = 5` becomes `min(x) <= 5 AND max(x) >= 5`, `IS NULL` a null count above 0) and evaluates it with the
engine's own evaluator over a batch with one row per row group. A group is skipped only where the result is FALSE; a
missing statistic is NULL and keeps it. It's safe because it's only ever an optimization: the Filter node stays above
the scan and checks every row read, so the property test (`tests/pruning.rs`) can demand the same rows with and
without pruning for random data and filters.

**What's the catch with floats?**
NaN. Writers leave it out of min and max, and NaN is the greatest value here, so a group with max 1.0 can still hold a
NaN that `x > 5.0` keeps. For float columns only the min is used: `<`, `<=`, and `=` against a number. The first mutant
I ran (allowing `>` on floats) is exactly that bug, and the property test catches it.

**What is the top-k?**
`ORDER BY ... LIMIT n OFFSET m` keeps only the best `k = n + m` rows (`exec::top_k`): batches are buffered to `2k` rows,
then cut back to `k` by Arrow's `lexsort_to_indices` with a limit. Each row carries its input position as the last key,
which makes the order total, so the result is exactly the full sort's first `k`, ties included. Buffering to `2k`
before cutting means each row takes part in a constant number of `O(k log k)` sorts, so a large `k` doesn't go
quadratic.

## What went wrong on day 2

**What did the review find?**
Three bugs, from about 230 queries diffed against DuckDB at four batch sizes (README, "Review of day 2"). The most
instructive: the fix for -0.0 had overshot. Normalizing floats was right for *comparing* (-0.0 = 0.0), but the
normalized bytes were also what MIN, MAX and the GROUP BY keys decoded into the result, so a lone -0.0 came back as
0.0; `1 / min(f)` showed it (`inf` instead of DuckDB's `-inf`). Now the normalized form is only the comparison and
hash key; each group keeps its first row's key as it was, and MIN/MAX keep the winning value itself, the last of
equal ones, which is what DuckDB returns too.

**What was the LIMIT bug, and why didn't the batch-size tests catch it?**
`SELECT 10 % (id - 3) FROM t LIMIT 2` should never compute row 3, and at batch sizes 1 and 2 it didn't; at 3 and up
the projection computed the whole first batch, row 3 included, before the LIMIT sliced it, and failed. The tests ran
every query at several batch sizes, but none had an error past the limit. The fix applies a LIMIT below projections
(a projection makes one row of each input row, so that's always valid). Under an ORDER BY the select list is computed
before the sort, for every row, so there the query still fails. I first wrote that DuckDB fails it too; the second
review showed that's only half true: DuckDB fails `10 % (id - 3)` there but computes `CAST(s AS INT)` after its
top-N, for the kept rows only. Doing the same belongs with day 3's top-k. A filter computes its predicate
per batch too, so an error in a WHERE under a LIMIT can depend on the batch size; DESIGN.md says so rather than
pretending otherwise.

**Did the property tests miss anything the review found?**
Not the bugs (the review ran at an earlier commit, before the property tests existed), but the mutations showed what
the tests didn't check: of the review's 30 mutations, 7 survived, and 4 of those then failed the property tests. The
other 3 needed hand-written tests; one of mine was wrong at first. I wrote a test for the final check of results
against the plan's schema with `SELECT *`, and the mutant still passed: the projection that `SELECT *` becomes rebuilds
its batches and caught the bad type itself. The test now runs a plan that is a bare Scan, which only the final check
guards, and the mutant fails it.

**What else surprised you?**
Three Arrow behaviours that differ from SQL engines, each handled explicitly: float comparisons use IEEE's total
order (-0.0 < 0.0, NaNs distinct); a float-to-integer cast truncates (DuckDB rounds half to even); and a Float64 too
big for a Float32 casts to infinity even in Arrow's strict mode (DuckDB fails, the review found it). And one of my
own: the first property-test failure was in the reference interpreter, not the engine (its date arithmetic overflowed
an i64 on `i64::MAX` days).

## What went wrong on day 1

**What did the review find?**
Ten bugs, each reproduced as a failing test before it was fixed (README, "Review of day 1"). The instructive ones:
`GROUP BY a` silently fell back to a select alias when `a` was an *ambiguous* FROM column (the fallback ran on any
resolution error, not just "not found"); `avg(DISTINCT a)` deduplicated the values after casting them to Float64,
so two integers above 2^53 counted once; and a long `OR` chain overflowed the stack.

**Why did a long OR chain crash, and how is it handled?**
sqlparser parses `a = 0 OR a = 1 OR ...` in a loop, but the result is a left-deep tree, and the binder, the printer
and even `Drop` recurse once per level. A debug build ran out of an 8 MiB stack at about 800 terms. The binder now
counts depth in its `Scope` and refuses past `MAX_EXPR_DEPTH` (128) with an error; a test binds, prints and drops the
deepest allowed chain on a 2 MiB stack, the size of a test thread. Deep parentheses never get that far: sqlparser
has its own recursion limit.

**How do you know a printed plan means what the plan means?**
The property tests now print every bound expression, bind the printed text again, and require the identical tree.
That caught the printer giving LIKE the comparisons' precedence (sqlparser binds LIKE more loosely than `=`), and
then, on its own in a 100,000-case search, `-(-0)` printing as `-0`, which binds back as the literal 0 rather than a
negation: a minus before a number is part of the literal when parsed. A number under unary minus is now printed
in parentheses.

**How did you test the tests?**
By mutation: change one line of the engine, run the suite, restore the file from a copy. The review's 31 mutations
left 25 undetected at its commit; after the property tests 12 still were, and each got a test until all 22 that
still apply fail the suite. One run's results were noise at first: a proptest failure file saved under one mutant
was replayed under the next, so the runner now deletes it before each.

## Limits and next steps

**What doesn't it support, and what happens if you try?**
Subqueries, `WITH`, set operations, window functions, `RIGHT`/`FULL` joins, intervals and scalar functions other
than `CAST`. Each is a `QuarryError::Unsupported` naming what it was, never a panic or a wrong plan
(`unsupported_sql_is_named`).

**What comes next?**
Day 3 adds a physical plan between the logical plan and the operators: the hash join (joins bind today but don't run),
a top-k sort for `ORDER BY ... LIMIT` (today's sort sorts everything), predicate pushdown into Parquet row groups
(the footer is already read once per table; day 3 reads its per-column min and max) and optimizer rules (constant
folding must keep a failing constant lazy, see above). Day 4 compares every result with DuckDB's on TPC-H-style data.
