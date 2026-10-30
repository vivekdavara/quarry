# Changelog

## 1.0.0 (2026-10-30)

The first release: a SQL engine for analytical queries over Parquet and CSV files, in Rust on Apache Arrow, built
over five days (README, "Status").

**SQL.** One `SELECT` at a time: `WHERE`, `GROUP BY` with `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` (each with
`DISTINCT`), `HAVING`, `DISTINCT`, `ORDER BY` (names, positions, expressions, `NULLS FIRST`/`LAST`), `LIMIT` and
`OFFSET`; comma joins, `JOIN ... ON` and `LEFT JOIN ... ON`; arithmetic, comparisons, `AND`/`OR`/`NOT` (lazy, as in
DuckDB), `CASE`, `IN`, `BETWEEN`, `LIKE`/`ILIKE`, `IS NULL`, `CAST`, string concatenation and date arithmetic; and
`EXPLAIN`. DESIGN.md is the specification: types, names, each plan node, and every rule where engines differ, checked
against DuckDB 2.0.

**Engine.** A binder that resolves every name and type before any data is read; vectorized operators over Arrow
record batches (filter, projection, hash aggregate, hash and nested-loop joins, sort, top-k, limit); an optimizer
(constant folding, `OR` factoring, predicate pushdown through joins and into Parquet row groups, projection
pushdown); row-group pruning by the footer's statistics. Errors, never panics, on any query or file.

**Tools.** `quarry schema`, `head`, `plan`, `query` (with `--output FILE` as Parquet and `--batch-size`), and
`quarry repl`, an interactive shell that also runs scripts from standard input (statements end where the parser's
own tokenizer sees a `;`; a script stops at text the tokenizer rejects). `tpch-gen` writes TPC-H-style tables
at any scale factor; eight TPC-H queries come with DuckDB's answers.

**Testing.** 219 tests, among them property tests of the binder, the evaluator, GROUP BY, ORDER BY, joins, pruning,
whole optimized queries and the REPL's statement splitter, each against an independent reference; the TPC-H answers
against DuckDB's; and a differential test of random queries against DuckDB 2.0.0 in CI. Each day's review findings
are kept as tests (README, "Review of day 1" to "Review of day 5").

**What a 1.x keeps.** The `quarry` subcommands, their flags and their exit statuses (0 success, 1 an engine error or
a failed statement in the REPL, 2 a bad command line); the REPL's commands and its `error: line N:` messages; and the library's `quarry::query`,
`Catalog` and `sql::bind`. The printed plan format and the table layout may change.

**Limits.** One thread; no subqueries, `WITH`, set operations, window functions, `RIGHT`/`FULL` joins, intervals or
decimal arithmetic; everything in memory (README, "Limitations").
