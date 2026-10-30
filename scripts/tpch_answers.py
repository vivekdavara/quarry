"""DuckDB's answers to the TPC-H-style queries, written as CSV files that `tests/tpch.rs` compares Quarry's with.

    python scripts/tpch_answers.py DATA_DIR OUT_DIR

DATA_DIR holds the tables `tpch-gen` wrote (`cargo run --release -p quarry-tpch -- 0.01 DATA_DIR`); OUT_DIR gets
`q01.csv` and the rest, one per file in crates/quarry-tpch/queries/. Each file has a header of the result's column
names; floats are written with `repr` (the shortest text that reads back as the same double), dates as YYYY-MM-DD.
DuckDB runs on one thread, so its float sums add in one order.
"""

import csv
import pathlib
import sys

import duckdb

TABLES = ["region", "nation", "supplier", "customer", "part", "orders", "lineitem"]
QUERIES = (
    pathlib.Path(__file__).resolve().parent.parent
    / "crates"
    / "quarry-tpch"
    / "queries"
)


def connect(data: pathlib.Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET threads = 1")
    for table in TABLES:
        path = str(data / f"{table}.parquet").replace("'", "''")
        con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{path}')")
    return con


def text(value) -> str:
    if value is None:
        raise ValueError(
            "a NULL in an answer; the CSV format here has no way to write one"
        )
    if isinstance(value, float):
        return repr(value)
    return str(value)


def main() -> None:
    if len(sys.argv) != 3:
        sys.exit("usage: tpch_answers.py DATA_DIR OUT_DIR")
    data, out = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
    out.mkdir(parents=True, exist_ok=True)
    con = connect(data)
    for query in sorted(QUERIES.glob("q*.sql")):
        result = con.execute(query.read_text())
        names = [d[0] for d in result.description]
        rows = result.fetchall()
        with open(out / f"{query.stem}.csv", "w", newline="") as f:
            writer = csv.writer(f, lineterminator="\n")
            writer.writerow(names)
            writer.writerows([text(v) for v in row] for row in rows)
        print(f"{query.stem}: {len(rows)} rows")


if __name__ == "__main__":
    main()
