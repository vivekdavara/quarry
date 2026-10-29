"""Times DuckDB on the TPC-H-style queries over the tables `tpch-gen` wrote, as `tpch_times` times Quarry.

    python scripts/tpch_duckdb_times.py DIR [RUNS] [THREADS ...]

Each query runs once to warm up, then RUNS times (5 by default), at each thread count given (1 and 8 by default).
Prints a Markdown table of medians, and checks that every run gives the first run's row count.
"""

import pathlib
import statistics
import sys
import time

import duckdb

ROOT = pathlib.Path(__file__).resolve().parent.parent
QUERIES = sorted((ROOT / "crates" / "quarry-tpch" / "queries").glob("q*.sql"))
TABLES = ["region", "nation", "supplier", "customer", "part", "orders", "lineitem"]


def main() -> None:
    if len(sys.argv) < 2:
        sys.exit("usage: tpch_duckdb_times.py DIR [RUNS] [THREADS ...]")
    data = pathlib.Path(sys.argv[1])
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    threads = [int(t) for t in sys.argv[3:]] or [1, 8]
    if runs < 1 or any(t < 1 for t in threads):
        sys.exit("RUNS and THREADS must be at least 1")
    con = duckdb.connect()
    for table in TABLES:
        path = str(data / f"{table}.parquet").replace("'", "''")
        con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{path}')")
    print(f"DuckDB {duckdb.__version__}, median of {runs} runs after one warm-up run")
    print()
    print("| Query | Result rows | " + " | ".join(f"{t} thread{'s' if t > 1 else ''}" for t in threads) + " |")
    print("|---|---|" + "---|" * len(threads))
    for query in QUERIES:
        sql = query.read_text()
        cells = []
        rows = None
        for t in threads:
            con.execute(f"SET threads = {t}")
            first = len(con.execute(sql).fetchall())
            rows = first if rows is None else rows
            assert first == rows, (query.stem, first, rows)
            times = []
            for _ in range(runs):
                start = time.perf_counter()
                n = len(con.execute(sql).fetchall())
                times.append(time.perf_counter() - start)
                assert n == rows, (query.stem, n, rows)
            cells.append(f"{statistics.median(times) * 1e3:.1f} ms")
        print(f"| {query.stem} | {rows} | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    main()
