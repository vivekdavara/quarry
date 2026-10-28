"""Times time_queries.rs's queries in DuckDB over the same files, for scale (one thread, then the default).

    pip install duckdb==2.0.0
    python scripts/duckdb_times.py data/orders_10m.parquet [data/customers.parquet]

Each query runs once to warm up, then five times; prints the median as Markdown table rows, and each query's answer
once (to compare with Quarry's). The join query needs the customers file.
"""
import duckdb, time, sys, statistics
path = sys.argv[1]
customers = sys.argv[2] if len(sys.argv) > 2 else None
qs = [
 ("count", "SELECT count(*) FROM orders"),
 ("filter + count", "SELECT count(*) FROM orders WHERE status = 'shipped' AND total > 500"),
 ("group by status (3 groups)", "SELECT status, count(*), sum(total), avg(qty) FROM orders GROUP BY status ORDER BY status"),
 ("group by customer (10,000 groups), top 5", "SELECT customer, sum(total) AS spent FROM orders WHERE placed >= '2023-01-01' GROUP BY customer ORDER BY spent DESC LIMIT 5"),
 ("arithmetic + filter", "SELECT sum(total * qty), avg(qty * 2 + 1) FROM orders WHERE placed >= '2023-01-01'"),
 ("count distinct", "SELECT count(DISTINCT customer) FROM orders"),
 ("sort everything, top 10", "SELECT order_id, total FROM orders ORDER BY total DESC, order_id LIMIT 10"),
 ("order_id range (1 row group of 10)", "SELECT count(*), sum(total) FROM orders WHERE order_id <= 1000000"),
]
if customers:
    qs.append(("join customers (10,000 rows), group by region",
               "SELECT c.region, count(*), sum(o.total) FROM orders o JOIN customers c ON o.customer = c.customer "
               "GROUP BY c.region ORDER BY c.region"))
for threads in (1, None):
    c = duckdb.connect()
    if threads: c.execute(f"SET threads={threads}")
    t = c.execute("SELECT current_setting('threads')").fetchone()[0]
    c.execute(f"CREATE VIEW orders AS SELECT * FROM read_parquet('{path}')")
    if customers:
        c.execute(f"CREATE VIEW customers AS SELECT * FROM read_parquet('{customers}')")
    print(f"threads={t}")
    for name, q in qs:
        answer = c.execute(q).fetchall()
        if threads:
            print(f"  {name}: {answer[:5]}")
        ts = []
        for _ in range(5):
            s = time.perf_counter(); c.execute(q).fetchall(); ts.append(time.perf_counter() - s)
        print(f"| {name} | {statistics.median(ts)*1e3:.1f} ms |")
