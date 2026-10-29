"""Differential testing against DuckDB: queries over the TPC-H-style tables, run by Quarry and by DuckDB, must give the
same rows.

    python scripts/differential.py QUARRY DATA_DIR [--random N] [--seed S] [--failures DIR]

QUARRY is the `quarry` binary (`cargo build --release -p quarry-cli`), DATA_DIR the tables `tpch-gen` wrote. The
script runs the TPC-H queries in crates/quarry-tpch/queries/, then N random ones (default 300) from a generator
seeded with S (default 1): filters (comparisons, BETWEEN, IN, LIKE, IS NULL, date arithmetic, AND/OR/NOT, ORs whose
branches share a conjunct), joins along the schema's keys (comma joins, JOIN ... ON, LEFT JOIN ... ON with a
condition on the right), GROUP BY with every aggregate and HAVING, and select lists of expressions with ORDER BY,
LIMIT and OFFSET. Quarry runs through its CLI (`quarry query --output`, so types, NULLs and floats come back
exactly); DuckDB on one thread over the same files.

Every query orders its output completely (by every group key, or by every select item), so rows are compared in
order: NULL with NULL, numbers equal (floats within a relative 1e-9: sums add in other orders), the rest equal. A
query one engine fails and the other doesn't is a difference; both failing is counted, not compared. The generator
stays inside the SQL both engines define the same way (DESIGN.md's table of rules): no casts, no division or modulo
by a column, no arithmetic on literals alone (DuckDB's DECIMAL literals), no float literal next to an integer.
Exit status 1 if any query differs; with --failures, each differing query is saved there as a .sql file.
"""

import argparse
import datetime
import math
import pathlib
import random
import subprocess
import sys
import tempfile

import duckdb

ROOT = pathlib.Path(__file__).resolve().parent.parent
QUERIES = ROOT / "crates" / "quarry-tpch" / "queries"
TABLES = ["region", "nation", "supplier", "customer", "part", "orders", "lineitem"]

MODES = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"]
INSTRUCTIONS = ["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"]
SEGMENTS = ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"]
PRIORITIES = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]
WORDS = ["special", "requests", "final", "pinto", "ironic", "foxes", "blithely"]
NATION_NAMES = ["ALGERIA", "BRAZIL", "CHINA", "FRANCE", "GERMANY", "JAPAN", "PERU", "UNITED STATES"]

# Each column: (table, kind, constants for comparisons). Kinds: key and int (BIGINT), float (DOUBLE), cat (VARCHAR
# with few values), text (VARCHAR, free), date (DATE).
FLOATS = {
    "l_quantity": ["1.0", "10.0", "24.0", "25.5", "49.0"],
    "l_extendedprice": ["1000.0", "20000.0", "45000.5", "90000.0"],
    "l_discount": ["0.0", "0.02", "0.05", "0.07", "0.1"],
    "l_tax": ["0.0", "0.03", "0.08"],
    "o_totalprice": ["1000.0", "100000.0", "250000.0"],
    "c_acctbal": ["-500.0", "0.0", "1000.0", "5000.5"],
    "s_acctbal": ["-500.0", "0.0", "4500.0"],
    "p_retailprice": ["950.0", "1500.0", "2000.0"],
}
CATEGORIES = {
    "l_returnflag": ["R", "A", "N"],
    "l_linestatus": ["O", "F"],
    "l_shipmode": MODES,
    "l_shipinstruct": INSTRUCTIONS,
    "o_orderstatus": ["O", "F", "P"],
    "o_orderpriority": PRIORITIES,
    "c_mktsegment": SEGMENTS,
    "p_brand": ["Brand#11", "Brand#23", "Brand#34", "Brand#55"],
    "p_container": ["SM CASE", "MED BOX", "LG PACK", "JUMBO JAR", "WRAP DRUM"],
    "p_mfgr": ["Manufacturer#1", "Manufacturer#3", "Manufacturer#5"],
    "n_name": NATION_NAMES,
    "r_name": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"],
}
INTS = {
    "l_linenumber": (1, 7),
    "p_size": (1, 50),
    "o_shippriority": (0, 0),
    "n_regionkey": (0, 4),
}
# Each key column's largest value; the scaled ones are read from the data (`Engines.__init__`), so constants cover
# the keys at any scale.
KEYS = {
    "l_orderkey": "orders", "o_orderkey": "orders", "l_partkey": "part", "p_partkey": "part",
    "l_suppkey": "supplier", "s_suppkey": "supplier", "o_custkey": "customer", "c_custkey": "customer",
    "c_nationkey": 24, "s_nationkey": 24, "n_nationkey": 24, "r_regionkey": 4,
}
TEXTS = ["l_comment", "o_comment", "c_comment", "p_name", "p_type", "c_address", "o_clerk"]
DATES = ["l_shipdate", "l_commitdate", "l_receiptdate", "o_orderdate"]

TABLE_COLUMNS = {
    "lineitem": "l_", "orders": "o_", "customer": "c_", "part": "p_", "supplier": "s_", "nation": "n_",
    "region": "r_",
}

# Joins along the schema's keys: (table, table, condition).
EDGES = [
    ("lineitem", "orders", "l_orderkey = o_orderkey"),
    ("orders", "customer", "o_custkey = c_custkey"),
    ("lineitem", "part", "l_partkey = p_partkey"),
    ("lineitem", "supplier", "l_suppkey = s_suppkey"),
    ("customer", "nation", "c_nationkey = n_nationkey"),
    ("supplier", "nation", "s_nationkey = n_nationkey"),
    ("nation", "region", "n_regionkey = r_regionkey"),
]


def kind(column: str) -> str:
    if column in FLOATS:
        return "float"
    if column in CATEGORIES:
        return "cat"
    if column in INTS:
        return "int"
    if column in KEYS:
        return "key"
    if column in TEXTS:
        return "text"
    if column in DATES:
        return "date"
    raise KeyError(column)


ALL_COLUMNS = list(FLOATS) + list(CATEGORIES) + list(INTS) + list(KEYS) + TEXTS + DATES


def columns_of(tables) -> list[str]:
    prefixes = tuple(TABLE_COLUMNS[t] for t in tables)
    return [c for c in ALL_COLUMNS if c.startswith(prefixes)]


class Generator:
    def __init__(self, seed: int):
        self.r = random.Random(seed)

    def date(self) -> str:
        day = datetime.date(1992, 1, 1) + datetime.timedelta(days=self.r.randrange(0, 2557))
        return f"DATE '{day.isoformat()}'"

    def constant(self, column: str) -> str:
        k = kind(column)
        if k == "float":
            return self.r.choice(FLOATS[column])
        if k == "cat":
            return "'" + self.r.choice(CATEGORIES[column]) + "'"
        if k == "int":
            lo, hi = INTS[column]
            return str(self.r.randint(lo, hi))
        if k == "key":
            return str(self.r.randint(0, KEYS[column] + 1))
        if k == "date":
            return self.date()
        raise ValueError(column)

    def leaf(self, columns: list[str], nullable: set[str]) -> str:
        r = self.r
        c = r.choice(columns)
        k = kind(c)
        if c in nullable and r.random() < 0.3:
            return f"{c} IS {r.choice(['', 'NOT '])}NULL"
        op = r.choice(["=", "<>", "<", "<=", ">", ">="])
        if k == "text":
            word = r.choice(WORDS)
            pattern = r.choice([f"%{word}%", f"{word}%", f"%{word}", f"%{word}%{r.choice(WORDS)}%"])
            return f"{c} {r.choice(['', 'NOT '])}LIKE '{pattern}'"
        if k == "cat":
            values = CATEGORIES[c]
            choice = r.random()
            if choice < 0.3:
                picked = r.sample(values, min(len(values), r.randint(1, 3)))
                return f"{c} {r.choice(['', 'NOT '])}IN ({', '.join(repr(v) for v in picked)})"
            if choice < 0.45:
                return f"{c} LIKE '{r.choice(values)[:r.randint(1, 3)]}%'"
            return f"{c} {op} '{r.choice(values)}'"
        if k == "date":
            choice = r.random()
            same_table = [d for d in DATES if d in columns and d[0] == c[0] and d != c]
            if choice < 0.2 and same_table:
                return f"{c} {op} {r.choice(same_table)}"
            if choice < 0.35:
                return f"{c} + {r.randint(-60, 60)} {op} {self.date()}"
            if choice < 0.5:
                a, b = sorted([self.date(), self.date()])
                return f"{c} {r.choice(['', 'NOT '])}BETWEEN {a} AND {b}"
            return f"{c} {op} {self.date()}"
        # Numbers.
        choice = r.random()
        if choice < 0.2:
            a, b = self.constant(c), self.constant(c)
            return f"{c} BETWEEN {a} AND {b}"
        if choice < 0.3 and k in ("int", "key"):
            return f"{c} % {r.randint(2, 7)} = {r.randint(0, 1)}"
        if choice < 0.4:
            return f"{c} IN ({', '.join(self.constant(c) for _ in range(3))})"
        return f"{c} {op} {self.constant(c)}"

    def condition(self, columns: list[str], nullable: set[str], depth: int = 2) -> str:
        r = self.r
        if depth == 0 or r.random() < 0.35:
            return self.leaf(columns, nullable)
        choice = r.random()
        a = self.condition(columns, nullable, depth - 1)
        b = self.condition(columns, nullable, depth - 1)
        if choice < 0.4:
            return f"({a}) AND ({b})"
        if choice < 0.7:
            return f"({a}) OR ({b})"
        if choice < 0.85:
            s = self.leaf(columns, nullable)
            return f"(({s}) AND ({a})) OR (({b}) AND ({s}))"
        return f"NOT ({a})"

    def from_clause(self):
        """The tables, their FROM clause, the WHERE conditions the joins need, and the columns that can be NULL.

        A query's joins are all commas or all JOINs: in `a, b JOIN c ON <a condition>` the standard (and PostgreSQL,
        and Quarry) scope the ON to `b JOIN c` and refuse it, where DuckDB 2.0 reads `(a, b) JOIN c`.
        """
        r = self.r
        tables = [r.choice(TABLES)]
        joins, where, nullable = [], [], set()
        commas = r.random() < 0.35
        for _ in range(r.choice([0, 0, 1, 1, 2, 3])):
            options = [
                (a, b, on) if a in tables else (b, a, on)
                for a, b, on in EDGES
                if (a in tables) != (b in tables)
            ]
            if not options:
                break
            _, new, on = r.choice(options)
            tables.append(new)
            if commas:
                joins.append(f", {new}")
                where.append(on)
            elif r.random() < 0.5:
                joins.append(f" JOIN {new} ON {on}")
            else:
                # A LEFT join, sometimes with a condition on the right that leaves left rows unmatched.
                extra = ""
                if r.random() < 0.6:
                    extra = " AND " + self.leaf(columns_of([new]), set())
                joins.append(f" LEFT JOIN {new} ON {on}{extra}")
                nullable.update(columns_of([new]))
        return tables, tables[0] + "".join(joins), where, nullable

    def expression(self, columns: list[str]) -> str:
        r = self.r
        c = r.choice(columns)
        k = kind(c)
        floats = [x for x in columns if kind(x) == "float"]
        ints = [x for x in columns if kind(x) in ("int", "key")]
        if k == "float" and r.random() < 0.6:
            other = r.choice(floats)
            return r.choice([f"{c} * (1 - {other})", f"{c} + {other}", f"{c} - {other} * 2.5", f"-{c}"])
        if k in ("int", "key") and r.random() < 0.5:
            return r.choice([f"{c} + {r.randint(1, 9)}", f"{c} * {r.choice(ints)}", f"{c} % {r.randint(2, 9)}",
                             f"{c} / {r.randint(1, 9)}"])
        if k == "date" and r.random() < 0.5:
            others = [d for d in DATES if d in columns and d != c]
            if others:
                return f"{c} - {r.choice(others)}"
            return f"{c} + {r.randint(1, 100)}"
        if k in ("cat", "text") and r.random() < 0.3:
            return f"{c} || '/' || {r.choice([x for x in columns if kind(x) in ('cat', 'text')])}"
        if r.random() < 0.15 and floats:
            return f"CASE WHEN {self.leaf(columns, set())} THEN {r.choice(floats)} ELSE {r.choice(floats)} END"
        return c

    def aggregate(self, columns: list[str]) -> str:
        r = self.r
        numbers = [c for c in columns if kind(c) in ("float", "int", "key")]
        c = r.choice(columns)
        choice = r.random()
        if choice < 0.2:
            return "count(*)"
        if choice < 0.3:
            return f"count({c})"
        if choice < 0.4:
            return f"count(DISTINCT {c})"
        if choice < 0.55:
            return f"min({c})"
        if choice < 0.7:
            return f"max({c})"
        n = r.choice(numbers) if numbers else None
        if n is None:
            return "count(*)"
        if choice < 0.85:
            return f"sum({self.expression([n] + [x for x in numbers if kind(x) == kind(n)])})"
        if choice < 0.95:
            return f"avg({n})"
        return f"sum(DISTINCT {n})" if kind(n) != "float" else f"avg({n})"

    def query(self) -> str:
        r = self.r
        tables, from_, where, nullable = self.from_clause()
        columns = columns_of(tables)
        if r.random() < 0.8:
            where.append(self.condition(columns, nullable))
        where_sql = f" WHERE {' AND '.join(f'({w})' for w in where)}" if where else ""
        if r.random() < 0.5:
            groupable = [c for c in columns if kind(c) in ("cat", "int")]
            keys = r.sample(groupable, min(len(groupable), r.choice([0, 1, 1, 2])))
            aggregates = [self.aggregate(columns) for _ in range(r.randint(1, 4))]
            select = keys + [f"{a} AS a{i}" for i, a in enumerate(aggregates)]
            sql = f"SELECT {', '.join(select)} FROM {from_}{where_sql}"
            if keys:
                sql += f" GROUP BY {', '.join(keys)}"
                if r.random() < 0.3:
                    sql += f" HAVING count(*) > {r.randint(0, 50)}"
                sql += " ORDER BY " + ", ".join(f"{i + 1} {r.choice(['ASC', 'DESC'])}" for i in range(len(keys)))
            return sql
        items = [self.expression(columns) for _ in range(r.randint(1, 4))]
        select = ", ".join(f"{e} AS c{i}" for i, e in enumerate(items))
        order = ", ".join(f"{i + 1} {r.choice(['ASC', 'DESC'])}" for i in range(len(items)))
        distinct = "DISTINCT " if r.random() < 0.1 else ""
        sql = f"SELECT {distinct}{select} FROM {from_}{where_sql} ORDER BY {order} LIMIT {r.randint(1, 60)}"
        if r.random() < 0.3:
            sql += f" OFFSET {r.randint(0, 40)}"
        return sql


def same(a, b) -> bool:
    if a is None or b is None:
        return a is None and b is None
    numbers = (int, float)
    if isinstance(a, numbers) and isinstance(b, numbers) and not isinstance(a, bool) and not isinstance(b, bool):
        if isinstance(a, int) and isinstance(b, int):
            return a == b
        if math.isnan(a) or math.isnan(b):
            return math.isnan(a) and math.isnan(b)
        return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-9)
    return type(a) is type(b) and a == b


def differs(got, want) -> str | None:
    if len(got) != len(want):
        return f"{len(got)} rows here, {len(want)} in DuckDB"
    for i, (g, w) in enumerate(zip(got, want)):
        if len(g) != len(w) or not all(same(x, y) for x, y in zip(g, w)):
            return f"row {i}: {g} here, {w} in DuckDB"
    return None


class Engines:
    def __init__(self, quarry: str, data: pathlib.Path, scratch: pathlib.Path):
        self.quarry = quarry
        self.out = scratch / "result.parquet"
        self.table_args = []
        for t in TABLES:
            self.table_args += ["-t", f"{t}={data / f'{t}.parquet'}"]
        self.duck = duckdb.connect()
        self.duck.execute("SET threads = 1")
        for t in TABLES:
            path = str(data / f"{t}.parquet").replace("'", "''")
            self.duck.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{path}')")
        for column, table in list(KEYS.items()):
            if isinstance(table, str):
                (KEYS[column],) = self.duck.execute(f"SELECT count(*) FROM {table}").fetchone()

    def run_quarry(self, sql: str):
        done = subprocess.run(
            [self.quarry, "query", *self.table_args, "--output", str(self.out), "--", sql],
            capture_output=True, text=True, timeout=120,
        )
        if done.returncode != 0:
            return None, done.stderr.strip()
        path = str(self.out).replace("'", "''")
        return duckdb.execute(f"SELECT * FROM read_parquet('{path}')").fetchall(), None

    def run_duckdb(self, sql: str):
        try:
            return self.duck.execute(sql).fetchall(), None
        except duckdb.Error as e:
            return None, str(e).splitlines()[0]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("quarry")
    parser.add_argument("data", type=pathlib.Path)
    parser.add_argument("--random", type=int, default=300)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--failures", type=pathlib.Path)
    args = parser.parse_args()
    if args.random < 0:
        parser.error("--random must be 0 or more")
    with tempfile.TemporaryDirectory() as scratch:
        engines = Engines(args.quarry, args.data, pathlib.Path(scratch))
        queries = [(q.stem, q.read_text()) for q in sorted(QUERIES.glob("q*.sql"))]
        if not queries:
            print(f"no queries in {QUERIES}", file=sys.stderr)
            return 1
        gen = Generator(args.seed)  # after Engines, which reads the key ranges
        queries += [(f"random {i + 1}", gen.query()) for i in range(args.random)]
        agree = both_failed = rows = 0
        failures = []
        for name, sql in queries:
            got, quarry_error = engines.run_quarry(sql)
            want, duckdb_error = engines.run_duckdb(sql)
            if quarry_error and duckdb_error:
                both_failed += 1
                continue
            if quarry_error or duckdb_error:
                problem = f"Quarry: {quarry_error}" if quarry_error else f"DuckDB: {duckdb_error}"
            else:
                problem = differs(got, want)
            if problem:
                failures.append((name, sql, problem))
                print(f"DIFFERS {name}: {problem}\n  {sql}")
            else:
                agree += 1
                rows += len(got)
        print(f"{len(queries)} queries (seed {args.seed}): {agree} agree ({rows} rows), {both_failed} failed in "
              f"both, {len(failures)} differ")
        if failures and args.failures:
            args.failures.mkdir(parents=True, exist_ok=True)
            for name, sql, problem in failures:
                (args.failures / f"{name.replace(' ', '_')}.sql").write_text(f"-- {problem}\n{sql}\n")
        return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
