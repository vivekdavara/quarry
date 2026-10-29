-- TPC-H Q6, forecasting revenue change. The specification writes the discount range as 0.06 - 0.01 to 0.06 + 0.01;
-- in DOUBLE arithmetic 0.06 + 0.01 is 0.06999999999999999, below the 0.07 the column holds, so the bounds are
-- written out (DuckDB reads such literals as DECIMAL and keeps 0.07).
SELECT sum(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= DATE '1994-01-01'
    AND l_shipdate < DATE '1995-01-01'
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 24
