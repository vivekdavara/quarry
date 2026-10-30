-- A script for `quarry repl` (README, "Try it"): statements end with ';', commands start with '.'.
.tables
SELECT c.tier, count(*) AS orders, sum(o.total) AS spent
  FROM orders o JOIN customers c ON o.customer = c.customer
 WHERE o.status <> 'cancelled'
 GROUP BY c.tier
 ORDER BY spent DESC;
SELECT 'a ; inside a string' AS s; -- and a comment; neither ends a statement
SELECT nope FROM orders;
.drop customers
.load people=examples/customers.csv
SELECT count(*) AS people FROM people;
