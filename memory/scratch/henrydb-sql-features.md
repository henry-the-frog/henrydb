# HenryDB SQL Feature Catalog

## Data Definition (DDL)
- CREATE TABLE (with column types, PRIMARY KEY, NOT NULL, DEFAULT, CHECK, UNIQUE, AUTOINCREMENT)
- CREATE INDEX (single, composite, unique)
- CREATE VIEW
- CREATE TRIGGER (AFTER/BEFORE INSERT/DELETE, NEW/OLD refs)
- CREATE FUNCTION (SQL and JS languages)
- DROP TABLE/INDEX/VIEW/TRIGGER
- ALTER TABLE

## Data Manipulation (DML)
- INSERT (single, multi-row, INSERT...SELECT)
- INSERT ON CONFLICT (UPSERT)
- UPDATE (with WHERE, SET)
- DELETE (with WHERE)
- MERGE (partial — needs table source, not VALUES)
- REPLACE

## SELECT
- Column selection, aliases
- DISTINCT
- WHERE with AND/OR/NOT/BETWEEN/IN/LIKE/GLOB
- ORDER BY (ASC/DESC, NULLS FIRST/LAST)
- LIMIT / OFFSET
- GROUP BY with ROLLUP/CUBE/GROUPING SETS
- HAVING
- CASE (simple and searched)
- Subqueries (scalar, IN, EXISTS, correlated)

## JOINs
- INNER JOIN
- LEFT/RIGHT/FULL OUTER JOIN
- CROSS JOIN
- NATURAL JOIN
- LATERAL JOIN
- Self-join

## Set Operations
- UNION / UNION ALL
- INTERSECT
- EXCEPT

## Common Table Expressions (CTEs)
- Basic WITH clause
- Recursive WITH RECURSIVE
- Multiple CTEs in one query

## Window Functions (10+)
- ROW_NUMBER()
- RANK() / DENSE_RANK()
- NTILE(n)
- LAG(expr, offset) / LEAD(expr, offset)
- FIRST_VALUE(expr) / LAST_VALUE(expr)
- SUM/AVG/COUNT/MIN/MAX OVER
- PARTITION BY + ORDER BY
- Frame specifications (ROWS BETWEEN, RANGE, GROUPS, EXCLUDE)

## Aggregate Functions
- COUNT (with DISTINCT)
- SUM, AVG, MIN, MAX
- GROUP_CONCAT
- MEDIAN, PERCENTILE_CONT, PERCENTILE_DISC
- STRING_AGG

## JSON Functions
- json_extract(data, '$.path')
- json_object('key', value)
- json_array(elem1, elem2)
- json_type(json)
- json_array_length(json)

## String Functions
- UPPER, LOWER, TRIM, LTRIM, RTRIM
- LENGTH, SUBSTR, REPLACE
- INSTR, LIKE, GLOB
- FORMAT, PRINTF
- HEX, UNHEX
- QUOTE
- UNICODE, CHAR

## Math Functions
- ABS, ROUND, FLOOR, CEIL
- MIN, MAX (scalar)
- RANDOM, TYPEOF
- COALESCE, IFNULL, NULLIF, IIF
- CAST

## Date Functions
- DATE, TIME, DATETIME
- STRFTIME
- date('now'), datetime('now')

## Table Functions
- GENERATE_SERIES(start, stop, step)
- UNNEST(array)
- VALUES clause as table source

## Other Features
- Prepared statements (PREPARE/EXECUTE)
- db.execute(sql, params) — parameter binding
- EXPLAIN / EXPLAIN ANALYZE
- ARRAY[...] literal syntax
- Type affinity (SQLite-compatible)
- Transactions (BEGIN/COMMIT/ROLLBACK)
- Savepoints (SAVEPOINT/RELEASE/ROLLBACK TO)
- MVCC with snapshot isolation
- Serializable Snapshot Isolation (SSI)
- Write-Ahead Log (WAL)
- PostgreSQL wire protocol (psql compatible)

## Feature Count: ~120+ SQL features
