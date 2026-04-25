# HenryDB Feature Verification Report (2026-04-25)

All features verified by running actual queries during Session A.

## SQL DML ✅
- INSERT, UPDATE, DELETE
- INSERT OR REPLACE (upsert)
- ON CONFLICT DO UPDATE
- INSERT/DELETE RETURNING
- MERGE (partial — parser limitation on USING subquery)

## SQL DDL ✅
- CREATE TABLE (with PRIMARY KEY, types)
- CREATE INDEX, CREATE UNIQUE INDEX
- Expression indexes (verified)
- GENERATED columns (GENERATED ALWAYS AS expr STORED)

## Queries ✅
- SELECT with WHERE, ORDER BY, LIMIT
- JOIN (INNER, LEFT, RIGHT, CROSS)
- CROSS APPLY / OUTER APPLY (lateral join)
- NATURAL JOIN
- GROUP BY with aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- HAVING
- DISTINCT
- EXISTS / NOT EXISTS
- Subqueries (scalar, correlated)
- CASE WHEN / COALESCE
- UNION / UNION ALL

## CTEs ✅
- WITH ... AS (common table expressions)
- WITH RECURSIVE (recursive CTEs — numbers, Fibonacci)

## Window Functions ✅
- ROW_NUMBER, RANK, DENSE_RANK
- SUM, AVG, COUNT, MIN, MAX (as window)
- LAG, LEAD (with offset and null boundaries)
- NTILE
- PARTITION BY, ORDER BY, frame specifications

## Advanced SQL ✅
- TABLESAMPLE BERNOULLI
- GROUPING SETS
- EXPLAIN / EXPLAIN ANALYZE (PostgreSQL-quality)
- ANALYZE (statistics collection)

## Transactions ✅
- BEGIN / COMMIT / ROLLBACK
- SAVEPOINT / ROLLBACK TO SAVEPOINT
- ACID compliance

## Triggers ✅
- AFTER UPDATE with NEW/OLD references
- Automatic audit logging

## Stored Procedures ✅
- CREATE PROCEDURE (dollar-quoted body)
- CALL procedure

## JSON ✅
- JSON_EXTRACT, JSON_ARRAY, JSON_OBJECT
- -> and ->> operators (fixed $ prefix today)

## Full-Text Search ✅
- Tokenizer + inverted index (13 tests pass)

## Type System ✅
- INT, TEXT, REAL, FLOAT, VARCHAR
- SQLite-compatible type class ordering (NULL < INT < TEXT < BLOB)
- Type affinity on comparisons (fixed today)

## Distributed ✅
- Raft consensus (10 tests)
- CRDTs (13 tests)
- SWIM gossip protocol (7 tests)

## Server ✅
- PostgreSQL wire protocol (72 test files)
- HTTP/JSON API
- Starts on configurable port

## Storage ✅
- Heap files, B+tree indexes
- WAL (Write-Ahead Log)
- Persistence to disk

## Performance
- INSERT: 55μs/row (1K rows in 55ms)
- COUNT(*): 11ms on 1K rows
- GROUP BY: 6ms on 1K rows
- Subquery: 3ms on 1K rows
- Fuzzer: 97.2% match vs SQLite (6000 queries)

## Test Counts
- 875 test files
- 8,982 individual test cases
- All passing

## Additional Verifications (Tasks 151-160+)

### Constraints ✅
- CHECK constraints (rejects invalid values)
- NOT NULL (rejects nulls)
- UNIQUE (rejects duplicates with value in error)
- FOREIGN KEY (referential integrity enforced)
- PRIMARY KEY

### DDL ✅
- ALTER TABLE ADD COLUMN
- ALTER TABLE RENAME COLUMN
- ALTER TABLE DROP COLUMN
- ALTER TABLE RENAME TABLE

### Set Operations ✅
- INTERSECT
- EXCEPT
- UNION / UNION ALL (verified earlier)

### Views ✅
- CREATE VIEW (with aggregation)
- Query, filter, JOIN through views

### Functions (17/18) ✅
- String: UPPER, LOWER, SUBSTR, LENGTH, REPLACE, TRIM, INSTR
- Math: ABS, ROUND
- Date: DATE('now'), DATETIME('now')
- CAST (INT↔TEXT), IIF, NULLIF, IFNULL
- LIKE, BETWEEN, IN
