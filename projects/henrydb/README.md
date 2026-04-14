# HenryDB

A complete SQL database engine built from scratch in JavaScript. Features MVCC transactions, cost-based query optimization, B+Tree indexes, WAL crash recovery, and a PostgreSQL-compatible wire protocol.

## Features

### SQL Support
- **DDL**: CREATE/DROP TABLE, ALTER TABLE, CREATE INDEX, CREATE VIEW, CREATE MATERIALIZED VIEW
- **DML**: SELECT, INSERT, UPDATE, DELETE, UPSERT (ON CONFLICT), MERGE (SQL:2003)
- **Queries**: JOINs (INNER, LEFT, RIGHT, CROSS, self, LATERAL), subqueries (scalar, correlated, EXISTS), CTEs (WITH, WITH RECURSIVE), UNION/INTERSECT/EXCEPT (ALL)
- **Aggregates**: COUNT, SUM, AVG, MIN, MAX, ARRAY_AGG, STRING_AGG, COUNT(DISTINCT) with GROUP BY + HAVING
- **Window Functions**: ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, SUM/AVG/COUNT OVER (PARTITION BY ... ORDER BY ...)
- **Expressions**: CASE/WHEN, BETWEEN, LIKE, ILIKE, SIMILAR TO (regex), IN, IS NULL, COALESCE, NULLIF, GREATEST, LEAST
- **ORDER BY**: ASC/DESC, NULLS FIRST/LAST, ordinal positions, expression ordering
- **JSON Functions**: JSON_EXTRACT (path navigation), JSON_TYPE, JSON_ARRAY_LENGTH, JSON_OBJECT, JSON_ARRAY, JSON_VALID
- **Date Functions**: DATE_ADD, DATE_DIFF, DATE_TRUNC, EXTRACT (YEAR/MONTH/DAY)
- **Math Functions**: POWER, SQRT, LOG, EXP, CEIL, FLOOR, ABS, ROUND
- **String Functions**: UPPER, LOWER, LENGTH, SUBSTR, POSITION, TRIM, REPLACE, CONCAT
- **Advanced**: GROUPING SETS/ROLLUP/CUBE, DISTINCT ON, UPDATE FROM, DELETE USING, LATERAL JOIN, MERGE
- **Other**: PREPARE/EXECUTE, RETURNING clause, GENERATE_SERIES, EXPLAIN ANALYZE, SERIAL columns, sequences (NEXTVAL/CURRVAL), generated columns, information_schema, CAST, FETCH FIRST N ROWS, CSV import/export

### Storage & Transactions
- **MVCC**: Snapshot isolation with serializable snapshot isolation (SSI)
- **WAL**: Write-ahead logging with crash recovery
- **HeapFile + BufferPool**: Page-based storage with LRU buffer management
- **B+Tree Indexes**: Primary key and secondary indexes with cost-based selection
- **Savepoints**: SAVEPOINT, ROLLBACK TO, RELEASE
- **Foreign Keys**: With CASCADE, SET NULL, RESTRICT

### Query Optimization
- **Cost-Based Optimizer**: Join ordering, index selection, predicate pushdown
- **Histograms**: Equi-height histograms for cardinality estimation
- **Decorrelation**: Semi-join transformation for EXISTS/IN subqueries
- **Result Cache**: Query result caching (correlated-subquery-aware)
- **EXPLAIN ANALYZE**: Shows estimated vs actual rows, planning/execution time

### Architecture
```
src/
├── sql.js           # SQL parser (2060 lines) — tokenizer + recursive descent
├── db.js            # Query evaluator + engine (6712 lines) — the core
├── transactional-db.js  # MVCC transaction manager
├── decorrelate.js   # Subquery decorrelation optimizer
├── heap-file.js     # Page-based heap storage
├── buffer-pool.js   # LRU buffer pool manager
├── btree.js         # B+Tree index implementation
├── wal.js           # Write-ahead log
├── disk-manager.js  # Disk I/O management
└── server.js        # PostgreSQL wire protocol server
```

## Quick Start

```javascript
import { Database } from './src/db.js';

const db = new Database();
db.execute('CREATE TABLE users (id INT, name TEXT, email TEXT)');
db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')");

const result = db.execute('SELECT * FROM users WHERE id = 1');
console.log(result.rows);
// [{ id: 1, name: 'Alice', email: 'alice@example.com' }]
```

## Example Queries

```sql
-- 4-way JOIN with aggregation
SELECT c.name, SUM(oi.quantity * p.price) AS revenue
FROM customers c
JOIN orders o ON o.customer_id = c.id
JOIN order_items oi ON oi.order_id = o.id
JOIN products p ON p.id = oi.product_id
GROUP BY c.name
ORDER BY revenue DESC;

-- Window function ranking
SELECT name, salary,
  ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS dept_rank
FROM employees;

-- JSON querying
SELECT JSON_EXTRACT(metadata, '$.service') AS service,
  AVG(JSON_EXTRACT(metadata, '$.latency')) AS avg_latency
FROM logs
GROUP BY service
ORDER BY avg_latency DESC;

-- CTE + correlated EXISTS
WITH active AS (SELECT * FROM orders WHERE status = 'completed')
SELECT c.name FROM customers c
WHERE EXISTS (SELECT 1 FROM active a WHERE a.customer_id = c.id);

-- TPC-H Q14 (promotion revenue percentage)
SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
  / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM lineitem JOIN part ON l_partkey = p_partkey
WHERE l_shipdate >= '1995-09-01' AND l_shipdate < '1995-10-01';
```

## Tests

**673 test files** covering every feature:
- SQL parsing (expressions, joins, subqueries, CTEs, window functions)
- Query evaluation (aggregates, GROUP BY, HAVING, ORDER BY)
- MVCC transactions (isolation levels, savepoints, rollback)
- Index operations (B+Tree, unique constraints, foreign keys)
- Crash recovery (WAL replay, durability)
- Optimizer accuracy (histogram estimation, join ordering)
- JSON functions (27 tests)
- Window functions (comprehensive)
- Multi-table JOINs (3-4 way, star schema)
- TPC-H micro-benchmark (Q1, Q6, Q14)
- E-commerce showcase (11 real-world queries)
- Sequences, SERIAL, generated columns
- MERGE, LATERAL JOIN, GROUPING SETS
- CSV import/export, information_schema

```bash
npm test
```

## Performance

TPC-H micro-benchmark (1000 lineitem rows):
- Q1 (Pricing Summary — 6 aggregates, GROUP BY): **24ms**
- Q6 (Revenue Forecast — filter + SUM): **13ms**
- Q14 (Promotion Effect — JOIN + conditional aggregate): **~30ms**

## License

MIT
