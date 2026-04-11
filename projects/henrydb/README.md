# HenryDB

A SQL database written from scratch in JavaScript. PostgreSQL wire protocol compatible.

## What Is This?

HenryDB is a complete relational database engine implemented in ~148,000 lines of JavaScript (core + tests). It speaks the PostgreSQL wire protocol, so you can connect with `psql`, `pg` npm module, or any PostgreSQL client library.

```bash
# Start the server
node src/server.js --data-dir ./data --port 5432

# Connect with psql
psql -h 127.0.0.1 -p 5432

# Or from Node.js
const { Client } = require('pg');
const client = new Client({ host: '127.0.0.1', port: 5432 });
await client.connect();
await client.query('CREATE TABLE users (id INT PRIMARY KEY, name TEXT)');
await client.query("INSERT INTO users VALUES (1, 'Alice')");
const result = await client.query('SELECT * FROM users');
```

## SQL Compliance Scorecard

153/156 checks passing across 20+ categories:

| Category | Score | Features |
|----------|-------|----------|
| DDL | 9/9 | CREATE TABLE/INDEX/VIEW, ALTER, DROP, IF NOT EXISTS, CTAS, RENAME |
| DML | 7/7 | INSERT, INSERT RETURNING, UPDATE, DELETE, UPSERT, TRUNCATE, INSERT INTO SELECT |
| SELECT | 21/21 | WHERE (=, !=, <, >, LIKE, BETWEEN, IN, EXISTS), DISTINCT, ORDER BY, LIMIT, OFFSET |
| SELECT+ | 6/6 | Correlated subqueries, nested CASE, IN list, aggregate WHERE subquery |
| JOIN | 7/7 | INNER, LEFT, RIGHT, FULL OUTER, CROSS, NATURAL, USING |
| Aggregates | 10/10 | COUNT, SUM, AVG, MIN, MAX, GROUP BY, HAVING, COUNT DISTINCT, STRING_AGG |
| Windows | 5/5 | ROW_NUMBER, RANK, SUM OVER, running totals, PARTITION BY |
| Subqueries | 3/3 | Scalar, FROM, IN |
| CTEs | 4/4 | WITH, multiple CTEs, WITH RECURSIVE (factorial, fibonacci, tree traversal) |
| Expressions | 5/5 | Arithmetic (with precedence), \|\|, CASE, COALESCE, CAST |
| GENERATE_SERIES | 4/4 | Basic, aggregate, GROUP BY, window |
| Set Ops | 2/2 | UNION, UNION ALL |
| Types | 4/4 | INT, TEXT, NULL, BOOLEAN, float literal, negative numbers |
| Strings | 8/8 | UPPER, LOWER, LENGTH, TRIM, SUBSTRING, SUBSTR, REPLACE, CONCAT |
| Math | 7/7 | ABS, CEIL, FLOOR, ROUND, MOD, POWER, EXP |
| Date/Time | 2/2 | NOW(), CURRENT_DATE |
| JSON | 2/2 | JSON_EXTRACT, nested objects |
| Conditionals | 4/4 | NULLIF, GREATEST, LEAST |
| Error Handling | 4/4 | Table not found, syntax errors |
| Full-text | 2/2 | FTS indexing, phrase search |
| **Total** | **156/156** | **100%** |

Run `node sql-compliance-scorecard.js` to verify.

## Feature Showcase

```sql
-- Window functions with CTEs
WITH ranked AS (
  SELECT name, department, salary,
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank
  FROM employees
)
SELECT * FROM ranked WHERE dept_rank <= 3;

-- STRING_AGG with GROUP BY
SELECT department, STRING_AGG(name, ', ') as team_members
FROM employees GROUP BY department;

-- FULL OUTER JOIN
SELECT a.id, a.name, b.order_id
FROM customers a FULL OUTER JOIN orders b ON a.id = b.customer_id;

-- NATURAL JOIN
SELECT * FROM orders NATURAL JOIN customers;

-- CREATE TABLE AS SELECT
CREATE TABLE top_earners AS
SELECT name, salary FROM employees WHERE salary > 100000;

-- Correlated subquery with EXISTS
SELECT name FROM products p
WHERE EXISTS (SELECT 1 FROM orders WHERE product_id = p.id);

-- Generate series with window function
SELECT value, SUM(value) OVER (ORDER BY value) as running_total
FROM GENERATE_SERIES(1, 10);

-- Recursive CTE: fibonacci sequence
WITH RECURSIVE fib(n, a, b) AS (
  SELECT 1, 0, 1
  UNION ALL
  SELECT n + 1, b, a + b FROM fib WHERE n < 15
)
SELECT n, a as fibonacci FROM fib;

-- Recursive CTE: org chart traversal
WITH RECURSIVE org(id, name, level, path) AS (
  SELECT id, name, 0, name FROM employees WHERE manager_id IS NULL
  UNION ALL
  SELECT e.id, e.name, org.level + 1, org.path || ' > ' || e.name
  FROM employees e JOIN org ON e.manager_id = org.id
)
SELECT * FROM org ORDER BY path;

-- Parameterized queries (via wire protocol)
SELECT * FROM users WHERE age > $1 AND region = $2;
```

## Features

### SQL Engine
- **Full SQL parser** — SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, ALTER TABLE, DROP TABLE
- **JOINs** — INNER, LEFT, RIGHT, FULL OUTER, CROSS, NATURAL, USING, with multiple join algorithms (nested loop, hash join, merge join, Grace hash join, sort-merge)
- **Aggregations** — COUNT, SUM, AVG, MIN, MAX, STRING_AGG, GROUP_CONCAT, GROUP BY, HAVING
- **Window functions** — ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, SUM/AVG OVER
- **Subqueries** — scalar, correlated, EXISTS, IN, ANY/ALL
- **CTEs** — WITH clauses, recursive CTEs
- **Set operations** — UNION, INTERSECT, EXCEPT
- **DISTINCT, ORDER BY, LIMIT, OFFSET**
- **Expressions** — arithmetic with proper operator precedence (* / % > + -), string functions (UPPER, LOWER, LENGTH, TRIM, REPLACE, LEFT, RIGHT, REPEAT, REVERSE, LTRIM, RTRIM, SUBSTR), math functions (ABS, FLOOR, CEIL, ROUND, POWER, SQRT, EXP, LOG, MOD, GREATEST, LEAST), CASE WHEN, COALESCE, NULLIF, CAST, string concatenation (||)
- **Date/Time** — NOW(), CURRENT_TIMESTAMP, CURRENT_DATE, EXTRACT(YEAR/MONTH/DAY/HOUR/QUARTER/EPOCH FROM ...), DATE_PART(), INTERVAL arithmetic
- **Constraints** — PRIMARY KEY, UNIQUE, NOT NULL, CHECK, FOREIGN KEY
- **SERIAL** — auto-incrementing primary keys
- **RETURNING** — INSERT/UPDATE/DELETE RETURNING for affected rows
- **UPSERT** — INSERT ON CONFLICT DO UPDATE/DO NOTHING
- **GENERATE_SERIES** — table function for generating number sequences
- **Views and materialized views**
- **Full-text search** with inverted indexes
- **JSON support** — JSON column type, jsonpath queries

### Query Optimizer
- **Cost-based optimizer** with statistics collection (ANALYZE)
- **Query plan cache** — prepared statements and plan reuse
- **Predicate pushdown** and projection pushdown
- **Join ordering optimization**
- **Index selection** — B+ tree, hash, and composite indexes
- **Compiled query engine** — JIT-style compilation for large table scans with DISTINCT, ORDER BY, GROUP BY, HAVING
- **Adaptive engine** — automatically chooses between Volcano iterator and compiled execution

### Storage & Persistence
- **Persistent storage** — data survives process restarts with `--data-dir`
- **Write-Ahead Log (WAL)** — crash recovery via ARIES protocol
- **Buffer pool** — LRU page management with clock-sweep replacement
- **File-backed heaps** — slotted page format
- **Group commit** — batch fsync for 70x throughput improvement
- **Checkpointing** — WAL truncation after flushing dirty pages
- **Auto-checkpoint** — configurable threshold (default 16MB)
- **VACUUM** — dead tuple reclamation after UPDATE/DELETE

### Transactions
- **MVCC** — Multi-Version Concurrency Control with snapshot isolation
- **SSI** — Serializable Snapshot Isolation (prevents write skew)
- **BEGIN/COMMIT/ROLLBACK** through wire protocol
- **Savepoints** — SAVEPOINT, RELEASE, ROLLBACK TO
- **SELECT FOR UPDATE / FOR SHARE** — row-level locking
- **Advisory locks** — pg_advisory_lock, pg_try_advisory_lock, pg_advisory_unlock
- **Auto-commit** mode for individual statements

### Wire Protocol
- **PostgreSQL v3 protocol** — Simple Query and Extended Query
- **Prepared statements** — Parse/Bind/Execute/Describe/Close
- **Parameterized queries** — `$1`, `$2` placeholders
- **Connection pooling** — multiple concurrent clients
- **COPY protocol** — bulk data import/export (37x faster than INSERT)
- **LISTEN/NOTIFY** — pub/sub messaging
- **Table change notifications** — automatic CDC via LISTEN table_changes
- **Advisory locks** — application-level coordination
- **EXPLAIN ANALYZE** — query plan with actual execution statistics
- **Cursors** — DECLARE, FETCH, CLOSE
- **ORM compatibility** — CREATE EXTENSION, COMMENT ON, GRANT/REVOKE stubs

### Data Structures
The codebase includes implementations of 50+ data structures:
- B+ tree, B-epsilon tree, LSM tree, Red-black tree, AVL tree, Splay tree, Treap
- Skip list, Trie, Radix tree, Suffix array
- Bloom filter, Cuckoo filter, Count-Min sketch, HyperLogLog
- Buffer pool, Slotted pages, WAL, Heap files
- Consistent hashing, Merkle tree, Vector clock
- And many more...

## Architecture

```
┌─────────────────────────────────────────┐
│           PostgreSQL Wire Protocol       │
│         (Simple + Extended Query)        │
├─────────────────────────────────────────┤
│              Query Layer                 │
│  ┌─────────┐  ┌──────────┐  ┌────────┐ │
│  │  Parser  │→│ Optimizer │→│Executor│ │
│  └─────────┘  └──────────┘  └────────┘ │
│                    ↓                     │
│  ┌─────────────────────────────────────┐ │
│  │      Adaptive Engine                │ │
│  │  Volcano Iterator ↔ Compiled Query  │ │
│  └─────────────────────────────────────┘ │
├─────────────────────────────────────────┤
│            Transaction Layer             │
│      MVCC / SSI / WAL / Recovery        │
├─────────────────────────────────────────┤
│             Storage Layer                │
│   Buffer Pool → File-Backed Heaps       │
│   B+ Tree Indexes → Disk Manager        │
└─────────────────────────────────────────┘
```

## Performance

TPC-B-style benchmark results (single-threaded, macOS, NVMe SSD):

| Mode | Throughput | Notes |
|------|-----------|-------|
| In-memory (direct) | ~478 TPS | No persistence overhead |
| Persistent (direct) | ~3,704 TPS | Group commit (batch fsync every 5ms) |
| Persistent (wire protocol) | ~53 TPS | pg client over TCP |
| Persistent (immediate fsync) | ~53 TPS | fsync per commit |

## Getting Started

```bash
# Clone
git clone https://github.com/henry-the-frog/henrydb.git
cd henrydb

# Install dependencies
npm install

# Run tests
node --test src/sql.test.js        # Parser tests
node --test src/server.test.js     # Server tests
node --test src/tpcb-benchmark.test.js  # Benchmark

# Start server (in-memory)
node src/server.js

# Start server (persistent)
node src/server.js --data-dir ./data

# Connect
psql -h 127.0.0.1 -p 5432
```

## CLI Options

```
node src/server.js [options]
  --port <port>       Port number (default: 5432)
  --data-dir <path>   Enable persistent storage
  --quiet             Disable verbose logging
```

## Testing

543+ test files with **5,600+ individual tests** covering:
- SQL parsing and execution
- Query optimization
- Transaction isolation (MVCC, SSI)
- Crash recovery (ARIES)
- Wire protocol compatibility
- TPC-B benchmark with ACID verification
- Data structure correctness
- Connection pooling and concurrency

## Project Stats

- **~63,000 lines** of source code
- **~76,000 lines** of tests
- **539 test files**
- **1,094 commits**
- **Pure JavaScript** — no native extensions

## Why?

Because building a database teaches you how databases work in a way that reading about them never will. Every subsystem — parser, optimizer, executor, buffer pool, WAL, MVCC, wire protocol — had to be built, debugged, and tested. The result is a working (if not production-ready) database that you can actually connect to with `psql`.

## License

MIT
