# HenryDB

A PostgreSQL-compatible relational database built from scratch in JavaScript. No dependencies, pure educational implementation with production-grade SQL features.

## Quick Start

```bash
# In-process usage
import { Database } from './src/db.js';

const db = new Database();
db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)');
db.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
const result = db.execute('SELECT * FROM users WHERE age > 25');
console.log(result.rows); // [{id: 1, name: 'Alice', age: 30}]
```

```bash
# HTTP server
node src/server.js --port 3000

curl -X POST http://localhost:3000/query \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT * FROM users"}'
```

```bash
# PostgreSQL wire protocol server (connect with psql or any PG client!)
node src/pg-server.js --port 5433

# In-memory mode (default)
node src/pg-server.js --port 5433

# Persistent mode (data survives restarts)
node src/pg-server.js --port 5433 --dir ./data

# Connect with pg npm client
import pg from 'pg';
const client = new pg.Client({ host: 'localhost', port: 5433 });
await client.connect();
const result = await client.query('SELECT * FROM users WHERE id = $1', [1]);
```

## Features

### Core SQL
- **DDL**: CREATE/DROP TABLE, ALTER TABLE (ADD/DROP/RENAME COLUMN), CREATE/DROP INDEX, CREATE/DROP VIEW
- **DML**: SELECT, INSERT (multi-row), UPDATE, DELETE, UPSERT (ON CONFLICT DO UPDATE)
- **Joins**: INNER, LEFT, RIGHT, FULL OUTER, CROSS, NATURAL, LATERAL
- **Subqueries**: Correlated and uncorrelated, ANY/ALL/EXISTS, IN
- **Set Operations**: UNION [ALL], INTERSECT, EXCEPT
- **CTEs**: WITH and WITH RECURSIVE

### Advanced SQL
- **Window Functions**: ROW_NUMBER, RANK, DENSE_RANK, SUM/AVG/COUNT/MIN/MAX OVER (PARTITION BY ... ORDER BY ...)
- **User-Defined Functions**: `CREATE FUNCTION ... AS $$ body $$` — SQL and JavaScript bodies
- **Table-Returning Functions**: `CREATE FUNCTION ... RETURNS TABLE(...)` — use in FROM clause
- **Dollar-Quoting**: `$$body$$` and `$tag$body$tag$` for function bodies
- **Aggregates**: COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT, ARRAY_AGG, STRING_AGG
- **GROUP BY / HAVING**: Full support with arbitrary expressions

### Indexing & Optimization
- **B-Tree Indexes**: CREATE INDEX, UNIQUE INDEX, multi-column support
- **HOT Chains**: Heap-Only Tuples — skip index updates when non-indexed columns change
- **Index-Only Scans**: Reads from index when all needed columns are included
- **Index-Based Constraint Checking**: O(log N) UNIQUE/PK validation instead of O(N) heap scan
- **ANALYZE**: Gather table statistics (ndistinct, null fraction, most common values, histograms)
- **Selectivity Estimation**: Predict query selectivity from statistics (equality, range, IN, BETWEEN)
- **Cost-Based Optimizer**: Choose between index scan and sequential scan based on estimated selectivity
- **EXPLAIN / EXPLAIN ANALYZE**: View query plans with estimated and actual row counts + timing

### MVCC & Transactions
- **Snapshot Isolation**: Each transaction sees a consistent snapshot
- **MVCC Heap**: xmin/xmax version tracking with hint bits
- **Active Snapshot Sets**: Proper in-progress transaction exclusion (like PostgreSQL's xip[])
- **Savepoints**: SAVEPOINT / ROLLBACK TO / RELEASE
- **Row-Level Locking**: SELECT FOR UPDATE / FOR SHARE / NOWAIT / SKIP LOCKED

### Persistence & Recovery
- **Write-Ahead Logging**: All mutations logged before applying
- **Crash Recovery**: Replay committed transactions, skip uncommitted
- **File-Backed Heap**: Pages persisted to disk via buffer pool
- **VACUUM**: Reclaim dead tuples, rebuild indexes, clear HOT chains

### Other
- **Triggers**: BEFORE/AFTER on INSERT/UPDATE/DELETE
- **Constraints**: PRIMARY KEY, UNIQUE, NOT NULL, CHECK, FOREIGN KEY (with CASCADE)
- **Sequences**: CREATE SEQUENCE, NEXTVAL, CURRVAL, SERIAL/AUTOINCREMENT
- **JSON**: Basic JSON operations
- **Type System**: INT, FLOAT, TEXT, BOOLEAN, DATE, SERIAL

## Architecture

```
┌─────────────────────────────────┐
│     PG Wire Protocol Server     │  ← src/pg-server.js (psql/pg compatible)
│          HTTP Server            │  ← src/server.js (REST API)
├─────────────────────────────────┤
│         SQL Parser              │  ← src/sql.js (2700+ lines)
├─────────────────────────────────┤
│       Query Executor            │  ← src/db.js (7000+ lines)
│   ┌───────────────────┐        │
│   │  Index Optimizer   │        │  ← Index scan planning
│   │  UDF Evaluator     │        │  ← Function catalog
│   │  Constraint Engine │        │  ← PK, UNIQUE, FK, CHECK
│   └───────────────────┘        │
├─────────────────────────────────┤
│     MVCC Layer                  │  ← src/mvcc.js
│   ┌───────────────────┐        │
│   │  Snapshot Manager  │        │
│   │  Version Catalog   │        │
│   │  VACUUM            │        │
│   └───────────────────┘        │
├─────────────────────────────────┤
│     Storage Layer               │
│   ┌─────────┐ ┌───────────┐   │
│   │HeapFile │ │BTreeTable  │   │
│   │(page)   │ │(btree-PK)  │   │
│   └─────────┘ └───────────┘   │
│   ┌─────────┐ ┌───────────┐   │
│   │B+Tree   │ │HOT Chains  │   │
│   │(index)  │ │(optimize)  │   │
│   └─────────┘ └───────────┘   │
├─────────────────────────────────┤
│     WAL (Write-Ahead Log)       │  ← src/wal.js
│   ┌─────────┐ ┌───────────┐   │
│   │MemWAL   │ │FileWAL     │   │
│   └─────────┘ └───────────┘   │
├─────────────────────────────────┤
│     Disk Manager                │  ← src/disk-manager.js
│     Buffer Pool                 │  ← src/buffer-pool.js
└─────────────────────────────────┘
```

## PostgreSQL Wire Protocol

Full PG wire protocol v3 implementation — connect with `psql`, `pg` npm client, or any PostgreSQL driver:

- **Simple query protocol**: Direct SQL execution
- **Extended query protocol**: Prepared statements, parameterized queries ($1, $2...)
- **pg_catalog interceptor**: SET, SHOW, version(), current_database()
- **Persistent mode**: `--dir ./data` flag for crash-safe storage
- **Connection pooling**: Works with `pg.Pool` for concurrent connections
- **Transaction state tracking**: Idle (I), In-Transaction (T), Error (E) indicators
- **SSL negotiation**: Graceful refusal (sends 'N')

## HTTP API

```
GET  /health   → { status, version, tables, functions }
POST /query    → { type, rows, duration_ms }
POST /execute  → { type, message, duration_ms }
GET  /tables   → { tables: { name: { columns, indexes } } }
```

## Examples

### User-Defined Functions
```sql
-- SQL scalar function
CREATE FUNCTION celsius_to_f(c FLOAT) RETURNS FLOAT
AS $$ SELECT c * 9.0 / 5.0 + 32 $$;

-- JavaScript function
CREATE FUNCTION distance(x1 FLOAT, y1 FLOAT, x2 FLOAT, y2 FLOAT) RETURNS FLOAT
LANGUAGE js AS $$ Math.sqrt((x2-x1)**2 + (y2-y1)**2) $$;

-- Table-returning function
CREATE FUNCTION active_users(min_age INT) RETURNS TABLE(name TEXT, age INT)
AS $$ SELECT name, age FROM users WHERE age >= min_age AND active = 1 $$;

SELECT * FROM active_users(18) WHERE name LIKE 'A%';
```

### Row-Level Locking
```sql
BEGIN;
SELECT * FROM accounts WHERE id = 1 FOR UPDATE;
-- Other transactions cannot modify this row until COMMIT
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
COMMIT;
```

### Window Functions
```sql
SELECT name, dept, salary,
  RANK() OVER (PARTITION BY dept ORDER BY salary DESC) as rank,
  SUM(salary) OVER (PARTITION BY dept) as dept_total
FROM employees;
```

### Recursive CTEs
```sql
WITH RECURSIVE tree AS (
  SELECT id, name, parent_id, 0 as depth FROM categories WHERE parent_id IS NULL
  UNION ALL
  SELECT c.id, c.name, c.parent_id, t.depth + 1
  FROM categories c JOIN tree t ON c.parent_id = t.id
)
SELECT * FROM tree ORDER BY depth, name;
```

## Tests

782 test files, ~70K lines of source code. Run with:

```bash
node --test src/hot-chains.test.js src/udf.test.js src/table-func.test.js src/row-locking.test.js
```

## Performance (10K rows, in-memory)

| Operation | Throughput |
|-----------|-----------|
| PK SELECT | ~13,000 q/sec |
| Index SELECT | ~34,000 q/sec |
| Aggregate scan | ~5,500 q/sec |
| INSERT | ~240 rows/sec |
| UPDATE | ~100 rows/sec |
| UDF call | ~9,000 q/sec |

Note: This is a pure-JavaScript educational implementation. Production databases (SQLite, PostgreSQL) are 100-1000x faster.

## License

MIT
