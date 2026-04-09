# HenryDB 🐸

A PostgreSQL-compatible relational database engine built from scratch in JavaScript. No database dependencies. No shortcuts. Every component—from the SQL parser to the B+Tree indexes to the write-ahead log—is hand-written.

**50,000+ lines of source code | 441 test files | Full PostgreSQL wire protocol**

## Quick Start

### In-Memory Mode
```javascript
import { Database } from './src/db.js';

const db = new Database();
db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)');
db.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
db.execute("INSERT INTO users VALUES (2, 'Bob', 25)");

const result = db.execute('SELECT name, age FROM users WHERE age > 28');
console.log(result.rows); // [{ name: 'Alice', age: 30 }]
```

### Persistent Mode (data survives restart)
```javascript
import { PersistentDatabase } from './src/persistent-db.js';

const db = PersistentDatabase.open('./my-database');
db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT)');
db.execute("INSERT INTO users VALUES (1, 'Alice')");
db.close();

// Later — data is still there
const db2 = PersistentDatabase.open('./my-database');
db2.execute('SELECT * FROM users'); // [{ id: 1, name: 'Alice' }]
```

### Start the Server
```bash
node src/server.js
# HenryDB server listening on 127.0.0.1:5433
```

### Connect with the CLI
```bash
node src/cli.js
# henrydb> CREATE TABLE demo (id INT PRIMARY KEY, name TEXT);
# henrydb> INSERT INTO demo VALUES (1, 'hello');
# henrydb> SELECT * FROM demo;
#  id | name
# ----+------
#  1  | hello
# (1 row) — 2ms
```

### Connect with psql or any PostgreSQL client
```bash
psql -h 127.0.0.1 -p 5433
```

Works with Knex, Sequelize, pg.js, and any PostgreSQL driver.

## Features

### SQL Engine
- **DDL**: CREATE TABLE, ALTER TABLE, DROP TABLE, CREATE INDEX, TRUNCATE, RENAME
- **DML**: INSERT, SELECT, UPDATE, DELETE, UPSERT (ON CONFLICT)
- **Queries**: WHERE, ORDER BY, GROUP BY, HAVING, LIMIT, OFFSET, DISTINCT
- **Joins**: INNER, LEFT, RIGHT, FULL, CROSS, self-joins, multi-way joins
- **Subqueries**: Scalar, IN, EXISTS, ANY/ALL, correlated subqueries
- **Aggregates**: COUNT, SUM, AVG, MIN, MAX, STRING_AGG, ARRAY_AGG
- **Window Functions**: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTILE, SUM/AVG OVER
- **Set Operations**: UNION, INTERSECT, EXCEPT (with ALL)
- **CTEs**: WITH clauses, recursive CTEs
- **Expressions**: CASE WHEN, COALESCE, NULLIF, CAST, LIKE, BETWEEN, IN
- **Transactions**: BEGIN, COMMIT, ROLLBACK, SAVEPOINT

### Storage Engine
- **Heap Files**: Slotted-page architecture with tuple-level storage
- **BTreeTable**: Clustered B+tree storage engine (`CREATE TABLE ... USING BTREE`), 5,578x faster point lookups
- **B+Tree Indexes**: Balanced tree indexes for primary keys and secondary columns
- **Hash Indexes**: Extendible hash table (`CREATE INDEX ... USING HASH`), O(1) equality lookups
- **Full-Text Search**: Inverted index with TF-IDF ranking and stemming (`MATCH ... AGAINST`)
- **Buffer Pool**: Configurable LRU or Clock-sweep replacement with pin counting, dirty page tracking
- **Write-Ahead Log (WAL)**: ARIES-style recovery with LSN tracking
- **Disk Manager**: Page-level I/O with persistence through close/reopen
- **Point-in-Time Recovery (PITR)**: Restore to any WAL position

### Query Optimizer
- **Cost-Based Optimizer**: PostgreSQL-inspired cost model (seq_page_cost, random_page_cost), sort elimination for BTree, direct PK lookup
- **Index Scan**: Automatically uses indexes when beneficial
- **Hash Join**: 186x faster than nested loops for large joins
- **Sort-Merge Join**: For ordered output
- **Batched WAL**: 96x throughput improvement for bulk operations
- **EXPLAIN**: Query plan visualization

### Server & Protocol
- **PostgreSQL Wire Protocol v3**: Full compatibility with psql and ORMs
- **Prepared Statements**: Parse/Bind/Execute extended query protocol
- **LISTEN/NOTIFY**: Pub/sub notifications
- **COPY IN/OUT**: Bulk data import/export
- **Authentication**: MD5 password authentication
- **Connection Tracking**: Per-connection state, query stats, slow query log

### CLI Tool
- **psql-like REPL**: Multi-line SQL, table formatting, timing
- **Meta-commands**: `\dt` (tables), `\di` (indexes), `\d <table>` (describe)
- **Error handling**: Graceful display of SQL errors

## Architecture

```
┌─────────────────────────────────────────┐
│              CLI (cli.js)               │
│         psql-like REPL client           │
└──────────────────┬──────────────────────┘
                   │ PostgreSQL Wire Protocol
┌──────────────────▼──────────────────────┐
│          Server (server.js)             │
│  Wire protocol, auth, connection mgmt   │
└──────────────────┬──────────────────────┘
                   │
┌──────────────────▼──────────────────────┐
│         SQL Parser (sql.js)             │
│     Tokenizer → Parser → AST            │
└──────────────────┬──────────────────────┘
                   │
┌──────────────────▼──────────────────────┐
│     Query Engine (db.js + optimizer)    │
│  Volcano model, cost optimizer, JIT     │
└──────────────────┬──────────────────────┘
                   │
┌──────────────────▼──────────────────────┐
│         Storage Engine                  │
│  ┌─────────┐ ┌─────────┐ ┌──────────┐  │
│  │ Heap    │ │ B+Tree  │ │ Buffer   │  │
│  │ Files   │ │ Indexes │ │ Pool     │  │
│  └────┬────┘ └─────────┘ └────┬─────┘  │
│       │                       │         │
│  ┌────▼───────────────────────▼─────┐   │
│  │     Write-Ahead Log (WAL)        │   │
│  └──────────────┬───────────────────┘   │
│                 │                        │
│  ┌──────────────▼───────────────────┐   │
│  │     Disk Manager (pages)         │   │
│  └──────────────────────────────────┘   │
└─────────────────────────────────────────┘
```

## Testing

```bash
# Run all tests
npm test

# Run specific test file
node --test src/persistence-e2e.test.js

# Run with pattern matching
node --test --test-name-pattern="index" src/db.test.js
```

**Test coverage:**
- 421 test files
- 4,100+ individual tests
- ~98% pass rate across all tests
- Dedicated test suites for: SQL parser, query engine, joins, indexes, persistence, wire protocol, stress testing

## Performance

| Operation | Performance |
|-----------|------------|
| Hash Join vs Nested Loops | **186x faster** |
| Batched WAL vs Single | **96x throughput** |
| In-memory SELECT (1K rows) | < 1ms |
| Index scan vs full scan | 10-100x faster |
| Wire protocol roundtrip | ~1ms per query |

## Project Stats

| Metric | Count |
|--------|-------|
| Source files | 228 |
| Test files | 421 |
| Source lines | ~52,000 |
| Total lines (incl. tests) | ~115,000 |
| Tests | 4,100+ |
| Dependencies | 0 (core engine) |

## What's NOT Here (Yet)

- Full MVCC transaction isolation (read committed only)
- Vacuum / garbage collection
- Replication
- Partitioning
- Full-text search
- JSON/JSONB data type

## License

MIT

---

*Built from scratch, one page at a time.* 🐸
