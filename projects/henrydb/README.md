# HenryDB

A SQL database written from scratch in JavaScript. PostgreSQL wire protocol compatible.

## What Is This?

HenryDB is a complete relational database engine implemented in ~63,000 lines of JavaScript. It speaks the PostgreSQL wire protocol, so you can connect with `psql`, `pg` npm module, or any PostgreSQL client library.

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

## Features

### SQL Engine
- **Full SQL parser** — SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, ALTER TABLE, DROP TABLE
- **JOINs** — INNER, LEFT, RIGHT, FULL OUTER, CROSS, with multiple join algorithms (nested loop, hash join, merge join, Grace hash join, sort-merge)
- **Aggregations** — COUNT, SUM, AVG, MIN, MAX, GROUP BY, HAVING
- **Window functions** — ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, SUM/AVG OVER
- **Subqueries** — scalar, correlated, EXISTS, IN, ANY/ALL
- **CTEs** — WITH clauses, recursive CTEs
- **Set operations** — UNION, INTERSECT, EXCEPT
- **DISTINCT, ORDER BY, LIMIT, OFFSET**
- **Expressions** — arithmetic, string functions, date functions, CASE WHEN, COALESCE, CAST
- **Constraints** — PRIMARY KEY, UNIQUE, NOT NULL, CHECK, FOREIGN KEY
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
- **Checkpointing** — periodic WAL truncation

### Transactions
- **MVCC** — Multi-Version Concurrency Control with snapshot isolation
- **SSI** — Serializable Snapshot Isolation (prevents write skew)
- **BEGIN/COMMIT/ROLLBACK** through wire protocol
- **Savepoints** — SAVEPOINT, RELEASE, ROLLBACK TO
- **Auto-commit** mode for individual statements

### Wire Protocol
- **PostgreSQL v3 protocol** — Simple Query and Extended Query
- **Prepared statements** — Parse/Bind/Execute/Describe/Close
- **Parameterized queries** — `$1`, `$2` placeholders
- **Connection pooling** — multiple concurrent clients
- **COPY protocol** — bulk data import
- **LISTEN/NOTIFY** — pub/sub messaging

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
| Persistent (direct) | ~3,704 TPS | Group commit, batch fsync |
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

539 test files with **5,572 individual tests** covering:
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
