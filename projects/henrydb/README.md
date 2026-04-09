# HenryDB 🐸

A PostgreSQL-compatible database engine built from scratch in JavaScript. No dependencies. No shortcuts.

## Quick Start

```javascript
import { Database } from './src/db.js';

const db = new Database();

// Create a table
db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');

// Insert data
db.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
db.execute("INSERT INTO users VALUES (2, 'Bob', 25)");

// Query
const result = db.execute('SELECT name, age FROM users WHERE age > 28');
console.log(result.rows); // [{ name: 'Alice', age: 30 }]
```

## Start the Server

```bash
node src/cli.js --port 5432
```

Connect with any PostgreSQL client:
```bash
psql -h localhost -p 5432 -U test -d test
```

## Features

### SQL Support
- **DDL**: CREATE TABLE, DROP TABLE, ALTER TABLE, CREATE INDEX
- **DML**: INSERT, UPDATE, DELETE with full WHERE clause support
- **Queries**: SELECT with JOIN, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET
- **Advanced**: Subqueries, CTEs (WITH), Window Functions, UNION/INTERSECT/EXCEPT
- **Prepared Statements**: Parameterized queries with plan caching

### Storage Engine
- Slotted page heap files with in-place updates
- B+Tree indexes (unique and non-unique)
- Hash indexes for equality lookups
- Buffer pool with LRU eviction
- LSM tree with bloom filters and compaction

### Transaction Support
- Write-Ahead Logging (WAL)
- ARIES-style fuzzy checkpoints with dirty page tracking
- Point-in-Time Recovery (PITR)
- MVCC (Multi-Version Concurrency Control)
- Savepoints
- Deadlock detection via wait-for graph

### Query Engine
- Hand-written recursive descent SQL parser
- Volcano-style iterator execution model
- Hash join optimization (auto-detected for equi-joins)
- Cost-based query optimizer with histogram statistics
- Query plan cache with DDL invalidation

### Network
- PostgreSQL wire protocol v3 (Simple Query)
- Compatible with psql, pgAdmin, and PostgreSQL drivers

## Benchmark

Tested on Apple Silicon, in-memory mode:

| Operation | 1K rows | 10K rows |
|-----------|---------|----------|
| INSERT rate | 14,396/sec | 29,514/sec |
| Point query | 14,009/sec | 9,005/sec |
| Full table scan | 16ms | 73ms |
| Range scan (WHERE) | 8ms | 46ms |
| Aggregation | 8ms | 44ms |
| GROUP BY | 8ms | 39ms |
| ORDER BY + LIMIT | 11ms | 50ms |
| JOIN (hash join) | 28ms | 86ms |
| UPDATE | 9ms | 77ms |
| DELETE | 8ms | 48ms |

Run the benchmark:
```bash
node benchmark.js
```

## Data Structures

HenryDB includes implementations of:
- B+Tree (indexes)
- Skip List (memtable for LSM)
- Bloom Filter (SSTable lookups)
- R-Tree (spatial indexing)
- Cuckoo Hash Table
- Trie (prefix queries)
- Ring Buffer
- LRU Cache

## Architecture

See [docs/architecture-blog.md](docs/architecture-blog.md) for a deep dive into the design, including the WAL, SQL parser, and query execution engine.

## Test Suite

```bash
npm test
```

3,000+ tests covering SQL parsing, storage engine, WAL/recovery, protocol, and data structures.

## What This Is (and Isn't)

**This is:** A learning project and portfolio piece. It demonstrates understanding of database internals — the kind of code that makes systems work.

**This isn't:** Production-ready. Don't put your data in it. (Yet.)

## License

MIT
