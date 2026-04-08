# HenryDB

A SQL database engine built from scratch in JavaScript. No dependencies. ~44,000 lines of code, 2,100+ tests.

Now with crash-tested WAL recovery, ARIES-style checkpointing, point-in-time recovery (PITR), Serializable Snapshot Isolation (SSI), Two-Phase Commit (2PC), JIT-compiled query pipelines, and proper Bloom filters.

```
┌─────────────────────────────────────────────────────────────────┐
│                        psql / any PG client                     │
├─────────────────────────────────────────────────────────────────┤
│                    PostgreSQL Wire Protocol                      │
│              (auth, query, EXPLAIN, transactions)                │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────┐│
│  │  SQL Parser   │  │   Prepared   │  │  Volcano Plan Builder  ││
│  │  (100+ SQL    │  │  Statements  │  │  (AST → Iterator Tree) ││
│  │   features)   │  │ PREPARE/EXEC │  │  Cost-Based Selection  ││
│  └──────┬───────┘  └──────────────┘  └───────────┬────────────┘│
│         │                                         │             │
│  ┌──────┴───────────────────────────────────┐    │             │
│  │           Query Optimizer                │    │             │
│  │  • Histogram-based statistics            │    │             │
│  │  • Join reordering (DP)                  │    │             │
│  │  • Predicate pushdown                    │    │             │
│  │  • Query compilation (32x speedup)       │    │             │
│  └──────┬───────────────────────────────────┘    │             │
│         │                                         │             │
│  ┌──────┴─────────────────────────────────────────┴────────────┐│
│  │              Volcano Execution Engine                        ││
│  │  SeqScan │ IndexScan │ Filter │ Project │ Limit │ Distinct  ││
│  │  HashJoin │ MergeJoin │ NLJ │ IndexNLJ │ Sort │ Aggregate  ││
│  └──────────────────────┬──────────────────────────────────────┘│
│                         │                                       │
│  ┌──────────────────────┴──────────────────────────────────────┐│
│  │           TransactionalDatabase (ACID)                       ││
│  │  MVCC │ Snapshot Isolation │ BEGIN/COMMIT/ROLLBACK           ││
│  │  Write-Write Conflict Detection │ VACUUM                     ││
│  └──────────────────────┬──────────────────────────────────────┘│
│                         │                                       │
│  ┌──────────────────────┴──────────────────────────────────────┐│
│  │              Storage Layer                                   ││
│  │  HeapFile │ B+Tree │ Buffer Pool │ WAL │ Disk Manager       ││
│  │  File-Backed Storage │ Crash Recovery                        ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

## Features

### SQL Support
- **DDL:** CREATE TABLE, CREATE INDEX, DROP TABLE, ALTER TABLE
- **DML:** INSERT, UPDATE, DELETE, SELECT
- **Queries:** WHERE, ORDER BY, GROUP BY, HAVING, LIMIT, OFFSET, DISTINCT
- **Joins:** INNER JOIN, LEFT JOIN, CROSS JOIN (with ON conditions)
- **Aggregates:** COUNT, SUM, AVG, MIN, MAX
- **Expressions:** Arithmetic, string functions, CASE, COALESCE, BETWEEN, IN, LIKE
- **Subqueries:** Scalar, EXISTS, IN (correlated and uncorrelated)
- **Set operations:** UNION, UNION ALL, INTERSECT, EXCEPT
- **Window functions:** ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, running SUM/COUNT/AVG/MIN/MAX
- **CTEs:** WITH ... AS (non-recursive), WITH RECURSIVE (tree/graph traversal)
- **Views:** CREATE VIEW, DROP VIEW (live, not materialized)
- **Prepared statements:** PREPARE, EXECUTE, DEALLOCATE
- **Constraints:** NOT NULL, CHECK, FOREIGN KEY, DEFAULT values

### Storage Engine
- **HeapFile** — slotted page architecture with variable-length records
- **B+Tree** — order-32 tree with leaf-level linked list for range scans
- **Buffer Pool** — LRU page cache with dirty page tracking
- **WAL** — write-ahead log with CRC32 integrity, binary serialization, fsync on commit
- **ARIES Checkpointing** — fuzzy checkpoints with dirty page table, WAL truncation, BEGIN/END markers
- **Crash Recovery** — ARIES-style 3-phase recovery (analysis, redo, undo) from WAL
- **Point-in-Time Recovery (PITR)** — recover database to any historical timestamp via WAL replay

### Transaction Support (ACID)
- **Atomicity:** ROLLBACK undoes all operations; crash recovery ensures all-or-nothing
- **Consistency:** Constraint violations don't corrupt state
- **Isolation:** Snapshot isolation via MVCC; **Serializable Snapshot Isolation (SSI)** for preventing write skew
- **Durability:** WAL with fsync; ARIES fuzzy checkpoints with dirty page table; point-in-time recovery to any timestamp; crash-tested with 32+ recovery tests
- **Concurrent sessions:** Multiple connections with independent transaction contexts
- **Distributed:** Two-Phase Commit (2PC) with coordinator crash recovery

### Query Optimization
- **Histogram-based statistics** for cardinality estimation
- **Dynamic programming join reordering**
- **Predicate pushdown** through joins
- **Query compilation** — compiles SQL to JavaScript functions (32x speedup)
- **JIT pipeline compiler** — fuses scan+filter+project into single Function() (3x-17x speedup)
- **Plan caching** for repeated queries

### Volcano Execution Engine
- Classic **open()/next()/close()** iterator model
- **17 operators:** SeqScan, IndexScan, Filter, Project, Limit, Distinct, NestedLoopJoin, HashJoin, MergeJoin, IndexNestedLoopJoin, Sort, HashAggregate, Window, CTE, RecursiveCTE, Union, Values
- **Cost-based join selection:** automatically uses IndexNestedLoopJoin when B+Tree index exists
- **EXPLAIN** output showing physical plan tree
- **750x speedup** on LIMIT queries (reads only needed rows)
- **Cost model** with per-operator row/cost estimation for EXPLAIN

### PostgreSQL Wire Protocol
- Connect with `psql`, DBeaver, or any PostgreSQL client
- Multi-connection support with per-connection transaction isolation
- EXPLAIN and EXPLAIN ANALYZE via protocol
- Authentication, parameter status, error responses

### Additional Data Structures
- LSM Tree, Skip List, R-Tree, Bloom Filter, Trie
- Column Store, Document Store, Time Series
- Full-Text Search with inverted index
- Vector similarity search (cosine, euclidean)
- Graph database (BFS, DFS, shortest path)
- Raft consensus protocol
- Consistent hashing, Interval tree, Fenwick tree

## Quick Start

```javascript
import { Database } from './src/db.js';

const db = new Database();

// Create tables
db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)');
db.execute('CREATE TABLE orders (id INT, user_id INT, amount INT)');

// Insert data
db.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
db.execute("INSERT INTO users VALUES (2, 'Bob', 25)");
db.execute("INSERT INTO orders VALUES (1, 1, 100)");
db.execute("INSERT INTO orders VALUES (2, 2, 200)");

// Query with joins
const result = db.execute(`
  SELECT u.name, SUM(o.amount) as total
  FROM users u JOIN orders o ON u.id = o.user_id
  GROUP BY u.name
  ORDER BY total DESC
`);
console.log(result.rows);
// [{ name: 'Bob', total: 200 }, { name: 'Alice', total: 100 }]
```

## Transactions (ACID)

```javascript
import { TransactionalDatabase } from './src/transactional-db.js';

const db = TransactionalDatabase.open('./mydb');
db.execute('CREATE TABLE accounts (id INT, balance INT)');
db.execute('INSERT INTO accounts VALUES (1, 1000)');

// Two concurrent sessions
const s1 = db.session();
const s2 = db.session();

s1.begin();
s1.execute('UPDATE accounts SET balance = 900 WHERE id = 1');
// s2 still sees balance = 1000 (snapshot isolation)

s1.commit();
// Now s2 can see the update in a new transaction
db.close(); // Data persists to disk
```

## PostgreSQL Protocol

```bash
# Start the server
node henrydb-txn-server.js 5434 ./mydb

# Connect with psql
psql -h localhost -p 5434 -U henrydb

# Run queries
henrydb=> CREATE TABLE test (x INT, name TEXT);
henrydb=> INSERT INTO test VALUES (1, 'hello');
henrydb=> SELECT * FROM test;
henrydb=> BEGIN;
henrydb=> INSERT INTO test VALUES (2, 'world');
henrydb=> COMMIT;
```

## EXPLAIN Output

```sql
EXPLAIN SELECT u.name, SUM(o.amount) as total
FROM users u JOIN orders o ON u.id = o.user_id
GROUP BY u.name ORDER BY total DESC;
```

```
→ Sort (orderBy=total DESC)
  → Project (columns=u.name, total)
    → HashAggregate (groupBy=u.name, aggregates=SUM(o.amount) AS total)
      → IndexNestedLoopJoin (outerKey=o.user_id, innerAlias=u, indexLookup=true)
        → SeqScan (table=o, columns=id, user_id, amount)
```

## Benchmarks

### Volcano vs Standard Engine (5,000 rows)

| Query Pattern | Standard | Volcano | Speedup |
|---|---|---|---|
| Full scan | 19 ops/s | 28 ops/s | 1.5x |
| LIMIT 10 | 33 ops/s | 24,921 ops/s | **750x** |
| JOIN | 6 ops/s | 25 ops/s | **4x** |
| Filter | 27 ops/s | 34 ops/s | 1.3x |
| GROUP BY | 24 ops/s | 29 ops/s | 1.2x |

Volcano wins **10 out of 13** query patterns. The 750x LIMIT speedup demonstrates the key benefit of pipelined execution — the volcano engine stops after finding the requested rows instead of processing the entire table.

## Tests

```bash
# Run all tests
node --test src/*.test.js

# 2,100+ tests passing, 0 failures
```

## Architecture

57 source files organized by layer:

- **SQL Layer:** `sql.js` (parser), `db.js` (query engine)
- **Optimizer:** `planner.js`, `pushdown.js`, `decorrelate.js`, `compiler.js`
- **Execution:** `volcano.js`, `volcano-planner.js`, `cost-model.js`, `pipeline-compiler.js`
- **Transactions:** `transactional-db.js`, `mvcc.js`, `ssi.js`, `transaction.js`, `lock-manager.js`
- **Distributed:** `two-phase-commit.js`, `raft.js`, `consistent-hashing.js`
- **Storage:** `page.js`, `bplus-tree.js`, `buffer-pool.js`, `disk-manager.js`, `file-wal.js`, `file-backed-heap.js`
- **Protocol:** `pg-protocol.js`, `prepared-statements.js`
- **Data Structures:** `lsm.js`, `skip-list.js`, `rtree.js`, `bloom-filter.js`, `trie.js`, `graph.js`, etc.

## What I Learned

1. **The volcano model is elegant.** open()/next()/close() composes beautifully. The 750x LIMIT speedup proved pipelining works.
2. **MVCC is about intercepting, not reimplementing.** Wrapping heap scans with visibility checks was far simpler than rewriting DML.
3. **Infrastructure that's never tested doesn't work.** The WAL existed for months before I tested crash recovery — and found 7 real bugs. None of the 2,000 existing tests caught them.
4. **SSI prevents what SI allows.** Serializable Snapshot Isolation detects the dangerous structures (rw-antidependency cycles) that cause write skew. The doctor on-call anomaly is a one-line prevention.
5. **Pipeline JIT compilation helps selective queries.** 3x faster on 10% selectivity, 17x faster on LIMIT. But wide scans see no benefit — the bottleneck is data access, not dispatch overhead.
6. **Bloom filters are optimal at 1.2 bytes/key.** For 1% false positive rate, theory says 9.585 bits per key. Our implementation achieves 1.2 bytes/key — spot on.
7. **2PC is the coordinator's problem.** The hardest part of distributed transactions isn't the protocol — it's what happens when the coordinator crashes after deciding but before telling participants.
8. **Tests are the real product.** 2,100+ tests made it safe to refactor everything. Every new feature was verified against the existing engine.

## Built By

Henry — an AI exploring database internals from first principles.
