# HenryDB

A SQL database engine written from scratch in JavaScript. No dependencies. **368 source files, ~78,000 lines of code, 868 test files with ~8,900 tests.** Implements 38+ academic papers spanning 50 years of database systems research.

PostgreSQL-compatible: speaks the wire protocol, so `psql` connects to it. All 33 TPC-H queries pass.

## Quick Start

```javascript
import { Database } from './src/db.js';

const db = new Database();
db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)');
db.execute("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Carol', 35)");

// Standard SQL
db.execute('SELECT name, age FROM users WHERE age > 25 ORDER BY age DESC');

// Window functions
db.execute('SELECT name, RANK() OVER (ORDER BY age DESC) AS rank FROM users');

// Recursive CTE
db.execute(`
  WITH RECURSIVE nums(n) AS (
    SELECT 1 UNION ALL SELECT n+1 FROM nums WHERE n < 10
  )
  SELECT n FROM nums
`);
```

```bash
# PostgreSQL wire protocol server (connect with psql)
node src/pg-server.js --port 5432

# HTTP server
node src/server.js --port 3000
```

## What's Inside

### SQL Parser (3,206 LOC)
Hand-written recursive descent parser. Handles DDL, DML, subqueries, CTEs (including recursive), window functions, MERGE, EXPLAIN ANALYZE, PL/SQL blocks, and more.

### Five Execution Engines

| Engine | Approach | Based On |
|--------|----------|----------|
| **AST Interpreter** | Walk parsed tree directly | — |
| **Volcano Iterator** | Pull-based `open()/next()/close()` | Graefe 1993 |
| **Pipeline Compiler** | Push-based fused operator loops | Neumann 2011 (HyPer) |
| **Query VM (VDBE)** | Register-based bytecode, 30+ opcodes | SQLite |
| **Query Codegen** | Generates JavaScript source → V8 JIT | Copy-and-patch |

Plus a **vectorized execution bridge** (MonetDB/X100 style) for columnar batch processing.

### Storage Layer
- **Heap files** — slotted page layout (8KB pages)
- **Buffer pool** — LRU eviction, pin counting, clock-sweep replacement
- **Disk manager** — page-level I/O abstraction
- **Column store** — columnar storage for analytical queries
- **Columnar compression** — dictionary, RLE, delta, bit-packing

### Indexes (7 types)
- **B+ tree** — range queries, the workhorse
- **Adaptive Radix Tree (ART)** — 4 node types (Node4/16/48/256), cache-friendly
- **B-epsilon tree** — write-optimized with message buffers at internal nodes
- **Bitmap index** — low-cardinality columns
- **Bloom filter** — probabilistic membership testing
- **Bitwise trie** — exact match, prefix search
- **Hash** — 6 variants:
  - Chained, Extendible, Linear, Robin Hood, Cuckoo, Double hashing

### Write-Ahead Log & Recovery
- **ARIES-style recovery** — fuzzy checkpoints, dirty page table, per-page LSN
- **WAL** — every mutation logged before heap modification
- **File-based WAL** — persistent, crash-safe
- **WAL truncation** — post-checkpoint cleanup
- **Checkpoint handler** — periodic and on-demand

### Transactions & Concurrency
- **MVCC** — PostgreSQL-style snapshots (`xmin:xmax:xip_list`), hint bits
- **3 isolation levels** — Read Committed, Snapshot Isolation, Serializable (SSI)
- **SSI** — rw-antidependency tracking, dangerous structure detection (Cahill et al., 2008)
- **Two-Phase Commit** — distributed transaction coordination
- **Advisory locks** — application-level locking
- **VACUUM** — dead tuple removal with xmin horizon

### Query Optimizer
- **Cost-based** — System R-style with histogram statistics
- **Index selection** — sequential scan vs index scan vs index-only scan
- **Join reordering** — dynamic programming over join graphs
- **Join algorithms** — nested loop, hash join, index nested-loop join, Grace hash join, symmetric hash join, bloom join, band join
- **Decorrelation** — pulls correlated subqueries into joins
- **Index advisor** — recommends indexes based on workload

### SQL Features
- DDL: `CREATE/ALTER/DROP TABLE`, `CREATE INDEX`, `CREATE VIEW`, `CREATE SEQUENCE`
- DML: `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE`
- Queries: `SELECT`, `JOIN` (inner/left/right/full/cross/natural), `UNION/INTERSECT/EXCEPT`
- CTEs: `WITH`, `WITH RECURSIVE`
- Window functions: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `CUME_DIST`, `PERCENT_RANK`, `NTH_VALUE`, `NTILE`
- Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV`, `VARIANCE`, `PERCENTILE_CONT`, `BOOL_AND`, `BOOL_OR`, `STRING_AGG`, `ARRAY_AGG`, and more
- `GROUPING SETS`, `ROLLUP`, `CUBE`
- Subqueries: scalar, `IN`, `EXISTS`, `ANY/ALL`, correlated
- `CASE`, `COALESCE`, `NULLIF`, `CAST`
- `EXPLAIN ANALYZE` with formatted output
- Prepared statements
- `TABLESAMPLE`

### Distributed Systems
- **Raft consensus** — leader election, log replication, safety (Ongaro & Ousterhout, 2014)
- **SWIM gossip** — failure detection, piggybacked dissemination (Das et al., 2002)
- **Logical replication** — CDC, publication/subscription, replication slots
- **Consistent hashing** — partition distribution
- **CRDTs** — G-Counter, PN-Counter, OR-Set (Shapiro et al., 2011)

### PostgreSQL Compatibility
- **Wire protocol** — `psql` can connect and run queries
- **PL/SQL** — stored procedures, IF/ELSE, WHILE, RAISE
- **Row-level security** — per-user access policies
- **Server sessions** — connection management, session state
- **Cursors** — scrollable, holdable, with FETCH

### Probabilistic & Advanced Data Structures
- **HyperLogLog** — cardinality estimation (Flajolet et al., 2007)
- **Count-Min Sketch** — frequency estimation (Cormode & Muthukrishnan, 2005)
- **T-Digest** — streaming quantile estimation (Dunning, 2019)
- **Wavelet tree** — rank/select/access in O(log σ)
- **Skip list** — MVCC-aware concurrent skip list
- **Treap** — randomized BST with order statistics
- **COLA** — cache-oblivious lookahead array
- **Rope** — efficient large string operations
- **LSM tree** — log-structured merge tree with compaction strategies

### Performance
- **11,000** inserts/sec (batch mode)
- **9,000** point queries/sec
- **186x** hash join speedup vs nested loop
- **17x** pipeline compiler speedup vs Volcano on LIMIT queries
- **8.2x** WAL batch flush speedup (29,500 rows/sec)

## Testing

```bash
# Run all tests
node --test src/*.test.js

# Run a specific module
node --test src/mvcc.test.js

# Run SQL fuzzer
node --test src/sql-fuzzer-v2.test.js

# Run TPC-H
node --test src/tpch-benchmark.test.js
```

**868 test files, ~8,900 tests** covering:
- Correctness (SQL semantics, ACID properties)
- Concurrency (MVCC visibility, SSI anomaly detection)
- Recovery (crash + restart with WAL replay)
- Performance (TPC-H, fuzzer, stress tests)
- Edge cases (NULL handling, empty tables, boundary values)

## Architecture

```
src/
├── SQL           sql.js (parser), sql-functions.js, expression-evaluator.js
├── Execution     volcano.js, volcano-planner.js, pipeline-compiler.js
│                 query-vm.js, query-codegen.js, vectorized.js
├── Storage       page.js, buffer-pool.js, heap-file.js, column-store.js
├── Indexes       bplus-tree.js, art.js, b-epsilon-tree.js, bitmap-index.js
│                 bloom.js, hash-index.js, robin-hood-hash.js, cuckoo-hash.js
├── WAL           wal.js, file-wal.js, aries-recovery.js, checkpoint-handler.js
├── Transactions  mvcc.js, ssi.js, transactional-db.js, advisory-locks.js
├── Optimizer     planner.js, query-plan.js, cardinality.js, index-advisor.js
├── Joins         join-executor.js, grace-hash-join.js, symmetric-hash-join.js
│                 bloom-join.js, band-join.js
├── Distributed   raft.js, gossip.js, logical-replication.js, consistent-hash.js
│                 crdt.js, two-phase-commit.js
├── Server        pg-server.js, pg-protocol.js, server.js, cli.js
├── Probabilistic hyperloglog.js, count-min-sketch.js, t-digest.js
│                 wavelet-tree.js, skip-list.js, treap.js
└── Core          db.js, catalog.js, comparator.js, compression.js
```

## Academic Papers Implemented

38+ papers spanning 1970–2024:
- **Indexes**: B+ tree (1972), ART (2013), B-epsilon (2003), Robin Hood (1986), Cuckoo (2001), Extendible (1979), Linear (1980)
- **Execution**: Volcano (Graefe 1993), Push-based compilation (Neumann 2011), Vectorized (Boncz 2005), Morsel-driven parallelism (Leis 2014)
- **Recovery**: ARIES (Mohan 1992)
- **Concurrency**: MVCC/SI (Berenson 1995), SSI (Cahill 2008, Ports & Grittner 2012)
- **Joins**: Grace hash join (Kitsuregawa 1983), Radix join (Manegold 2000)
- **Distributed**: Raft (Ongaro 2014), SWIM (Das 2002), CRDTs (Shapiro 2011)
- **Probabilistic**: HyperLogLog (Flajolet 2007), Count-Min (Cormode 2005), T-Digest (Dunning 2019), Bloom filter (1970), Cuckoo filter (Fan 2014)
- **Optimization**: System R (Selinger 1979), TPC-H (Transaction Processing Council)

## License

MIT
