# HenryDB 🗄️

**A teaching database engine built from scratch in JavaScript.**

4,460+ tests · 92K+ lines · 543 files · Every major concept implemented.

## What Is This?

HenryDB is an educational database engine that implements virtually every concept from a database systems textbook. It's not meant for production — it's meant to *understand* how databases actually work, from B+ trees to Raft consensus.

Built as a deep-dive into database internals. **Connects to real PostgreSQL clients and ORMs.**

## 🔥 New: Real PostgreSQL Server

HenryDB now runs as a TCP server that speaks the PostgreSQL wire protocol. Connect with `psql`, the `pg` driver, or any ORM.

```bash
# Start the server
node src/server.js 5433

# Connect with psql
psql -h 127.0.0.1 -p 5433

# Or use the built-in CLI
node src/cli.js
```

### Works with Real ORMs

```javascript
import knex from 'knex';

const db = knex({
  client: 'pg',
  connection: { host: '127.0.0.1', port: 5433, user: 'app', database: 'app' },
});

// Full SQL support via Knex
await db.raw('CREATE TABLE users (id INTEGER, name TEXT, email TEXT)');
await db.raw('INSERT INTO users VALUES (?, ?, ?)', [1, 'Alice', 'alice@example.com']);
const result = await db.raw('SELECT * FROM users WHERE id = ?', [1]);
// → [{ id: 1, name: 'Alice', email: 'alice@example.com' }]
```

### Crash Recovery with WAL

```javascript
import { Database } from './src/db.js';

// Enable persistent WAL
const db = new Database({ dataDir: './mydata', walSync: 'immediate' });
db.execute('CREATE TABLE accounts (id INTEGER, name TEXT, balance INTEGER)');
db.execute("INSERT INTO accounts VALUES (1, 'Alice', 1000)");
db.close();

// After crash: recover from WAL
const recovered = Database.recover('./mydata');
recovered.execute('SELECT * FROM accounts');
// → [{ id: 1, name: 'Alice', balance: 1000 }]  ✅ Data survived!
```

## Features

### 🌐 PostgreSQL Wire Protocol
- **TCP Server** — accepts real PostgreSQL client connections
- **Simple Query Protocol** — `psql` compatible
- **Extended Query Protocol** — Parse/Bind/Describe/Execute/Sync/Close
- **Prepared Statements** — named statement reuse, parameter substitution
- **COPY Protocol** — bulk load (COPY FROM STDIN) + export (COPY TO STDOUT)
- **LISTEN/NOTIFY** — real-time pub/sub through wire protocol
- **Server-Side Cursors** — DECLARE/FETCH/MOVE/CLOSE for streaming results
- **EXPLAIN ANALYZE** — execution stats with timing, row counts, engine info
- **System Query Interception** — `version()`, `SET`, `pg_catalog` for ORM compatibility
- **Connection Pooling** — multiple concurrent clients
- **ORM Support** — tested with Knex, pg driver, connection pools
- **Express Integration** — full REST API demo (Express + Knex → HenryDB)
- **Streaming Replication** — primary-replica sync via LISTEN/NOTIFY + SQL replay

### 💾 Write-Ahead Log (WAL)
- **Binary format** with CRC32 checksums and LSN tracking
- **Segment rotation** (16MB segments)
- **fsync modes** — immediate, batch (100ms), or none
- **Two-pass replay** — committed transactions only, uncommitted rolled back
- **Crash recovery** — `Database.recover()` rebuilds state from WAL
- **Corruption detection** — CRC verification stops at first corrupt record
- **Multiple recovery cycles** — works across repeated crash/recover sequences

### Query Execution (5 Engines)
- **Volcano** — classic iterator-based tuple-at-a-time
- **Compiled** — pipeline JIT compilation via closures (365x faster)
- **Vectorized** — columnar batch processing (220x faster)
- **Codegen** — `new Function()` query compilation (143x faster)
- **Adaptive** — auto-selects best engine based on query characteristics

### Index Structures (12+)
B+ tree · R-tree (spatial) · ART (Adaptive Radix Tree) · Skip list · Trie (prefix search) · Inverted index (BM25) · Suffix array · Cuckoo hash · Robin Hood hash · Double hashing · Extendible hashing · Linear hashing

### Join Algorithms (10)
Hash join · Sort-merge join · Nested loop · Index nested loop · Semi/anti join (EXISTS/NOT EXISTS) · Band join (BETWEEN) · Theta join · Grace hash join · Radix-partitioned join · Symmetric hash join

### Probabilistic Data Structures (7)
Bloom filter · Cuckoo filter · XOR filter · Count-Min Sketch · HyperLogLog · T-Digest · MinHash

### Concurrency Control (7 Protocols)
MVCC · Two-Phase Locking (2PL) · Optimistic CC (OCC) · Timestamp Ordering · Lock Manager (S/X/IS/IX/SIX) · Deadlock Detector (wait-for graph) · Savepoints (nested transactions)

### Storage Engine
LSM-tree with compaction (leveled + size-tiered) · Write-Ahead Log (WAL) · Buffer Pool Manager (LRU) · Slotted Page · Heap File · Column Store · Log-structured Hash Table · Page versioning

### Distributed Systems
Raft consensus (leader election + log replication) · Lamport clocks · Vector clocks · CRDT counters (G-Counter, PN-Counter) · Gossip protocol · Consistent hashing

### Compression
Run-Length Encoding · Delta encoding · Bit-packing · Dictionary encoding · Frame-of-Reference

### SQL Features
Window functions (ROW_NUMBER, RANK, LAG, LEAD, SUM OVER) · Common Table Expressions (WITH) · Materialized views · Correlated subqueries · Expression compiler · Constant folder · Query rewriter · Plan visualization (DOT/text) · EXPLAIN

### PostgreSQL Wire Protocol (Full Server)
- **TCP server** with PostgreSQL v3 wire protocol handshake
- **Simple query protocol** + **Extended query protocol** (Parse/Bind/Describe/Execute/Sync)
- **MD5 authentication** with salt
- **SSL negotiation** rejection (graceful downgrade)
- **LISTEN/NOTIFY** — real-time pub/sub messaging
- **COPY protocol** — bulk COPY FROM STDIN and COPY TO STDOUT
- **Server-side cursors** — DECLARE/FETCH/MOVE/CLOSE
- **EXPLAIN ANALYZE** — execution stats with timing and row counts
- **EXPLAIN (FORMAT JSON/YAML/DOT)** — multiple output formats, Graphviz visualization
- **VACUUM/ANALYZE/TRUNCATE** — maintenance commands
- **SAVEPOINT/RELEASE/ROLLBACK TO** — nested transaction support
- **RETURNING clause** — INSERT/UPDATE/DELETE RETURNING * through wire protocol
- **INSERT ON CONFLICT DO NOTHING** — upsert support
- **CREATE/DROP INDEX** — B-tree indexes through wire protocol
- **ALTER TABLE** — ADD/DROP COLUMN, RENAME TABLE/COLUMN
- **CREATE TEMP TABLE** — connection-scoped, auto-dropped on disconnect
- **SET/SHOW parameters** — per-connection runtime configuration

### Server Monitoring & Observability
- **HTTP Health Check** — /health (JSON) + /metrics (Prometheus format) on port+1
- **pg_stat_statements** — query pattern tracking with timing stats
- **pg_stat_activity** — connection monitoring
- **pg_stat_bgwriter** — WAL/checkpoint metrics
- **pg_stat_slow_queries** — slow query log with configurable threshold
- **pg_locks** — transaction-level lock information
- **pg_cancel_backend/pg_terminate_backend** — connection management
- **information_schema** — tables/columns views
- **Query cache** — LRU with table-level invalidation and hit rate stats

### ORM & Driver Compatibility
- **pg (node-postgres)** — full integration including connection pools
- **Knex.js** — CRUD, aggregates, JOINs, subqueries, transactions
- **Sequelize** — connect, raw queries, model operations, aggregates, JOINs
- **pg_dump/pg_restore** — SQL and COPY format export/import

### Application Patterns (Tested Through Wire Protocol)
- **Express REST API** — full task management app with Knex + HenryDB
- **TPC-H Benchmark** — 6 tables, 800+ rows, 10 adapted queries
- **E-Commerce** — 4-table schema with order history, revenue analytics, inventory
- **Social Network** — friends graph, mutual friends, news feed, engagement metrics
- **Multi-Tenant SaaS** — tenant isolation, admin dashboard, usage billing
- **Time-Series** — IoT sensor data, range queries, downsampling, anomaly detection
- **Document Store** — JSON CRUD with nested extraction, aggregates
- **Analytics/BI** — star schema, revenue by region/category, cross-tabulation
- **ETL Pipeline** — extract-transform-load with staging/summary/report tables
- **CQRS** — event sourcing, materialized read models, event replay
- **CDC** — change data capture via LISTEN/NOTIFY
- **Geospatial** — bounding box queries, distance ordering, nearest neighbors
- **Full-Text Search** — LIKE, LOWER, multi-column, relevance ranking
- **Schema Migrations** — versioned schema changes with rollback tracking
- **GraphQL-like** — field selection, filtering, nested relations
- **Banking** — accounts, transactions, transfers, customer assets
- **School/University** — students, courses, enrollments, dean's list, GPA analytics
- **Music Streaming** — artists, albums, tracks, playlists, genre stats
- **Fitness Tracking** — workouts, calories, step counts, leaderboard
- **Travel Booking** — flights, hotels, bookings, route search
- **Log Analysis** — 100-entry access logs, status codes, error rates, top endpoints
- **Real Estate** — property listings, agent performance, sold analysis
- **Stress Tests** — 10 concurrent connections, connection pools, rapid cycling

### Analytics & Observability
Statistics collector (histograms, NDV, selectivity) · Cursor pagination · Change Data Capture · Time series engine · Graph database primitives · Data generator (TPC-H style)

### More Data Structures
Fenwick tree · Segment tree · Union-Find · Treap · Splay tree · Binary heap · Quadtree · Interval tree · Order statistics tree · Ring buffer · LRU-K cache · LFU cache

## Performance Highlights

| Benchmark | Speedup vs Volcano |
|---|---|
| Compiled query engine | **365x** |
| Vectorized execution | **220x** |
| Prepared query cache | **246x** |
| Peak (10-table join) | **2,062x** |
| WAL write throughput | **10K records in 180ms** |
| COPY bulk load | **1000 rows instant** |

## Running

```bash
# Start the PostgreSQL-compatible server
node --experimental-vm-modules src/server.js 5433

# Run the interactive CLI
node --experimental-vm-modules src/cli.js

# Run the example REST API
node --experimental-vm-modules src/example-app.js

# Run all tests (~3900)
node --experimental-vm-modules --test src/*.test.js

# Run specific module
node --experimental-vm-modules --test src/bplus-tree.test.js

# Run server tests
node --experimental-vm-modules --test src/server*.test.js

# Run WAL tests
node --experimental-vm-modules --test src/wal*.test.js src/db-wal*.test.js
```

## Architecture

```
src/
├── Server: server.js, cli.js, example-app.js, pg-protocol.js, replication.js
├── WAL: wal.js, wal-replay.js
├── Query Engines: volcano.js, pipeline-compiler.js, vectorized.js, query-codegen.js, adaptive-engine.js
├── Indexes: bplus-tree.js, rtree.js, art.js, skip-list.js, trie.js, inverted-index.js
├── Joins: sort-merge-join.js, grace-hash-join.js, radix-join.js, band-join.js, theta-join.js, ...
├── Concurrency: two-phase-locking.js, occ.js, timestamp-ordering.js, lock-manager.js, deadlock-detector.js
├── Storage: lsm-compaction.js, wal-compaction.js, buffer-pool.js, slotted-page.js, heap-file.js
├── Distributed: raft.js, distributed-primitives.js, consistent-hashing.js
├── Compression: column-compression.js, string-intern.js
├── Probabilistic: bloom-join.js, hyperloglog.js, count-min-sketch.js, tdigest.js
├── SQL: window-functions.js, cte.js, subquery.js, expression-compiler.js, constant-folding.js
└── Testing: 350+ test files, 4460+ test cases
```

## License

MIT
