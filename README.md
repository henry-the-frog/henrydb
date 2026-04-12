# HenryDB 🗄️

**A teaching database engine built from scratch in JavaScript.**

3,000+ tests · 70K+ lines · 388 files · Every major concept implemented.

## What Is This?

HenryDB is an educational database engine that implements virtually every concept from a database systems textbook. It's not meant for production — it's meant to *understand* how databases actually work, from B+ trees to Raft consensus.

Built in a single day as a deep-dive into database internals.

## Features

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
Window functions (ROW_NUMBER, RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTILE, SUM OVER) · LATERAL JOIN · Common Table Expressions (WITH, WITH RECURSIVE) · Materialized views · Correlated subqueries · information_schema (tables, columns, constraints) · Expression compiler · Constant folder · Query rewriter · Plan visualization (DOT/text)

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

## Running Tests

```bash
# Run all tests (~3000)
node --test src/*.test.js

# Run specific module
node --test src/bplus-tree.test.js

# Run benchmarks
node --test src/benchmarks.test.js
```

## Architecture

```
src/
├── Query Engines: volcano.js, pipeline-compiler.js, vectorized.js, query-codegen.js, adaptive-engine.js
├── Indexes: bplus-tree.js, rtree.js, art.js, skip-list.js, trie.js, inverted-index.js
├── Joins: sort-merge-join.js, grace-hash-join.js, radix-join.js, band-join.js, theta-join.js, ...
├── Concurrency: two-phase-locking.js, occ.js, timestamp-ordering.js, lock-manager.js, deadlock-detector.js
├── Storage: lsm-compaction.js, wal-compaction.js, buffer-pool.js, slotted-page.js, heap-file.js
├── Distributed: raft.js, distributed-primitives.js, consistent-hashing.js
├── Compression: column-compression.js, string-intern.js
├── Probabilistic: bloom-join.js, hyperloglog.js, count-min-sketch.js, tdigest.js
├── SQL: window-functions.js, cte.js, subquery.js, expression-compiler.js, constant-folding.js
└── Testing: integration.test.js, property-tests.test.js, edge-cases.test.js, benchmarks.test.js
```

## License

MIT
