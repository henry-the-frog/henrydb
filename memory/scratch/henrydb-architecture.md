# HenryDB Architecture Reference — Compiled Apr 23, 2026 (Session B)

## Scale
- **367 source files**, ~78K LOC (excluding tests)
- **868 test files**
- **True test pass rate: ~99%** (2/200 sampled failures)

## Storage Engines (3)
1. **Heap File** (row-oriented, unordered) — 125 lines + slotted pages
2. **Column Store** (columnar, OLAP) — 228 lines + compression (RLE/delta/bit-packing)
3. **B+tree Table** (clustered, sorted by PK) — 265 lines (SQLite/InnoDB style)

## Execution Engines (5!)
1. **Volcano** (row-at-a-time pull) — 1483 lines, 19 iterator types
2. **Pipeline Compiler** (JIT push) — 342 lines (HyPer/Neumann 2011 style)
3. **Vectorized** (columnar batch, 1024 rows) — 463 lines (MonetDB/X100 style)
4. **Vectorized Codegen** (columnar batch + JIT) — 454 lines (DuckDB style)
5. **Query VM** (bytecode, 30+ opcodes) — 595 lines (SQLite VDBE style)
6. **Adaptive Engine** — 307 lines, picks best engine per query with runtime feedback

## Index Types (10+)
- B+ tree (432 lines, 56/56 tests)
- ART — Adaptive Radix Tree (281 lines, Leis et al. 2013)
- B-epsilon tree (188 lines, write-optimized)
- GiST — Generalized Search Tree (335 lines, Hellerstein 1995)
- Bitmap index (201 lines, OLAP)
- Hash index (104 lines, chained)
- R-tree (136 lines, spatial/PostGIS)
- Bloom filter (184 lines, probabilistic membership)
- Inverted index (189 lines, full-text search)
- Predicate index (145 lines, materialized partial index)
- Skip list (235 lines, P=0.25, LSM memtable)
- LSM tree (893 lines total, leveled + tiered compaction)

## Query Processing
- **Parser**: 3206-line recursive descent (CTEs, window fns, UNION, MERGE, RETURNING)
- **Expression Evaluator**: 1191 lines, 113 cases, proper SQL NULL handling
- **SQL Functions**: 716 lines, 70+ built-in functions
- **Query Rewriter**: 365 lines (view expansion, predicate pushdown, constant folding, subquery flattening)
- **Cost Model**: PostgreSQL-style (1/ndistinct, min/max range selectivity)
- **Stats Collector**: MCV, histograms, NDV, null fraction
- **Join Ordering**: System R-style DP over bitmask subsets (≤6 tables)
- **Decorrelation**: IN/EXISTS → semi-join transformation (432 lines)
- **Predicate Pushdown**: 154 lines, single-table predicates below joins
- **EXPLAIN**: PostgreSQL-style output (874 lines)

## Concurrency & Recovery
- **MVCC**: PostgreSQL snapshot model (523 lines, {xmin, xmax, activeSet})
- **SSI**: Serializable Snapshot Isolation (277 lines, Cahill 2008 / Ports & Grittner 2012)
- **Lock Manager**: 5 modes (IS/IX/S/SIX/X) with deadlock detection (162 lines)
- **Row Locks**: FOR UPDATE/SHARE, NOWAIT, SKIP LOCKED
- **Advisory Locks**: Session + transaction level with deadlock detection (260 lines)
- **WAL**: Binary format, CRC32, 3 sync modes (immediate/batch/none) (615 lines)
- **ARIES Recovery**: Full Mohan et al. 1992 — Analysis→Redo→Undo, CLRs, DPT+ATT (479 lines)
- **Crash Recovery**: 31 tests pass (12 crash + 7 wal + 12 aries)

## Distributed Systems (8 primitives)
- **Raft Consensus** (353 lines, Ongaro 2014)
- **SWIM Gossip** (225 lines, Das et al. 2002)
- **Consistent Hashing** (158 lines, Karger 1997, 150 vnodes)
- **Replication**: Statement-based primary-replica (172 lines)
- **2PC**: Two-Phase Commit with crash recovery (191 lines)
- **Vector Clocks** (172 lines, Lamport/Fidge): Event ordering
- **Phi Detector** (169 lines, Hayashibara 2004): Failure detection
- **Merkle Tree** (148 lines): Data integrity/sync

## Data Processing
- **Full-Text Search**: TSVector/TSQuery, Porter stemmer, TF-IDF, inverted index (374 lines)
- **JSONPath**: RFC 9535 / PostgreSQL jsonpath (389 lines)
- **Zone Maps**: Per-page min/max for data skipping (163 lines)
- **Analytics**: T-Digest + Segment Tree (177 lines)
- **Streaming**: Count-Min Sketch (145 lines)
- **HyperLogLog**: Cardinality estimation (100 lines)
- **Bloom Join**: Pre-filter with bloom filter (132 lines)
- **Band Join**: Range-based sort-merge-sweep (110 lines)

## PostgreSQL Compatibility
- **Wire Protocol v3**: Full message lifecycle (1936 + 513 lines)
- **Connection Pool**: Min/max, health checks, timeouts (220 lines)
- **CLI**: psql-like REPL (244 lines)
- **Prepared Statements**: PREPARE/EXECUTE/DEALLOCATE with $1 params (179 lines)
- **Information Schema**: tables + columns + pg_catalog (296 lines)
- **LISTEN/NOTIFY**: Pub/sub (part of 510-line event system)

## Languages
- **SQL**: 3206-line parser, comprehensive coverage
- **PL/HenryDB**: Full PL/pgSQL-like procedural language (854 lines)
  - DECLARE/BEGIN/END, IF/ELSIF/ELSE, WHILE, FOR, RETURN, RAISE, EXECUTE, EXCEPTION handlers

## Operational
- **Buffer Pool**: Pin counting, WAL force, Clock/LRU replacer (389 lines)
- **Disk Manager**: 4KB pages, CMU 15-445 style (191 lines)
- **Table Partitioning**: Range + hash with partition pruning
- **Aggregate Cache**: TTL + table invalidation (236 lines)
- **Query Cache**: SQL+params key, FIFO eviction (56 lines)
- **Schema Migrations**: Versioned up/down (198 lines)
- **Schema Diff**: Generates migration SQL (113 lines)
- **Batch Executor**: SQL script runner (114 lines)
- **Audit Log**: 9 event types (212 lines)
- **Index Advisor**: Workload-based recommendations (437 lines)

## Hash Tables (6 variants!)
1. Chained hashing (hash-index.js, 104 lines)
2. Extendible hashing (extendible-hash.js, 284 lines, CMU 15-445)
3. Linear hashing (linear-hashing.js, 115 lines, round-robin split)
4. Robin Hood hashing (robin-hood-hash.js, 254 lines, variance-reducing)
5. Cuckoo hashing (cuckoo-hash.js, 161 lines, O(1) worst-case)
6. Double hashing (advanced-ds.js, open addressing probe)

## Additional Data Structures
- van Emde Boas tree (103 lines): O(log log u) predecessor/successor
- Wavelet tree (125 lines): O(log σ) rank/select/access on sequences
- Treap (144 lines): BST + heap, expected O(log n)
- R-tree (136 lines): Spatial index (PostGIS-style)
- Interval tree (70 lines): Augmented BST for temporal queries
- Skip list (235 lines, P=0.25): LSM memtable
- Trie (157 lines): Prefix tree for strings
- Wildcard trie (78 lines): LIKE query support
- Bitwise trie (68 lines): HAMT-style, 5 bits/level
- AVL tree (92 lines): Balanced BST
- Fenwick tree, Segment tree, Union-Find, Suffix Array
- COLA (117 lines): Cache-oblivious lookahead array
- Circular list (25 lines): Clock replacer

## Parallelism
- Morsel-driven parallelism (204 lines, Leis et al. 2014)
- Work-stealing thread pool (213 lines, Blumofe & Leiserson 1999)
- SIMD-style numeric operations (103 lines)

## Probabilistic / Streaming
- Bloom filter (184 lines): FNV-1a + double hashing
- Cuckoo filter: Probabilistic membership with deletion
- Count-Min Sketch (145 lines): Frequency estimation
- HyperLogLog (100 lines): Cardinality estimation
- T-Digest: Streaming quantile estimation
- Zobrist hash (87 lines): Incremental XOR hashing

## Miscellaneous
- Varint encoding (31 lines, protobuf/SQLite style)
- Write batch (16 lines, atomic batch writer)
- Checksum (18 lines: Adler-32, Fletcher-16, XOR)
- Cursor (36 lines: array-backed iterator)

## Concurrency Control (ALL 4 major schemes!)
- **2PL** (Eswaran 1976): Growing/shrinking phases (140 lines)
- **MVCC** (Reed 1978): PostgreSQL snapshot model (523 lines)
- **SSI** (Cahill 2008): Serializable snapshot isolation (277 lines)
- **OCC** (Kung & Robinson 1981): Validation-based optimistic CC (100 lines)

## Security
- **RBAC** (333 lines): Roles, privileges, GRANT/REVOKE
- **Row-Level Security** (226 lines): Policy expressions per table
- **SCRAM-SHA-256** (230 lines): Modern authentication (PG 10+)
- **Audit Log** (212 lines): 9 event types, enterprise compliance

## Known Bugs (found in Session B)
1. AFTER DELETE trigger: _fireTriggers wrapper drops OLD row values (6th arg)
2. Optimizer-quality test failure (cost model estimation)

## Performance
- Read queries: 10-26ms over 5K rows (well-optimized)
- INSERT: 2ms/insert with index (constraint + btree bottleneck)
- No major query-level bottlenecks
