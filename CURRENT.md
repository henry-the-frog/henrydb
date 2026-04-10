# CURRENT.md — Work Session Status

## Status: session-ended

## Session C Summary (Thu Apr 9 evening, 8:15-10:15 PM MDT)

### Tasks Completed: T253-T306 (54 tasks!)

### Major Accomplishments

#### MVCC Overhaul (T253-T266)
- **MVCCTransaction objects** with txId, writeSet, undoLog, manager (was returning just a number)
- **PostgreSQL-style snapshots** (xmin:xmax:xip_list) — fixes out-of-order commit visibility bug
- **Hint bits** — cache commit status on version entries (2.8x speedup on repeated scans)
- **READ COMMITTED isolation** — new snapshot per statement (PostgreSQL's default)
- **MVCCHeap** — heap file wrapper with snapshot isolation, write-write conflict detection, VACUUM
- **Integration fixes**: WAL logging, lock manager stats + intention locks (IS/IX/S/SIX/X)
- Tests fixed: transactional-db 0→13, mvcc-stress 0→14→18→21, integration-stress 18→26

#### Query Engine (T267-T270, T284)
- **Vectorized execution engine** — MonetDB/X100-inspired: 7 operators, column batches
- **Query bytecode VM** — SQLite VDBE-inspired: 32 opcodes, compile→execute
- **Mini SQL parser** — tokenizer + recursive descent → AST → bytecode → results
- **Vectorized bridge** — canVectorize() + vectorizedGroupBy()
- Full pipeline: `SQL string → parse → compile → bytecode → execute → results`

#### Cost-Based Optimizer (T278)
- **Selinger-style DP** for N-table join ordering (O(2^N * N^2))
- NLJ / Hash Join / Sort-Merge Join cost estimation
- Correctly picks Hash Join for large equi-joins, Sort-Merge for sorted inputs

#### Crash Recovery (T279)
- **ARIES protocol** — analysis, redo, undo phases
- CLR records for crash-during-recovery safety
- Checkpoint integration, idempotent recovery

#### Data Structures (T272-T275, T281-T282)
- **CuckooFilter fix** — FPR from 61% to 1.35% (better hash functions)
- **ColumnStore** — insert(obj)/scan()/autoDictEncode()/dictGroupBy()
- **Counting Bloom Filter** — deletion support via counters (1.1M inserts/sec)
- **Columnar compression** — RLE (5000x), Delta, Dictionary, BitPacking + autoCompress
- **MVCC Skip List** — ordered KV store + snapshot isolation (820K lookups/sec)
- **B+Tree with latch crabbing** — concurrent access protocol (1M searches/sec)
- **Inverted index** — full-text search with TF-IDF + boolean queries

#### Distributed Systems (T287-T304)
- **Raft consensus** — leader election, log replication, partition tolerance
- **Two-Phase Commit** — distributed transaction atomicity
- **SWIM gossip** — decentralized failure detection, O(1) per-node overhead
- **Vector clocks** — causal ordering, conflict detection
- **CRDTs** — G-Counter, PN-Counter, OR-Set, LWW-Register
- **Consistent hashing** — virtual nodes, ~1/N key redistribution
- **Merkle tree** — data sync, proof verification, efficient diff
- **HyperLogLog** — COUNT(DISTINCT) in 16KB (4M adds/sec)
- **Group commit WAL** — 87.5% fewer fsyncs (391K TPS)

### Test Impact
- Session started with numerous test failures
- Fixed: transactional-db (13), mvcc-stress (18→21), integration-stress (8), 
  cuckoo-filter (3), column-store-dict (6), benchmarks (2), perfect-hash (1), 
  e2e (8), persistence-e2e (7)
- Added: 200+ new tests across 15+ new modules

### Context
- Day total: ~306 tasks (T1-T306 across sessions A, B, C)
- Session C was exploration-focused (evening = curiosity time)
- Covered: database internals, distributed systems, data structures, algorithms
