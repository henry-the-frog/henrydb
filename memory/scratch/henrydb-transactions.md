# HenryDB: Transactional Correctness Learnings

uses: 6
created: 2026-04-07
tags: database, mvcc, wal, crash-recovery, transactions, aries, checkpoints

## Key Bugs Found & Fixed (2026-04-07)

### 1. WAL Was a Ghost
FileBackedHeap had a WAL reference but never used it. `insert()` and `delete()` operated
on pages without generating WAL records. Fix: added WAL logging to both heap operations
with `_currentTxId` tracking.

### 2. COMMIT/ABORT Not in WAL
TransactionalDatabase committed MVCC transactions without writing COMMIT/ABORT markers
to the WAL. Recovery had no way to know which transactions were committed.

### 3. MVCC Deletes Were Invisible to WAL
The MVCC interceptor replaces `heap.delete` to just set `xmax` (no physical delete).
This meant deletes never generated WAL records. Fix: explicit `_walLogDeletes()` before
COMMIT record, then physical delete after.

### 4. WAL Record Ordering Matters
DELETE WAL records MUST appear before COMMIT. Recovery only replays records for
committed transactions. If DELETE comes after COMMIT, recovery might assign it to
the wrong transaction or skip it.

### 5. Recovery Page/Slot Mismatch
Original inserts go to page 0. After crash, page 0 might still have data from
buffer pool eviction. Recovery inserts into page 1 (different location). DELETE
targeting page 0 slot 1 hits empty slot. Fix: clear heap and replay from scratch.

### 6. Physical Deletes Break Active Snapshots
`_physicalizeDeletes` removes rows from heap. But other active transactions might
still need those rows (their snapshots include them). Fix: defer physical deletion
when other transactions are active — VACUUM cleans up later.

## Architecture Insights

- **Single `_activeTx` field is a race hazard**: TransactionalDatabase uses one field
  for the "current" transaction. In truly concurrent (async/threaded) execution, this
  would cause data corruption. Fine for synchronous interleaving.

- **MVCC version map is memory-only**: `xmin/xmax` metadata doesn't survive crashes.
  Recovery rebuilds from WAL. This means recovery is always a full replay (no incremental).

- **Write-ahead constraint works**: Buffer pool eviction correctly calls `_enforceWriteAhead`
  before flushing dirty pages. This ensures WAL records reach disk before page data.

### 7. Bug #8: Ghost Interceptors After Crash Recovery
After crash + recovery, MVCC interceptors (which wrap heap.insert/delete for version
tracking) weren't reinstalled. SSI rw-dependency tracking was completely absent post-recovery.
Fix: TransactionalDatabase reinstalls interceptors as part of recovery sequence.

### 8. Bug #9: isVisible(0) Returns False
Rows with xmin=0 (inserted during recovery replay, no real txId) were invisible to
subsequent queries because isVisible() treated txId 0 as "uncommitted." Fix: treat
xmin=0 as always-visible (bootstrap rows).

## Serializable Snapshot Isolation (SSI)

- Strongest isolation level — prevents ALL anomalies including write skew
- Implementation: rw-dependency tracking on top of MVCC snapshots
- **Dangerous structure**: T1 →rw→ T2 →rw→ T3 where T1 committed before T3 started
  - 3-way cycle detection: if a transaction has both incoming and outgoing rw-dependencies
    with committed transactions, abort it
- Tracks: `rwInDeps` (who read something I overwrote) and `rwOutDeps` (whose data I read that they overwrote)
- SSI is the hardest isolation level in database engineering — PostgreSQL only added it in 9.1 (2011)
- Doctor on-call write skew example: two doctors both read "2 on call", both remove themselves → 0 on call. SSI detects and aborts one.

## Two-Phase Commit (2PC)

- Distributed transaction protocol for multi-node atomicity
- **Phase 1 (Prepare)**: Coordinator asks all participants to vote COMMIT or ABORT
- **Phase 2 (Commit/Abort)**: If all vote COMMIT, coordinator sends COMMIT; if any votes ABORT, all abort
- WAL-backed decisions: coordinator and participants log their votes before responding
- Crash recovery: read WAL to determine vote, re-send if coordinator asks
- Timeout handling: if coordinator doesn't hear back, abort the transaction
- Tested with 5 concurrent 2PC transactions — all or nothing semantics proven

## Pipeline JIT Compilation

- Push-based query execution (HyPer-inspired) via Function() constructor
- Generates tight JS loops with inlined predicates and projections
- Benchmark on 10K rows:
  - Selective filter (1% selectivity): JIT **3.07x** faster than Volcano iterator
  - LIMIT queries: JIT **17.41x** faster (short-circuits without iterator overhead)
  - Filter+project: JIT **1.11x** (marginal gain when work per tuple dominates)
- Key insight: Volcano's per-tuple virtual dispatch dominates for selective queries; compiled code eliminates it

## Bloom Filters in LSM Tree

- Probabilistic set membership: false positives possible, false negatives impossible
- FPR achieved: 1.02% (target 1%) at 1.2 bytes/key — near optimal
- Counting Bloom filter variant: supports deletions (4-bit counters per slot)
- Integrated into LSM SSTables: skip SSTable disk reads when key definitely not present
- Measured: skips 1 in 5 SSTables on average (ideal: 1 in 1, but depends on key distribution)

## SQLite Comparison Benchmark (10K rows)

- COUNT(*): 39,000x slower (full heap scan vs SQLite's B-tree count)
- Filter (WHERE x > 5000): 22x slower
- GROUP BY: 5.5x slower
- Key insight: ratio shrinks with query complexity because algorithmic overhead
  (hash joins, sorts) dominates over raw I/O speed
- HenryDB is algorithmically correct but not I/O-optimized (no page cache tuning, JS overhead)

## Property-Based Testing

- 13 property-based tests across 10 random seeds
- SQL invariants verified: COUNT complement, SUM additivity, ORDER BY determinism,
  DISTINCT uniqueness, WHERE monotonicity, MIN/MAX/AVG correctness, JOIN cardinality, LIMIT bounds
- Random seed approach catches edge cases deterministic tests miss

## Point-in-Time Recovery (PITR)

- "Recover my database to 3pm yesterday" — critical for disaster recovery
- Implementation: replay WAL records up to target timestamp, only include
  transactions that COMMITTED before the target time
- Key design decisions:
  - Timestamp is wall-clock (Date.now()) stored on each WAL record
  - Analysis phase: scan all COMMIT records, partition by target timestamp
  - Redo phase: replay only committed-before-target, stop at timestamp boundary
  - Uncommitted transactions at target time are excluded (correct: they hadn't committed yet)
- Works with fuzzy checkpoints: can use checkpoint LSN as replay start point
- In a real DB: timestamps would be serialized to WAL on disk; in HenryDB they're
  in-memory metadata (sufficient for the simulation)
- PostgreSQL equivalent: pg_basebackup + WAL archiving + recovery_target_time

## Test Coverage

- 16 concurrent MVCC tests (dirty reads, phantoms, write skew, etc.)
- 12 crash recovery tests (committed survives, uncommitted lost, repeated crashes)
- 9 bank transfer invariant tests (200 random transfers, sum conserved)
- 10 SSI tests (write skew prevention, dangerous structure detection)
- 14 2PC tests (coordinator/participant, crash recovery, concurrent)
- 4 SSI + crash recovery integration tests (bugs #8 and #9)
- 13 property-based tests (10 seeds × 8 invariants)
- 13 bloom filter tests
- 14 LSM tree tests (bloom filter integration)
- 20 pipeline JIT tests
- Total: ~125 new correctness tests (session total 2054+)

## ARIES-Style Checkpointing (2026-04-09)

### Dirty Page Table (DPT)
- Tracks `pageKey -> recLSN` (first-write-wins)
- pageKey format: `tableName:pageId`
- DPT is snapshot during checkpoint, then cleared

### Fuzzy Checkpoints
- BEGIN_CHECKPOINT record: snapshots DPT + active transactions
- END_CHECKPOINT record: references beginLsn
- Pages flushed between BEGIN and END (the "fuzzy" part)
- WAL truncation: removes records before min(recLSN in DPT)

### Key Design: beginTransaction() is a no-op
- No WAL record for BEGIN — only bookkeeping
- Tests expected 3 records (INSERT, INSERT, COMMIT), not 4 (with BEGIN)
- Recovery uses COMMIT presence to determine committed txns
- This matches real databases (PostgreSQL doesn't write BEGIN to WAL)

### WALRecord Class
- Serialize: JSON payload + CRC32 footer
- Deserialize: returns `null` on any error (CRC mismatch, truncation, etc.)
- On success: `{ record, bytesRead }` (for multi-record parsing)
- Fields: lsn, txId, type, tableName, before, after

### In-Memory WAL Mode
- WriteAheadLog() with no args = in-memory mode
- _memRecords (all records) vs _stableRecords (flushed on COMMIT)
- Auto-checkpoint disabled by default in memory mode
- setAutoCheckpoint(N) enables commit-based checkpointing (every N commits)

### Recovery Function
- recoverFromWAL: replays committed transactions after latest checkpoint
- recoverToTimestamp: PITR with timestamp filtering + skippedTxns count
- Both handle mock heaps (._data.push) and full Database objects (.execute())
