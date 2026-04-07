# HenryDB: Transactional Correctness Learnings

uses: 3
created: 2026-04-07
tags: database, mvcc, wal, crash-recovery, transactions

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

## Test Coverage

- 16 concurrent MVCC tests (dirty reads, phantoms, write skew, etc.)
- 12 crash recovery tests (committed survives, uncommitted lost, repeated crashes)
- 9 bank transfer invariant tests (200 random transfers, sum conserved)
- Total: 37 new correctness tests
