# Database Transactions & Recovery — Lessons Learned

Promoted from scratch notes (henrydb-transactions.md, aries-gap-analysis.md). Accumulated over Apr 6–12, 2026.

## MVCC Fundamentals

### Snapshot Isolation (PostgreSQL-style)
- Snapshot = `{xmin, xmax, activeSet}` — xmin is earliest active txId, xmax is next to assign, activeSet is in-flight txIds
- Visibility: row is visible if `xmin` committed before snapshot AND `xmax` either null or not committed before snapshot
- **Bug pattern:** `snapshot.has(txId)` doesn't work — snapshot is an object, not a Set. Need `_wasVisibleInSnapshot()` method.

### Serializable Snapshot Isolation (SSI)
- Prevents ALL anomalies including write skew via rw-dependency tracking
- Dangerous structure: T1 →rw→ T2 →rw→ T3 where T1 committed before T3 started
- Track `rwInDeps` and `rwOutDeps` per transaction
- **Bug pattern:** Scan interceptors record reads too broadly (all visible rows, not just WHERE-matching). Fix: `suppressReadTracking` during UPDATE/DELETE scans.
- **Bug pattern:** Sequential transactions create false rw-dependencies because `readSets` includes committed txns. Fix: skip if otherTx was visible in current snapshot.

## WAL (Write-Ahead Logging)

### Critical Design Rules
1. WAL records MUST be written before page modifications reach disk (write-ahead constraint)
2. COMMIT/ABORT markers MUST be in the WAL — recovery can't determine tx status without them
3. DELETE WAL records MUST appear before COMMIT — ensures recovery assigns them to the right tx
4. Auto-commit txId=0 has no COMMIT record — recovery relies on page files for auto-commit data

### Performance
- **fsync dominates:** 54/s → 11K/s switching from per-commit fsync to batch sync (77x tax)
- **O(n²) flush bug:** `Array.includes()` in flush loop. Fix: track `_lastFlushedIdx`, append only new records.
- **Batch commits:** UPDATE/DELETE should use one txId for entire operation, commit once. 29x-96x improvement.

## ARIES Recovery

### The Three Phases
1. **Analysis:** Scan from last checkpoint, rebuild Active Transaction Table + Dirty Page Table
2. **Redo:** Replay from `min(recLSN in DPT)`, skip if `pageLSN >= record.lsn`
3. **Undo:** Walk backward through loser txns, write CLRs

### PageLSN Is the Linchpin
- Single integer per page header: LSN of last WAL record applied to that page
- Enables per-page redo decisions (skip if already applied)
- Makes recovery **idempotent by construction** — replay any number of times, same result
- Enables **parallel recovery** — pages can be recovered independently
- Without it: recovery is all-or-nothing per table, fragile heuristics needed

### Compensation Log Records (CLRs)
- Every undo action during abort/recovery gets logged
- CLRs have `undoNext` pointer to skip already-undone work on re-crash
- Needed for savepoint rollback: ROLLBACK TO SAVEPOINT must write compensating DELETE records

### Checkpoint Design
- `beginTransaction()` is a no-op — no WAL record for BEGIN (PostgreSQL behavior)
- Fuzzy checkpoints: txns continue during checkpoint, ATT+DPT snapshotted
- WAL truncation: remove records before `min(recLSN in DPT)`

## MVCC + Persistence Interaction Bugs

These are the hardest bugs — each subsystem works in isolation but fails at boundaries.

### Dead Rows Survive Close/Reopen
MVCC UPDATE sets `xmax` (logical delete) but doesn't physically remove. On close, version map discarded. Recovery treats all heap rows as live → duplicates.
**Fix:** `_compactDeadRows()` on close.

### Savepoint Rollback Rows Resurrected
ROLLBACK TO SAVEPOINT physically deletes rows. WAL still has INSERT. Recovery replays it → rolled-back rows return.
**Fix:** Compensating DELETE WAL records during savepoint rollback.

### PK Index Not Rebuilt After Recovery
Recovery replays heap data but doesn't rebuild indexes. WHERE with PK fails, full scans work.
**Fix:** Rebuild PK indexes from heap scan after recovery.

### Query Cache + Adaptive Engine Bypass MVCC
Wire protocol server's cache and adaptive engine serve results outside MVCC context. Inside BEGIN blocks, UPDATE is invisible to subsequent SELECT.
**Fix:** Skip cache and adaptive engine when `conn.txStatus === 'T'`.
**Rule:** Any query shortcut (cache, rewriter, adaptive engine) MUST check transaction state.

### Cache Not Invalidated on ROLLBACK
Query cache invalidated on DML but NOT on ROLLBACK/COMMIT. After BEGIN→UPDATE→SELECT(cached)→ROLLBACK, next SELECT returns stale cached value.
**Fix:** `invalidateAll()` on both COMMIT and ROLLBACK.

## Prevention Checklist
- [ ] Always test close → reopen → read-back cycle
- [ ] Null-guard all comparisons (`null >= -10` is `true` in JS)
- [ ] Run server tests after touching QueryCache or server.js
- [ ] Any transaction state change must invalidate query cache
- [ ] Test the "scary" scenarios: tiny buffer pools, crash without close, checkpoint+truncate+reopen
- [ ] Integration tests > unit tests for database correctness

## Crash Recovery for DDL Operations (Added 2026-04-17)

### 3-Phase Recovery Architecture
Standard ARIES recovery handles DML (INSERT/UPDATE/DELETE) but DDL (ALTER TABLE, CREATE VIEW, etc.) needs special treatment:

1. **Schema Replay Phase**: Read ALL WAL DDL records, replay schema-only changes (no heap modification)
2. **DML Replay Phase**: Read WAL from lastCheckpointLsn, replay committed DML per-heap
3. **Cleanup Phase**: Update catalog, remove orphaned heaps, rebuild version maps

### Schema-Only Replay
When replaying ALTER TABLE during crash recovery, only modify the table schema — do NOT modify heap data. Heap data will be corrected by the DML replay phase. Using `db.execute(ALTER TABLE)` for replay would double-apply changes because ALTER TABLE both modifies schema AND scans/modifies existing rows.

### txId=0: Auto-Committed DDL Operations
DDL operations that generate DML side-effects (ALTER TABLE ADD COLUMN backfilling rows with NULL) use txId=0. These records must be treated as always-committed in all recovery paths:
- `hasUncommitted` check: exclude txId=0
- `_replayRecords` committed check: skip for txId=0
- Checkpoint boundary: txId=0 records must not be re-replayed after checkpoint

### Table Rename Cascading Effects
ALTER TABLE RENAME TO requires updating 5 things:
1. Physical `.db` heap file (renameSync)
2. Heap object in heaps Map (rekey + update heap.name)
3. DiskManager in diskManagers Map
4. Version map in versionMaps Map  
5. Table object reference in db.tables (wire new heap into table)

Missing any of these causes data loss or query failures after restart.

### DDL Persistence Checklist
Every DDL object type needs:
- [ ] In-memory execution works
- [ ] WAL logged (logDDL or logDDL in FileWAL)
- [ ] Catalog persisted (save SQL to catalog.json)
- [ ] Restored on open (replay SQL from catalog)
- [ ] Recovered after crash (WAL DDL replay)
- [ ] Concurrent with transactions (no corruption)
- [ ] Constraints enforced after restart

As of 2026-04-17: TABLE ✅, INDEX ✅, VIEW ✅, ALTER TABLE ✅. TRIGGER ❌, SEQUENCE ❌, MATERIALIZED VIEW ❌.

## Catalog Persistence Layer Gaps (Apr 17 evening)

### The Pattern: Subsystems Built Independently, Never Connected
When TransactionalDatabase wraps Database, it maintains its own catalog persistence layer. New features added to Database (triggers, sequences, materialized views) are NOT automatically persisted by TransactionalDatabase.

**Concrete bugs found:**
1. `_saveCatalog()` only saved tables + views — triggers and sequences silently lost on restart
2. WAL only logged ALTER TABLE and CREATE/DROP INDEX — CREATE TABLE, CREATE VIEW, DROP TABLE, CREATE TRIGGER, CREATE SEQUENCE had no WAL records
3. Result: stale catalog after crash couldn't recover any DDL except ALTER TABLE and indexes

**Detection strategy:** DDL lifecycle test harness — test each DDL through 7 phases:
- In-memory, clean restart, crash, stale catalog crash, checkpoint+crash, concurrent tx, DDL+DML race
- 9 DDL types × 7+ phases = 70 tests from ~300 lines of specs
- Generator pattern makes adding new DDL types trivial

**Lesson:** When a wrapper layer (TransactionalDatabase) maintains its own persistence, EVERY new feature in the inner layer needs explicit persistence support in the wrapper. This is the same "two subsystems not connected" pattern seen with BEGIN/COMMIT and WAL.

### ALTER TABLE Backfill + WAL Recovery Conflict (Apr 17 evening)

**The bug:** ALTER TABLE ADD/DROP COLUMN creates duplicate rows on crash recovery.

**Root cause chain:**
1. ALTER TABLE backfill modifies tuples in-place (or via delete+re-insert)
2. The old INSERT WAL records for the original data still exist
3. On crash recovery, WAL replays old INSERT records alongside the backfilled data
4. Result: each original row appears 2-3 times

**The fix hierarchy:**
- `updateInPlace()` — modify tuples without creating WAL INSERT/DELETE records
- Post-ALTER checkpoint — truncate WAL to remove old INSERT records
- Per-page LSN recovery — don't destroy checkpointed pages during recovery

**The lesson:** In-place data modification and WAL-based recovery are fundamentally in tension. WAL recovery assumes it can replay records from empty state. In-place modifications create data that bypasses WAL. The resolution: use checkpoints as synchronization points between the two systems.

**Secondary lesson:** Always check for duplicate method definitions in large files. Two `_alterTable` methods existed — the second silently overrode the first, using a completely different backfill strategy (delete+re-insert vs in-place updateTuple). The first was dead code.

### Recovery with Uncommitted Transactions (Apr 17 evening)

**Bug:** Committed rows lost after close with uncommitted transactions.

**Root cause:** The "hasUncommitted" recovery path cleared ALL pages and replayed only committed WAL records. But `dm._writeHeader()` with `_pageCount=0` truncated the data file to 0 bytes. The subsequent `heap.insert()` during replay allocated new pages, but the DiskManager's internal state was inconsistent.

**Fix:** Hybrid recovery strategy:
1. If pages already have data → just delete uncommitted tuples in-place
2. If pages are empty (unflushed crash) → traditional clear + replay

**Insight:** "Clear and replay" recovery is dangerous when the WAL might not have all records (e.g., after checkpoint or non-flush close). In-place deletion of uncommitted records is safer when page data exists.
