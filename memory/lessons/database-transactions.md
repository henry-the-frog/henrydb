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
