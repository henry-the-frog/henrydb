# MVCC + Persistence: Interaction Bugs and Solutions

## Key Insight
MVCC and persistence are independently correct systems that have critical interactions:

### Bug 1: Dead Rows Survive Close/Reopen
**Scenario:** MVCC UPDATE marks old row with `xmax` (logical delete) but doesn't physically remove it. On close, the version map is discarded. Recovery treats all heap rows as live data.

**Result:** Duplicate rows after reopen. SUM/COUNT are wrong.

**Fix:** `_compactDeadRows()` on close — scan version maps, physically delete rows with committed `xmax`.

### Bug 2: Savepoint Rollback Rows Resurrected
**Scenario:** ROLLBACK TO SAVEPOINT physically deletes rows from heap. But WAL still has the INSERT record. On reopen, recovery replays the INSERT.

**Result:** Rolled-back rows come back from the dead.

**Fix:** Write compensating DELETE WAL records during savepoint rollback (simplified CLR).

### Bug 3: Primary Key Index Not Rebuilt
**Scenario:** TransactionalDatabase opens → creates tables → recovers heap data → but never rebuilds primary key indexes. WHERE lookups using PK index fail. Full scans work.

**Result:** `SELECT * FROM t WHERE id = 1` returns empty, but `SELECT * FROM t` returns the row.

**Fix:** Rebuild PK indexes from heap scan after recovery.

## Pattern
The bugs all live at the **boundary between in-memory state and on-disk state**:
- Version maps (in-memory) → page files (on-disk)
- WAL records (on-disk) → heap inserts (in-memory)  
- PK indexes (in-memory) → heap positions (on-disk, potentially changed by recovery)

## Prevention
1. Close should always compact dead rows before flush
2. Any logical undo (savepoint, abort) needs corresponding WAL compensation
3. All in-memory indexes must be rebuilt from disk state after recovery
4. Test the close/reopen cycle with EVERY type of transaction operation
