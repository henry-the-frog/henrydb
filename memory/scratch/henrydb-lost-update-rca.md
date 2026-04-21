# HenryDB Lost Update Bug — Root Cause Analysis
- created: 2026-04-20
- uses: 1
- tags: henrydb, mvcc, bug, concurrency

## Bug Summary
When two transactions concurrently UPDATE the same row, the second transaction's UPDATE silently affects 0 rows instead of detecting a conflict.

## Reproduction
```
T1: BEGIN; UPDATE t SET val = 200 WHERE id = 1; COMMIT;
T2: BEGIN; (started before T1 commit)
T2: UPDATE t SET val = 300 WHERE id = 1;  → "0 rows updated" (WRONG)
```

## Root Cause
The bug is in `db.js _update()` (L4762-4800), NOT in the MVCC layer itself.

### Sequence of Events
1. Initial state: Row at `0:0` with val=100, PK index points to `0:0`
2. T1 UPDATE: MVCC delete `0:0` (xmax=T1), INSERT new row at `0:1` with val=200. PK index updated to point to `0:1`.
3. T1 COMMIT: Physical delete skipped (T2 still active). Version map preserved.
4. T2 UPDATE with `WHERE id = 1`:
   a. `_update()` detects simple equality WHERE → tries **index scan**
   b. PK index.search(1) returns `{pageId:0, slotIdx:1}` (T1's NEW row)
   c. `heap.get(0, 1)` → MVCC interceptor checks visibility → `xmin=T1` not visible to T2's snapshot → returns **null**
   d. Index scan found 0 matching rows. Sets `usedIndex = true`
   e. **Full table scan fallback is SKIPPED** because `usedIndex = true`
   f. `toUpdate` array is empty → "0 rows updated"

### Why it works for SELECT but not UPDATE
SELECT uses `heap.scan()` (intercepted), which iterates ALL physical rows and checks MVCC visibility. Row `0:0` is visible to T2 (xmin=1 committed, xmax=T1 not visible in T2's snapshot).

UPDATE uses index scan first, which goes through `heap.get()` on the index's RID — but the index points to T1's NEW row, not the OLD row that T2 should see.

## Fix Options

### Option A: Fall through to scan (minimal, correct)
In `_update()`, after index scan: if `rids.length > 0` but `toUpdate.length === 0` (all invisible), set `usedIndex = false` to trigger fallback scan.

```javascript
if (rids.length > 0) {
  // ... existing index scan logic ...
  usedIndex = true;
  // Fall through if index returned stale entries invisible under MVCC
  if (toUpdate.length === 0) usedIndex = false;
}
```

### Option B: First-updater-wins (PostgreSQL approach)
When T2 finds that the row it wants to update has been modified by T1 (xmax set by another committed tx), either:
- Block until T1 commits/aborts (not possible in single-threaded JS)
- Error with "could not serialize access" (SI/SSI behavior)

Option A is simpler but doesn't prevent lost updates (T2 would then update the old row, T1's update becomes invisible). Option B is correct for production systems.

### Option C: Hybrid
Fall through to scan (Option A) AND check version map for concurrent modifications:
- If scan finds a visible row whose version map entry shows xmax set by a committed txn not in T2's snapshot → raise conflict error.

## What works correctly
- Concurrent DELETE: ✅ (both target same physical row → write-write conflict detected)  
- Snapshot isolation for reads: ✅
- Phantom prevention: ✅
- SSI write skew (different rows): ✅
- Dirty read prevention: ✅

## What's broken
- Concurrent UPDATE of same row: ❌ (index-scan path)
- Would also affect DELETE + UPDATE concurrently (index-scan same path)
