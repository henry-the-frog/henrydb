# HenryDB: MVCC Multi-Version Visibility Bug — Deep Analysis
- created: 2026-04-20
- uses: 0
- tags: henrydb, mvcc, bug, p1

## Symptoms
After repeated concurrent UPDATE cycles on the same row:
1. **Duplicate rows**: SELECT returns multiple versions of the same logical row
2. **Missing rows**: After enough cycles, SELECT returns 0 rows

## Root Cause Analysis

### Problem 1: No Logical Row Deduplication
The MVCC scan interceptor returns ALL physical rows that pass the visibility check:
- created (xmin committed before snapshot) AND NOT deleted (xmax=0 or xmax not visible)

But UPDATE creates a NEW physical row + marks the old one with xmax. If the session's UPDATE
changes a different physical row than the external UPDATE did (because of the lost-update fix
fall-through), the "old" versions from previous iterations may still appear visible.

Example from iteration 2 (session txId=4, snapshot xmin=5):
- 0:1 (xmin:3, xmax:0) → visible (tx3 committed, not deleted) → val=100
- 0:2 (xmin:2, xmax:5) → visible (tx2 committed, deleted by tx5 but tx5 is not yet committed at scan time) → val=1000
- Both are the "same" logical row but different physical versions!

### Problem 2: Session UPDATE Operates on Multiple Visible Rows
When the session scans for rows to UPDATE (WHERE id=1), it finds MULTIPLE visible rows 
and updates ALL of them. This creates even more physical rows.

### Problem 3: Eventually Visibility Fails Entirely  
By iteration 4, the combination of accumulated versions and complex xmin/xmax relationships
causes isVisible() to return false for all versions, yielding 0 rows.

Specifically: rows 0:6 (xmin:7), 0:7 (xmin:6), 0:8 (xmin:6) all have xmax=0 and
xmin < snapshot xmin (9), so they SHOULD be visible. The fact that session sees [] means
isVisible() is returning false — likely a bug in the committed-txn tracking.

## What PostgreSQL Does
PostgreSQL solves both problems:
1. **HOT (Heap-Only Tuples)**: Chain row versions together so scan follows the chain
2. **ctid chain**: Each old version points to the new version via `t_ctid`
3. **Unique index enforces one visible version per key**

## Fix Options

### Option A: Logical Row Identity (simplest)
- Each logical row gets a unique rowId
- Version map tracks: rowId → [list of physical slots, newest first]
- scan() deduplicates: for each rowId, yield only the latest visible version

### Option B: Version Chains (PostgreSQL-like)
- Each row version stores a `nextVersion` pointer
- UPDATE stores pointer from old → new version
- scan() follows chains to find latest visible

### Option C: PK-based Dedup in SELECT (workaround)
- After scan, deduplicate by PK values
- Doesn't fix the root cause but prevents duplicate results

## Recommendation
Option A is the most pragmatic for the codebase. Add a `_logicalRowId` counter to each table.
INSERT assigns a new rowId. UPDATE preserves the rowId. Scan deduplicates by rowId.
