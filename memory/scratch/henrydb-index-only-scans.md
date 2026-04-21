# HenryDB Index-Only Scans — Current State & Improvement Path
- created: 2026-04-20
- uses: 0
- tags: henrydb, index, optimization

## Current Implementation
HenryDB already has covering indexes (CREATE INDEX ... INCLUDE (col)):
- Index entries store `includedValues` for included columns
- `_tryIndexScan()` detects when all requested columns exist in index
- When covered: builds row from index data, skips heap access

## What's Missing

### 1. Visibility Map Check for Index-Only Scans
In PostgreSQL, index-only scans ONLY skip the heap if the visibility map confirms the page
is all-visible. If the page has uncommitted tuples, PostgreSQL falls back to heap access
to check MVCC visibility.

HenryDB's index-only scan currently doesn't check visibility — it always returns the index
data. This means it can return rows that should be invisible under MVCC.

### 2. Index Maintenance After MVCC Updates
When a row is UPDATEd, the new row version gets a new heap location but the index still
points to the old location. The index needs to be updated too, or the index-only scan
will return stale data.

### 3. EXPLAIN Output
The EXPLAIN plan should distinguish between "Index Scan" and "Index Only Scan" to help
users understand when the optimization is active.

## Fix Plan

### Phase 1: Visibility Map Check (Low Risk)
In the index-only scan path, after building the row from index data:
1. Look up the heap page/slot from the index entry
2. Check the visibility map: is this page all-visible?
3. If yes: use the index-only row (fast path)
4. If no: fall back to heap access + MVCC check (safe path)

### Phase 2: MVCC-Aware Index-Only Scans
For truly correct index-only scans under MVCC:
1. The index must track xmin/xmax per entry (like PostgreSQL's bt_index_xmin)
2. Or: always fall back to heap for MVCC check (simpler, still fast for all-visible pages)

## Performance Impact
Index-only scans are critical for read-heavy workloads:
- Analytics queries (SELECT count(*), SELECT avg(col))
- Covering index for frequently accessed columns
- The visibility map determines whether this optimization can fire
