# MVCC-Unaware Index Lookups

uses: 1
created: 2026-04-20
tags: henrydb, mvcc, index, snapshot-isolation, critical

## Problem
BPlusTree (and other index) lookups bypass the MVCC scan interceptor.
When `SELECT * FROM t WHERE pk_col = x` uses the PK index, it returns
the current physical heap row without checking version map visibility.

## Impact
**Snapshot isolation is broken for any query that uses an index.**
After UPDATE, the index points to the new row. A concurrent reader
with an older snapshot sees the updated value instead of the old value.

## Architecture Root Cause
The MVCC layer intercepts `HeapFile.scan()` to add visibility filtering.
But index lookups go: BPlusTree.search(key) → returns (pageId, slotIdx) → 
HeapFile.get(pageId, slotIdx) → returns row directly. This path never
invokes the scan interceptor.

## Fix Options

### 1. Intercept HeapFile.get() (Quick Fix)
Also intercept `heap.get(pageId, slotIdx)` to check MVCC visibility
before returning the row. If the version at that slot is invisible,
return null (or indicate not-found).

### 2. MVCC-aware index (Proper Fix)
Store txId/version info in the index entries themselves. Each index
entry includes the xmin that created it. On lookup, filter entries by
MVCC visibility before returning.

### 3. Version chain in index (PostgreSQL approach)
Index entries point to the HEAD of a version chain. Walk the chain
to find the version visible to the current snapshot. This is how PG
handles HOT chains for index lookups.

## Recommendation
Start with #1 (intercept heap.get). It's minimal and covers the common case.
The index-level MVCC (#2/#3) is needed for correctness with HOT chains
but can wait.
