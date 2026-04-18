# ALTER TABLE Backfill Duplicate Tuples Bug

created: 2026-04-17
tags: henrydb, bug, persistence, alter-table

## Bug Description
After ALTER TABLE ADD COLUMN + UPDATE + checkpoint + crash, duplicate rows appear.
Row has both the pre-UPDATE version and the post-UPDATE version.

## Root Cause
ALTER TABLE ADD COLUMN backfills existing rows by calling:
```javascript
table.heap.pages.find(p => p.id === pageId)?.updateTuple(slotIdx, encoded);
```

This operates at the **page level**, directly modifying page data. But the MVCC layer
operates at the **heap level** with version tracking (xmin/xmax in version maps).

When a subsequent UPDATE modifies the same row, it goes through MVCC:
DELETE (mark old version as deleted) + INSERT (new tuple in new slot).

The backfill's `updateTuple` and the MVCC's INSERT both create tuples on disk.
After checkpoint, both tuples are flushed. On recovery, both are visible.

## Fix
ALTER TABLE ADD COLUMN backfill should go through the MVCC layer:
```javascript
for (const { pageId, slotIdx, values } of table.heap.scan()) {
  values.push(defaultValue ?? null);
  // Instead of page-level updateTuple:
  // heap.delete(pageId, slotIdx);
  // heap.insert(values);
  // This ensures version maps stay consistent
}
```

OR: after `updateTuple`, update the version map to reflect the in-place change.

## Impact
- Data corruption: duplicate rows after checkpoint + crash
- Only affects: ALTER TABLE ADD COLUMN + UPDATE + checkpoint sequence
- Clean close/reopen works (checkpoint not involved)

## Priority
Normal — requires careful coordination between page-level and heap-level operations.
