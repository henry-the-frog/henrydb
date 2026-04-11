# PageLSN Implementation Path for HenryDB

## The Insight
PageLSN is a single integer stored in each page header. It's the LSN of the last WAL record that modified this page.

## What Changes

### 1. SlottedPage: Add pageLSN field
```javascript
constructor(pageId, pageSize) {
  // ...existing...
  this.pageLSN = 0;  // LSN of last modification
}
```
- Must be serialized/deserialized with the page
- Stored in the first 8 bytes of the header (or after existing header)

### 2. FileBackedHeap: Set pageLSN on every modification
```javascript
insert(values) {
  const lsn = this._wal.appendInsert(txId, tableName, pageId, slotIdx, values);
  page.pageLSN = lsn;  // <-- NEW
  // ...
}
```
- Same for update() and delete()
- The WAL append must return the LSN it assigned

### 3. WAL: Return LSN from append methods
Currently `appendInsert` etc. don't return the assigned LSN. They need to:
```javascript
appendInsert(txId, tableName, pageId, slotIdx, values) {
  const lsn = this._nextLsn++;
  // ...write record...
  return lsn;
}
```

### 4. Recovery: Per-page redo decision
```javascript
for (const record of walRecords) {
  if (!committed.has(record.txId)) continue;
  
  const page = fetchPage(record.pageId);
  if (page.pageLSN >= record.lsn) continue; // SKIP — already applied
  
  // Apply the record
  applyRecord(page, record);
  page.pageLSN = record.lsn;
}
```

This completely eliminates:
- The `lastAppliedLSN` per-table tracking
- The full-vs-incremental heuristic
- The "wipe all pages" recovery path
- The close()-must-update-LSN bug class entirely

### 5. Page Serialization
SlottedPage.serialize() / deserialize() need to include pageLSN.
Current header layout: `[pageId(4), slotCount(4), freeSpaceStart(4), freeSpaceEnd(4)]` = 16 bytes
New: `[pageId(4), slotCount(4), freeSpaceStart(4), freeSpaceEnd(4), pageLSN(8)]` = 24 bytes

Or use BigUint64 for pageLSN since LSNs can grow large.

## Complexity Assessment
- Lines of code: ~50-80
- Risk: Medium — touches page serialization format (breaks existing DB files)
- Migration: Add version byte to page header, or just bump format version
- Tests needed: All existing persistence tests + new pageLSN-specific tests

## Why Not Today
This is a structural change to the page format. Should be a dedicated task with:
1. Format migration story
2. Full test suite verification
3. Remove the lastAppliedLSN hack after pageLSN works

## Non-Obvious Observation
With pageLSN, the recovery algorithm becomes **idempotent by construction** — you can replay the WAL any number of times and get the same result, because each page knows exactly which records have already been applied. Our current approach achieves idempotence via the lastAppliedLSN hack, which is more fragile.

Also: pageLSN enables **parallel recovery** — each page can be recovered independently since the redo decision is per-page, not per-table.
