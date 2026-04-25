# String Truncation Bug Fix (2026-04-25)

## Root Cause
- `disk-manager.js`: `PAGE_SIZE = 4096` (4KB)
- `page.js`: `PAGE_SIZE = 32768` (32KB)
- In-memory Database used 32KB pages → strings up to ~30KB worked
- PersistentDatabase used DiskManager with 4KB pages → rows >4076 bytes silently dropped

## The Silent Failure
`FileBackedHeap.insert()` called `page.insertTuple(tupleBytes)` on a fresh page.
When tuple didn't fit, `insertTuple` returned `-1` (slotIdx).
The code never checked for -1 — it continued with `slotIdx = -1`, incremented `_rowCount`, but the data was never stored.

**Result**: INSERT appeared to succeed, but SELECT returned no rows. Data corruption.

## Fix
1. Changed `disk-manager.js` PAGE_SIZE from 4096 → 32768 (match page.js)
2. Changed DiskManager constructor default from `4096` → `PAGE_SIZE`
3. Added error check in `FileBackedHeap.insert()`: throw if `insertTuple` returns -1 on fresh page

## Boundary
- Old: 4075 bytes max (4096 - 20 header - 1 slot)
- New: ~32700 bytes max (32768 - header/slots overhead)
- Truly oversized rows now get descriptive error: "Row too large: encoded size N bytes exceeds page capacity of 32768 bytes"

## Tests Updated
- `file-backed-heap.test.js`: increased tuple size for eviction test (200→2000 bytes)
- `persistence-e2e.test.js`: increased tuple size for eviction test
- New: `string-truncation.test.js` — 7 regression tests

## Pattern
This is the "path not handled" pattern from Apr 24 — a feature combination (persistence + large strings) that was never tested together. The in-memory path masked the bug.
