# HOT Chains (Heap-Only Tuples) — Implementation Notes

uses: 1
created: 2026-04-18
tags: database, hot-chains, index-optimization, mvcc, update

## What HOT Chains Solve
When an UPDATE only modifies non-indexed columns, the secondary index entries don't need to change. Without HOT, every UPDATE does: delete old row → insert new row → update ALL indexes. With HOT, non-indexed-column updates skip index updates entirely.

## HenryDB Implementation

### Data Structures
- `HeapFile._hotChains` — Map from `"oldPageId:oldSlotIdx"` → `{pageId, slotIdx}` of new version
- Same on `BTreeTable._hotChains`

### Detection (in db.js `_update()`)
```js
let isHotUpdate = false;
if (table.indexes.size > 0) {
  isHotUpdate = true;
  for (const [colName, index] of table.indexes) {
    const colIdx = table.schema.findIndex(c => c.name === colName);
    if (colIdx >= 0 && item.values[colIdx] !== newValues[colIdx]) {
      isHotUpdate = false;
      break;
    }
  }
}
```

### Chain Following
- `_heapGetFollowHot(heap, pageId, slotIdx)` — tries direct get, falls back to following chain
- Used in all 5 index-scan paths: equality, range, BETWEEN, IN, join

### PostgreSQL Comparison (from source research)
| Aspect | PostgreSQL | HenryDB |
|--------|-----------|---------|
| Storage | In-tuple t_ctid link (same page) | Separate Map (cross-page OK) |
| Same-page required | Yes (cache locality) | No (simpler, less cache-friendly) |
| Tuple flags | HEAP_HOT_UPDATED, HEAP_ONLY_TUPLE | None (Map-based) |
| Pruning | In-page pruning during SELECT + VACUUM | Not yet implemented |
| Criteria | No indexed column changes + space on page | No indexed column changes only |
| PG 16+ | Relaxed for BRIN indexes | N/A |
| fillfactor | Used to reserve page space for HOT | N/A |

## Gaps
1. **MVCC visibility** — Index scans in MVCC mode should check visibility after following chain
2. **VACUUM pruning** — Chains grow without bound; need pruning when old versions invisible
3. **Multi-hop chains** — Supported but not stress-tested at scale

## Lesson
The detection logic is simple (compare old vs new indexed column values), but the integration touches every index-scan code path. Missed paths = silent data loss or stale reads.
