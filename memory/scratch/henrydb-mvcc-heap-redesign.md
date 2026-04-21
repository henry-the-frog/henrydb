# MVCC Heap Monkey-Patching → Wrapper Class Redesign
- created: 2026-04-21
- tags: henrydb, mvcc, refactoring, architecture

## Current State: Monkey-Patching

TransactionalDatabase overrides 4 heap methods via closure-based patching:
1. `heap.scan` → MVCC-visible scan + PK dedup (~60 LOC)
2. `heap.delete` → Soft-delete with xmax + PK-level conflict detection (~90 LOC)
3. `heap.findByPK` → MVCC-visible PK lookup + scan fallback (~30 LOC)
4. `heap.get` → MVCC-visible row get (~20 LOC)

Original methods saved as `heap._origScan`, `heap._origDelete`, `heap._origGet`.

## Fragility Risks
1. **Order dependence**: Overrides applied sequentially, later ones reference earlier ones
2. **New heap types**: Must repeat patching for each new heap implementation
3. **New methods**: Any new heap method (e.g., scanRange) bypasses MVCC
4. **Stale references**: ALTER TABLE recreating heap invalidates `_orig*` closures
5. **Implicit closure context**: Captures `tdb` and `origScan` — brittle if heap moves contexts

## Proposed: MVCCHeap Wrapper Class

```javascript
export class MVCCHeap {
  constructor(baseHeap, { versionMap, mvccManager, tableName, activeTxGetter, pkIndices }) {
    this._base = baseHeap;
    this._vm = versionMap;
    this._mvcc = mvccManager;
    this._table = tableName;
    this._getActiveTx = activeTxGetter;
    this._pkIndices = pkIndices;
  }
  
  *scan() { /* existing MVCC scan logic from transactional-db.js */ }
  get(pageId, slotIdx) { /* existing MVCC get logic */ }
  delete(pageId, slotIdx) { /* existing MVCC soft-delete logic */ }
  insert(values) { return this._base.insert(values); }
  findByPK(pkValue) { /* existing MVCC findByPK logic */ }
  
  // Direct access to underlying heap (for vacuum, recovery, etc.)
  get base() { return this._base; }
  
  // Pass-through for non-MVCC operations
  get pageCount() { return this._base.pageCount; }
  flush() { return this._base.flush(); }
  addHotChain(...args) { return this._base.addHotChain?.(...args); }
}
```

## Migration Plan
1. Create `mvcc-heap.js` with the MVCCHeap class
2. Move all monkey-patching logic into the class methods
3. In `_interceptHeapForMVCC()`, create `new MVCCHeap(heap, ...)` and replace `table.heap` with it
4. Update vacuum/checkpoint to use `mvccHeap.base` instead of `_origScan`
5. Remove all `_orig*` property stashing

## Estimated Effort: ~2 hours
- Class creation and logic transfer: 1 hour
- Call site updates: 30 min
- Testing: 30 min

## Benefits
- New heap types automatically get MVCC
- No method override surprises
- Clean separation of concerns
- Easy to add new MVCC-aware methods
- Vacuum/recovery can cleanly access base heap
