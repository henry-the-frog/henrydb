# Inline Caching for Monkey-lang

**Created:** 2026-04-15 | **Uses:** 2

## Key Lessons
1. **Interpreter-level IC for mutable values DOES NOT WORK.** Caching `(shape, key) → value` goes stale on mutation. T24 proved this.
2. **Cache structure, not values.** The correct approach: `(shapeId, keyStr) → slotIndex`, then read `slots[slotIndex]`. Always reads the current value.
3. **ShapedHash + slot-based storage works.** Replace `Map<key, value>` with `Shape` (key→slot mapping) + `slots[]` array. 100% IC hit rate after warmup.

## Implementation (T83-T84, Session B)
- **Shape class**: `keyMap: Map<string, slotIndex>`, transition chains for property addition, global registry with sorted-key dedup
- **ShapedHash**: `shape` + `slots[]` + `keys[]` (original MonkeyObjects for iteration). `.pairs` getter for backward compat.
- **InlineCache**: Per-bytecode-position. Monomorphic (1 entry) → Polymorphic (up to 4) → Megamorphic (give up).
- **VM integration**: OpHash creates ShapedHash, OpIndex passes `icIp` to executeIndexExpression, IC fast path on ShapedHash.
- **objectKeyString()**: Converts MonkeyInteger/String/Boolean to canonical string form for shape lookup.
- **Results**: 48 tests, 866 total all pass. 100% monomorphic hit rate on repeated access.

## Concept
Inline caching (IC) speeds up property/key lookups by caching the result at the access site.

In monkey-lang, hashes are used as objects/records. Most hash accesses hit the same keys on hashes with the same "shape" (set of keys). We can exploit this.

## Shapes / Hidden Classes
A "shape" = the set of keys in a hash. Two hashes with the same keys (regardless of values) have the same shape.

```
let point = {"x": 1, "y": 2}    // shape: ["x","y"]
let other = {"x": 5, "y": 10}   // same shape: ["x","y"]
let named = {"x": 1, "name": "p"} // different shape: ["name","x"]
```

Shape ID = hash of sorted key names. Computed lazily on first IC check.

## Monomorphic IC (simplest, highest ROI)
At each hash access site (bytecode offset), cache:
- `expectedShape`: the shape ID seen last time
- `cachedOffset`: the Map entry key for the accessed property

On hit: skip hash computation, return cached value directly.
On miss: do normal lookup, update cache.

## Where to apply in monkey-lang
1. **VM interpreter (OpIndex on hashes)**: Add IC slot per bytecode position
2. **JIT traces**: Add GUARD_SHAPE instruction, then direct slot access
3. **Module access**: import "math" → math["sqrt"] always same shape

## Implementation Plan
1. Add `_shapeId` to MonkeyHash (lazy, cached)
2. Add IC array to VM (indexed by instruction pointer)
3. On OpIndex with hash target:
   - Check IC: if shape matches, use cached result
   - Otherwise: normal lookup, update IC
4. JIT: emit GUARD_SHAPE + direct access instead of INDEX_HASH

## Expected Impact
- Hash property access: ~2-5x faster (eliminate hashing + Map lookup)
- Module method calls: significant speedup
- JIT traces with hash access: near-native speed

## Risks
- Shape computation overhead on first access
- Memory for IC slots (but small: 2 words per site)
- Hash mutation invalidates shape → need cache invalidation
- Polymorphic sites (>1 shape) need fallback strategy

## Reference
- V8's hidden classes: same concept, more elaborate (transition chains)
- LuaJIT: uses hash part of tables with similar caching
- PyPy: map-based approach (maps ≈ shapes)
