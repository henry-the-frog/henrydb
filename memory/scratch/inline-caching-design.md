# Inline Caching for Monkey-lang

**Created:** 2026-04-15 | **Uses:** 1

## Key Lesson (from implementation attempt)
**Interpreter-level IC for mutable values DOES NOT WORK.** If you cache `(shape, key) → value`, the cached value goes stale when the hash is mutated. Tests failed: `h["count"] = h["count"] + 1` returned 2 instead of 4 because the IC returned the old value.

Correct approaches:
1. **Cache structure, not values** — IC can cache that a shape exists and how to access it, but must always read the current value from the Map
2. **JIT-level IC** — emit GUARD_SHAPE instruction, then direct offset access. The JIT can specialize for known shapes and bail out on shape change
3. **Hidden classes** — convert hashes to fixed-layout objects (like V8). Property access becomes array index instead of Map.get(). This is the big win but a major refactor.

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
