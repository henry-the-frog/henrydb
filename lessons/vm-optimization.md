# VM Optimization — Lessons

_Promoted from scratch/inline-caching-design.md (uses: 2, Apr 15)_

## Inline Caching: Cache Structure, Not Values
1. **Interpreter-level IC for mutable values DOES NOT WORK.** Caching `(shape, key) → value` goes stale on mutation.
2. **Correct approach:** Cache `(shapeId, key) → slotIndex`, then read `slots[slotIndex]`. Always reads current value.
3. **ShapedHash + slot-based storage:** Replace `Map<key,value>` with `Shape` (key→slot mapping) + `slots[]` array. 100% IC hit rate after warmup.

## Shapes / Hidden Classes
- Shape = the set of keys in a hash. Two hashes with same keys = same shape.
- Shape ID = hash of sorted key names. Computed lazily.
- Transition chains: adding a property creates a new shape linked to the parent.
- Global registry with sorted-key dedup prevents shape explosion.

## IC Hierarchy
- **Monomorphic** (1 entry): common case, highest performance
- **Polymorphic** (up to 4 entries): multiple shapes at same access site
- **Megamorphic** (give up): fall back to hash lookup

## Implementation Results (Monkey-lang)
- ShapedHash: shape + slots[] + keys[] (for iteration backward compat)
- Per-bytecode-position IC slots
- OpHash creates ShapedHash, OpIndex passes icIp to executeIndexExpression
- 48 tests, 866 total all pass. 100% monomorphic hit rate on repeated access.

## Broader VM Optimization Lessons
- Constant folding means `1+2` becomes `3` at compile time — tests must use variables to prevent it
- Opcode enums in JS don't enforce uniqueness — need explicit collision checks
- Debugger resume semantics: IP at instruction vs past instruction is a classic design decision; use `_resuming` flag
- Generational GC write barrier is crucial for old→young references
