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

## Integer Unboxing (Apr 28)
- **Raw JS numbers on VM stack** instead of MonkeyInteger objects. 1.76x fib(25) speedup.
- **Re-box at boundaries**: lastPoppedStackElem, callBuiltin args, array/hash construction.
- **`?? NULL` not `|| NULL`**: Raw number 0 is falsy, so `0 || NULL` → NULL. `??` only coerces null/undefined.
- **objectKeyString must handle raw numbers**: `typeof obj === 'number'` check needed for `int:N` format.
- **GC must skip primitives**: `typeof obj !== 'object'` guard in markObject.

## Superinstructions (Apr 28)
- **V8 JIT negates dispatch reduction**: <0.5% benefit. C/WASM interpreters would benefit.
- **Type-unsafe OpAdd**: Can't combine OpAdd + OpSet because OpAdd handles int/float/string/array.
- **Type-safe pattern**: Only emit superinstructions when compiler can prove operand types (e.g., `set x = x + <int_literal>`).
- **AST caching breaks with constant folding**: Compiler mutates AST nodes in-place. Cached ASTs get corrupted on second compile.

## String Interning (Apr 28)
- **Don't intern concat results**: intern table is for repeated constants, not unique runtime strings.
- **Map.get + Map.set overhead**: ~4x slower than direct MonkeyString construction for unique strings.
- **Keep interning for**: string literals, identifier names, short constants.

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
