# WASM GC Lessons Learned — Apr 29, 2026

## Key Technical Insights

### 1. eqref is the Universal Value Type
- All monkey-lang values can be represented as `eqref` locals
- Integers: `ref.i31(value)` — 31-bit signed, sufficient for most programs
- Heap objects (arrays, closures, structs): GC refs cast from eqref
- Strings: could be i31 pointers into linear memory, or GC byte arrays
- Extraction requires explicit `ref.cast (ref i31)` before `i31.get_s`

### 2. GC Arrays are Dramatically Faster (but only for pure execution)
- **3.5μs** for create+fill+sum of 1000 elements (GC arrays)
- **1040μs** for the same via current linear memory compiler (includes compilation)
- **Compilation dominates**: ~800μs compilation vs ~50μs actual execution
- GC benefit only materializes with module caching (which already exists!)
- With cached modules: both paths are <1ms; the difference is in repeated execution

### 3. Host Import Overhead is Significant
- Every host import (strEq, keys, push, etc.) crosses the WASM→JS boundary
- GC arrays eliminate ALL host calls for array operations (pure WASM)
- The `__iter_prepare` pattern shows how host imports can add flexibility but at perf cost
- Long-term goal: minimize host imports by moving more logic into WASM

### 4. The Migration Path
- **Phase 1** (done): GC type system in module builder, verified working
- **Phase 2** (next): Hybrid approach — use GC arrays for new allocations, keep linear memory for strings
- **Phase 3** (future): Full eqref unification — all values as GC refs, i31 for ints
- Phase 3 breaks backward compat with all existing host imports (they expect i32 pointers)

### 5. Struct Types Enable Closures Without Linear Memory
- Closure = struct { func_idx: i32, ...captured_vars }  
- Currently closures use linear memory with a function pointer + env pointer
- GC structs eliminate env allocation and enable captured variable mutation naturally
- Each closure type needs a unique struct definition (code size concern)

## Compiler Design Lessons

### Keep Host Imports and WASM Functions in Sync
- When adding native WASM hash functions, the host import `__index_get/set` also needed updating
- The `__keys/__values` offset bug happened because the host code was written before the WASM layout was finalized
- **Rule**: whenever you change a memory layout, grep for ALL readers of that layout

### Type Detection at Runtime vs Compile Time
- Current compiler can't easily distinguish arrays from hashes at compile time
- `__iter_prepare` solves this with runtime dispatch (TAG check)
- Better long-term: compile-time type inference tells you the type, no runtime check needed
- GC backend would solve this naturally: different ref types for arrays vs hashes

### Binary Size Grows with Runtime Functions
- Each new WASM function (find_slot_str, hash_set_str, etc.) adds ~200-500 bytes
- Binary size thresholds in tests needed bumping (1500 → 2500 bytes for fib)
- Long-term: tree-shake unused runtime functions from the binary

## What Surprised Me
1. `i31ref` only gives 31 bits, not 32 — loses 1 bit vs current i32 implementation
2. `ref.cast` is REQUIRED before i31.get_s — can't directly read from eqref
3. The `__keys/__values` offset bug was a simple field swap that passed for days
4. Module caching was ALREADY implemented — I didn't need to add it
5. Compilation cost (0.8ms) dominates execution (0.05ms) for small programs
