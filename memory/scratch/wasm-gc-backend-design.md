# WASM GC Backend Design — monkey-lang

## Status: Design Phase (Apr 28, 2026)

## Overview
Replace linear memory heap management with WASM GC-managed types for:
- Arrays → WASM GC arrays (auto-managed, no manual alloc/free)
- Strings → Keep in linear memory (interned, immutable, efficient)
- Closures → WASM GC structs { funcref, envref }
- Hash maps → WASM GC structs with linear probing (or keep in linear memory)
- Boxes → WASM GC structs with mutable field (replaces heap box cells)

## Type Definitions

```
;; Monkey value: anyref (union of all types)
;; Use eqref for equality comparisons

;; Array type
(type $MonkeyArray (array (mut i32)))

;; Closure type: [funcref (table index), envref (GC struct)]
(type $ClosureEnv (array (mut i32)))  ;; variable-length env
(type $Closure (struct 
  (field $func i32)        ;; table index for indirect call
  (field $env (ref null $ClosureEnv))))

;; Box type: single mutable cell
(type $Box (struct (field $val (mut i32))))

;; Hash map: keep in linear memory (complex structure, not worth GC-managing)
```

## Key Decisions

### 1. Value Representation
**Option A: Tagged i32 (current approach + GC for heap objects)**
- Integers: raw i32
- Arrays: (ref $MonkeyArray) — but how to distinguish from i32?
- Problem: i32 and ref types can't coexist on the stack

**Option B: Everything is anyref**
- Integers: (ref $IntBox) where IntBox = (struct (field i32))
- Slow: every arithmetic op boxes/unboxes
- But: GC handles all memory

**Option C: Dual-mode compilation**
- Functions that only use integers: pure i32 WASM (fast)
- Functions that use mixed types: anyref-based (slower but correct)
- Complex but optimal

**Recommendation: Option A with externref bridge**
- Keep i32 for local computation
- Use externref for heap objects passed between functions
- Convert at function boundaries

### 2. Array Representation
- Use `(type $MonkeyArray (array (mut i32)))` for integer arrays
- For mixed arrays, use `(type $AnyArray (array (mut anyref)))` — slower
- `array.new` replaces `__make_array` runtime function
- `array.get/set/len` replace `__array_get/__array_set/len`
- Push: `array.copy` + grow (WASM GC doesn't have resize, need new array)

### 3. Closure Representation
- `$Closure = struct { i32 funcref, (ref null $ClosureEnv) }`
- Env is a GC array holding captured variable values
- For boxed vars: env holds `(ref $Box)` instead of raw i32
- Box = `struct { (mut i32) }` — GC-managed mutable cell
- This REPLACES the current linear memory box allocation

### 4. Migration Path
1. **Phase 1**: Add GC type definitions to WasmModuleBuilder (new opcodes)
2. **Phase 2**: Replace box allocation with GC struct.new
3. **Phase 3**: Replace array allocation with GC array.new
4. **Phase 4**: Replace closure env with GC struct
5. **Phase 5**: Move strings to GC if beneficial

Each phase should be independently testable with the existing 339 tests.

### 5. Blockers / Questions
- **anyref vs i32**: Can't easily mix i32 integers with GC refs in the same value space. Need NaN-boxing or tagged pointers in externref.
- **Performance**: GC arrays may be slower than linear memory for sequential access (cache locality)
- **Browser support**: WASM GC is in Chrome/V8 and Node.js, but not Firefox/Safari yet
- **String handling**: GC strings (using `(array (mut i8))`) would be more convenient but less efficient than interned strings in linear memory

## Estimated Effort
- Phase 1 (builder support): 2-3 hours
- Phase 2 (box → GC struct): 1-2 hours
- Phase 3 (arrays): 4-6 hours (many code sites)
- Phase 4 (closures): 3-4 hours
- Phase 5 (strings, optional): 2-3 hours

Total: 12-18 hours across multiple sessions
