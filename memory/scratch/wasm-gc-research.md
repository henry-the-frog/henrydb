# WASM GC Proposal Research — 2026-04-27

## Key Findings

### Node.js v22 Support
- **Struct types** (0x5f): ✅ Supported — can define structs with typed fields
- **Array types** (0x5e): ✅ Supported — can define GC-managed arrays  
- **Exception handling** (WebAssembly.Tag/Exception): ✅ Available
- **GC flag** (--experimental-wasm-gc): Not needed — GC types compile without flags

### What This Means for monkey-lang

**Current approach:** Objects (strings, arrays, closures) stored in linear memory with manual tag+length headers. GC via mark-sweep on JS side. Host-backed hash maps for complex objects.

**GC approach:** Objects as WASM GC structs/arrays. No manual memory management. No host-backed workarounds. The WASM runtime handles GC.

### Benefits
1. **No integer/pointer confusion** — GC refs are typed, not raw i32
2. **No manual allocation/free** — runtime manages memory
3. **No mark-sweep implementation** — WASM GC handles it
4. **Better performance** — GC structs are optimized by V8

### Challenges
1. **Binary encoding** — need to emit struct.new, struct.get, struct.set, array.new etc.
2. **Closure representation** — closures as structs with function ref + env fields
3. **Hash maps** — no built-in hash table in GC; would need an array-of-pairs approach
4. **Existing tests** — 200 tests assume i32-based value representation

### String Encoding
Current: strings as [TAG_STRING:i32][length:i32][utf8_bytes...] in linear memory, interned in data segment.

With GC: strings could be GC arrays of i8, or use the stringref proposal (not yet widely available). For now, the linear memory approach is fine since strings don't benefit as much from GC management (they're immutable and interned).

### Recommendation
Build a parallel WASM-GC backend that uses:
- `structref` for closures (func_index + env struct)
- `arrayref` for arrays 
- Keep linear memory strings (they're interned and immutable)
- Replace host-backed hash maps with GC struct-based open addressing

This is a medium-term project (~1-2 weeks of focused work). The current i32-based backend works and has 200 tests. The GC backend would be a new target, not a replacement.
