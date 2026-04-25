# Monkey-Lang to WASM Compiler — Design Analysis

## Current Architecture
```
Source → Lexer → Parser → AST → Compiler → Bytecode → VM
```

## Proposed WASM Architecture
```
Source → Lexer → Parser → AST → [optimize] → WASMCompiler → .wasm → Browser/Runtime
```

## Key Challenges

### 1. Dynamic Types → Static WASM Types
- Monkey-lang is dynamically typed; WASM has i32/i64/f32/f64
- **Solution**: Box all values (like CPython). Each value is a (tag, data) pair.
  - tag: i32 (0=int, 1=float, 2=string, 3=bool, 4=array, 5=hash, 6=function, 7=null)
  - data: i32 pointer to heap
- **Alternative**: Use the type checker output for optimization — known-type variables use unboxed WASM types

### 2. Closures → WASM Functions
- WASM functions are flat (no closures)
- **Solution**: Closure conversion. Each closure becomes:
  - A struct with captured variables (on WASM linear memory)
  - A function that takes the struct as first argument
  - `funcref` table for indirect calls

### 3. GC → WASM GC or Linear Memory
- **Option A**: Use WASM GC proposal (Chrome 119+, experimental)
  - Structured types, gc instructions
  - Most natural but not widely supported
- **Option B**: Implement GC in linear memory
  - Mark-sweep like the current VM
  - More portable but more complex
  
### 4. Strings → WASM Memory
- Strings are immutable byte arrays in linear memory
- Need: string interning, concatenation via copy
- UTF-8 encoding, length-prefixed

### 5. Arrays/Hashes → Linear Memory
- Arrays: (length, capacity, pointer-to-elements)
- Hashes: Need hash table implementation in WASM
  - Robin Hood hashing or open addressing
  - Keys and values as boxed pointers

## Compilation Strategy

### Phase 1: Minimal Viable WASM (integers + functions)
1. Integer arithmetic → i64 operations
2. Let bindings → WASM locals
3. If/else → WASM if/else blocks
4. Functions → WASM functions (no closures)
5. While loops → WASM loop/br

### Phase 2: Boxing + Strings
1. Value boxing (tag + pointer)
2. String allocation in linear memory
3. Runtime library for type checking, coercion

### Phase 3: Closures + GC
1. Closure conversion pass
2. Mark-sweep GC in linear memory
3. Function table for indirect calls

### Phase 4: Full Feature Parity
1. Arrays, hashes
2. Pattern matching → cascading if/else
3. Spread/rest → array manipulation
4. try-catch → WASM exception handling proposal

## Estimated Effort
- Phase 1: ~500 LOC, 1-2 days
- Phase 2: ~800 LOC, 2-3 days
- Phase 3: ~1000 LOC, 3-5 days
- Phase 4: ~2000 LOC, 1-2 weeks
- Total: ~4300 LOC, 2-3 weeks

## Reusable Components
- Lexer and Parser: 100% reusable
- Type checker: Could drive optimizations
- AST: Used as input
- Escape analysis: Determines what needs heap allocation
- SSA: Could drive WASM register allocation

## Alternative: Compile to C/LLVM IR
- More mature toolchain
- Better optimization (LLVM backend)
- But: heavier dependency, less portable than WASM
- Verdict: WASM is more aligned with the project's educational goals

## Key Insight
The existing escape analysis is crucial for WASM: non-escaping closures can be stack-allocated (WASM locals) instead of heap-allocated. This is exactly the optimization that matters for WASM performance.
