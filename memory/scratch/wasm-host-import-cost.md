# WASM Performance: Host Import Crossing Cost

**Created:** 2026-04-27
**Project:** monkey-lang WASM compiler

## The Insight
The single biggest performance bottleneck in the WASM backend was calling host imports for basic arithmetic. Every `x + y` where types weren't known at compile time required:
1. WASM → JS boundary crossing (call `__add` import)
2. JS runtime type checking (isStrPtr validation)
3. JS → WASM return crossing

## The Fix
Track `knownInt` flag on variable bindings in the scope. When `let x = 5` is compiled, mark `x` as `knownInt`. Then `x + y` where both are `knownInt` compiles to direct `i32_add` instead of `call __add`.

## The Impact
- **Before:** WASM vs VM: 4.7x speedup (with precompile)
- **After:** WASM vs VM: 36.7x speedup — an 8x improvement from this single optimization
- WASM now beats the JS transpiler on 4/10 benchmarks (sum 10k, nested 100x100, closure factory, if/else)
- `sum 10k`: 0.07ms (WASM) vs 0.13ms (Transpiler/V8) — WASM is 2x faster than V8's JIT

## Lesson
In WASM, the boundary between WASM and JS host is the dominant cost. Any operation that can stay entirely in WASM (without crossing to host) is orders of magnitude faster. Type inference that eliminates host calls has outsized impact.

This is the same insight that makes WASM performant for numeric workloads in general — it's the escape from JS that matters, not the instruction-level speed.
