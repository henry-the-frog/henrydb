# TASKS.md

## Active Projects

### monkey-lang (WASM Compiler)
- **Tests:** 1386 pass, 0 fail (183 WASM-specific: 63 array, 38 string, 82 core)
- **WASM compiler LOC:** 2180 (wasm-compiler.js)
- **WASM Features (session B2, Apr 28):**
  - Arrays: dynamic reallocation (grow beyond initial capacity, tested to 50K elements)
  - Array comprehensions: `[x * 2 for x in arr if x > 0]`
  - For-in loops: `for (x in arr) { body }`
  - Strings: concat (`+`), comparison (`==`/`!=`), `len()`
  - Type inference: variable type tracking (string/int/array), call-site inference for function params
  - Memory management: `memory.grow` for >64KB allocations, bump allocator
  - Internal WASM functions: `__alloc`, `__array_ensure_cap`, `__str_concat`, `__str_eq`
  - Playground updated with new examples and rebuilt bundle
- **Critical bug fixed (Apr 28):** Top-level let execution order — lets were initialized before expression statements
- **Known limitations:**
  - String operations on fully generic functions (no call-site type info) fall back to integer semantics
  - GC is no-op (bump allocator never frees)
  - i32 overflow for large numbers

### HenryDB
- **Tests:** 4327 pass, 0 fail; 54/54 SQL feature categories verified
- **LOC:** 81K source + 130K test = 211K total
- **JSON support:** Full JSON1 extension (16+ functions) + json_each/json_tree TVFs
- **Recent (Apr 27):** Triggers (UPDATE OF + WHEN + INSTEAD OF + cascade + recursion depth limit), unified cost model, adaptive engine integration, EXPLAIN ANALYZE multi-engine, PL/SQL recursive functions + string concat fixes, json_set/replace/insert/remove/patch, generate_series, printf, total()
- **INSERT perf:** ✅ Fixed (Apr 28) — 167x faster (3.5ms → 0.021ms/row) by removing redundant heap scans
- **Known issues:**
  - Cost model multipliers are aspirational (not calibrated — all engines ~same speed at current scale)

### neural-net
- **Status:** Very mature (170 source modules, 1305 tests, gradient checkpointing, mixed precision)
- **No active work needed**

## Backlog
- monkey-lang: NaN-boxing for typed value representation
- monkey-lang: Module resolution for import statements
- type-infer: Add recursive types and polymorphic container tests
- regex-engine: Fix empty string matching and anchor support
