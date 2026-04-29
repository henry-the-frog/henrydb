# TASKS.md

## Active Projects

### monkey-lang (WASM Compiler)
- **Tests:** 1479 pass, 0 fail (270 WASM-specific: 68 array, 104 string, 16 hash, 82 core)
- **WASM compiler LOC:** 4118 (wasm-compiler.js)
- **Playground:** 13 examples (194KB bundle), deployed at GitHub Pages
- **WASM Features (session B2, Apr 28):**
  - Arrays: dynamic reallocation (50K elements), for-in loops, comprehensions with filter, break/continue
  - Strings: 11 methods (concat, len, charAt, substring, indexOf, toUpperCase, toLowerCase, replace, trim, split, intToString)
  - String comparison: ==, !=, <, >, <=, >= (lexicographic via __str_cmp)
  - Hash maps: open addressing with integer keys, get/set, frequency counter pattern
  - Type inference: variable type tracking (string/int/array/hash), call-site inference for function params
  - Memory management: memory.grow for >64KB, bump allocator with reallocation
  - Internal WASM functions: __alloc, __array_ensure_cap, __str_concat, __str_eq, __str_cmp, __str_indexOf, __int_to_str, __hash_new/get/set
- **Critical bug fixed (Apr 28):** Top-level let execution order — lets were initialized before expression statements
- **Known limitations:**
  - Hash map: no auto-resize (cap 16 default, max ~12 entries), integer keys only
  - String operations on fully generic functions need call-site type info
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
