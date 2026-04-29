# TASKS.md

## Active Projects

### monkey-lang (WASM Compiler)
- **Tests:** 1259 pass (interpreter/compiler) + 2034 pass (WASM sub-project) = 3293 total, 0 fail
- **WASM compiler LOC:** ~5800 (wasm-compiler.js)
- **Playground:** 13 examples (194KB bundle), deployed at GitHub Pages
- **CI:** ✅ Green (all 3 Node versions: 18, 20, 22)
- **WASM Features (session A, Apr 29):**
  - Arrays: dynamic reallocation (50K elements), for-in loops, comprehensions with filter, break/continue
  - Strings: 11 methods (concat, len, charAt, substring, indexOf, toUpperCase, toLowerCase, replace, trim, split, intToString)
  - String comparison: ==, !=, <, >, <=, >= (lexicographic via __str_cmp)
  - Hash maps: open addressing, **integer + string keys** (FNV-1a), get/set, frequency counter, auto-resize, **for-in iteration via keys()/values()**
  - Type inference: variable type tracking (string/int/array/hash), call-site inference for function params
  - Memory management: memory.grow for >64KB, bump allocator with reallocation
  - Internal WASM functions: __alloc, __array_ensure_cap, __str_concat, __str_eq, __str_cmp, __str_indexOf, __int_to_str, __hash_new/get/set/resize, **__hash_find_slot_str, __hash_set_str_native, __hash_get_str_native**
  - **WASM GC support:** struct/array type definitions, GC ref types, 21 GC instruction tests, 3.5μs per 1K-element array create+fill+sum
- **Critical bug fixed (Apr 29):** __keys/__values hash field offset swap — capacity at +4 was read from +8
- **CI fixed (Apr 29):** workflow workdir + Node 18 import.meta.dirname compatibility
- **Known limitations:**
  - GC is no-op for linear memory (bump allocator never frees) — but WASM GC backend scaffolding ready
  - i32 overflow for large numbers
  - String operations on fully generic functions need call-site type info

### HenryDB
- **Tests:** 4327 pass, 0 fail; 54/54 SQL feature categories verified
- **LOC:** 81K source + 130K test = 211K total
- **JSON support:** Full JSON1 extension (16+ functions) + json_each/json_tree TVFs
- **Recent (Apr 29):** Compiled SET expressions in UPDATE pipeline (compileSetExpr/compileSetBatch), 19 new UPDATE compilation tests
- **Recent (Apr 27):** Triggers (UPDATE OF + WHEN + INSTEAD OF + cascade + recursion depth limit), unified cost model, adaptive engine integration, EXPLAIN ANALYZE multi-engine, PL/SQL recursive functions + string concat fixes, json_set/replace/insert/remove/patch, generate_series, printf, total()
- **INSERT perf:** ✅ Fixed (Apr 28) — 167x faster (3.5ms → 0.021ms/row) by removing redundant heap scans
- **Known issues:**
  - Cost model multipliers are aspirational (not calibrated — all engines ~same speed at current scale)

### neural-net
- **Status:** Very mature (170 source modules, 1305 tests, gradient checkpointing, mixed precision)
- **No active work needed**

## Backlog
- ~~monkey-lang: NaN-boxing for typed value representation~~ ✅ Done as integer unboxing (Apr 28)
- monkey-lang: Module resolution for import statements
- monkey-lang: Superinstructions for common opcode sequences (next perf optimization)
- type-infer: Add recursive types and polymorphic container tests
- regex-engine: Fix empty string matching and anchor support
