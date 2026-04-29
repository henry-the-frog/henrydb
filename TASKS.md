# TASKS.md

## Active Projects

### monkey-lang (WASM Compiler)
- **Tests:** 1259 pass (interpreter/compiler) + 2007 pass (WASM sub-project) = 3266 total, 0 fail
- **WASM compiler LOC:** ~5570 (wasm-compiler.js)
- **Playground:** 13 examples (194KB bundle), deployed at GitHub Pages
- **WASM Features (session B2, Apr 28):**
  - Arrays: dynamic reallocation (50K elements), for-in loops, comprehensions with filter, break/continue
  - Strings: 11 methods (concat, len, charAt, substring, indexOf, toUpperCase, toLowerCase, replace, trim, split, intToString)
  - String comparison: ==, !=, <, >, <=, >= (lexicographic via __str_cmp)
  - Hash maps: open addressing, integer keys + string keys (FNV-1a), get/set, frequency counter pattern, **auto-resize at 75% load factor**
  - Type inference: variable type tracking (string/int/array/hash), call-site inference for function params
  - Memory management: memory.grow for >64KB, bump allocator with reallocation
  - Internal WASM functions: __alloc, __array_ensure_cap, __str_concat, __str_eq, __str_cmp, __str_indexOf, __int_to_str, __hash_new/get/set/resize
- **Critical bug fixed (Apr 28):** Top-level let execution order — lets were initialized before expression statements
- **VM optimization (Apr 28):** Integer unboxing — raw JS numbers on stack instead of MonkeyInteger objects (1.76x speedup on fib(25))
- **Bug fix (Apr 28):** Mutable closures in hash literals — compiler's AST walker skipped Map-typed pairs
- **Bug fix (Apr 28):** `|| NULL` → `?? NULL` in VM/evaluator (0/false/"" were coerced to NULL)
- **Known limitations:**
  - ~~Hash map: no auto-resize~~ ✅ Fixed (Apr 28)
  - String operations on fully generic functions need call-site type info
  - GC is no-op (bump allocator never frees)
  - i32 overflow for large numbers
  - Hash map: integer keys only (string key support via JS fallback)

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
- ~~monkey-lang: NaN-boxing for typed value representation~~ ✅ Done as integer unboxing (Apr 28)
- monkey-lang: Module resolution for import statements
- monkey-lang: Superinstructions for common opcode sequences (next perf optimization)
- type-infer: Add recursive types and polymorphic container tests
- regex-engine: Fix empty string matching and anchor support
