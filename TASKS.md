# TASKS.md

## Active Projects

### monkey-lang (WASM Compiler)
- **Tests:** 1930 pass, 0 fail, 0 skip (386 WASM-specific, 345 wasm-compiler tests)
- **Performance:** WASM 457x eval on fib(30), 530x on loop 1M, 50x improvement from knownInt param inference
- **LOC:** 24K source + 15K test = 39K total
- **Features:** Full language in WASM — closures, classes (3-level inheritance + super), exceptions (native WASM EH), floats, type inference, TCO, 9 string methods, 9 utility builtins, 6 HOF builtins (map/filter/reduce/find/any/every + sort/forEach), module cache (LRU-64), REPL incremental compilation
- **Optimizations:** knownInt param inference, return type inference, closure capture type propagation, constant folding, dead code elimination, tail-call optimization, 0-capture env skip
- **Box/cell closures:** ✅ Fixed (Apr 28) — shared mutable state, self-ref+captures, recursive+mutable all working
- **Array push:** ✅ Fixed (Apr 28) — amortized O(1) with capacity-based growth. 5K pushes in 106ms (was crashing at 3-5K).
- **Known issues:** 
  - GC is no-op for WASM-internal allocations (bump allocator never frees)
  - i32 overflow for large numbers (factorial(20), sum 100k)

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
