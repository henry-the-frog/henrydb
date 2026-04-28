# WASM Closure Bugs — RESOLVED (Apr 28, 2026)

## Status: ✅ FIXED

All 3 bugs fixed via box/cell pattern. 10 regression tests added. 1940/1940 tests pass.

## Solution: Box/Cell Pattern
Variables that need heap boxing (shared mutable state across closures):
- **Captured AND mutated** — closure reads/writes a variable that's also written elsewhere
- **Captured by 2+ closures** — multiple closures share the same variable
- **Self-referencing with other captures** — `let f = fn(){x; f(n-1)}` where f captures both itself and x

### Key Implementation Details
1. **Analysis pass** (`_analyzeBoxedVariables`) runs before compilation, identifies which vars need boxing per scope
2. **Box allocation**: `alloc(4)` → 4-byte heap cell, local holds pointer to cell
3. **Read**: `i32.load(box_ptr)` instead of `local.get`
4. **Write**: `i32.store(box_ptr, value)` instead of `local.set`
5. **Closure capture**: env stores box pointer (shared reference), not value (snapshot)
6. **Scope ID tracking**: `_scopeIdStack` mirrors analysis scope IDs during compilation. Must push/pop in BOTH `compileFunctionLiteral` AND `_compileFunctions` paths.

### Critical Optimization
Pure self-recursive functions (like `fib`) are NOT boxed — they use the fast direct-call path. Only self-referencing closures WITH OTHER captures need boxing. Without this optimization, fib was 10x slower.

## Lessons Learned
1. **Scope tracking is non-obvious**: Analysis and compilation use different scope structures. The `_compileFunctions` path (for direct functions) also needs scope ID tracking.
2. **Performance vs correctness tradeoff**: Naive boxing causes 10x regression on common patterns. Smart analysis (skip pure self-recursion) preserves performance.
3. **The remote had an incomplete fix**: `captured`/env write-back approach was incomplete (didn't handle shared mutable state). Box/cell is the correct pattern (same as Python cells, Lua upvalues).

### Additional Bugs Found During Stress Testing (same session)
4. **Hash literal values not traversed**: Closures inside hash literals (`{"inc": fn(){...}}`) were not analyzed for captures/mutations. The analysis walker didn't traverse hash pairs.
5. **Deep nesting limited to 2 levels**: `findNestedCaptures` stopped at nested FunctionLiterals, missing captures from 3+ level deep closures. Fixed by making it fully recursive.

Both fixes are critical for real-world patterns (counter objects, deeply nested closures).

### Key Lesson
The box analysis AST walker MUST traverse ALL node types (hash pairs, spread elements, etc.) and recurse through ALL nesting levels. Missing any node type or depth limit will cause silent bugs that only appear in real-world patterns.
