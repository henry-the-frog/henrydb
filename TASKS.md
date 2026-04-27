# TASKS.md

## Active Projects

### monkey-lang (WASM Compiler)
- **Tests:** 265 WASM compiler tests, 1850/1854 total suite
- **Performance:** 30-34x VM, 8-11x JIT on computation-heavy benchmarks
- **Features:** Full AST, closures, classes (3-level inheritance), super calls, exceptions, floats, type inference, 9 string methods, 9 utility builtins
- **Known issues:** 
  - Nested closure captures (3+ levels) return 0 for outer variables
  - i32 overflow for large numbers (factorial(20), sum 100k)
  - filter/map/reduce builtins need host→WASM callback mechanism

### HenryDB
- **Tests:** 33 regression, 323/323 SQL compliance
- **JSON support:** Full JSON1 extension (16+ functions) + json_each/json_tree TVFs
- **Recent:** GLOB support, printf, total(), blob literals, zeroblob, unicode/char/hex/unhex/typeof/quote
- **Known issues:**
  - Some ESM module evaluation order quirks in testing

### neural-net
- **Status:** Very mature (170 source modules, gradient checkpointing, mixed precision)
- **No active work needed**

## Backlog
- monkey-lang: Fix nested closure captures (environment chain or box references)
- monkey-lang: NaN-boxing for typed value representation
- monkey-lang: Module resolution for import statements
- monkey-lang: WASM binary caching for recompilation avoidance
- monkey-lang: filter/map/reduce via host→WASM callback (__call1 export)
- HenryDB: Unified cost model across execution engines
- HenryDB: Window functions (ROW_NUMBER, RANK, etc.)
- HenryDB: UPDATE OF column syntax for triggers
