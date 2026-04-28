# TASKS.md

## Active Projects

### monkey-lang (WASM Compiler)
- **Tests:** 265 WASM compiler tests, 1850/1854 total suite
- **Performance:** 30-34x VM, 8-11x JIT on computation-heavy benchmarks
- **Features:** Full AST, closures, classes (3-level inheritance), super calls, exceptions, floats, type inference, 9 string methods, 9 utility builtins
- **Known issues:** 
  - Sibling closures don't share mutable state (need box/cell pattern)
  - Self-referencing closures with multiple captures fail (env stores 0 for uninitialized self-ref)
  - Recursive closure + mutable state causes compiler hang
  - i32 overflow for large numbers (factorial(20), sum 100k)

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
- monkey-lang: Box/cell pattern for mutable captured variables (fixes 3 closure bugs)
- monkey-lang: NaN-boxing for typed value representation
- monkey-lang: Module resolution for import statements
- monkey-lang: WASM binary caching for recompilation avoidance
- HenryDB: Unified cost model across execution engines
- HenryDB: Window functions (ROW_NUMBER, RANK, etc.)
- HenryDB: UPDATE OF column syntax for triggers
