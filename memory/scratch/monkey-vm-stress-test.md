# Monkey-lang VM Stress Test (2026-04-25)

## Hypothesis Results
1. ❌ Deeply nested closures (4 levels) — WORKS correctly (returns 10 for adder(1)(2)(3)(4))
2. N/A Pattern matching — language uses standard if/else, not pattern matching
3. N/A GC circular references — not tested (would need RISC-V backend which doesn't exist in codebase)

## What Works
- Deeply nested closures (3+ levels) ✅
- Recursive fibonacci ✅ (fib(10) = 55)
- Deep recursion (sum(100) = 5050) ✅
- Higher-order functions (map) ✅
- Nested hash/array access ✅
- All basic operations ✅
- Evaluator/VM parity: 9/9 tests match ✅

## What Doesn't Work
- Mutable closure state: `count = count + 1` in closure — parser error "no prefix parse function for ="
  - This is by design: Monkey language doesn't have reassignment, only `let` bindings
  - Would need `set` or `:=` syntax to support mutation

## README Discrepancy
- README claims "WASM Backend" and "RISC-V Backend" but neither exists in the codebase
- Only two backends: tree-walking evaluator and bytecode VM
- Should update README to be accurate

## Observations
- The VM is well-implemented — 894 tests all passing
- Escape analysis exists but results unused (documented in separate scratch note)
- The type system (Hindley-Milner) is a real implementation, not a toy
