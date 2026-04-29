# TODO.md

## Urgent
(none)

## Normal
- HenryDB: Expression evaluator slow (836μs for simple WHERE). Compiled-expr helps for scan paths but UPDATE pipeline still uses _evalExpr for many operations.
- monkey-lang WASM: String key support in hash maps (FNV-1a hash added, needs find_slot_str + set/get integration)

## Low  
- monkey-lang: WASM GC backend (feasibility confirmed! Design in scratch/wasm-gc-design.md, 6-phase plan)
- monkey-lang: Inline sort/forEach compilation (Phase 2-3 of HOF inlining)
- monkey-lang: WASM class support (new feature)
- monkey-lang: Prelude bytecode caching (avoid 6ms recompile; needs immutable AST or bytecode snapshot)
- monkey-lang: Module resolution for import statements
- HenryDB: Hash join optimization for large table joins
- type-infer: Add recursive types and polymorphic container tests


