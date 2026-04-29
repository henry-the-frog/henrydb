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

## Done (Session C, Apr 28)
- ~~monkey-lang: NaN-boxing~~ → Implemented as integer unboxing (raw JS numbers on VM stack, 1.76x speedup)
- ~~monkey-lang WASM: Hash map auto-resize~~ → 75% load factor, tested with 50 entries
- ~~monkey-lang WASM: Hash map playground example~~ → hashmap-playground.monkey
- ~~monkey-lang: Mutable closures in hash literals~~ → Fixed compiler AST walker (Map vs Object.keys)
- ~~monkey-lang: || NULL bugs~~ → All converted to ?? NULL across vm.js + evaluator.js
- ~~monkey-lang: String concat interning overhead~~ → Skip interning for concat results (4.1x faster)
- ~~monkey-lang: Superinstructions~~ → Infrastructure added, V8 JIT negates benefit (<0.5%)
