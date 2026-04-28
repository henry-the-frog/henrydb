# TODO.md

## Urgent
(none)

## Normal
- HenryDB: Expression evaluator is slow (836μs for simple WHERE id=2500). Needs compiled/JIT expression evaluation.
- HenryDB: Composite unique constraint check still does heap scan (dml-insert.js ~line 188)

## Low
- monkey-lang: NaN-boxing for typed value representation  
- monkey-lang: WASM GC backend (structs/arrays verified in Node.js v22, design in scratch/wasm-gc-backend-design.md)
- monkey-lang: Inline sort/forEach compilation (Phase 2-3 of HOF inlining)
- type-infer: Add recursive types and polymorphic container tests
