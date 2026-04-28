# TODO.md

## Urgent
(none)

## Normal
- HenryDB: UPDATE fast-path for single-row PK operations (skip constraint validation/FK/WAL for simple cases). Manual cycle: 13μs, current: 985μs.
- HenryDB: Expression evaluator slow (836μs for simple WHERE). Compiled-expr helps for scan paths but UPDATE pipeline still uses _evalExpr for many operations.
- ~~HenryDB: JSON_TYPE~~ — Not a bug (SQLite convention)
- ~~HenryDB: Composite unique constraint~~ — Fixed in Session B (O(N) → O(log N) index lookup)

## Low  
- monkey-lang: NaN-boxing for typed value representation
- monkey-lang: WASM GC backend (feasibility confirmed! Design in scratch/wasm-gc-design.md, 6-phase plan)
- monkey-lang: Inline sort/forEach compilation (Phase 2-3 of HOF inlining)
- HenryDB: Hash join optimization for large table joins
- type-infer: Add recursive types and polymorphic container tests
