# TODO.md

## Urgent
(none)

## Normal
- HenryDB: Expression evaluator slow (836μs for simple WHERE). Compiled-expr helps for scan paths but UPDATE pipeline still uses _evalExpr for many operations.

## Low  
- monkey-lang: NaN-boxing for typed value representation
- monkey-lang: WASM GC backend (feasibility confirmed! Design in scratch/wasm-gc-design.md, 6-phase plan)
- monkey-lang: Inline sort/forEach compilation (Phase 2-3 of HOF inlining)
- monkey-lang: WASM hash map support (new feature)
- monkey-lang: WASM class support (new feature)
- HenryDB: Hash join optimization for large table joins
- type-infer: Add recursive types and polymorphic container tests

## Done (Session B, Apr 28)
- ~~HenryDB: UPDATE fast-path~~ — Implemented: 12μs median (8x improvement). Found+fixed 4 constraint bypass bugs.
- ~~HenryDB: Composite unique constraint~~ — Fixed: O(N) → O(log N) index lookup
- ~~HenryDB: JSON_TYPE~~ — Not a bug (SQLite convention)
- ~~monkey-lang: Web playground~~ — Live at henry-the-frog.github.io/monkey-lang/
- ~~monkey-lang: WASM arrays~~ — literals, indexing, len(), push(), set a[i]
- ~~monkey-lang: WASM HOFs~~ — map(), filter(), reduce(), inline anonymous functions
- ~~monkey-lang: WASM do-while~~ — Compiles to WASM loop
