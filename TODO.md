# TODO.md

## Urgent
(none)

## Normal
- HenryDB: Composite unique constraint check still does heap scan (dml-insert.js ~line 188) — use composite index lookup
- HenryDB: Add tests for ? placeholder support and executeMany batch inserts
- monkey-lang: Add pipe operator tests to WASM compiler test suite (desugared by parser, but no explicit tests)

## Low
- monkey-lang: NaN-boxing for typed value representation  
- monkey-lang: WASM GC backend (structs/arrays verified in Node.js v22, design in scratch/wasm-gc-backend-design.md)
- monkey-lang: Inline reduce/sort/forEach compilation (Phase 2-3 of HOF inlining)
- type-infer: Add recursive types and polymorphic container tests
