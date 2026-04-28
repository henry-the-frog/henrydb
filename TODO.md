# TODO.md

## Urgent
(none)

## Normal
- HenryDB: OOM at 5K rows with 3 indexes (memory efficiency issue)
- HenryDB: Redundant heap scan in dml-insert.js (found in profiling, lines 168-175 — O(N) linear scan before insertRow which also validates)

## Low
- monkey-lang: Lazy runtime function emission (~35ms compile time savings, 72→N functions)
- monkey-lang: NaN-boxing for typed value representation  
- monkey-lang: WASM GC backend (structs/arrays verified in Node.js v22, design in scratch/wasm-gc-backend-design.md)
- monkey-lang: Inline reduce/sort/forEach compilation (Phase 2-3 of HOF inlining)
- type-infer: Add recursive types and polymorphic container tests
