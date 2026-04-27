# TODO.md

## Urgent
- monkey-lang: WASM Y-combinator bug — closure parameter captured by inner closure returns heap pointer (131108) instead of actual value. Closure env pointer corrupted when passed through multiple HOF layers. (since 2026-04-27)

## Normal
- HenryDB: Unified cost model across all 4 execution engines

## Low
- monkey-lang: NaN-boxing for typed value representation  
- monkey-lang: Module resolution for import statements
- type-infer: Add recursive types and polymorphic container tests
- regex-engine: Fix empty string matching and anchor support
