# TODO.md

## Urgent
- monkey-lang: WasmCompiler OOMs at 2GB+ for standalone compilation (since 2026-04-27)
  - Blocks: binary caching, CLI, WASM playground
  - Likely cause: massive import chain creates huge object graphs

## Normal
- monkey-lang: Fix nested closure captures (3+ levels return 0 for outer vars)
- HenryDB: Unified cost model across all 4 execution engines

## Low
- monkey-lang: NaN-boxing for typed value representation  
- monkey-lang: Module resolution for import statements
- monkey-lang: filter/map/reduce builtins (needs __call1 export mechanism)
- monkey-lang: WASM binary caching
- HenryDB: Window functions (ROW_NUMBER, RANK, etc.)
- HenryDB: UPDATE OF column syntax for triggers
- type-infer: Add recursive types and polymorphic container tests
- regex-engine: Fix empty string matching and anchor support
