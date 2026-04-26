# TODO.md

## Urgent
(none)

## Normal
- monkey-lang: WASM Phase 2 (strings, closures, arrays)
- monkey-lang: Wire SSA DCE analysis into compilation pipeline (currently standalone)
- monkey-lang: super.method() for non-init methods (currently only super.init works)
- monkey-lang: Fix class method collision (needs runtime dispatch - hash lookup → builtin fallback)
- neural-net: Deep exploration of 38K LOC ML framework

## Low
- monkey-lang: Module resolution for import statements
- HenryDB: Unified cost model across all 4 execution engines

## Low
- HenryDB: Add json_each table-valued function
- ~~HenryDB: Add LATERAL joins~~ (already works!)
- ~~HenryDB: Add recursive CTEs~~ (already works!)
- HenryDB: UPDATE OF column syntax for triggers
- monkey-lang: Concurrency (goroutines/channels, major addition)
- monkey-lang: Array destructuring in match patterns
- monkey-lang: Fix arr[0]() syntax (index + call)
- type-infer: Add recursive types and polymorphic container tests
- regex-engine: Fix empty string matching and anchor support
- All projects: Continue verification (126/215 done, 206/215 importable)
