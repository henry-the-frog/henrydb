# TODO.md

## Urgent
(none)

## Normal
- monkey-lang: super.method() for non-init methods (currently only super.init works)
- monkey-lang: Fix class method collision (needs runtime dispatch - hash lookup → builtin fallback)
- monkey-lang: WASM return type inference for closures (captures don't propagate knownInt yet)
- monkey-lang: WASM remove env_ptr for non-capturing functions (currently always passed)
- HenryDB: Unified cost model across all 4 execution engines

## Low
- monkey-lang: Module resolution for import statements
- monkey-lang: Concurrency (goroutines/channels, major addition)
- monkey-lang: Array destructuring in match patterns
- monkey-lang: Fix arr[0]() syntax (index + call)
- monkey-lang: NaN-boxing for typed value representation (eliminates int/ptr confusion)
- HenryDB: Add json_each table-valued function
- HenryDB: UPDATE OF column syntax for triggers
- type-infer: Add recursive types and polymorphic container tests
- regex-engine: Fix empty string matching and anchor support
