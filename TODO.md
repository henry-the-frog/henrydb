# TODO.md

## Urgent
(none)

## Normal
- monkey-lang: Per-function SSA → dead code elimination (SSA-level annotation, ~200 LOC)
- monkey-lang: VM callback mechanism for native HOF builtins (prelude is 19x slower)
- HenryDB: Fix COUNT(DISTINCT) bug in multi-join scenarios
- HenryDB: Wire PL/SQL to procedure handler (854 LOC exists, just needs connection)
- HenryDB: Close remaining ~1.4% fuzzer gap (mixed type comparisons + UNION type affinity)
- monkey-lang: Class syntax (design ready, ~200 LOC implementation)
- monkey-lang: WASM compiler Phase 1 (int+fn, ~500 LOC)
- neural-net: Deep exploration of 38K LOC ML framework

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
