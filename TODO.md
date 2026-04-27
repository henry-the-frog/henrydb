# TODO.md

## Urgent
(none)

## Normal
- monkey-lang: Fix class method collision (needs runtime dispatch - hash lookup → builtin fallback)
- HenryDB: Unified cost model across all 4 execution engines
- HenryDB: json_tree() recursive table-valued function

## Low
- monkey-lang: Module resolution for import statements
- monkey-lang: Concurrency (goroutines/channels, major addition)
- monkey-lang: Array destructuring in match patterns
- monkey-lang: Fix arr[0]() syntax (index + call)
- monkey-lang: NaN-boxing for typed value representation (eliminates int/ptr confusion)
- HenryDB: UPDATE OF column syntax for triggers
- type-infer: Add recursive types and polymorphic container tests
- regex-engine: Fix empty string matching and anchor support
