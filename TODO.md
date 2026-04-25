# TODO.md

## Urgent
- monkey-lang: GitHub token needs `workflow` scope for CI (since 2026-04-25)

## Normal
- HenryDB: Close remaining ~2.4% fuzzer gap (long-tail type edge cases)
- monkey-lang: Per-function SSA → dead code elimination (analysis wired, optimization next)
- monkey-lang: VM callback mechanism for native HOF builtins (prelude is 3x slower)
- lambda-calculus/PL theory: Deep exploration of 215-project collection

## Low
- HenryDB: Add LATERAL joins
- HenryDB: Add recursive CTEs
- HenryDB: Improve GENERATE_SERIES step parameter handling
- monkey-lang: Add import statement resolution (filesystem, not just STDLIB)
- monkey-lang: Add type annotations (:: syntax to avoid : hash conflict)
- type-infer: Add recursive types and polymorphic container tests
- regex-engine: Fix empty string matching and anchor support
- All projects: Systematic test addition across 215 projects
