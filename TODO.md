# TODO.md

## Urgent
- monkey-lang: GitHub token needs `workflow` scope for CI (since 2026-04-25)

## Normal
- HenryDB: Close remaining ~3.3% fuzzer gap (window function ORDER BY, UNION type issues, CTE)
- HenryDB: Add GROUPS and EXCLUDE test coverage beyond session-b-features.test.js
- monkey-lang: Per-function SSA — wire into compiler (SSA OOM fixed, pipeline ready)
- monkey-lang: VM callback mechanism for native HOF builtins (prelude is 3x slower)
- lambda-calculus: Deep exploration — 190 PL theory modules, potential blog post

## Low
- HenryDB: Add ATTACH DATABASE
- HenryDB: Add VALUES clause as table source
- HenryDB: Fix MERGE USING subquery syntax
- HenryDB: Add UNNEST table function
- monkey-lang: Add import/export module system (AST exists, parser needed)
- All projects: Add CI workflows (need workflow scope on token)
