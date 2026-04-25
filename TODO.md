# TODO.md

## Urgent
- monkey-lang: GitHub token needs `workflow` scope for CI (since 2026-04-25)

## Normal
- HenryDB: Close remaining 2.8% fuzzer gap (type coercion edge cases in WHERE)
- HenryDB: Fix parameter binding ($1, $2 params not injected into queries)
- HenryDB: Add GROUPS and EXCLUDE to window frame parser
- HenryDB: Add MEDIAN and PERCENTILE_CONT aggregates
- monkey-lang: Fix VM closure mutation (evaluator returns 3, VM returns 0 for counter pattern)
- monkey-lang: Wire escape analysis for stack-allocated closures
- monkey-lang: Per-function SSA (enable register allocator for real interference)
- monkey-lang: Add startsWith, endsWith, padStart, toCharArray, bool built-ins
- lambda-calculus: Deep exploration — 190 PL theory modules, potential blog post

## Low
- HenryDB: Add ATTACH DATABASE
- HenryDB: Add VALUES clause as table source
- HenryDB: Fix MERGE USING subquery syntax
- HenryDB: Add UNNEST table function
- monkey-lang: Add comment syntax (//)
- monkey-lang: Add import/export module system (AST exists, parser needed)
- All projects: Add CI workflows (need workflow scope on token)

## Completed (Session A, Apr 25)
**210+ tasks**, 15 bugs fixed, 2 fuzzers, 1053/1053 monkey-lang (100%),
97.2% HenryDB fuzzer, 112 SQL constructs verified, 40+ feature categories,
559K lines, 22K tests, 320+ commits.
