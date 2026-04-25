# TODO.md

## Urgent
- monkey-lang: GitHub token needs `workflow` scope for CI (since 2026-04-25)

## Normal
- HenryDB: Close remaining ~3.5% fuzzer gap (window function ORDER BY, UNION type issues)
- HenryDB: Fix db.execute(sql, params) convenience method (PREPARE/EXECUTE works, direct doesn't)
- HenryDB: Fix EXECUTE with too few params silently returns empty (should error)
- HenryDB: Add GROUPS and EXCLUDE to window frame parser
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

## Completed (Session B, Apr 25)
- ~~VM closure mutation~~ — was already fixed by Session A
- ~~Type affinity on INSERT~~ — implemented, fuzzer improved to 96.5%
- ~~Escape analysis wired into compiler~~ — end-to-end pipeline working
- ~~MEDIAN and PERCENTILE_CONT/DISC aggregates~~ — added to all paths
- ~~Wire escape analysis for stack-allocated closures~~ — done
