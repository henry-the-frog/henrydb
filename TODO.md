# TODO.md

## Urgent
- monkey-lang: git push needs `workflow` scope on GitHub token (since 2026-04-25)

## Normal
- HenryDB: Fix INSERT column count validation (fuzzer-found bug)
- HenryDB: Fix ORDER BY non-existent column validation (fuzzer-found bug)
- HenryDB: Refactor parseSelectColumn to use parseExpr for general expressions
- HenryDB: Add JOIN queries to differential fuzzer
- monkey-lang: Wire constant propagation into compiler (quick win)
- monkey-lang: Add 50+ type checker tests (only 2 for real HM system)

## Low
- HenryDB: Add ATTACH DATABASE
- monkey-lang: Wire escape analysis for stack closures
- monkey-lang: Per-function SSA (enable register allocator)
- lambda-calculus: Explore — 43K lines, 469 tests, potentially under-explored
- regex-engine: Add tests (0 test cases currently)
