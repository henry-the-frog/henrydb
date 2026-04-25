# TODO.md

## Urgent
- monkey-lang: git push needs `workflow` scope on GitHub token (since 2026-04-25)

## Normal
- HenryDB: Add JOIN queries to differential fuzzer
- HenryDB: Refactor parseSelectColumn to use parseExpr for general expressions
- monkey-lang: Wire constant propagation into compiler (quick win)
- monkey-lang: Add 50+ type checker tests (only 2 for real HM system)

## Low
- HenryDB: Add ATTACH DATABASE
- monkey-lang: Wire escape analysis for stack closures
- monkey-lang: Per-function SSA (enable register allocator)
- lambda-calculus: Explore — 43K lines, 469 tests, potentially under-explored
- regex-engine: Add tests (0 test cases currently)
