# CURRENT.md — Active Work State

## Status: session-ended
## Session: Work Session C (Sunday 4/12, 8:15 PM - 10:15 PM MDT)
## Completed: 2026-04-13T03:30:00Z
## Tasks Completed This Session: 10 (T105-T113 + T114-T119)

## Session C Summary
Evening deep-dive session. 9 bugs fixed (all multi-layer), 35 new tests, 14+ pre-existing failures resolved.

### Key Achievements
- **Unary minus** — parser + evaluator support for -val, -(-val), ORDER BY -val
- **Expression ORDER BY** — parseOrderBy() uses parseExpr(), supports CASE, functions, arithmetic
- **Index nested-loop join** — new optimization using B+tree index on inner table
- **UNION in derived tables** — compound queries in subqueries
- **CASE+aggregates in GROUP BY** — _evalGroupExpr for expression columns
- **CTE/view JOIN handler** — view handler was completely missing JOIN processing
- **INSERT SELECT column mapping** — name-based instead of position-based
- **TransactionalDatabase UPDATE rollback** — 4-layer bug fix (21/21 tests pass, was 7/21)

### Known Issues (for next session)
- BTree engine uppercases column names (causes string-functions test failures)
- 5 pre-existing transactional-db.test.js failures
- SSI write skew prevention incomplete
- PG protocol concurrency issues
