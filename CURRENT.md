# CURRENT.md — Active Work State

## Status: session-ended
## Session: Work Session C (Sunday 4/12, 8:15 PM - 10:15 PM MDT)
## Completed: 2026-04-13T03:51:00Z
## Tasks Completed This Session: 17 (T105-T125)

## Session C Final Summary
Evening deep-dive on HenryDB quality. All fixes pushed to henrydb/main.

### Bugs Fixed: 13
1. Unary minus parser
2. CASE expression in ORDER BY
3. ORDER BY numeric column reference (qualified key issue)
4. CTE/view JOIN handler (completely missing)
5. INSERT SELECT column mapping (name-based)
6. UNION/INTERSECT/EXCEPT in derived tables
7. CASE+aggregates in GROUP BY projection
8. Post-aggregate arithmetic in parser (SUM(a)*100)
9. TransactionalDatabase UPDATE rollback (4-layer fix)
10. Column case normalization (SQL keywords as column names)
11. LIKE case-insensitive (SQLite-compatible)
12. EquiWidthHistogram class wrapper
13. Implicit type coercion (number vs string comparisons)

### New Feature: Index nested-loop join optimization

### Tests: 35 new + ~25 pre-existing fixed
### Full suite: ~6734/6761 = 99.6% (was ~99.4% pre-session)
### Commits: 13 pushed

### Known Issues (next session)
- Savepoints (not implemented)
- PG wire protocol prepared statements
- SSI write skew prevention
- Index lookup performance (slower than full scan for some queries)
- 5 pre-existing transactional-db.test.js failures
