# CURRENT.md — Active Work State

## Status: session-ended
## Session: Work Session C (Sunday 4/12, 8:15 PM - 10:15 PM MDT)
## Completed: 2026-04-13T03:42:00Z
## Tasks Completed This Session: 15 (T105-T124)

## Session C Final Summary
Evening deep-dive session focused on HenryDB bug fixing and quality improvement.

### Stats
- **12 bugs found and fixed** (most multi-layer)
- **35 new tests** across 6 test files
- **~25+ pre-existing failures fixed** across the test suite
- **12 git commits** pushed to henrydb/main
- **Test suite pass rate**: ~99.37% → ~99.7%+

### Key Achievements
1. **Unary minus** in parser + evaluator
2. **Expression ORDER BY** (CASE, functions, arithmetic)
3. **Index nested-loop join** optimization
4. **UNION in derived table** subqueries
5. **CASE+aggregates in GROUP BY** projection
6. **CTE/view JOIN handler** (was completely missing)
7. **INSERT SELECT column mapping** (name-based)
8. **TransactionalDatabase UPDATE rollback** — 4-layer bug (21/21 tests)
9. **Column case normalization** for keyword-named columns
10. **LIKE case-insensitive** (SQLite-compatible)
11. **EquiWidthHistogram class wrapper**

### Known Issues (for next session)
- 5 pre-existing transactional-db.test.js failures (MVCC deep issues)
- SSI write skew prevention incomplete
- PG protocol concurrency issues
- Histogram estimation accuracy for high-frequency values
- ~10 remaining test failures in batch 7 (mostly transaction/perf)
