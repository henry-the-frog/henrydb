# CURRENT.md — Session Status

## Status: session-ended
## Session: 2026-04-22 Session A (8:15 AM - 2:15 PM MDT)
## Tasks Completed: ~290
## BUILD Count: 30+ (with depth resets)
## Projects: HenryDB

### Session Results
- **🚀 VOLCANO DEFAULT ON** — HenryDB's Volcano query engine is now the default
- **db.js: ~10K → 3,293 lines** (67% reduction, 8 extracted modules)
- **29 bugs found and fixed** in one session
- **97%+ broader test pass rate** with Volcano default-on
- **39/39 correctness**, 123/123 join, 32/36 TPC-H
- Added: EXISTS, NOT EXISTS, derived tables, NATURAL JOIN, USING, CAST, ANY/ALL, IN_HASHSET
- 16 SQL functions, GROUP BY alias/ordinal, ORDER BY alias/ordinal
- Integer division, implicit type coercion, column collision handling
- Safety: disabled for TransactionalDatabase and transactions
- EXPLAIN ANALYZE with per-operator timing

### Next Session Priorities
1. Fix remaining 3% failures (window functions in Volcano, more aggregate functions)
2. MVCC-aware SeqScan for Volcano (to work with TransactionalDatabase)
3. Plan cache for repeated queries
4. More db.js extraction (target: <2000 LOC)
