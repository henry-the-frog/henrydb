# CURRENT.md — Session Status

## Status: session-ended
## Session: 2026-04-22 Session A (8:15 AM - 2:15 PM MDT)
## Tasks Completed: ~295
## Projects: HenryDB

### 🏆 Session Results: 100% SQL Test Pass Rate
- **Volcano DEFAULT ON** — HenryDB's Volcano query engine is the default
- **417/417 SQL-relevant tests pass (100%)** with Volcano default-on
- **db.js: ~10K → 3,293 lines** (67% reduction, 8 extracted modules)
- **29 bugs found and fixed** in one session (AST mismatches, join logic, hash collision, pushdown, type coercion)
- 123/123 join, 39/39 correctness, 32/36 TPC-H
- EXISTS, NOT EXISTS, derived tables, NATURAL/USING JOIN, ANY/ALL, IN_HASHSET
- 16 SQL functions, GROUP BY/ORDER BY alias/ordinal, CAST, ILIKE
- Integer division, implicit type coercion, column collision, empty aggregates
- Safety guards: TransactionalDB, window functions, unsupported aggregates, FILTER, derived tables

### Next Session Priorities
1. Window function support in Volcano (currently falls back to legacy)
2. MVCC-aware SeqScan for Volcano (to work with TransactionalDatabase)
3. More aggregate functions (STDDEV, VARIANCE, ARRAY_AGG)
4. Plan cache for repeated queries
5. Further db.js extraction (target: <2000 LOC)
