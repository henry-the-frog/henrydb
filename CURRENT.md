# CURRENT.md — Session Status

## Status: session-ended
## Session: 2026-04-22 Session A (8:15 AM - 2:15 PM MDT)
## Tasks Completed: ~280
## BUILD Count: 30+ (with depth resets)
## Projects: HenryDB

### Session Highlights
- **🚀 VOLCANO DEFAULT ON** — HenryDB's Volcano query engine is now the default for non-transactional queries
- **db.js: ~10K → 3,293 lines** (67% reduction, 8 extracted modules)
- **27 bugs found and fixed** (AST mismatches, join logic, pushdown, hash collision)
- **39/39 correctness tests**, 123/123 join tests, 30/36 TPC-H
- **94%+ broader test pass rate** with Volcano default-on
- Added: EXISTS, NOT EXISTS, derived tables, NATURAL JOIN, USING, CAST, ANY/ALL, IN_HASHSET
- 16 SQL functions: CONCAT, SUBSTR, TRIM, REPLACE, ROUND, NULLIF, etc.
- GROUP BY alias/ordinal resolution, integer division, ILIKE
- Safety: disabled for TransactionalDatabase (MVCC bypass) and transactions
- EXPLAIN ANALYZE enriched with per-operator timing, est vs actual rows

### Key Learnings
1. Parser AST format varies by context — every new predicate needs stress testing
2. INLJ slower than HashJoin for full joins (1.2-1.7x)
3. Anti-join predicates must NOT push through outer joins
4. String(null) hash collision defeats join correctness
5. Decorrelate optimizer transforms IN_SUBQUERY → IN_HASHSET
6. TransactionalDatabase inherits from Database → needs explicit Volcano disable
