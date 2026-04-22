# CURRENT.md — Session Status

## Status: session-ended
## Session: 2026-04-22 Session A (8:15 AM - 2:00 PM MDT)
## Tasks Completed: ~300
## Projects: HenryDB

### 🏆 Session A: Volcano Becomes Default
- **465/465 SQL tests pass (100%)** with Volcano as default query engine
- **353/367 broader tests pass (96.2%)** including wire protocol, window functions
- **29 bugs found and fixed** in one session
- **db.js: ~10K → 3,293 lines** (67% reduction, 8 extracted modules)

### What Changed
The Volcano iterator-model query engine went from an experimental side-path to the DEFAULT query engine for HenryDB in a single 6-hour session:
- 29 bugs (AST mismatches, join logic, hash collision, pushdown rules, type coercion)
- EXISTS, NOT EXISTS, derived tables, NATURAL/USING JOIN, ANY/ALL, IN_HASHSET
- 16 SQL functions, GROUP BY/ORDER BY alias/ordinal, CAST, ILIKE
- Integer division, implicit type coercion, column collision, empty aggregates  
- Safety guards: TransactionalDB, window functions, unsupported aggregates, recursive CTEs
