# HenryDB Architecture Review (Session B, Apr 20 2026)

## Overview
- **Codebase**: 94K lines, 172 modules, 151 SQL functions
- **Tests**: 4204/4208 pass (99.9%), 360+ window fn tests, full TPC-H schema coverage
- **Commits**: 3063+

## Module Quality Assessment

### db.js (6.2K lines) — The Monolith
**Grade: C+ (correct for most cases, but fragile)**
- Contains parser invocation, DDL, DML, SELECT, expression evaluation, JOIN execution, window functions, aggregation, triggers, constraints, views, CTEs, and more
- 11 bugs found in this file alone
- Main issues:
  - Division always truncates (expression evaluator)
  - _evalExpr default returns true (affects CASE WHEN)
  - SUM(empty) = 0, LIMIT 0 returns all
  - JOIN always nested loop (no hash join integration)
  - Index after rollback corrupted
  - VIEW-TABLE JOIN drops table columns
  - INSERT FROM CTE no-op
  - Trigger NEW/OLD returns NULL
  - UNIQUE not enforced (table-level)
  - NATURAL JOIN acts as cross join

### sql.js (~2.5K lines) — The Parser
**Grade: B- (impressive surface area, but dual parsing path is a time bomb)**
- Handles 100+ SQL statement types
- parseSelectColumn vs parseExpr divergence: 6 bugs
- Recursive CTE arithmetic parsing fails
- MERGE subquery USING not supported
- OVERLAY syntax fails
- TEMP TABLE not recognized

### planner.js (971 lines) — Dead Code with Potential
**Grade: A for design, F for integration**
- DP join reorder, histogram selectivity, MCV, 3 join strategies
- None of it matters because db.js ignores the planner's join decisions
- Single-table index selection DOES work through the planner

### vectorized.js — Parallel Universe
**Grade: B (works, modest speedup, not integrated)**
- 1.6-1.8x speedup on 10K rows for filter and GROUP BY
- HashJoin has API bug (buildColumns not iterable)
- Not used by any SQL query — must be called programmatically

### transactional-db.js — Solid Foundation
**Grade: A- (correctness, recovery, isolation)**
- MVCC with proper snapshot isolation
- WAL + ARIES recovery verified
- Write conflict detection works
- Index-after-rollback is the main gap
- Write skew (SSI) not enforced

## Feature Coverage (vs SQLite)

### Better than SQLite
✅ MERGE statement, ROLLUP/CUBE/GROUPING SETS, FILTER on aggregates
✅ SSI-based MVCC (SQLite uses journal locking)
✅ Window functions (full coverage)
✅ User-defined functions (PostgreSQL syntax)
✅ Vectorized execution engine (exists, even if not integrated)
✅ Full-text search, LATERAL JOIN, UPDATE FROM, RETURNING

### Comparable to SQLite
≈ JOINs (syntax), CTEs (basic + recursive), subqueries
≈ String functions (25/26), JSON functions (5/10)
≈ Constraints (CHECK, NOT NULL, FK with CASCADE)
≈ Indexes, EXPLAIN ANALYZE, prepared statements

### Worse than SQLite
❌ Division correctness (10.0/3 = 3)
❌ CASE WHEN truthiness (always true for unhandled types)
❌ Join performance (100-1000x slower, no hash join)
❌ NULL handling in SELECT (IS NULL broken)
❌ Boolean expressions in SELECT (entire category broken)
❌ UNIQUE constraint enforcement
❌ Triggers (NEW/OLD broken)
❌ No TEMP tables

## Structural Issues

### 1. Feature Theater (Critical Pattern)
Multiple subsystems exist in isolation but aren't connected:
- **Planner → Executor**: Cost model is theater for joins
- **Vectorized → SQL**: Columnar engine is programmatic only
- **Parser → Executor**: parseSelectColumn/parseExpr diverge

This pattern suggests breadth-first development without integration testing.

### 2. db.js Monolith
6.2K lines in one file. This makes bugs invisible because related code paths are hundreds of lines apart. The disconnect between planner output and executor consumption went unnoticed because they're in different files with no integration point.

### 3. Expression Evaluation Fragility
_evalExpr's default case returns true. Any new expression type that isn't explicitly handled silently passes all boolean checks. This is a correctness time bomb.

## Recommendations (Priority Order)

1. **Fix correctness bugs** (1 day): Division, CASE WHEN, SUM empty, LIMIT 0
2. **Wire hash join** (0.5 day): ~30 lines in _executeJoin, 100-1000x speedup
3. **Fix index rollback** (1 day): B-tree must be restored on transaction rollback
4. **Unify expression parsing** (2-3 days): parseSelectColumn delegates to parseExpr
5. **Split db.js** (1 week): Extract expression eval, join execution, DDL into separate modules
6. **Integration tests**: End-to-end queries that prove planner output drives execution
