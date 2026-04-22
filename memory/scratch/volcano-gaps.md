# Volcano Path: What's Missing for Primary Executor (2026-04-22)

## Current Status: Backup Executor
The Volcano path is used for EXPLAIN ANALYZE instrumentation. The legacy path handles actual query execution. If buildPlan fails, EXPLAIN ANALYZE falls back silently.

## What Works in Volcano
- ✅ Simple scans (SeqScan, IndexScan)  
- ✅ Filters (WHERE with simple predicates)
- ✅ Hash joins (equi-join) + Nested loop joins
- ✅ Hash aggregates (GROUP BY + SUM/COUNT/AVG/MIN/MAX)
- ✅ HAVING
- ✅ Projection
- ✅ Sort (ORDER BY)
- ✅ Limit/Offset
- ✅ Distinct
- ✅ CTEs (materialized)
- ✅ UNION (basic set operations)
- ✅ Window functions (ROW_NUMBER, etc.)

## Known Bugs (Volcano produces wrong results)
1. **IN list** — `WHERE id IN (1,2,3)` returns ALL rows. buildPredicate doesn't handle IN_LIST AST type.
   - Fix: convert IN_LIST to series of OR comparisons in buildPredicate
   - Priority: HIGH — this is a correctness bug

## Known Gaps (Volcano can't handle these)
1. **Correlated subqueries** — `WHERE x > (SELECT MAX(y) FROM ...)` 
2. **NOT IN subquery** — `WHERE id NOT IN (SELECT id FROM ...)`
3. **EXISTS subquery** — `WHERE EXISTS (SELECT ...)`
4. **Scalar subquery in SELECT** — `SELECT (SELECT COUNT(*) FROM ...)`
5. **LATERAL joins** — outer row context needed in inner scan
6. **Complex expressions** — CASE WHEN, COALESCE, etc. in WHERE may not build predicate correctly

## Path to Primary Executor
1. Fix IN_LIST handling in buildPredicate
2. Add expression evaluation for complex predicates (CASE, COALESCE, BETWEEN, LIKE)
3. Add SubqueryScan iterator for correlated subqueries
4. Add MaterializeScan for uncorrelated subqueries (run once, cache result)
5. Test every SQL pattern against both paths and compare results
