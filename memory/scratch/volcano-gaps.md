# Volcano Gaps — Priority List (as of 2026-04-22)

## Status: 39/39 correctness, 56/57 stress test, 23 bugs fixed

## Working (✅)
- SeqScan, Filter, Project, Limit (with offset), Distinct
- HashJoin, IndexNestedLoopJoin, NestedLoopJoin
- HashAggregate (all funcs: SUM, COUNT, AVG, MIN, MAX, COUNT DISTINCT, GROUP_CONCAT)
- INNER, LEFT, RIGHT, FULL OUTER, CROSS, self-joins
- 3-way and 4-way joins
- CTEs (materialize and scan)
- Derived tables (subquery in JOIN)
- EXISTS / NOT EXISTS (correlated)
- IN / NOT IN subqueries
- Window functions (ROW_NUMBER)
- All predicates: =, <, >, <=, >=, !=, LIKE, BETWEEN, IN, IS NULL, IS NOT NULL, CASE, NOT
- Expressions: arith, string concat, CAST, function calls (COALESCE, ABS, UPPER, etc.)
- GROUP BY with expressions (CASE)
- EXPLAIN with Volcano plan tree
- EXPLAIN ANALYZE with per-operator timing

## Not Working (❌) — Priority Order
1. **HAVING with subquery** — buildAggregatePredicate doesn't handle subquery. High impact (analytics).
2. **ANY/ALL** — not implemented in buildPredicate. Medium impact.
3. **DISTINCT ON** — PostgreSQL extension. Low priority.
4. **Derived table in FROM** — only works in JOIN, not as the main FROM source.
5. **Plan cache** — re-parsing every query. Would 2-3x improve repeat query perf.
6. **Cost-based INLJ selection** — INLJ slower than HashJoin for full joins. Need selectivity check.
7. **Query compilation** — JIT-compile hot queries for 10-100x speedup.

## Next Session Priorities
1. Fix HAVING subquery (easy: evaluate subquery in buildAggregatePredicate)
2. Add ANY/ALL support (evaluate set comparison)
3. Wire Volcano into main execute() path (currently dual-path)
4. Consider plan cache for prepared statements
