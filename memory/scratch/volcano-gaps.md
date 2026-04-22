# Volcano Path: Status & Gaps (2026-04-22, final update)

## Current Status: Comprehensive EXPLAIN ANALYZE Executor
The Volcano path is used for EXPLAIN ANALYZE instrumentation. 28+ SQL patterns verified correct.
Legacy path is ~100x faster for small data — Volcano overhead from iterator object creation.

## What Works ✅ (verified against legacy path)
- Basic predicates: =, !=, <, <=, >, >= (with NULL safety)
- Boolean operators: AND, OR, NOT
- LIKE (with % and _ patterns)
- BETWEEN
- IS NULL, IS NOT NULL
- IN (literal list)
- IN (subquery — uncorrelated, eager execution)
- NOT IN (both literal and subquery)
- CASE WHEN (in both WHERE and SELECT)
- Arithmetic expressions in WHERE (a + b > 15)
- Hash joins (equi-join)
- Nested loop joins (non-equi)
- LEFT JOIN, RIGHT JOIN, FULL JOIN
- Hash aggregates (GROUP BY + SUM/COUNT/AVG/MIN/MAX)
- HAVING (with aggregate predicates)
- Projection (column selection + aliases)
- Sort (ORDER BY)
- Limit/Offset
- Distinct
- CTEs (materialized, including CTE-referencing-CTE)
- UNION ALL
- Window functions (ROW_NUMBER, etc.)
- IndexScan with I/O cost-based decisions
- Estimated rows with ANALYZE stats (ndistinct, histogram)

## Known Gaps ❌
1. **Correlated subqueries** — EXISTS, correlated WHERE conditions. Need per-row execution.
2. **Scalar subquery in SELECT** — `SELECT (SELECT COUNT(*) FROM ...)` returns null
3. **Function evaluation** — COALESCE, ABS, UPPER, LENGTH return null in Volcano (not wired to db._evalExpr)
4. **String concatenation** — `a || b` not handled
5. **LATERAL joins** — not supported

## Bugs Fixed Today (15 total)
1. tpch-compiled: compile threshold vs test scale
2. Histogram selectivity: object vs number comparison
3. CTE AST: type='SELECT' vs type='WITH'
4. HAVING aggregate arg: object vs string
5-8. Aggregate arg normalization (4 functions in volcano-planner)
9. explain-executor: bare 'this' references (4 instances) + UNION dispatch
10. IN_LIST: type mismatch (IN_LIST vs IN)
11. LIKE pattern: AST node vs string
12. BETWEEN: left vs expr field name
13. IS NULL: added left fallback
14. CASE elseResult vs else
15. IN_SUBQUERY: not handled (returned all rows)
16. NOT handler: missing ctx for IN_SUBQUERY
17. arith: type mismatch (arith vs binary_expr)
18. NULL comparison: JS null coercion vs SQL three-valued logic

## Performance
- SeqScan: ~2μs per row
- HashJoin: ~1.4μs per row pair
- Legacy path: ~100x faster for small data (<1000 rows)
- Volcano advantage: better query plans, EXPLAIN ANALYZE instrumentation
- Volcano overhead: iterator object creation (~0.8ms fixed cost)

## Path to Primary Executor
1. ❌ NOT READY — Legacy is much faster for small data
2. Need batch-at-a-time processing to reduce iterator overhead
3. Function evaluation needs wiring to db._evalExpr
4. Correlated subquery support (SemiJoin operator)
