# CURRENT.md — Session Status

## Status: session-ended
## Session: 2026-04-22 Session B-2 (5:45 PM - 8:15 PM MDT)
## Tasks Completed: 11 (T76-T86)
## BUILDs: 5 (T82, T83, T84, T85, trigger fix)

### Session Summary
Exceptional depth session. Found 7 edge case bugs via adversarial query generation,
then fixed ALL of them plus 15 pre-existing regression test failures.

**Bugs found and fixed:**
1. GROUP BY on derived table (FROM subquery) — missing aggregate handling
2. CTE referenced twice in CROSS JOIN — missing JOIN execution in view path
3. SUM(CASE...) [object Object] column names — AST arg stringification
4. GROUP BY + window function — new _applyWindowFunctionsToGrouped method
5. CTE UNION ALL + aggregate crash — materialize UNION CTEs via execute_ast
6. Parser: ROWS BETWEEN frame spec — added full frame clause parsing
7. Parser: CASE WHEN (subquery) > 1 — comparison after subquery in CASE WHEN

**Regression fixes (15→0):**
8. Division truncation (10.0/3 → 3.33)
9. CASE WHEN NULL → ELSE
10. SUM on empty set → NULL
11. LIMIT 0 → 0 rows
12. SELECT NULL IS NULL → 1
13. SELECT 1 > 2 → 0
14. SELECT TRUE/FALSE
15. NATURAL JOIN implementation
16. INSERT FROM CTE
17. Trigger NEW/OLD references
18. UNIQUE constraint enforcement (table + column level)

**Test results:** 4,188/4,193 pass (99.88%), 0 new regressions.
