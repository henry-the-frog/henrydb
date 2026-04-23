# Volcano Gaps — Session B Final (2026-04-22)

## Session B Results
- **Start:** 372/403 tests, many Volcano skip guards
- **End:** 985/989 tests (99.60%), Volcano as comprehensive query engine
- **Features added:** 27
- **Bugs fixed:** 10+
- **BUILDs:** 20 (session cap)

## All Features Added
1. CTE alias qualification fix
2. Function-wrapped aggregates (COALESCE(SUM(x),0))
3. Scalar correlated subqueries in SELECT
4. Derived tables (FROM subquery)
5. LATERAL JOIN fix
6. Aggregate FILTER clause
7. Correlated WHERE subqueries
8. SELECT without FROM
9. DISTINCT aggregates
10. CTE UNION support
11. Recursive CTE compatibility
12. STRING_AGG/ARRAY_AGG/GROUP_CONCAT
13. Expression-wrapped aggregates (100 * SUM(CASE...) / SUM(x))
14. Window functions (ROW_NUMBER, RANK, DENSE_RANK, SUM/COUNT/AVG/MIN/MAX OVER)
15. LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE, NTILE
16. Window frame specs (ROWS BETWEEN N PRECEDING AND M FOLLOWING)
17. Window expression args (SUM(a*b) OVER)
18. Window-in-expression (CASE WHEN ROW_NUMBER()... = 1)
19. Multi-OVER support (separate Window per unique OVER spec)
20. IFNULL/NVL/TYPEOF functions
21. JSON guard case-sensitivity fix
22. FTS MATCH_AGAINST guard
23. Wire protocol _slowQueryLog fix
24. containsAggregate false positive fix
25. Non-correlated scalar subquery detection
26. ORDER BY hidden column support
27. Star expansion in Project iterator

## Remaining Failures (4/989)
1. NOT NOT NOT (pre-existing parser issue)
2. SELECT * + window in planner (needs careful star case, regresses e2e queries)
3. E2E analytics rep performance (related to #2)
4. E2E analytics monthly trend (related to #2)

## Key Architectural Insights
1. **Volcano steal pattern**: Always add guards before implementing features
2. **Multi-OVER**: Chain separate Window operators per unique OVER spec
3. **Star expansion**: Safe in Project iterator, needs careful planner integration
4. **containsAggregate**: Must NOT recurse into scalar subqueries
5. **Non-correlated detection**: Check for outer table refs in WHERE before pre-evaluating
6. **NULL vs undefined**: Use `!== undefined` not `??` for LEFT JOIN null preservation

## Bugs Found During Session B Stress Testing — STATUS: ALL RESOLVED (2026-04-23)
1. **GROUP BY CASE expression** — ✅ FIXED (verified 2026-04-23, likely from HAVING rewrite)
2. **Subquery in ORDER BY** — ✅ No longer crashes (pre-existing limitation in both engines)
3. **Derived table with UNION** — ✅ FIXED (works in both engines)

### GROUP BY CASE Expression Bug — Details
- Simple GROUP BY works (grp: a=2, b=2, c=2)
- CASE values are computed correctly per row (X for 'a', Y otherwise)
- HashAggregate GROUP BY with CASE expression loses 2 of 6 rows
- Only 4 rows appear in output (2 X + 2 Y) instead of 6 (2 X + 4 Y)
- Root cause: likely in GROUP BY key computation — the CASE evaluator might return undefined for some rows instead of 'Y'
- The GROUP BY expression is parsed correctly (type: case_expr with WHEN conditions)
- Investigation needed: add logging to HashAggregate's key computation for CASE expressions

### Remaining 3 Decorrelation Stress Test Failures (2026-04-23)
1. **Correlated IN with aggregate + nested subquery** — Double-nested correlation (outer → inner → innermost). Batch decorrelation only handles single-level.
2. **Correlated vs uncorrelated parity** — The uncorrelated version works, correlated returns empty. May be the same root cause as #1.
3. **NULL handling** — Correlated IN with NULL values in the correlation column. NULL != NULL so the hashmap key lookup fails. Needs NULL-safe comparison in the hashmap.

**Root causes:** All three require either (a) recursive batch decorrelation for multi-level nesting, or (b) NULL-safe hashmap keys. These are significant enhancements to the decorrelation optimizer.

**Priority:** Medium. The basic correlated IN (single-level, non-NULL) now works correctly. These are edge cases.
