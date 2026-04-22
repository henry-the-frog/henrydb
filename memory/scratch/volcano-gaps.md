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

## Bugs Found During Session B Stress Testing
1. **GROUP BY CASE expression** — Volcano produces wrong row counts when GROUP BY uses CASE WHEN expression. Legacy correct (X=17, Y=33), Volcano wrong (X=17, Y=17). Missing rows.
2. **Subquery in ORDER BY** — `ORDER BY (SELECT MAX(val) FROM t) - val` crashes. Unsupported in Volcano.
3. **Derived table with UNION** — `FROM (SELECT ... UNION ALL SELECT ...) sub` not supported (UNION in derived table subquery).
