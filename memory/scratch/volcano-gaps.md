# Volcano Gaps — Final Session B Update (2026-04-22)

## Session B Results
- **Start:** 372/403 broader tests, many Volcano skip guards
- **End:** 979/989 comprehensive tests (98.99%), massive Volcano expansion
- **Features added:** 15+ Volcano features in one session

## All Features Added This Session

### Bug Fixes
1. **CTE alias qualification**: Strip qualifiers from materialized CTE columns (c.region → region)
2. **LATERAL JOIN fix**: Skip Volcano for correlated context (_outerRow)
3. **JSON guard case-sensitivity**: JSON_EXTRACT → json_extract comparison fix
4. **FTS MATCH_AGAINST guard**: Added missing guard for fulltext predicates
5. **Wire protocol _slowQueryLog**: Undeclared variable fix
6. **containsAggregate false positive**: Skip scalar_subquery nodes
7. **Non-correlated scalar subquery detection**: Check for outer refs before pre-evaluating
8. **ctx propagation**: Pass context through buildValueGetter for nested function subqueries

### New Features
9. **Function-wrapped aggregates**: COALESCE(SUM(x),0), NULLIF(AVG(x),0), ROUND(AVG(x),2)
10. **Scalar correlated subqueries in SELECT**: (SELECT SUM(x) WHERE correlated)
11. **Derived tables (FROM subquery)**: SELECT ... FROM (SELECT ...) sub
12. **Aggregate FILTER clause**: COUNT(*) FILTER (WHERE status = 'shipped')
13. **Correlated WHERE subqueries**: WHERE salary > (SELECT AVG(...) WHERE correlated)
14. **SELECT without FROM**: SELECT 1+1, SELECT NOW()
15. **DISTINCT aggregates**: COUNT(DISTINCT x), SUM(DISTINCT x)
16. **CTE UNION**: UNION ALL and UNION inside CTEs
17. **Recursive CTE compatibility**: Views → Volcano tables bridge
18. **STRING_AGG/ARRAY_AGG/GROUP_CONCAT**: In HashAggregate with separator
19. **Expression-wrapped aggregates**: 100 * SUM(CASE WHEN...) / SUM(x)
20. **Window functions**: ROW_NUMBER, RANK, DENSE_RANK, SUM/COUNT/AVG/MIN/MAX OVER
21. **Value window functions**: LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE, NTILE
22. **Window frame specifications**: ROWS BETWEEN N PRECEDING AND M FOLLOWING
23. **Window expression args**: SUM(a*b) OVER
24. **IFNULL/NVL/TYPEOF**: New scalar functions in Volcano

## Remaining Volcano Skip Guards
1. `_outerRow` set → LATERAL context → legacy
2. `ast.ctes` with window functions → legacy
3. `ast.recursive` → recursive CTEs → legacy (materialized, then Volcano)  
4. JSON operations → legacy
5. MATCH_AGAINST / TS_MATCH → legacy
6. Unsupported aggregates (PERCENTILE_CONT, STDDEV, etc.) → legacy

## Remaining Test Failures (10/989 = 1.01%)
1. Window-in-CASE (3): Window functions nested in CASE WHEN expressions
2. ORDER BY hidden column (1): Sort by column not in SELECT
3. Correlated-subquery-edge (3): Complex nesting patterns
4. E2E analytics (2): Complex multi-feature queries
5. EXISTS nested (1): Deeply nested EXISTS subquery

## Key Insights
1. **Volcano steal pattern**: Volcano silently handles queries it doesn't support → wrong results. Guard first, implement second.
2. **containsAggregate false positive**: Recursive helpers must NOT recurse into subquery nodes.
3. **Correlated detection**: Must explicitly check for outer table references in WHERE.
4. **Frame-aware window functions**: NTH_VALUE, FIRST_VALUE, LAST_VALUE must respect frame bounds.
5. **ctx propagation**: New buildValueGetter cases need ctx from ALL callers (function args, expression projection, predicate building).
