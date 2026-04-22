# Volcano Gaps — Updated 2026-04-22 Session B (Final)

## Fixed this session (10 features)
1. **CTE alias qualification**: Strip qualifiers from materialized CTE columns (c.region → region)
2. **Function-wrapped aggregates**: COALESCE(SUM(x),0), NULLIF(AVG(x),0), ROUND(AVG(x),2)
3. **Scalar correlated subqueries in SELECT**: (SELECT SUM(x) FROM t WHERE t.id = c.id)
4. **Derived tables (FROM subquery)**: SELECT ... FROM (SELECT ...) sub
5. **LATERAL JOIN fix**: Skip Volcano for correlated context
6. **Aggregate FILTER clause**: COUNT(*) FILTER (WHERE x > 0)
7. **Correlated WHERE subqueries**: WHERE salary > (SELECT AVG(...) WHERE correlated)
8. **ctx propagation fix**: COALESCE((SELECT ...)) and nested function subqueries
9. **SELECT without FROM**: SELECT 1+1, SELECT NOW()
10. **DISTINCT aggregates**: COUNT(DISTINCT x), SUM(DISTINCT x)
11. **CTE UNION**: UNION ALL and UNION inside CTEs
12. **Recursive CTE compatibility**: Views → Volcano tables bridge

## Test results
- **Core SQL suite: 425/425 (100%)**
- Broader suite (incl JSON/FTS/TPC-H wire): 797/828 (96.3%)
- LATERAL: 18/18 (was 10/18)
- Window: all pass
- Transaction/Persistent: all pass

## Remaining Volcano skip guards (from _tryVolcanoSelect)
1. `_outerRow` set — LATERAL context → legacy
2. `ast.ctes` with window functions → legacy
3. `ast.recursive` — recursive CTEs → legacy (materialized, then Volcano)
4. Window functions in columns → legacy
5. Unsupported aggregates (ARRAY_AGG, STRING_AGG, STDDEV, etc.) → legacy
6. JSON operations → legacy
7. PIVOT/UNPIVOT → legacy

## Remaining pre-existing failures
- JSON: 22 failures (extraction operators, JSON functions)
- Full-text search: 5 failures (FTS integration)
- TPC-H wire protocol: 4 failures (wire protocol format)

## Key insights
1. **Volcano steal problem**: Volcano silently handles queries it doesn't support → wrong results. Always add guards.
2. **ctx propagation**: When adding new cases to buildValueGetter, pass ctx through all callers.
3. **CTE materialization**: Views → virtual tables bridge needed for recursive CTEs → Volcano.
4. **Unnamed column fix**: SELECT without FROM needs synthetic column names (col0, col1).
