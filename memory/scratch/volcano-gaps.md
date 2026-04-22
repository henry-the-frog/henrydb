# Volcano Gaps — Updated 2026-04-22 Session B

## Fixed this session
- **CTE alias qualification**: JOINs inside CTEs produced double-qualified names (c.region → rs.c.region). Strip qualifiers during CTE materialization.
- **Function-wrapped aggregates**: COALESCE(SUM(x),0), NULLIF(AVG(x),0), ROUND(AVG(x),2) now native in Volcano. Extract inner aggs, apply outer function in projection.
- **Scalar correlated subqueries in SELECT**: (SELECT SUM(x) FROM t WHERE t.id = c.id) now works via buildCorrelatedSubqueryPlan per outer row.
- **Derived tables (FROM subquery)**: SELECT ... FROM (SELECT ...) sub now materialized in Volcano. Guard for unsupported features (window functions) in subquery.
- **LATERAL JOINs**: Were silently going through Volcano (produces empty results). Now correctly skip Volcano when lateral:true in joins or _outerRow is set.

## Remaining gaps
1. **Window functions** (ROW_NUMBER, RANK, SUM OVER, etc.) — complex, falls back to legacy
2. **Recursive CTEs** — complex, falls back
3. **CTEs with UNION** — falls back
4. **Correlated WHERE subqueries**: `WHERE salary > (SELECT AVG(...) WHERE correlated)` — pre-existing, returns empty in Volcano. Decorrelator may not handle this pattern.
5. **Aggregate FILTER clause**: `COUNT(*) FILTER (WHERE x > 0)` — falls back
6. **Unsupported aggregates**: ARRAY_AGG, STRING_AGG, STDDEV, etc.
7. **JSON operations** — falls back
8. **No FROM clause**: `SELECT 1+1` — falls back (minor)
9. **JOIN subqueries (non-LATERAL)**: Volcano materializes them but with potential qualification issues

## Key insight
The Volcano steal problem: Volcano silently handles queries it doesn't fully support, producing wrong results instead of falling back. The LATERAL fix pattern (skip guard → legacy fallback) is the right approach. Always add guards BEFORE the `buildPlan` call.

## Test counts
- Core SQL: 157/157 pass
- LATERAL: 18/18 pass (was 10/18)
- Broader suite: 398/403 pass
- Remaining failures: 1 correlated WHERE subquery, 4 TPC-H wire protocol (pre-existing)
