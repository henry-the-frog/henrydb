# Volcano Gaps — Priority List (as of 2026-04-22 END OF SESSION A)

## Status: 465/465 SQL tests, 29 bugs fixed, DEFAULT ON

## Working (✅) — Everything Below
- All predicates, aggregates, joins (INNER/LEFT/RIGHT/FULL/CROSS/NATURAL/USING/self)
- CTEs, derived tables, EXISTS, NOT EXISTS, ANY/ALL, IN subquery/hashset
- GROUP BY/ORDER BY with alias, ordinal, and expressions
- 16 SQL functions, CAST, ILIKE, integer division, type coercion
- EXPLAIN with Volcano plan tree, EXPLAIN ANALYZE

## Skipped via Guards (falls back to legacy) — Working
- Window functions (ROW_NUMBER, RANK, SUM OVER, etc.)
- Recursive CTEs
- CTEs with UNION or window functions
- Unsupported aggregates (STDDEV, VARIANCE, ARRAY_AGG, etc.)
- FILTER clause in aggregates
- Function-wrapped aggregates (COALESCE(SUM(x), 0))
- Derived table in nested subqueries (__subquery)
- TransactionalDatabase (MVCC bypass)
- JSON operators
- PIVOT/UNPIVOT

## Next Priorities
1. Window function support in Volcano iterators
2. MVCC-aware SeqScan (for TransactionalDatabase)
3. CTE UNION support
4. Plan cache for repeated queries
5. Remove guards and implement natively
