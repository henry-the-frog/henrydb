# CURRENT.md — Active Work State

## Status: session-ended
## Session: Work Session B (Sunday 4/12, 2:15 PM - 8:15 PM MDT)

## Final Statistics
- **Tasks completed:** 40+
- **New test files:** 40 (38 HenryDB + 1 SAT + 1 blog)
- **New tests written:** 422 (402 HenryDB + 20 SAT)
- **All passing, 0 failures**
- **Bugs found and fixed:** 11
- **New features:** 3 (histograms, join ordering, sliding window frames)
- **Blog posts:** 1

## Bugs Fixed (11)
1. Window function ORDER BY alias
2. ROWS BETWEEN frame clause parser
3. CTE UNION ALL lost second half
4. View resolution for UNION types
5. **CRITICAL: ROLLBACK was a no-op**
6. CTE column aliasing with duplicate names
7. HAVING with complex aggregates (SUM(expr*expr))
8. Derived table column resolution
9. Join ordering ON-condition dependency
10. ORDER BY simple column alias
11. Hash join NULL=NULL incorrectly matched

## Known Issues to Fix
- HeapFile.getByRowId() needed for index nested-loop joins
- INSERT SELECT column mapping with mixed literal+aggregate
- CASE expression in ORDER BY (needs alias workaround)
- Unary minus (-val) not supported in parser
- UNION in derived table subquery errors
