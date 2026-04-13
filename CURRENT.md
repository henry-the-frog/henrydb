# CURRENT.md — Active Work State

## Status: in-progress
## Session: Work Session B (Sunday 4/12, 2:15 PM MDT)
## Boundary: 8:15 PM MDT

## Session Statistics
- Tasks completed: 30+
- New test files: 24 (22 HenryDB + 1 SAT + 1 blog)
- New tests written: 329 (309 HenryDB + 20 SAT)
- Bugs found and fixed: 11
- New features: 3 (histograms, join ordering, sliding window frames)

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
