# CURRENT.md — Session Status

## Status: in-progress
## Session: 2026-04-22 Session C (8:15 PM - 10:15 PM MDT)
## Tasks Completed: 1 (T94)
## BUILDs: 0

### Volcano HAVING audit — 5 bugs found and fixed
- **buildAggregatePredicate** only handled COMPARE type (returned `() => true` for everything else)
  - Added: AND, OR, NOT, BETWEEN, NOT_BETWEEN, IN_LIST, IS_NULL, IS_NOT_NULL, LIKE, NOT_LIKE, ILIKE, EXISTS
- **buildAggregateValueGetter** couldn't handle function_call (COALESCE etc.), column_ref, or arithmetic
  - Added: function_call with COALESCE/NULLIF/GREATEST/LEAST/ABS/ROUND/CEIL/FLOOR, column_ref, arith
- **Aggregates in HAVING not in SELECT** weren't collected into HashAggregate
  - Added: HAVING aggregate collection pass before HashAggregate creation
- **16 typeof arg checks** replaced with 4 helper functions (aggArgStr, isExprAgg, winArgName, winArgGetter)
- **14-test comprehensive HAVING suite** added (having-volcano-comprehensive.test.js)
- 149 volcano/aggregate tests pass, 25 TPC-H pass, 0 regressions
