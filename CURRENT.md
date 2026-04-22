# CURRENT.md — Session Status

## Status: session-ended
## Session: 2026-04-22 Session B (2:15 PM - 8:15 PM MDT)
## Tasks Completed: 50+
## BUILDs: 20 (session cap reached)

### Session B Summary
- **Test pass rate:** 372/403 → 985/989 (99.60%)
- **Features added:** 27 Volcano engine features
- **Bugs fixed:** 10+ (CTE alias, LATERAL, JSON, FTS, wire protocol, correlated subquery, containsAggregate, NULL preservation)
- **New tests written:** 31
- **Adversarial tests:** 40+ complex queries tested, all pass

### Key Accomplishments
- Window functions fully implemented (128/128 tests)
- Multi-OVER window support
- CTE UNION + recursive CTE compatibility
- Scalar correlated subqueries in SELECT + WHERE
- Derived tables, LATERAL JOIN fix, aggregate FILTER
- DISTINCT aggregates, STRING_AGG, ARRAY_AGG
- Expression-wrapped aggregates, window-in-expression
- SELECT without FROM, ORDER BY hidden column
- IFNULL, TYPEOF, star expansion

### Remaining Issues
- GROUP BY CASE expression bug (pre-existing)
- SELECT * + window needs careful planner integration
- SQL NULL arithmetic propagation (JS null coercion)
- NOT NOT NOT parser issue
