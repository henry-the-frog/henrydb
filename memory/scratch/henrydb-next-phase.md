# HenryDB Next Phase — Updated 2026-04-22 Session B

## Current State
- **Test pass rate:** 985/989 (99.60%) across comprehensive suite
- **Volcano engine:** Near-complete SQL feature coverage
- **db.js:** 3,293 lines (down from 10K+)
- **Total tests:** 989+ across 866 test files

## Session B Accomplishments
- 27 features added to Volcano engine
- Closed nearly ALL Volcano gaps
- Window functions fully implemented (128/128 tests)
- Multi-OVER window support
- Expression-wrapped aggregates
- CTE UNION, recursive CTE, derived tables
- STRING_AGG, ARRAY_AGG, DISTINCT aggregates
- FILTER clause, IFNULL, TYPEOF

## Remaining Work (Priority Order)

### High Priority
1. **SELECT * + window in planner** — Needs careful star case integration (currently causes e2e regressions)
2. **SQL NULL arithmetic propagation** — `10 - NULL` should be `NULL`, not `10` (JS coerces null to 0)
3. **NOT NOT NOT** — Parser doesn't handle triple/arbitrary negation
4. **Parser inconsistency** — From TODO, needs investigation

### Medium Priority
5. **Window functions in GROUP BY + HAVING** — Some combinations may not work
6. **Multiple different PARTITION BY** — Multi-OVER handles different ORDER BY but same PARTITION BY assumed
7. **RANGE frame** — Only ROWS frame supported, RANGE is different semantics

### Low Priority / Future
8. **JSON path operators in Volcano** — Currently falls back to legacy
9. **Computed/virtual columns** — SQL standard feature
10. **Materialized views** — Performance optimization
11. **Write about HenryDB** — Blog post / documentation
12. **Further db.js extraction** — Continue modularization

## Architecture Notes
- **Volcano steal pattern:** Always guard before implementing
- **Multi-OVER:** Chain separate Window operators per unique OVER spec
- **Expression extraction:** Reusable pattern for aggregates, windows, and projections
- **NULL semantics:** Need systematic NULL propagation in arithmetic, not just per-feature
