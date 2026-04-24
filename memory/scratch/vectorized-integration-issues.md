# Vectorized Engine Integration Issues (Apr 24, 2026)

## Root Cause Analysis

### Issue 1: Missing non-aggregate columns in GROUP BY results
When a query has `SELECT a, b, COUNT(*) FROM t GROUP BY a, b`, the VHashAggregate only outputs columns listed in `ast.columns`, not `ast.groupBy`. If `b` is in GROUP BY but not explicitly in SELECT, it's missing from the vectorized output.

**Fix:** Use `ast.groupBy` as the source of truth for group columns, not `ast.columns`.

### Issue 2: Column value resolution
The vectorized path reads raw values from the heap (array of values), but the standard path reads row objects. When the heap returns `[1, 'A', 10]`, the vectorized SeqScan maps to `{id: 1, category: 'A', val: 10}` using the schema. This mapping might fail if the schema order doesn't match the heap order.

### Issue 3: Expression evaluation
Queries with HAVING use the expression evaluator which expects standard row format. The vectorized output rows might not match the format expected by `_evalExpr`.

### Issue 4: Result column ordering
The standard path preserves the SELECT column order. The vectorized path outputs group columns first, then aggregates. This ordering difference causes differential fuzzer failures.

## Affected Test Files (15 failures across 4 files)
1. `diff-fuzzer-extended.test.js` — 7 failures (column ordering + missing values)
2. `diff-fuzzer-groupby.test.js` — 6 failures (GROUP BY compat)
3. `volcano-setops.test.js` — 1 failure (set operation after vectorized result)
4. `misc.test.js` — 1 failure (edge case)

## Fix Strategy
1. Ensure VHashAggregate outputs ALL group columns from `ast.groupBy`
2. Match the standard path's column ordering (SELECT list order, not group-first)
3. Ensure the result format is compatible with HAVING, ORDER BY, LIMIT processing
4. Consider running HAVING/ORDER BY/LIMIT after vectorized output (currently skipped)

## Priority
Medium — the engine works correctly for opt-in usage. Auto-enable needs ~4 specific fixes.
