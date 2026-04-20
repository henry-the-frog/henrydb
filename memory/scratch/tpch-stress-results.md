# HenryDB TPC-H Stress Test Results (Session B, Apr 20 2026)

## Test Setup
- 8 tables (TPC-H schema), 200 customers, 500 orders, 2000 lineitems, 100 suppliers, 200 parts, 400 partsupp, 25 nations, 5 regions
- 33 queries: 11 TPC-H (Q1-Q19), 22 analytical/feature tests

## Results: 31/33 passed, 2 failed

### Failures
1. **Recursive CTE arithmetic in WHERE with multi-table FROM**: `rc.rkey + 1` in WHERE clause when recursive CTE has comma-join (e.g., `FROM region r, rc WHERE r.r_regionkey = rc.rkey + 1`). Parser error: "Expected ), got PLUS". Works fine in SELECT clause. Root cause: CTE parser treating the recursive member's WHERE expressions as being inside parentheses.

2. **MERGE with subquery USING**: `MERGE INTO t USING (SELECT ...) AS s ON ...` fails with "Expected KEYWORD ON, got KEYWORD SELECT". MERGE parser only accepts table name for USING, not subquery.

### Performance Issues (CRITICAL)
- **Multi-table JOINs**: Q3, Q5, Q10, Q12 all take 17-18 seconds (500×2000 rows)
- **Root cause**: db.js `_executeJoinWithRows` ALWAYS does O(n*m) nested loop
- **Hash join exists** in planner.js but is NEVER USED by db.js executor
- The DP planner selects hash join but EXPLAIN only shows NESTED_LOOP_JOIN
- This means all the cost model work from today (T7 parametric cost model) has zero effect on execution
- **Impact**: 186x hash join speedup exists in code but is dead code for normal queries

### What Works Well
- All single-table operations: fast and correct
- Window functions (running total, RANK, PERCENT_RANK): work correctly
- Correlated subqueries: work correctly after today's scope fix  
- ROLLUP: works correctly
- FILTER on aggregates: works correctly
- Array/Date/Math functions: all work
- COPY TO: works
- SHOW commands: work
- Prepared statements: work

### Recommendations
1. **P0**: Wire planner output into executor — use hash join when planner selects it
2. **P1**: Fix recursive CTE WHERE arithmetic parsing
3. **P2**: Fix MERGE with subquery USING
4. **P2**: db.js monolith (6.2K lines) makes it hard to find these disconnects
