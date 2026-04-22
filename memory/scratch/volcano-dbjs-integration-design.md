# Volcano → db.js Integration Design

## Problem
The Volcano engine has hash/merge/NL join + cost model, but db.js always uses nested loop
join. Multi-table TPC-H queries take 17-18s instead of <1s.

## Approach: Volcano-for-Joins

### What to delegate
Only the JOIN step — building the cross product of rows from multiple tables with
the join predicate applied. Everything else (WHERE filter, GROUP BY, window functions,
ORDER BY, LIMIT, expression evaluation) stays in db.js.

### How

1. In `_select()` at line ~2095, when `hasJoins` is true:
   a. Build a minimal AST: `{from, joins, where: join_conditions_only}`
   b. Call `buildPlan(minimalAst, this.tables, this.indexCatalog)` 
   c. Execute the plan to get materialized rows
   d. Return these rows to db.js for the rest of the pipeline

2. The minimal AST strips:
   - GROUP BY, HAVING, ORDER BY, LIMIT, DISTINCT
   - SELECT columns (use SELECT *)
   - Non-join WHERE conditions (applied later by db.js)

3. The plan only builds: SeqScan → Join (hash/merge/NL) → Filter (join conditions)

### Challenges

1. **Column naming**: Volcano produces `table.col` qualified names. db.js expects both
   qualified and unqualified. The row format must be compatible.
   - Solution: SeqScan already produces both. Just pass through.

2. **Expression evaluation in ON clauses**: Volcano's buildPredicate is simpler than
   db.js's _evalExpr. Complex ON clauses (subqueries, function calls) may fail.
   - Solution: Fall back to nested loop when ON clause is too complex for Volcano.

3. **Index catalog**: Volcano needs the index catalog for IndexNestedLoopJoin.
   - Solution: Pass `this.indexCatalog` from db.js to buildPlan.

4. **LEFT/RIGHT/FULL joins**: Volcano HashJoin supports LEFT. Others may need work.
   - Solution: Start with INNER and LEFT only. Fall back for FULL/CROSS.

### Implementation Steps

1. Add `tryVolcanoJoin(ast)` method to db.js
2. Intercept at `hasJoins` check in `_select()`
3. Build minimal join plan, execute, return rows
4. If Volcano can't handle the join (complex ON, unsupported type), fall back
5. Add `this._useVolcano = true` flag (default on)

### Expected Impact
- TPC-H Q3 (3-table join): 17s → <1s (186x improvement)
- Simple 2-table equi-joins: ~10x improvement
- Non-equi joins: same (Volcano uses NL, same as db.js)

### Testing
- All existing tests must pass (with fallback, nothing breaks)
- New benchmark: TPC-H queries with Volcano vs without
- Differential: Volcano join results === db.js join results for same query
