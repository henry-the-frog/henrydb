# HenryDB Query Optimizer Gaps (2026-04-25)

## Hypothesis Results
1. ✅ Classic/Volcano use different cost models — Classic uses hardcoded selectivity constants (0.1, 0.33), Volcano uses ndistinct + histograms from ANALYZE
2. ✅ Selectivity estimates for non-equality predicates are hardcoded guesses in classic planner
3. ✅ Join optimizer doesn't consider index-nested-loop joins

## Gap 1: Dual Cost Model
- `query-plan.js` (classic): `_estimateSelectivity` returns hardcoded constants (0.1 for =, 0.33 for <, 0.25 for LIKE)
- `volcano-planner.js`: `estimateSelectivity` uses `ndistinct`, histograms with interpolation, falls back to defaults only when no stats
- These will produce different plans for the same query, especially when statistics are available

**Fix**: Make classic planner's selectivity estimator accept table stats and use the same logic as Volcano when stats are available. This is a moderate refactor — extract the Volcano's `estimateSelectivity` into a shared module.

## Gap 2: Missing Index Nested Loop Join
- `join-optimizer.js` considers: NLJ, Hash Join, Sort-Merge Join
- Missing: Index Nested Loop Join (INLJ)
- INLJ is optimal when inner table has an index on the join column and outer table is small
- PostgreSQL's optimizer heavily favors INLJ for indexed foreign key lookups

**Fix**: Add INLJ cost estimation: cost = outerRows × (indexHeight × random_page_cost + matchedRows × cpu_tuple_cost). When an index exists on the inner table's join column, compare INLJ cost against NLJ/Hash/SMJ.

## Gap 3: No Correlated Subquery Decorrelation
- Not investigated this session, but likely: correlated subqueries run the inner query once per outer row
- PostgreSQL decorrelates these into joins when possible

## Gap 4: No Materialized CTE Optimization
- WITH clauses are always re-evaluated if referenced multiple times
- Should be materialized on first evaluation and reused

## Priority Ranking
1. **Shared selectivity estimator** (Medium effort, high impact — removes plan disagreement)
2. **Index nested loop join** (Medium effort, high impact for indexed FK lookups)
3. **Correlated subquery decorrelation** (High effort, moderate impact)
4. **CTE materialization** (Low effort, low impact for most queries)
