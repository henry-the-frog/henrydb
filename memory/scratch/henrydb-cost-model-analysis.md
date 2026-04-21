# HenryDB Cost Model Analysis
- created: 2026-04-21
- tags: henrydb, cost-model, optimizer, volcano

## Current State (Apr 21)

### Three Separate Cost Paths
1. **db.js `_compareScanCosts/_compareJoinCosts`**: PostgreSQL-style with COST_MODEL constants. Uses `_tableStats` when available (from ANALYZE). Falls back to heuristic selectivity (1/3 range, 1/10 equality).
2. **db.js EXPLAIN `_explain()`**: Builds a plan tree with cost annotations, but uses generic defaults for selectivity, not actual statistics.
3. **Volcano planner**: No cost model at all. Uses simple heuristics (HashJoin for equi-joins, always IndexScan if available).

### Divergence Risk
- EXPLAIN says "SeqScan cost 100, IndexScan cost 50 → uses index" but actual execution might choose differently based on real statistics.
- Volcano planner always picks IndexScan regardless of selectivity — might be slower for non-selective predicates.

### Recommendation
1. **Short term**: Add a simple cost heuristic to the Volcano IndexScan decision — only use IndexScan when selectivity < 10-20%.
2. **Medium term**: Move db.js cost model into a shared module that both db.js and Volcano planner use.
3. **Long term**: Full Cascades-style optimizer with physical properties and cost-based search.

## PostgreSQL Cost Model Reference
- seq_page_cost = 1.0 (baseline)
- random_page_cost = 4.0 (4x sequential for disk; HenryDB uses 1.1 since in-memory)
- cpu_tuple_cost = 0.01
- cpu_index_tuple_cost = 0.005
- cpu_operator_cost = 0.0025
- HenryDB's model matches PostgreSQL's constants but adjusted for in-memory operation.
