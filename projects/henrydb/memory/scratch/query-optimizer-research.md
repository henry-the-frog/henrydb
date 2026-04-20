# Query Optimizer Research — HenryDB Next Steps

## Current State (2026-04-19)
- Predicate pushdown (WHERE conditions below JOINs)
- Projection pushdown (eliminate unused columns)
- Cost-based access path selection (seq scan vs B+tree vs hash index)
- Hash join for equi-joins (186x faster than nested loop)
- Cost model (PostgreSQL-inspired: page costs, CPU costs, selectivity)
- Table statistics (histogram-based selectivity)
- Volcano iterator model with EXPLAIN

## High-Impact Next Steps (ordered by ROI)

### 1. Join Ordering (HIGHEST IMPACT)
For N tables, there are N! possible join orderings. Current: left-to-right as written.

**Approach: Dynamic Programming (System R style)**
- For 2-4 tables: enumerate all orderings, pick cheapest
- For each pair (R1, R2), estimate join cost based on:
  - Inner table size × selectivity of join predicate
  - Available indexes on join columns
  - Whether hash join is applicable
- Build optimal plan bottom-up: best plans for 2-table subsets → 3-table → etc.

**Greedy alternative (for >4 tables):**
- Start with smallest table
- At each step, join with table that has cheapest estimated join cost
- O(n²) instead of O(n!)

**Key insight:** Put the smaller/more-selective table as the build side of hash join.

### 2. Predicate Pushdown into JOINs
Currently pushes predicates below JOINs to individual scans.
Missing: push predicates INTO the join condition when possible.
- `WHERE a.id = b.id AND a.x > 5` → push `a.x > 5` to scan of `a`, AND keep `a.id = b.id` as join condition
- This is partially done but could be more aggressive

### 3. Subquery Decorrelation
Convert correlated subqueries to JOINs:
- `WHERE x IN (SELECT y FROM t2 WHERE t2.id = t1.id)` → semi-join
- `WHERE EXISTS (...)` → semi-join with DISTINCT
- Scalar subquery in SELECT → LEFT JOIN

**Benefit:** O(n*m) correlated execution → O(n+m) with hash join

### 4. Sort Avoidance / Interesting Orders
If data is already sorted (e.g., from index scan), skip explicit sort:
- ORDER BY on indexed column → use index scan order
- GROUP BY can use pre-sorted input for streaming aggregation
- Merge join produces sorted output → free ORDER BY

### 5. Join Type Selection
Currently: nested loop or hash join.
Add: **sort-merge join** for when both inputs are sorted or need to be sorted.
- Better for large, roughly equal-sized tables
- Produces sorted output (interesting order!)
- No memory limit issues (unlike hash join build phase)

### 6. Common Subexpression Elimination
Detect duplicate subqueries/CTEs and compute them once:
- Multiple references to same CTE → materialize once
- Identical subqueries in WHERE and SELECT → compute once

## PostgreSQL Optimizer Architecture (Reference)
1. **Planner** → generate join tree from FROM clause
2. **geqo** (Genetic Query Optimizer) for >12 tables
3. **Dynamic programming** for ≤12 tables
4. **Path generation**: for each relation, generate all access paths
5. **Join paths**: for each pair, consider NestLoop/HashJoin/MergeJoin
6. **Physical properties**: sort order propagation
7. **Parameterized paths**: nested-loop with inner index scan

## Implementation Plan for Join Ordering
1. Extract join graph from AST (tables as nodes, join predicates as edges)
2. For each table, compute base cost (seq scan or index scan)
3. Bottom-up DP: for each subset S of tables, find cheapest plan
   - For each way to split S into two non-empty subsets (S1, S2)
   - Cost = best_plan(S1) + best_plan(S2) + join_cost(S1, S2)
4. join_cost considers: hash join (if equi-join), NL join (always), merge join (if sorted)
5. Return cheapest plan for full table set

## Decision
**Start with join ordering** — it's the single highest-impact optimization for multi-table queries. Even a simple greedy approach would be a big win. The planner stress tests already prove complex joins work; optimization makes them fast.
