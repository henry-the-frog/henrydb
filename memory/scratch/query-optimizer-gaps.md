# Query Optimizer Gaps — HenryDB
created: 2026-04-18
uses: 1

## What HenryDB Already Has (Impressive)
- ANALYZE: ndistinct, null fraction, MCV, equi-height histograms
- Selectivity estimation: equality (1/ndistinct), range (histogram or linear interpolation), IN, BETWEEN
- Cost-based index vs sequential scan selection
- System R-style DP join ordering (up to 6 tables)
- EXPLAIN ANALYZE with actual vs estimated rows + timing
- Sort cost modeling in EXPLAIN (N log N * CPU factor)

## Genuine Gaps (Ranked by Learning Value)

### 1. Join Method Selection (HIGH — most impactful)
Currently: always nested loop (or hash for equi-joins, but no cost comparison)
PG model: for each join pair, compare:
- Nested loop: outer_rows * (inner_index_lookup_cost OR inner_seq_scan_cost)
- Hash join: build_cost(inner) + probe_cost(outer) + memory_overhead
- Merge join: sort_cost(left) + sort_cost(right) + merge_cost

Selection based on: row counts, available indexes, available memory, sort order
**Implementation: ~100 lines.** Add to _optimizeJoinOrder DP to track method per edge.

### 2. Parametric Cost Model (MEDIUM — foundational)
Currently: cost = row_count (no I/O vs CPU distinction)
PG model uses 5 parameters:
- seq_page_cost = 1.0 (sequential I/O)
- random_page_cost = 4.0 (random I/O, ~4x sequential)
- cpu_tuple_cost = 0.01 (process a row)
- cpu_index_tuple_cost = 0.005 (process an index entry)
- cpu_operator_cost = 0.0025 (eval a WHERE clause)

Total cost = (pages_fetched * page_cost) + (rows * cpu_cost) + (operator_evals * op_cost)
In-memory DB: page costs are near-zero, but ratio still matters for cache behavior.
**Implementation: ~50 lines.** Replace raw row count with parametric formula.

### 3. Multi-Column Statistics (LOW — diminishing returns)
Currently: assumes column independence (selectivity = product of individual selectivities)
PG 10+ has CREATE STATISTICS for functional dependencies and MCV lists.
Problem: `WHERE city = 'NYC' AND state = 'NY'` — not independent!
Fix: correlation detection during ANALYZE, combined MCV lists.
**Implementation: ~200 lines. Mostly in ANALYZE.**

### 4. Histogram-Based Join Estimation (MEDIUM — improves join ordering)
Currently: join size = left_rows * right_rows / max(ndistinct_left, ndistinct_right)
Better: overlap histograms. For equi-join on key K:
- Walk both histograms, compute overlap at each bucket
- Sum overlapping buckets × bucket density
This handles skew much better (e.g., most orders are from top 10% of customers).
**Implementation: ~80 lines.**

### 5. Predicate Pushdown Through Joins (LOW — usually small impact)
Currently: filter applied after join
Better: push WHERE conditions through join tree to reduce intermediate sizes
E.g., `SELECT * FROM A JOIN B ON A.id=B.aid WHERE A.status='active'`
→ filter A first, then join (smaller intermediate)
**Implementation: ~60 lines in query planner.**

## Recommended Implementation Order
1. Parametric cost model (foundational, improves everything else)
2. Join method selection with cost comparison (biggest real-world impact)
3. Histogram-based join estimation (improves join ordering accuracy)
4. Predicate pushdown (nice to have)
5. Multi-column stats (diminishing returns for educational DB)

## Key Insight
HenryDB is already more sophisticated than most educational DBs. The gaps are in the same territory as PostgreSQL 7.x → 8.x improvements. The parametric cost model is the right foundation — everything else builds on it.
