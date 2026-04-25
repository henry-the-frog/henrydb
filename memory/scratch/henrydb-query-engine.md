# HenryDB Query Engine Architecture

## Three Execution Strategies

### 1. AST Interpreter (select-inner.js, 847 LOC)
- Direct AST walking, simplest path
- Handles the full SQL feature set
- Used as fallback when other strategies don't support a feature
- Most feature-complete but slowest for complex queries

### 2. Volcano Iterator Model (volcano.js + volcano-planner.js, 3422 LOC)
- Classic open()/next()/close() pull-based iterators
- Operators: SeqScan, IndexScan, Filter, Project, Sort, HashAggregate, NestedLoopJoin, HashJoin, IndexNestedLoopJoin, Window, Limit, Distinct, Union, CTE
- EXPLAIN output support
- Join strategy selection based on index availability and table sizes
- Most flexible for complex queries

### 3. Bytecode VM (query-vm.js + compiled-query.js, 1447 LOC)
- VDBE-inspired (like SQLite's Virtual DataBase Engine)
- Register-based instruction set
- Opcodes: HALT, GOTO, IF_TRUE/FALSE, LOAD_CONST, OPEN_TABLE, NEXT_ROW, COLUMN, arithmetic, comparison, aggregation, EMIT_ROW
- Compiled from SQL AST to bytecode array
- Fastest for simple queries (avoids iterator overhead)

## Cost-Based Optimizer (planner.js, 890 LOC)
- Column histograms (equi-depth, 10 buckets)
- Most Common Values (MCV) tracking
- Selectivity estimation for predicates
- Dynamic programming join reordering
- Cost model: IO_COST=1.0, RANDOM_IO_COST=4.0, CPU_TUPLE_COST=0.01
- Page-level cost estimation (4KB pages)

## Supporting Infrastructure
- `expression-evaluator.js` (1234 LOC) — evaluates SQL expressions
- `sql-functions.js` (878 LOC) — built-in SQL functions
- `selectivity.js` — predicate selectivity estimation
- `join-executor.js` (1078 LOC) — specialized join implementations
- `type-affinity.js` — SQLite-compatible type coercion
- `percentile.js` — MEDIAN/PERCENTILE_CONT/DISC

## Optimization Opportunities
1. **Predicate pushdown**: Push WHERE clauses below JOINs in Volcano planner
2. **Projection pushdown**: Only read needed columns from storage
3. **Merge join**: Add sort-merge join for sorted inputs
4. **Subquery decorrelation**: Convert correlated subqueries to joins
5. **Index-only scans**: Return data from index without table lookup
6. **Parallel scan**: Multi-threaded sequential scan for large tables
7. **Adaptive execution**: Switch strategies based on actual cardinalities
