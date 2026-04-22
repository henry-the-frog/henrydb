# Query Compilation Techniques — Research Notes (2026-04-22)

## Three Approaches

### 1. Volcano Iterator (HenryDB current)
- Pull-based: open()/next()/close()
- One row at a time
- Composable, correct, easy to test
- Overhead: virtual dispatch per row per operator

### 2. HyPer Data-Centric Codegen (Neumann 2011)
- Push-based: produce/consume paradigm
- Generate a single tight loop for entire pipeline
- Pipeline breakers (hash build, sort, agg) materialize
- Everything else streams through with zero materialization
- In JS: would mean generating a function string and eval()ing it
- Extremely fast for OLAP but complex to implement

### 3. Vectorized Execution (MonetDB/X100 — Boncz 2005)
- Batch-at-a-time: process 1024 rows through each operator
- Still pull-based (like Volcano) but processes vectors not single rows
- Amortizes virtual dispatch: 1 call per 1024 rows instead of 1 per row
- Cache-friendly: operate on column vectors that fit in L1/L2
- Simpler than full codegen, ~80% of the benefit

## What HenryDB Has
- Volcano: correct, 238+ tests, full SQL support
- CompiledQueryEngine: simple scan-level compilation
- Gap: no operator fusion, no vectorization

## Recommended Next Steps (in order of value/effort)
1. **Operator fusion in compiled path**: push filters into scans (skip rows earlier)
2. **Batch processing**: process N rows at a time through Volcano operators
3. **Column-at-a-time processing**: restructure internal row format to columnar for aggregates
4. **Full push-based codegen**: generate entire pipeline as single function (significant complexity)

## Key Insight
For JS (no SIMD, no native code), vectorization gives the biggest win because it:
- Reduces function call overhead (1 call per batch, not per row)
- Improves branch prediction (process all rows through one operator before moving to next)
- Better JIT optimization (tight loops on arrays → V8 can optimize)
