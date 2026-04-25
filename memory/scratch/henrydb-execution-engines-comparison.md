# HenryDB Execution Engines Comparison (2026-04-25)

## Overview
HenryDB has 6 execution engines + an adaptive layer. This is more sophisticated than most commercial databases.

## Engine Inventory

### 1. Volcano Iterator (1483 lines)
- **Model**: Pull-based (next() → tuple)
- **Best for**: General-purpose, complex queries, subqueries
- **Files**: volcano.js, volcano-iterator.js, volcano-select.js, volcano-planner.js, volcano-analyze.js
- **Features**: Cost-based planning with ANALYZE stats, EXPLAIN ANALYZE
- **Weakness**: Per-row overhead from virtual function dispatch

### 2. Pipeline JIT (342 lines)
- **Model**: Push-based (pipeline breakers)
- **Best for**: Selective queries, small-medium joins
- **Files**: pipeline-compiler.js, compiled-query.js
- **Features**: Compiles query plan to JS closures for zero-overhead loops
- **Weakness**: Limited operator support vs Volcano

### 3. Vectorized (463 lines)
- **Model**: Batch/columnar processing
- **Best for**: Large scans, analytics, aggregations
- **Files**: vectorized.js, vectorized-bridge.js
- **Features**: VectorBatch of ~1024 tuples, VHashAggregate, VHashJoin
- **Weakness**: Overhead for small queries

### 4. Vectorized Codegen (454 lines)
- **Model**: Code-generated vectorized operators
- **Best for**: Repeated analytics queries
- **Files**: vectorized-codegen.js
- **Features**: Generates specialized functions for each operator
- **Weakness**: Compilation overhead (amortized over executions)

### 5. Query VM (595 lines)
- **Model**: Register-based bytecode VM (SQLite VDBE-style)
- **Best for**: Simple queries, predictable performance
- **Files**: query-vm.js
- **Features**: ~30 opcodes, compile once → execute many times
- **Weakness**: Interpretation overhead vs native codegen

### 6. Adaptive Engine (307 lines)
- **Model**: Runtime strategy selector
- **Rules**:
  - Large scans (>5000 rows) → Vectorized
  - Selective queries → Codegen
  - Complex patterns → Volcano (fallback)
- **Files**: adaptive-engine.js
- **Features**: Per-query stats, dynamic switching

## Total Lines
3,644 lines of execution engine code (not counting planners, optimizers, or executors)

## Architectural Insight
This is a microcosm of the last 20 years of database engine evolution:
- Volcano (1990): Graefe's iterator model
- Pipeline JIT (2011): HyPer's push-based compilation
- Vectorized (2005): MonetDB/X100 → DuckDB
- Query VM (1999): SQLite's VDBE
- Adaptive (2018): Snowflake/DuckDB adaptive execution

All 5 paradigms, implemented from scratch in JavaScript. The adaptive engine ties them together.
