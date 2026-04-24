# HenryDB Query Execution Architecture — 5 Engines Deep Dive

**Created:** 2026-04-23 | **Uses:** 1

## The 5 Execution Engines

HenryDB has 5 distinct query execution strategies, representing the major approaches in database systems research:

### 1. Default Interpreter (db.js)
- Classic switch-on-AST-node execution
- Walks the parsed AST directly, evaluating expressions recursively
- Simple, correct, but slow (virtual dispatch on every node)

### 2. Volcano Iterator Model (volcano.js)
- Pull-based `open()/next()/close()` protocol (Graefe 1993)
- Composable operators: SeqScan, Filter, Project, Sort, HashJoin, HashAggregate, Window, etc.
- Clean abstraction but per-row virtual dispatch overhead
- Full operator set: 153+ tests

### 3. Pipeline Compiler (pipeline-compiler.js)
- **Push-based** compilation that fuses operators into tight loops
- Identifies pipeline segments between "breakers" (Sort, HashAggregate, Window, HashJoin)
- Two compilation modes:
  - Generator-based: fuses operators into a single generator function
  - JIT: uses `new Function()` to generate specialized code with no generators, no closures
- `compilePredicate()` and `compileProjection()` generate specialized functions
- 17x speedup on LIMIT queries vs Volcano

### 4. Query VM / VDBE (query-vm.js)
- SQLite-style Virtual DataBase Engine
- Register file (64 registers) + stack + hash tables + aggregate slots
- 30+ opcodes: control flow, data movement, arithmetic, comparison, string, aggregate, hash, output
- `QueryCompiler` compiles structured query objects to bytecode programs
- Full GROUP BY support via hash table opcodes (HASH_INIT/PUT/GET/NEXT)
- Program class with constant deduplication, instruction patching, pretty-print disassembly
- Execution stats tracking (instructions executed, rows scanned, rows emitted)

### 5. Query Codegen (query-codegen.js)
- Copy-and-patch style: generates JavaScript source code from query plans
- `new Function('db', source)` compiles to V8-optimized code
- Handles single-table scans, hash joins, WHERE filters, projections
- Left join support, multi-table joins, LIMIT inlining
- Query plan cache for repeated queries
- `explain()` method returns generated source for debugging

## Key Insight: The Compilation Spectrum
These 5 engines form a compilation spectrum, exactly matching database systems research:

1. **Interpreter** (db.js) — no compilation
2. **Volcano** — virtual dispatch per row
3. **Pipeline compiler** — fused operators, generator-based
4. **Query VM** — bytecode (SQLite approach)
5. **Codegen** — native code generation (like HyPer/Umbra)

This matches the progression: interpretation → bytecode → native codegen.

## Vectorized Bridge (vectorized-bridge.js)
There's also a vectorized execution bridge (124 LOC) that processes data in column batches rather than row-at-a-time, representing the columnar execution approach (MonetDB/DuckDB style).

## What Makes This Remarkable
A single JavaScript codebase implements all major query execution paradigms from database systems research. Each engine can be selected per-query, enabling performance comparisons and educational study of the tradeoffs.
