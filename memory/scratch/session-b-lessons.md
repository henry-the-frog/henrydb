# Session B Lessons Learned — April 25, 2026

## What Worked Exceptionally Well

### 1. Focused Depth Over Breadth
Concentrating on two main projects (monkey-lang + HenryDB) produced high-impact work:
- 60 BUILD tasks in 2h30m
- 9 bugs found and fixed
- 96 new tests for monkey-lang
- Every feature addition was tested immediately

### 2. Bug-Driven Exploration
Finding bugs during exploration was the highest-ROI activity:
- SSA OOM revealed a design issue (string-only input)
- WHERE coercion bug revealed a fundamental typing problem
- COUNT(DISTINCT) bug in multi-join found during stress testing
- Each bug fix improved understanding of the system

### 3. Existing Feature Discovery
Many "planned" features already existed:
- Pattern matching, destructuring, for-in, enums, try-catch in monkey-lang
- Window functions, CTEs, triggers, views in HenryDB
- This saved hours of implementation time and revealed the codebase is more complete than expected

### 4. The Prelude Pattern
Writing HOFs in monkey-lang itself (rather than as native builtins) was elegant:
- Simpler to implement and maintain
- Uses the language's own features
- Tradeoff: 19x slower than native for HOFs, but adequate for most use cases

### 5. Project Collection as Knowledge Base
The 215-project collection is a goldmine:
- ~64 verified working projects
- Covers nearly every CS fundamental
- Each project teaches a different concept through implementation

## What Could Be Improved

### 1. Benchmark Infrastructure
The benchmark kept OOMing due to heavy prelude compilation:
- compileWithPrelude() loads too much for quick benchmarks
- Need a lighter benchmark mode (compile once, run many)
- The existing bench.js works but is fragile (crashes on deep recursion)

### 2. PL/SQL Wiring Gap
The PL/SQL parser and interpreter exist (854 LOC) but aren't connected to the procedure handler.
This is a common pattern: features built but not integrated end-to-end.

### 3. API Naming Inconsistency
Across the 215 projects, method names are inconsistent:
- Some use `push/pop`, others `insert/remove`, others `add/delete`
- Some return `this` for chaining, others return void
- Some use getters (`value`), others use methods (`value()`)
- This makes exploration harder

### 4. COUNT(DISTINCT) Bug
The aggregation path doesn't properly handle DISTINCT in multi-join scenarios.
This is a real-world bug that would affect production queries.

## Key Metrics

| Metric | Value |
|--------|-------|
| BUILD tasks | 60 (ceiling) |
| Bugs found | 10 (9 fixed + 1 COUNT(DISTINCT) documented) |
| Tests added | 139 (96 monkey + 23 type-infer + 20 calc-lang) |
| Projects explored | ~64 verified |
| Scratch docs written | 6 new files |
| Time to ceiling | 2h30m |

## Recommendations for Next Session
1. **Fix COUNT(DISTINCT)** — it's a real bug that affects correctness
2. **Wire PL/SQL** — the implementation exists, just needs connection
3. **Benchmark mode** — lighter compilation for quick perf tests
4. **DCE implementation** — SSA-level annotation approach, ~200 LOC
5. **VM callback mechanism** — close the 19x HOF performance gap
