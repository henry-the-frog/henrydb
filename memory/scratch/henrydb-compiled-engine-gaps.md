# HenryDB Compiled Query Engine Coverage Gap
- created: 2026-04-20
- uses: 1
- tags: henrydb, query-compilation, jit

## Architecture Overview
4,041 LOC across 10 files. Three compilation layers:
1. **CompiledQueryEngine** (`compiled-query.js`, 777 LOC) — High-level: plan → compile → execute
2. **Pipeline JIT** (`pipeline-compiler.js`, 342 LOC) — Volcano operators → tight JS loops via `new Function()`
3. **Vectorized Engine** (`vectorized.js` + `vectorized-codegen.js`, 917 LOC) — Batch processing with DataBatch/selection vectors
4. **Adaptive Engine** (`adaptive-engine.js`, 305 LOC) — Chooses between interpreter/compiled/vectorized at runtime

## What the Compiled Engine Handles

### Expression Compilation (`_compileExpr`)
Only 4 types: **COMPARE (6 ops), AND, OR, NOT**

### Value Expressions (`_compileValueExpr`)
Only 2 types: **column_ref, literal**

### Aggregation (`_compiledAggregate`)
5 functions: **COUNT, SUM, MIN, MAX, AVG**

### Join Strategy
Hash join with equi-join detection. Falls back to nested loop.

## What's Missing (vs interpreter's `_evalExpr`)

### Critical Gaps (affect query correctness)
- **BETWEEN / NOT BETWEEN** — TODO item mentions this divergence
- **CASE / CASE WHEN** — No compiled CASE support
- **IS NULL / IS NOT NULL** — Common SQL patterns
- **IN / NOT IN** — Very common in queries
- **LIKE / ILIKE** — Pattern matching
- **Arithmetic** (+, -, *, /, %) — Can't compute expressions in SELECT
- **CAST** — Type coercion
- **COALESCE / NULLIF** — NULL handling

### Subquery Features (would need full interpreter fallback)
- EXISTS / NOT EXISTS
- ANY/SOME/ALL (subquery)
- Scalar subqueries

### Advanced Features (low priority for compilation)
- Window functions
- CTEs (recursive and non-recursive)
- Set operations (UNION/INTERSECT/EXCEPT)
- LATERAL joins
- String concatenation (||)
- Function calls (644 lines of switch/case in interpreter!)

## Divergence Risk Assessment
The compiled engine returns `null` and falls back to interpreter when:
1. Table has < 50 rows
2. Query has subqueries in WHERE
3. Any unrecognized AST node in `_compileExpr` (returns null → whole filter skipped)

**Problem**: If `_compileExpr` returns null for an unknown expr type, the filter is **skipped entirely**, meaning the compiled path may return MORE rows than the interpreter. This is a correctness bug, not just a performance issue.

Example: `SELECT * FROM t WHERE x BETWEEN 1 AND 10` compiled → `_compileExpr` returns null for BETWEEN → no filter applied → ALL rows returned.

**Severity**: Currently LATENT — CompiledQueryEngine is only used in EXPLAIN COMPILED and test code. Not wired into main db.js query path. AdaptiveEngine also not wired in. Would become P0 if either is activated for production queries.

## Recommendation
1. **P0**: Fix `_compileExpr` to throw/fallback on unknown types instead of silently returning null
2. **P1**: Add BETWEEN, IS NULL, IN, LIKE, CASE to `_compileExpr`
3. **P2**: Add arithmetic and function calls to `_compileValueExpr`
4. **P3**: Consider whether the compiled engine is worth maintaining vs just improving the vectorized path

The vectorized engine is architecturally better (batch processing, selection vectors) and likely to outperform the scalar compiled path for analytical queries anyway.
