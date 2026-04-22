## TODO

### Urgent
(none)

### Normal
- HenryDB: db.js now at ~5582 lines — further extraction possible: EXPLAIN ~350, GROUP BY ~166, cost model ~200 (since 2026-04-20, updated 2026-04-22)
- HenryDB: Volcano planner: integrate ANALYZE table stats for better cost estimates (since 2026-04-21)
- Neural-net: 3 pre-existing sliding-window.test.js failures (since 2026-04-21)

### Low
- RISC-V: Liveness-based register allocation
- Neural-net: Architecture exploration (attention, model serialization already done)
- RISC-V: IIFE pattern
- HenryDB: heap page overflow with very large values (>30KB). Need TOAST-style overflow pages.
- HenryDB: Hash-index performance (test takes 24s)
- HenryDB: Parser unification — parseSelectColumn should delegate to parseExpr
- HenryDB: Unified expression walker migration
- HenryDB: EXPLAIN ANALYZE: add est vs actual rows, Rows Removed by Filter (PG-style)
- HenryDB: Vectorized (batch-at-a-time) execution in Volcano operators

### Blog Post Idea
- "Wiring a Volcano Engine into a Database" — from Feature Theater to 37x speedup. Covers the integration strategy, EvalPlanQual bug find, and benchmark results. Good technical post.
