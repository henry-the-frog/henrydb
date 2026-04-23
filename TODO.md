## TODO

### Urgent
(none)

### Normal
- HenryDB: db.js now at ~3293 lines — further extraction possible: ~1370 LOC in 9 blocks (see scratch/henrydb-extraction-roadmap.md) (since 2026-04-20, updated 2026-04-22)
- HenryDB: Volcano planner: route CTEs through Volcano for 14x speedup (5ms vs 70ms) (since 2026-04-22)
- HenryDB: Volcano path: add correlated subquery support (see scratch/volcano-gaps.md) (since 2026-04-22)
- HenryDB: Parser inconsistency: aggregate arg is sometimes string, sometimes object (since 2026-04-22)
- Neural-net: Full backprop training with AdamW (currently only output-proj training works well)

### Low
- RISC-V: Liveness-based register allocation
- Neural-net: Architecture exploration (attention, model serialization already done)
- RISC-V: IIFE pattern
- HenryDB: heap page overflow with very large values (>30KB). Need TOAST-style overflow pages.
- HenryDB: Hash-index performance (test takes 24s)
- HenryDB: Parser unification — parseSelectColumn should delegate to parseExpr
- HenryDB: Unified expression walker migration
- HenryDB: Vectorized (batch-at-a-time) execution in Volcano operators (see scratch/vectorized-execution-explore.md — 5.8x for deep pipelines, needs columnar storage)
- HenryDB: EXPLAIN ANALYZE: add Planning Time, startup vs total time per operator

### Blog Post Idea
- "Wiring a Volcano Engine into a Database" — from Feature Theater to 37x speedup. Covers the integration strategy, EvalPlanQual bug find, and benchmark results. Good technical post.
