## TODO

### Urgent
(none)

### Normal
- HenryDB: 6 Volcano operators partially wired (IndexScan done, Window/CTE/RecursiveCTE/Union/MergeJoin remain) (since 2026-04-20, updated 2026-04-21)
- HenryDB: db.js still 8247 lines — continue extraction (DML handlers, expression evaluator) (since 2026-04-20, updated 2026-04-21)
- HenryDB: MVCC interception via heap monkey-patching — 5 fragility risks. findByPK falls back to full scan. (since 2026-04-20)
- HenryDB: Volcano planner needs cost model — currently no cost-based decisions (uses heuristics only) (since 2026-04-21)

### Low
- RISC-V: Liveness-based register allocation
- Neural-net: Architecture exploration (attention, model serialization already done)
- RISC-V: IIFE pattern
- HenryDB: heap page overflow with very large values (>30KB). Need TOAST-style overflow pages.
- HenryDB: Hash-index performance (test takes 24s)
- HenryDB: Parser unification — parseSelectColumn should delegate to parseExpr
- HenryDB: Unified expression walker migration
