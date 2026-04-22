## TODO

### Urgent
(none)

### Normal
- HenryDB: Volcano engine COMPLETE — all operators wired. Next: wire into db.js executor (P0) (since 2026-04-20, updated 2026-04-21)
- HenryDB: db.js now at 4939 lines (SUB-5000 achieved!) — further extraction possible (join exec ~220, EXPLAIN ~350, GROUP BY ~166) (since 2026-04-20, updated 2026-04-21)
- HenryDB: MVCC interception — MVCCHeap wrapper DONE, but findByPK still falls back to full scan. (since 2026-04-20, updated 2026-04-21)
- HenryDB: Volcano planner fully cost-based. Consider integrating table stats from ANALYZE. (since 2026-04-21, updated 2026-04-21)

### Low
- RISC-V: Liveness-based register allocation
- Neural-net: Architecture exploration (attention, model serialization already done)
- RISC-V: IIFE pattern
- HenryDB: heap page overflow with very large values (>30KB). Need TOAST-style overflow pages.
- HenryDB: Hash-index performance (test takes 24s)
- HenryDB: Parser unification — parseSelectColumn should delegate to parseExpr
- HenryDB: Unified expression walker migration
