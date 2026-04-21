## TODO

### Urgent
(none)

### Normal
- HenryDB: 3 Volcano operators partially wired (Window/Union/RecursiveCTE remain — CTE, MergeJoin, IndexScan done) (since 2026-04-20, updated 2026-04-21)
- HenryDB: db.js now at 5043 lines — continue extraction (join execution ~220 LOC, EXPLAIN ~350 LOC, _selectWithGroupBy ~166 LOC) (since 2026-04-20, updated 2026-04-21)
- HenryDB: MVCC interception — MVCCHeap wrapper DONE, but findByPK still falls back to full scan. (since 2026-04-20, updated 2026-04-21)
- HenryDB: Volcano planner needs cost model — currently no cost-based decisions (uses heuristics only) (since 2026-04-21)

### Low
- RISC-V: Liveness-based register allocation
- Neural-net: Architecture exploration (attention, model serialization already done)
- RISC-V: IIFE pattern
- HenryDB: heap page overflow with very large values (>30KB). Need TOAST-style overflow pages.
- HenryDB: Hash-index performance (test takes 24s)
- HenryDB: Parser unification — parseSelectColumn should delegate to parseExpr
- HenryDB: Unified expression walker migration
