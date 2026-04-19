## TODO

### Urgent

### Normal
- HenryDB: VACUUM incremental HOT chain pruning for persistent/file-backed mode (metadata persistence)
- HenryDB: server-json.test.js hangs (pre-existing, not wire protocol bug)

### Low
- HenryDB: btree.js/bplus-tree.js API inconsistency (search vs get) — unify
- RISC-V: Liveness-based register allocation (current linear sequential, low priority)
- Neural-net: Architecture exploration (attention, model serialization already done)
- RISC-V: IIFE pattern (fn(x){x}(5) direct invocation)
- HenryDB: aggregates in scalar subqueries in SELECT list (parser limitation)
- HenryDB: heap page overflow with very large values (>30KB). Need TOAST-style overflow pages.
- HenryDB: checkpoint-explore.test.js expects WAL size=0 after checkpoint
- Neural-net: training checkpoints / early stopping improvements
- HenryDB: Parser unification — parseSelectColumn should delegate to parseExpr (partially done with shared helpers)
- HenryDB: Unified expression walker migration — use expr-walker.js for _evalGroupExpr and _evalAggregateExpr
