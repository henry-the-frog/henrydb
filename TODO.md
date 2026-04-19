## TODO

### Urgent

### Normal
- HenryDB: VACUUM incremental HOT chain pruning for persistent/file-backed mode (metadata persistence)
- HenryDB: Savepoint ROLLBACK TO bug — undo log + physical row removal too aggressive (removes rows instead of restoring)
- HenryDB: server-json.test.js was flaky (intermittent hang — COPY TO connection issue?)

### Low
- HenryDB: btree.js/bplus-tree.js API inconsistency (search vs get) — unify
- RISC-V: Liveness-based register allocation (current linear sequential, low priority)
- Neural-net: Architecture exploration (attention, model serialization already done)
- RISC-V: IIFE pattern (fn(x){x}(5) direct invocation)
- HenryDB: heap page overflow with very large values (>30KB). Need TOAST-style overflow pages.
- Neural-net: training checkpoints / early stopping improvements
- HenryDB: Parser unification — parseSelectColumn should delegate to parseExpr (risky, deferred)
- HenryDB: Unified expression walker migration — use expr-walker.js for _evalGroupExpr and _evalAggregateExpr
- HenryDB: Hash-index performance (timeout in 1 test)
