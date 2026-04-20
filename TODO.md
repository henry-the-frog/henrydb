## TODO

### Urgent

### Urgent
- (none)

### Normal
- HenryDB: ALTER TABLE not persisted — schema changes lost on close/reopen (no WAL record for ALTER)
- HenryDB: VACUUM incremental HOT chain pruning for persistent/file-backed mode (metadata persistence)
- HenryDB: SSI false positives on disjoint-row UPDATEs (need index-backed UPDATE or deferred read recording)
- HenryDB: Savepoint isolation bug — concurrent sessions with savepoints don't see correct row counts
- HenryDB: Integration test (e-commerce scenario) returns 8 rows instead of 10



### Low
- RISC-V: Liveness-based register allocation (current linear sequential, low priority)
- Neural-net: Architecture exploration (attention, model serialization already done)
- RISC-V: IIFE pattern (fn(x){x}(5) direct invocation)
- HenryDB: heap page overflow with very large values (>30KB). Need TOAST-style overflow pages.
- Neural-net: training checkpoints / early stopping improvements
- HenryDB: Parser unification — parseSelectColumn should delegate to parseExpr (risky, deferred)
- HenryDB: Unified expression walker migration — use expr-walker.js for _evalGroupExpr and _evalAggregateExpr
- HenryDB: Hash-index performance (test takes 24s, needs optimization not bug fix)
