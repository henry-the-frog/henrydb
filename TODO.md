## TODO

### Urgent
- (none)

### Normal
- HenryDB: db.js is 7K+ lines — needs splitting (has duplicate _exprToString methods)
- HenryDB: WAL truncation after checkpoint (WAL grows forever)
- HenryDB: MVCC interception approach is fragile — consider proper visibility function in HeapFile API

### Low
- RISC-V: Liveness-based register allocation
- Neural-net: Architecture exploration (attention, model serialization already done)
- RISC-V: IIFE pattern
- HenryDB: heap page overflow with very large values (>30KB). Need TOAST-style overflow pages.
- HenryDB: Hash-index performance (test takes 24s)
- HenryDB: Parser unification — parseSelectColumn should delegate to parseExpr
- HenryDB: Unified expression walker migration
