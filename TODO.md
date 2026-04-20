## TODO

### Urgent
- HenryDB: Correlated subqueries don't resolve unqualified outer column references (since 2026-04-20)

### Normal
- HenryDB: db.js is 7K+ lines — needs splitting (has duplicate _exprToString methods)
- HenryDB: WAL truncation after checkpoint (WAL grows forever)
- HenryDB: MVCC interception approach is fragile — consider proper visibility function in HeapFile API
- HenryDB: Integration test (e-commerce scenario) — verify after naming fix

### Low
- RISC-V: Liveness-based register allocation
- Neural-net: Architecture exploration (attention, model serialization already done)
- RISC-V: IIFE pattern
- HenryDB: heap page overflow with very large values (>30KB). Need TOAST-style overflow pages.
- HenryDB: Hash-index performance (test takes 24s)
- HenryDB: Parser unification — parseSelectColumn should delegate to parseExpr
- HenryDB: Unified expression walker migration
