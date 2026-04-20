## TODO

### Urgent
- ~~HenryDB: Division always truncates to integer — FIXED: tokenizer now marks decimal literals with isFloat, division checks AST~~ (since 2026-04-20, fixed 2026-04-20)
- HenryDB: Planner-executor disconnect — RESOLVED: hash join IS used via _hashJoin at db.js:4047. Verified 5Kx5K in 32ms. Remove this item. (since 2026-04-20, resolved 2026-04-20)
- ~~HenryDB: NULL IS NULL returns string "NULL" not boolean TRUE — FIXED: parser now handles IS NULL after literal tokens~~ (since 2026-04-20, fixed 2026-04-20)

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
