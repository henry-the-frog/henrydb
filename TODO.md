## TODO

### Urgent

### Normal
- HenryDB: secondary index + MVCC snapshot after UPDATE (needs HOT chains)
- HenryDB: fix file-wal.test.js "uncommitted transactions are NOT recovered" test — test double-logs WAL records (heap.insert auto-logs + manual wal.appendInsert). Need to update test to not double-log.

### Low
- RISC-V: general tail call optimization (closures need special handling)
- RISC-V: IIFE pattern (fn(x){x}(5) direct invocation)
- HenryDB: cost-based optimizer improvements
- HenryDB: stored procedures (CREATE FUNCTION/PROCEDURE not yet in parser)
- HenryDB: UNIQUE constraint + SI concurrent transactions (needs unique index locks)
- HenryDB: INSERT OR REPLACE syntax (SQLite-style) — PostgreSQL ON CONFLICT already works
- HenryDB: aggregates in scalar subqueries in SELECT list (parser limitation)
- HenryDB: atomic checkpoint (current multi-step leaves inconsistent states)
- Neural-net: matrix-depth.test.js -0 vs 0 strict equality, learning tests need seed pinning
