## TODO

### Urgent

### Normal
- HenryDB: secondary index + MVCC snapshot after UPDATE (needs HOT chains)
- HenryDB: committed rows lost after close with uncommitted tx — full redo recovery truncates file, later insert goes to page 1 but only page 0 survives (pre-existing, found Apr 17)

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
