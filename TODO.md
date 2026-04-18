## TODO

### Urgent

### Normal
- HenryDB: VACUUM should prune HOT chains and update index entries (currently chains grow unbounded)
- HenryDB: FileBackedHeap needs HOT chain support (currently falls back to index update)

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

### Low
- HenryDB: checkpoint-explore.test.js expects WAL size=0 after checkpoint but checkpoint writes a CHECKPOINT record (129 bytes). Pre-existing test expectation mismatch.
- HenryDB: 4 AUTOINCREMENT tests in sequence-depth.test.js fail (pre-existing)
