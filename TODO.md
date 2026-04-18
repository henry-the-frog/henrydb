## TODO

### Urgent

### Normal
- HenryDB: PG wire protocol (~200 lines, enables psql/pg client connection)
- HenryDB: Persistent catalog (schema survives restart without catalog.json)
- HenryDB: FileBackedHeap needs HOT chain support (currently falls back to index update)
- HenryDB: VACUUM incremental HOT chain pruning (currently rebuilds all indexes)

### Low
- HenryDB: btree.js/bplus-tree.js API inconsistency (search vs get) — unify
- RISC-V: Liveness-based register allocation (current linear sequential, low priority)
- Neural-net: Architecture exploration (attention mechanisms, new optimizers)
- RISC-V: IIFE pattern (fn(x){x}(5) direct invocation)
- HenryDB: UNIQUE constraint + SI concurrent transactions (needs unique index locks)
- HenryDB: INSERT OR REPLACE syntax (SQLite-style) — PostgreSQL ON CONFLICT already works
- HenryDB: aggregates in scalar subqueries in SELECT list (parser limitation)
- HenryDB: atomic checkpoint (current multi-step leaves inconsistent states)
- Neural-net: matrix-depth.test.js -0 vs 0 strict equality, learning tests need seed pinning
- HenryDB: checkpoint-explore.test.js expects WAL size=0 after checkpoint but checkpoint writes a CHECKPOINT record (129 bytes). Pre-existing test expectation mismatch.
