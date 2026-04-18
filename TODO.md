## TODO

### Urgent

### Normal
- HenryDB: FileBackedHeap needs HOT chain support (currently falls back to index update)
- HenryDB: VACUUM incremental HOT chain pruning (currently rebuilds all indexes)
- HenryDB: System catalog tables (pg_class, pg_attribute) instead of catalog.json

### Low
- HenryDB: btree.js/bplus-tree.js API inconsistency (search vs get) — unify
- RISC-V: Liveness-based register allocation (current linear sequential, low priority)
- Neural-net: Architecture exploration (attention mechanisms, new optimizers)
- RISC-V: IIFE pattern (fn(x){x}(5) direct invocation)
- HenryDB: UNIQUE constraint + SI concurrent transactions (needs unique index locks)
- HenryDB: aggregates in scalar subqueries in SELECT list (parser limitation)
- HenryDB: atomic checkpoint (current multi-step leaves inconsistent states)
- Neural-net: matrix-depth.test.js -0 vs 0 strict equality, learning tests need seed pinning
- HenryDB: heap page overflow with very large values (>30KB). Currently throws error. Need TOAST-style overflow pages.
- HenryDB: checkpoint-explore.test.js expects WAL size=0 after checkpoint but checkpoint writes a CHECKPOINT record (129 bytes)
