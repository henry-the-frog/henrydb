## TODO

### Urgent

### Normal
- HenryDB: FileBackedHeap needs HOT chain support for persistent mode (HeapFile in-memory HOT done, file-backed mode needs adapter)
- HenryDB: VACUUM incremental HOT chain pruning for persistent/file-backed mode

### Low
- HenryDB: btree.js/bplus-tree.js API inconsistency (search vs get) — unify
- RISC-V: Liveness-based register allocation (current linear sequential, low priority)
- Neural-net: Architecture exploration (attention mechanisms already done, model serialization?)
- RISC-V: IIFE pattern (fn(x){x}(5) direct invocation)
- HenryDB: UNIQUE constraint + SI concurrent transactions (needs unique index locks)
- HenryDB: aggregates in scalar subqueries in SELECT list (parser limitation)
- HenryDB: atomic checkpoint (current multi-step leaves inconsistent states)
- HenryDB: heap page overflow with very large values (>30KB). Currently throws error. Need TOAST-style overflow pages.
- HenryDB: checkpoint-explore.test.js expects WAL size=0 after checkpoint but checkpoint writes a CHECKPOINT record (129 bytes)
- HenryDB: consolidate SQL function list (duplicated between parsePrimary and parseSelectColumn)
- Neural-net: model serialization (save/load trained weights)
