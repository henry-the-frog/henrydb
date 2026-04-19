## TODO

### Urgent

### Normal
- HenryDB: VACUUM incremental HOT chain pruning for persistent/file-backed mode (metadata persistence)
- Neural-net: 8 flaky test failures (scalar multiply by zero, XOR/AND/OR/linear learning) — investigate and fix (found 2026-04-19)
- HenryDB: ALTER TABLE ADD COLUMN NOT NULL with DEFAULT doesn't backfill existing rows (found 2026-04-18)
- HenryDB: INSERT INTO table (col) SELECT ... maps by position, ignores column list (found 2026-04-18)

### Low
- HenryDB: btree.js/bplus-tree.js API inconsistency (search vs get) — unify
- RISC-V: Liveness-based register allocation (current linear sequential, low priority)
- Neural-net: Architecture exploration (attention, model serialization already done)
- RISC-V: IIFE pattern (fn(x){x}(5) direct invocation)
- HenryDB: UNIQUE constraint + SI concurrent transactions (needs unique index locks)
- HenryDB: aggregates in scalar subqueries in SELECT list (parser limitation)
- HenryDB: atomic checkpoint (current multi-step leaves inconsistent states)
- HenryDB: heap page overflow with very large values (>30KB). Need TOAST-style overflow pages.
- HenryDB: checkpoint-explore.test.js expects WAL size=0 after checkpoint
- Neural-net: training checkpoints / early stopping improvements
- HenryDB: pg_stat_statements query normalization (parametrize literals for better grouping)
- HenryDB: CHECK constraint with multi-column expression (low < high) parser issues (found 2026-04-18)
