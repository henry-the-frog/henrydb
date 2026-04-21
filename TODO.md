## TODO

### Urgent
- **HenryDB: Lost update bug — write-write conflict not detected in MVCC** (since 2026-04-20). Two concurrent txns can update same row without error. T2's commit silently lost.

### Normal
- HenryDB: Compiled query engine silent-null correctness bug — returns all rows when filter can't compile (latent, only EXPLAIN COMPILED path) (since 2026-04-20)
- HenryDB: Dual cost model divergence — planTree uses generic defaults (1/3, 1/10), analysis uses real histograms (since 2026-04-20)
- HenryDB: 6 Volcano operators defined but unwired (Union, Window, CTE, RecursiveCTE, MergeJoin, IndexScan) (since 2026-04-20)
- HenryDB: db.js is 9844 lines — needs splitting. Duplicate _analyzeTable method (L1807 dead code) (since 2026-04-20)
- HenryDB: PersistentDB missing WAL checkpoint/truncation (WAL grows forever). TransactionalDB works correctly. (since 2026-04-20)
- HenryDB: MVCC interception via heap monkey-patching — 5 fragility risks. findByPK falls back to full scan. (since 2026-04-20)

### Low
- RISC-V: Liveness-based register allocation
- Neural-net: Architecture exploration (attention, model serialization already done)
- RISC-V: IIFE pattern
- HenryDB: heap page overflow with very large values (>30KB). Need TOAST-style overflow pages.
- HenryDB: Hash-index performance (test takes 24s)
- HenryDB: Parser unification — parseSelectColumn should delegate to parseExpr
- HenryDB: Unified expression walker migration
