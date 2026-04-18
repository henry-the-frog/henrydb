## TODO

### Urgent

### Normal
- HenryDB: secondary index + MVCC snapshot after UPDATE (needs HOT chains)
- HenryDB: ALTER TABLE backfill creates duplicate tuples in data file — updateTuple + later UPDATE INSERT both survive checkpoint (heap/buffer-pool interaction bug)
- HenryDB: DDL lifecycle integration test suite — test every DDL op through full lifecycle (execute, persist, crash recover, concurrent with DML)

### Low
- RISC-V: general tail call optimization (closures need special handling)
- RISC-V: IIFE pattern (fn(x){x}(5) direct invocation)
- HenryDB: cost-based optimizer improvements
- HenryDB: stored procedures (CREATE FUNCTION/PROCEDURE not yet in parser)
- HenryDB: UNIQUE constraint + SI concurrent transactions (needs unique index locks)
- HenryDB: UPSERT (INSERT OR REPLACE / ON CONFLICT) — only missing SQL feature
- HenryDB: aggregates in scalar subqueries in SELECT list (parser limitation)
- HenryDB: atomic checkpoint (current multi-step leaves inconsistent states)
