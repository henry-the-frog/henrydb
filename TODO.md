## TODO

### Urgent

### Normal
- HenryDB: secondary index + MVCC snapshot after UPDATE (needs HOT chains)
- HenryDB: ALTER TABLE backfill creates duplicate tuples in data file — updateTuple + later UPDATE INSERT both survive checkpoint (heap/buffer-pool interaction bug)

### Low
- RISC-V: general tail call optimization (closures need special handling)
- RISC-V: IIFE pattern (fn(x){x}(5) direct invocation)
- HenryDB: cost-based optimizer improvements
- HenryDB: stored procedures (CREATE FUNCTION/PROCEDURE not yet in parser)
- HenryDB: UNIQUE constraint + SI concurrent transactions (needs unique index locks)
