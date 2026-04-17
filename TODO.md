## TODO

### Urgent

### Normal
- HenryDB: VACUUM/GC correctness — do dead tuples get cleaned up? Does VACUUM interact with MVCC snapshots correctly?
- HenryDB: trigger/constraint enforcement + crash recovery interaction
- SAT Solver: SMT theory combination (QF_LIA + EUF) — test with mixed integer+uninterpreted function constraints
- Monkey Lang: GC correctness under stress — does the GC collect unreachable objects without collecting reachable ones?

### Low
- RISC-V: general tail call optimization (closures need special handling)
- RISC-V: IIFE pattern (fn(x){x}(5) direct invocation)
- HenryDB: cost-based optimizer improvements
- Neural-net: Network.toJSON/fromJSON for all layer types (KAN, MoE, etc.)
- Neural-net: mixed precision / numerical stability audit
