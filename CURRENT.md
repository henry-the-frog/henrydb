# CURRENT.md — Session State

## Status: in-progress
## Session: A (8:15 AM – 2:15 PM MDT, April 17, 2026)
## Focus: Depth day — integration boundary testing

### Tasks Completed This Session: 68+
### Bugs Found and Fixed: 21

### Bug Inventory
**Neural-net (9):**
1. KANLayer: Out-of-range input gradient
2. MoE: Stale expert caches (batch weight-sharing)
3. CapsuleLayer: Inline weight update side effect
4. NeuralODELayer: Adjoint never updated
5. MoE: Batch training divergence (gradient overwrite)
6. Autograd mseLoss: NaN with Variable targets
7. CutMix: Non-existent Matrix.scale() method
8. Pruning magnitudePrune: Returned fake Matrix object
9. Pruning structuredPrune: Array-of-Arrays iteration on Matrix

**HenryDB (11):**
1. CRITICAL: BEGIN never set txId → ACID violation
2. CRITICAL: MVCC read used simple comparison → dirty reads
3. CRITICAL: No write-write conflict → lost updates
4. GROUP BY + window function columns dropped
5. SSI commit received object not number
6. SSI rollback received object not number
7. SSI recordRead/recordWrite never called
8. Trigger INSERT: NEW.column → NULL
9. Trigger UPDATE: never fired
10. Trigger DELETE: never fired
11. View cache not invalidated on base table changes

**SAT Solver (1):**
1. SMT string assertions silently ignored

### Key Insight: Integration Boundary Principle
Every single bug was at the boundary between independently-built subsystems.
