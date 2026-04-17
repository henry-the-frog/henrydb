# Integration Boundary Testing

A repeatable methodology for finding bugs at the boundaries between independently-built subsystems.

## Core Insight

Every subsystem passes its own unit tests. The bugs are in the **CONTRACT** between subsystems — the implicit assumptions about data flow, type compatibility, and state management that exist only in the developer's mental model.

## Bug Taxonomy (from April 17, 2026 — 21 bugs)

### Type 1: Feature Exists But Not Wired Up (9/21 bugs)
The most common pattern. A subsystem provides hooks, interfaces, or processing steps that another subsystem never calls.

**Examples:**
- HenryDB: BEGIN never set txId that WAL needed
- HenryDB: MVCC read() never used the snapshot it computed
- HenryDB: _fireTriggers() never called for UPDATE/DELETE
- HenryDB: SSI recordRead/recordWrite hooks never called by base class
- HenryDB: View cache invalidation didn't track view→table dependencies
- SAT Solver: parseSmtExpr() existed but _processAssertion() never called it

**How to find:** For each pair of subsystems that share a concept (txId, snapshot, hooks), trace the data flow FROM one TO the other. If there's no call path, there's a bug.

### Type 2: Type Mismatch at Interface (5/21 bugs)
One subsystem outputs type A, another expects type B.

**Examples:**
- HenryDB: SSI commit() received MVCCTransaction object but expected number
- SAT Solver: assert() stored strings but _processAssertion() expected arrays
- Neural-net: cutmix used Matrix.scale() which doesn't exist
- Neural-net: pruning returned fake Matrix-like objects instead of real Matrix
- Neural-net: structuredPrune used Array-of-Arrays iteration on Matrix

**How to find:** Check every function that crosses a module boundary. Does the caller's output type match the callee's expected input type?

### Type 3: Shared State Not Updated (4/21 bugs)
Two subsystems share state (caches, version chains, weight gradients) but only one updates it.

**Examples:**
- HenryDB: MVCC write() had no write-write conflict detection
- Neural-net: MoE backward overwrote (not accumulated) expert gradients across batch
- Neural-net: CapsuleLayer backward applied weight updates inline
- Neural-net: NeuralODELayer backward never updated adjoint variable

**How to find:** For each shared data structure, verify that every writer updates it correctly and every reader sees the latest state.

### Type 4: Missing Integration Code Path (3/21 bugs)
Feature A and Feature B each work alone, but using them together hits an untested code path.

**Examples:**
- HenryDB: GROUP BY + window function (window processing path not called from GROUP BY path)
- Neural-net: Trigger INSERT with NEW.column (trigger body not parameterized)
- Neural-net: autograd mseLoss with Variable targets (type confusion)

**How to find:** For every pair of features, try using them together. The SQL pattern "SELECT feat_A, feat_B FROM t GROUP BY feat_A" tests many combinations.

## Repeatable Process

1. **Identify subsystem pairs** — list every pair of modules that share a concept or data flow
2. **For each pair, write a "contract test"** — a test that exercises the data flow FROM one TO the other
3. **Use the most natural API** — call functions the way a user would, not the way unit tests do
4. **Test feature combinations** — use Feature A and Feature B together in one operation
5. **Check shared state** — verify caches, version chains, hooks are properly maintained
6. **Type-check at boundaries** — verify output types match expected input types

## High-Value Test Patterns

- **Gradient check** (forward→backward contract): checkLayerGradients(layer, input)
- **Crash recovery** (transaction→WAL contract): BEGIN; INSERT; crash; recover; check
- **MVCC isolation** (snapshot→read contract): T1.begin, T2.begin, T1.write, T1.commit, T2.read
- **Feature combo** (A+B integration): GROUP BY + window, CTE + window, trigger + INSERT
- **Serialization roundtrip** (save→load contract): obj.toJSON() → fromJSON() → predict == same
- **Batch independence** (batch→individual contract): batch(3) == individual(1) + individual(1) + individual(1)
