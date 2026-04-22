# RISC-V Liveness-Based Register Allocation — Design Doc

## Current State
- Linear assignment: s1, s2, s3, ... in order of first `let` statement
- No deallocation: once assigned, register held for entire function
- 11 callee-saved regs available (s1-s11)
- Falls back to stack when exhausted
- Works but wasteful for functions with many short-lived variables

## Proposed: Linear Scan with Liveness Analysis

### Phase 1: Compute Last-Use for Each Variable
For each variable in a function, find the last instruction that reads it.
This can be done with a simple AST walk:

```javascript
function computeLastUse(funcBody) {
  const lastUse = new Map(); // varName → instruction index
  let idx = 0;
  
  function visit(node) {
    if (node.type === 'Identifier') {
      lastUse.set(node.value, idx);
    }
    // recurse...
    idx++;
  }
  visit(funcBody);
  return lastUse;
}
```

### Phase 2: Modified _allocLocal with Free List
```javascript
_allocLocal(name) {
  // Try freed registers first
  if (this.freeRegs.length > 0) {
    const reg = this.freeRegs.pop();
    this.variables.set(name, { type: 'reg', reg, lastUse: this._lastUse.get(name) });
    return { type: 'reg', reg };
  }
  // Then try new registers
  if (this.nextRegIdx < this.availableRegs.length) {
    const reg = this.availableRegs[this.nextRegIdx++];
    this.usedRegs.add(reg);
    this.variables.set(name, { type: 'reg', reg, lastUse: this._lastUse.get(name) });
    return { type: 'reg', reg };
  }
  // Stack fallback
  this.stackOffset += 4;
  const offset = -this.stackOffset;
  this.variables.set(name, { type: 'stack', offset });
  return { type: 'stack', offset };
}
```

### Phase 3: Free Registers After Last Use
After emitting code that uses a variable, check if this is the last use:
```javascript
_emitLoadVar(name) {
  const loc = this._lookupVar(name);
  // ... emit load instruction ...
  
  // Check if this is the last use
  if (loc.type === 'reg' && this._currentIdx >= (loc.lastUse || Infinity)) {
    this.freeRegs.push(loc.reg);
  }
}
```

### Complications
1. **Control flow**: In `if/else` and loops, last-use depends on which branch is taken.
   For correctness: use the LATEST use across all branches.
2. **Closures**: Variables captured in closures are live indefinitely.
   For correctness: never free closure-captured variables.
3. **Recursive calls**: Variables read after a recursive call must be preserved.
   This is already handled by callee-saved convention.

### Simpler First Step
Before full liveness analysis, implement **linear scan with explicit frees**:
- When processing a `let` binding, if the RHS is the last use of another variable,
  free that variable's register immediately.
- This catches the common pattern: `let y = f(x)` where x is dead after this line.

### Expected Impact
- Function with 20 `let` bindings, 5 max live: 0 spills (currently 9)
- Average function: 2-3 fewer spills
- Benchmark: fibonacci, quicksort, tree traversal with many local variables

## Existing Liveness Analysis in Monkey-lang

monkey-lang already has:
- `liveness.js` (237 LOC): backward dataflow, fixed-point iteration
- `regalloc.js` (198 LOC): register allocation using liveness info
- `ssa.js` (300 LOC): SSA form construction

These are working and tested (all pass). Options:
1. Import directly from monkey-lang into RISC-V codegen
2. Adapt the algorithm (simpler: no SSA needed for simple linear scan)

The liveness algorithm:
- liveIn(B) = use(B) ∪ (liveOut(B) \ def(B))
- liveOut(B) = ∪{liveIn(S) | S ∈ succs(B)}
- Iterate backward over CFG until fixed point
