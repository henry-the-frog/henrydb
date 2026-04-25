# Monkey-lang SSA + Optimization Pipeline (2026-04-25)

## Pipeline Architecture
Source → Parse → TypeCheck → DCE (AST level) → CFG → SSA → ConstProp → Liveness → Escape

## What Works
1. **CFG Construction**: Builds control flow graph with basic blocks
2. **SSA Conversion**: Phi nodes at join points (Cytron et al. 1991)
3. **Constant Propagation**: Folds arithmetic at compile time
   - `let a = 10 + 20` → `a_0 = 30`
   - `let b = a * 3` → `b_0 = 90` (propagated through `a`)
   - 2 iterations to reach fixpoint
4. **Dead Code Elimination**: Finds unused variables
   - `let unused = 100; result` → `unused` is dead
5. **Liveness Analysis**: Builds interference graph for register allocation
6. **Escape Analysis**: Classifies variables as stack/heap
   - Simple locals → stack allocatable
   - Closures that escape → heap required

## Key Observation
The pipeline analysis is real and correct, but results are NOT applied to code generation.
The VM still:
- Evaluates constant expressions at runtime
- Allocates all closures on heap
- Doesn't skip dead code

## Opportunity
Wire pipeline results into the compiler:
1. Constant folding: emit `OpConstant` for propagated constants
2. Dead code elimination: skip emission of dead assignments
3. Stack allocation: use `OpStackClosure` for non-escaping closures

## Test Coverage
- `src/pipeline.test.js`: exists but sparse
- `src/ssa.test.js`: tests SSA construction
- `src/const-prop.test.js`: tests constant propagation
- Each module has basic tests but no integration tests for "does optimization change execution?"
