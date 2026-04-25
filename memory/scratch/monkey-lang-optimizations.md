# Monkey-Lang Optimization Pipeline

## Level 1: AST Optimizations (compile time)
1. **Constant Substitution** (`const-subst.js`)
   - Propagate known constant values through variables
   - Handles `let x = 42; y = x + 1` → `y = 43`
   - Tracks mutated variables via `removeMutated()` (Session B bug fix)

2. **Constant Folding** (`Compiler.foldConstants()`)
   - Evaluate constant arithmetic at compile time
   - `2 + 3` → `5` directly in the AST

## Level 2: Analysis Passes (compile time)
3. **Escape Analysis** (`escape.js`)
   - Determines which closures escape their defining scope
   - Non-escaping closures skip GC tracking (Session B)
   - Fixed: anonymous FunctionLiteral + implicit return handling

4. **Per-Function SSA** (`ssa.js`)
   - Build CFG per function body
   - Transform to Static Single Assignment form
   - Currently diagnostic only — not yet used for optimization
   - Enables: dead code elimination, copy propagation

## Level 3: Bytecode Optimizations (post-compilation)
5. **Peephole Optimizer** (`optimizer.js`, 247 LOC)
   - Dead store elimination
   - Constant propagation at bytecode level
   - Redundant pop-push removal
   - Unreachable code removal
   - Jump optimization

## Level 4: VM Runtime Optimizations
6. **Inline Caching** (hidden shapes)
   - Property access memoized via shape chains
   - First access: slow path + cache
   - Subsequent access: fast path from cache

7. **Integer Cache** (`cachedInteger()`)
   - Small integers (-128 to 255) pre-allocated
   - Avoids allocation for common values

8. **String Interning** (`internString()`)
   - Common strings reused from pool
   - Reduces memory + speeds comparisons

## Potential Future Optimizations
- Dead code elimination via SSA
- Loop-invariant code motion
- Function inlining for small functions
- Register allocation improvements
- Tail call optimization in VM
