# Monkey-lang Type System Investigation (2026-04-25)

## Type System: Hindley-Milner (Algorithm W)

### What Works
- Basic type inference (int, bool, string, null)
- Function types with proper arrow types
- Let-polymorphism: `let id = fn(x) { x }; id(1); id(true)` ✅
- Unification: properly matches and reports mismatches
- Occurs check: `fn(x) { x(x) }` correctly rejected (infinite type)
- Type error detection: `1 + true`, mismatched if/else branches

### What Doesn't Work
- Mutual recursion: forward references unsupported (`a` can call `b` only if `b` is defined first)
- Mixed array types: `[1, "hello"]` should arguably be a type error but passes

### Not Implemented
- Higher-kinded types (no Functor/Monad/Applicative)
- Row polymorphism (for hash types)
- Type classes or traits
- Explicit type annotations (beyond what the inferrer finds)

### Test Coverage Gap
Only 2 tests for the entire type system! This is a major coverage gap.
The type system has real Algorithm W with TVar, TCon, TFun, Scheme, generalize, instantiate, unify.
It deserves 50+ tests covering all the edge cases.

### Hypothesis Results
1. ✅ Polymorphic functions work (let-polymorphism with generalize/instantiate)
2. Partial: Self-recursion works, mutual recursion doesn't (forward refs)
3. ✅ No higher-kinded types
