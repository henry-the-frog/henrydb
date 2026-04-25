# Architecture Comparison: monkey-lang VM vs lisp interpreter

## Implementation Spectrum

| Feature | lisp (199 LOC) | monkey-lang (22K LOC) |
|---------|----------------|----------------------|
| Execution | Tree-walking | Bytecode VM |
| Parsing | Recursive descent on S-exprs | Pratt parser on C-like syntax |
| Types | Dynamic (numbers, strings, lists) | Dynamic + optional typechecker |
| Closures | ✓ (Env chaining) | ✓ (Free variables, cells) |
| Tail calls | ✗ | ✓ (OpTailCall reuses frame) |
| GC | None (JS GC) | Generational mark-sweep |
| Optimization | None | Constant folding, escape analysis, SSA, peephole |
| Pattern matching | ✗ | ✓ (match expression) |
| Module system | ✗ | ✓ (import/export) |
| REPL | Not built-in | Full REPL with history |
| Speed | Baseline | 3-6x faster (VM) |

## What Each Can Learn

### Lisp → monkey-lang:
1. **Homoiconicity**: Lisp code is data. monkey-lang can't manipulate its own AST at runtime.
   - Could add: `quote`, `eval`, `macro` — monkey-lang has the runtime but not the syntax
2. **Simplicity**: 199 LOC vs 22K LOC. The lisp is easier to understand and modify.
3. **S-expression parsing**: Zero ambiguity. monkey-lang's parser is 1079 LOC; lisp's is 25 LOC.

### monkey-lang → lisp:
1. **Variadic builtins**: `(+ 1 2 3)` should work (currently broken — only uses first 2 args)
2. **Bytecode compilation**: The tree-walking approach limits performance
3. **Tail call optimization**: Essential for idiomatic Scheme/Lisp
4. **Type checking**: Even optional types catch bugs early
5. **Module system**: Real programs need code organization

## Key Architectural Insight
The lisp's `Env` class (parent chaining) is the same closure mechanism as monkey-lang's free variables + cells, just implemented differently:
- **Lisp**: Runtime scope chain lookup (walk parent pointers)
- **monkey-lang**: Compile-time free variable capture (resolved at compile time, stored in closure)

The monkey-lang approach is faster (O(1) vs O(n) depth lookup) but more complex to implement.

## Cross-Pollination Opportunities
1. Add `eval` to monkey-lang (parse string → AST → compile → execute)
2. Add variadic arithmetic to lisp (reduce over args)
3. Port monkey-lang's prelude HOFs to lisp (map already exists)
4. Add tail call optimization to lisp (simple: detect call in tail position)
