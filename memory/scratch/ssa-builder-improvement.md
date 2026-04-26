# SSA Builder Improvement Plan

## Current Limitation
The SSA builder stores complex expressions (if, while, function calls) as toString() strings.
This prevents:
1. SCCP from analyzing constants behind if-branches (both-branches-same-value case)
2. SSA DCE from tracking references inside complex expressions (needs regex fallback)
3. Proper interprocedural analysis

## Proposed Fix
Refactor `_renameExpr` to return structured SSA expressions instead of strings.

### SSA Expression Types
```javascript
class SSAExpr {
  // Leaf types
  static Const(value) { return { tag: 'const', value }; }
  static Var(name) { return { tag: 'var', name }; }
  
  // Binary operations
  static BinOp(op, left, right) { return { tag: 'binop', op, left, right }; }
  
  // Control flow (if/else)
  static If(cond, then, else_) { return { tag: 'if', cond, then, else_ }; }
  
  // Function call
  static Call(name, args) { return { tag: 'call', name, args }; }
}
```

### Benefits
1. SCCP can analyze if-branches: if both produce Const(42) → Const(42)
2. DCE can traverse references without regex
3. SSA→bytecode optimization becomes possible (dead expression elimination)

### Estimated Work
~200 LOC refactor of `_renameExpr` in ssa.js + update SCCP and SSA-DCE to use structured expressions.

### Risk
The SSA builder handles many AST node types. Each needs a structured mapping. Missing a case would cause failures in edge cases.

### Priority
Medium. Current regex-based approach works for most cases. The fix would improve analysis quality but the existing pipeline produces correct results.
