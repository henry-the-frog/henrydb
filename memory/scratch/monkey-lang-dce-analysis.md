# Monkey-Lang Dead Code Elimination Analysis

## Current Compilation Pipeline
```
Source → Lexer → Parser → AST
  → [AST] Constant Substitution
  → [AST] Escape Analysis  
  → [AST] Per-Function SSA (diagnostic only)
  → Compiler.compile() → bytecode
  → [BC] Tail Call Optimization
  → [BC] Escape Annotation
  → [BC] Peephole Optimizer
  → Final Bytecode → VM
```

## Where DCE Should Go

### Option A: AST-Level DCE (before compilation)
- **Pro**: Simpler, works on familiar AST structures
- **Con**: Limited — can only remove obvious dead code (after unconditional return, unreachable else)
- **Implementation**: Walk AST, remove statements after return, fold constant if-else branches
- **Effort**: ~100 LOC, low risk

### Option B: SSA-Level DCE (between SSA analysis and compilation)
- **Pro**: Most powerful — SSA makes dead defs trivially identifiable
- **Con**: Need to convert SSA back to AST or directly compile from SSA
- **Implementation**: Mark/sweep on SSA graph — mark all used values, sweep unmarked
- **Effort**: ~200 LOC for the analysis, but compiling from SSA is a major undertaking
- **Alternative**: Use SSA analysis to annotate AST nodes as dead, then skip during compilation

### Option C: Bytecode-Level DCE (extend peephole optimizer)
- **Pro**: Already have optimizer infrastructure, just add another pass
- **Con**: Working at bytecode level is harder than AST
- **Implementation**: Build basic block graph, mark reachable blocks, remove unreachable
- **Effort**: ~150 LOC, medium risk

## Recommendation
**Option B with annotation approach**: 
1. Run SSA analysis (already done)
2. Build def-use chains from SSA
3. Annotate AST nodes with "dead" flag
4. Compiler skips nodes flagged as dead

This gives us SSA-quality DCE without needing to compile from SSA form.

## Prerequisites
- perFunctionSSA() is working (Session B fixed the OOM)
- SSA builder produces proper phi nodes
- Need: def-use chain construction from SSA output
- Need: mapping from SSA variables back to AST nodes

## Impact Assessment
- **Constant folding already eliminates many dead branches** (if true → else is dead)
- **Tail call optimization removes dead return instructions**
- **Main remaining wins**: unused variables, dead assignments, unreachable branches from complex conditions
- **Estimated improvement**: 5-15% bytecode size reduction on typical programs
