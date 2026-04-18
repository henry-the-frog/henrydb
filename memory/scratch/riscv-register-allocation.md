# RISC-V Codegen Register Allocation Study

uses: 0
created: 2026-04-18
tags: riscv, compiler, register-allocation, codegen

## Current Approach: Linear Sequential

Variables assigned registers sequentially (s1, s2, ..., s11), overflow to stack.

### Strengths
- Simple, correct, predictable
- Callee-saved registers survive function calls (no save/restore around calls)
- Stack fallback means never fails

### Weaknesses
- No liveness analysis — dead variables waste registers
- No spill/reload — once a register is allocated, it's never freed
- Parameters are moved from a-regs to s-regs even if only read once
- No prioritization (hot variables vs cold)

## Variable Locations
```
{ type: 'reg', reg: 's1' }      — callee-saved register
{ type: 'stack', offset: -4 }    — stack slot relative to s0 (frame pointer)
{ type: 'func', label: 'foo' }   — known function (resolved at link time)
```

## Available Registers
RISC-V has s0-s11 callee-saved, but s0 is frame pointer. So s1-s11 = 11 registers.
The codegen uses `this.availableRegs` which is typically `['s1', 's2', ..., 's11']`.

## Improvement Ideas (not built, just notes)

### 1. Liveness Analysis
- Track which variables are live at each point
- When a variable dies, its register becomes available for reuse
- Biggest win for functions with many sequential let bindings

### 2. Parameter Forwarding
- If `return f(x)` where x came from parameter a0, don't move a0→s1→a0
- Keep parameters in a-registers when possible
- This is actually already handled by TCO (args stay in a-regs)

### 3. Graph Coloring
- Build interference graph (two variables interfere if live at same time)
- Color with K colors (K = number of available registers)
- Spill lowest-priority variables to stack
- Classic Chaitin algorithm

## Verdict
Current approach is good enough for Monkey. The language doesn't have enough local variables per function to exhaust 11 registers. Graph coloring would be interesting to implement but ROI is low.
