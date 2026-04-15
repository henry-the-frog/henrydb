# Monkey-lang → RISC-V Backend

**Created:** 2026-04-15 Session B | **Uses:** 1

## Architecture
- Monkey source → Parse (AST) → RiscVCodeGen → Assembly text → Assembler → Machine code → CPU emulator
- Stack-based code generation (all intermediate values on RISC-V stack)
- Optional: register allocation (s1-s11 callee-saved for locals) + peephole optimization

## Files (projects/riscv-emulator/src/)
- `monkey-codegen.js` — AST → RISC-V assembly
- `riscv-peephole.js` — Post-pass optimizer (5 patterns)
- `monkey-codegen.test.js` — 51 codegen tests
- `riscv-peephole.test.js` — 26 optimizer tests
- `riscv-regalloc.test.js` — 33 register allocation tests
- `pipeline-integration.test.js` — 8 pipeline analysis tests
- `stress-test.test.js` — 84 stress/edge case tests
- `heap-arrays.test.js` — 25 heap allocation tests
- Total: **227 tests**

## Key Decisions
1. **Stack-based codegen** (not register-based). Simpler, fewer bugs. Register optimization as optional pass.
2. **`li` pseudo-instruction** for all integer literals. DO NOT use manual lui+addi — the assembler's lui takes pre-shifted immediates.
3. **Frame layout**: `s0-4`=ra, `s0-8`=old_s0, `s0-12` onwards=locals. Stack grows down.
4. **Bump allocator**: gp register (x3) = heap pointer. Arrays on heap: `[length][elem0][elem1]...`.
5. **Functions**: Callee-saved registers. Parameters in a0-a7. Functions compiled as deferred bodies after main.

## Bugs Found
- **Stack frame collision**: ra saved at same offset as first local. Fix: reserve 8 bytes for ra+s0 before locals.
- **Large integer encoding**: Manual lui+addi encoded wrong immediate. Fix: use `li` pseudo.
- **Pipeline ecall timing**: PipelineCPU executes ecalls incorrectly (timing issue with pipeline stages). Not fixable without pipeline modifications.

## Performance Numbers
- fib(10): 5845 cycles (stack) → 5490 (reg+peep, 6% faster)
- Pipeline IPC: 0.97 (reg mode) vs 0.90 (stack mode)
- Register mode eliminates load-use stalls
- 100% IC hit rate in monkey-lang VM (separate from RISC-V backend)
