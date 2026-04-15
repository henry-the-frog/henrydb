# Monkey-lang → RISC-V Backend

**Created:** 2026-04-15 Session B | **Last updated:** 2026-04-15 Session B

## Architecture
- Monkey source → Parse (AST) → Type Inference → RiscVCodeGen → Assembly text → Assembler → Machine code → CPU emulator
- Stack-based code generation with optional register allocation
- Type-directed compilation for string/int/array distinction
- Full pipeline: parse → infer → codegen → (peephole) → assemble → execute

## Files (projects/riscv-emulator/src/)
- `monkey-codegen.js` — AST → RISC-V assembly (~800 LOC)
- `riscv-peephole.js` — Post-pass optimizer (5 patterns)
- `type-infer.js` — Forward type analysis: call-site param inference + return type inference
- `monkey-codegen.test.js` — 51 codegen tests
- `riscv-peephole.test.js` — 26 optimizer tests
- `riscv-regalloc.test.js` — 33 register allocation tests
- `pipeline-integration.test.js` — 8 pipeline analysis tests
- `stress-test.test.js` — 84 stress/edge case tests
- `heap-arrays.test.js` — 25 heap allocation tests
- `forin-builtins.test.js` — 23 for-in/push/first/last tests
- `strings.test.js` — 20 string tests
- `type-infer.test.js` — 16 type inference tests
- `string-concat.test.js` — 12 concat tests
- `showcase.test.js` — 7 showcase programs (FizzBuzz, prime sieve, etc.)
- **Total: 297 tests**

## Feature Coverage
- **Types**: integers, booleans, strings, arrays
- **Variables**: let/set mutation
- **Control flow**: if/else, while, for-in
- **Functions**: recursive, multi-arg, callee-saved calling convention
- **Builtins**: puts (int/string), len, first, last, push
- **String ops**: concatenation via +, type-directed printing
- **Optimization**: peephole (5 patterns, 15% fib speedup), register allocation (s1-s11)
- **Type inference**: call-site parameter inference, return type inference, body analysis
- **Analysis**: pipeline integration (IPC, stalls, forwarding stats)

## Showcase Programs
- FizzBuzz 1..20
- Sieve of Eratosthenes (primes to 50)
- String builder (repeat function)
- Tower of Hanoi move counter
- Binary search on sorted array
- Dot product of vectors
- Complete feature demo with all features
