# Monkey-lang → RISC-V Compilation Backend

A complete compiler toolchain that compiles Monkey programming language to RISC-V machine code, running on an in-browser RISC-V CPU emulator.

## Pipeline

```
Monkey Source → Parse (AST) → Type Inference → Closure Analysis → Code Generation → Peephole Optimization → Assembly → Machine Code → RISC-V CPU Emulator
```

## Quick Start

```bash
# Run a program
node src/monkey-riscv.js -e 'puts(3 + 4)'

# Show generated assembly
node src/monkey-riscv.js --dump -e 'puts("hello world")'

# Show machine code disassembly
node src/monkey-riscv.js --disasm -e 'let x = 42; puts(x)'

# Run with optimizations (register allocation + peephole)
node src/monkey-riscv.js --opt -e 'let fib = fn(n) { if (n <= 1) { return n }; return fib(n-1) + fib(n-2) }; puts(fib(15))'
```

## Supported Language Features

### Data Types
- **Integers**: Full 32-bit integer arithmetic (+, -, *, /, %)
- **Booleans**: `true`/`false` (represented as 1/0)
- **Strings**: Heap-allocated, char-by-char storage, concatenation with `+`
- **Arrays**: Heap-allocated, indexing, `len()`, `push()`, `first()`, `last()`
- **Hashes**: Heap-allocated key-value pairs, linear scan access

### Control Flow
- **if/else**: Conditional branching
- **while**: Loop with condition
- **for-in**: Array iteration

### Functions
- **Named functions**: `let f = fn(x) { x * 2 }`
- **Recursive functions**: Full recursion support (fib, factorial, etc.)
- **Multiple parameters**: Up to 8 (a0-a7 registers)
- **Closures**: Functions that capture outer variables

### Builtins
- `puts(x)`: Print integer or string (type-directed)
- `len(x)`: Array/string length
- `first(x)`: First array element
- `last(x)`: Last array element
- `push(arr, val)`: Create new array with element appended

### String Operations
- Concatenation: `"hello" + " " + "world"`
- Equality: `s1 == s2`, `s1 != s2` (char-by-char comparison)
- Length: `len(s)`
- Indexing: `s[i]` returns character code

## Architecture

### Code Generation (`monkey-codegen.js`, 1271 LOC)
- Stack-based code generation targeting RV32I + RV32M
- Standard RISC-V calling convention (a0-a7 arguments, ra return)
- Frame layout: `[s0-4]=ra, [s0-8]=old_s0, [s0-12+]=locals`
- Optional register allocation for locals (callee-saved s1-s11)

### Heap Allocation
- **Bump allocator** using `gp` register (x3)
- Heap starts at 0x10000 (64KB)
- Array layout: `[length][elem0][elem1]...`
- String layout: `[length][char0][char1]...` (4 bytes per char)
- Hash layout: `[num_pairs][key0][val0][key1][val1]...`

### Type Inference (`type-infer.js`, 166 LOC)
- Forward type analysis from literal expressions
- Call-site parameter type inference
- Return type inference from function body analysis
- Enables type-directed code generation (e.g., string vs integer `puts`)

### Closure Analysis (`closure-analysis.js`, 135 LOC)
- Free variable identification in nested functions
- Closure objects: heap-allocated `[fn_id][num_captured][var0][var1]...`
- Environment pointer passed as implicit first argument

### Peephole Optimizer (`riscv-peephole.js`, 119 LOC)
5 optimization patterns:
1. Self-move elimination (`mv a0, a0` → removed)
2. Push/pop same register → removed (4 instructions eliminated)
3. Push/pop different regs → single `mv`
4. Consecutive `addi sp` → merged
5. Store-load same address → eliminated

Results: **15% cycle reduction** on recursive fibonacci.

### Register Allocator
- Maps locals to callee-saved registers s1-s11
- Spills to stack when registers exhausted
- Save/restore only used registers in prologue/epilogue
- Combined with peephole: **6% improvement** on fibonacci

### Disassembler (`disassembler.js`, 163 LOC)
- Decodes all RV32I and RV32M instructions
- Pseudo-instruction detection: `li`, `mv`, `ret`, `j`, `jr`
- Full program listing with hex and addresses

## Performance

| Program | Cycles (stack) | Cycles (reg+peep) | Improvement |
|---------|---------------|-------------------|-------------|
| fib(10) | 5,845 | 4,959 | 15.2% |
| fib(15) | ~70K | ~61K | ~13% |
| fact(10) | 361 | 340 | 5.8% |
| sum 1..1000 | ~6K | ~6K | ~0% |

## Test Suite

| Test File | Tests | Coverage |
|-----------|-------|---------|
| monkey-codegen.test.js | 51 | Core codegen |
| riscv-peephole.test.js | 26 | Optimizer patterns + correctness |
| riscv-regalloc.test.js | 33 | Register allocation |
| stress-test.test.js | 84 | Edge cases + adversarial inputs |
| heap-arrays.test.js | 25 | Array allocation + access |
| forin-builtins.test.js | 23 | for-in loops + push/first/last |
| strings.test.js | 20 | String literals + printing |
| type-infer.test.js | 16 | Type inference |
| string-concat.test.js | 12 | String concatenation |
| string-ops.test.js | 16 | String equality + indexing |
| hash-ops.test.js | 12 | Hash literals + access |
| showcase.test.js | 7 | FizzBuzz, prime sieve, etc. |
| closure-analysis.test.js | 10 | Free variable analysis |
| closures.test.js | 14 | Closure compilation |
| self-hosting.test.js | 9 | Complex real-world programs |
| disassembler.test.js | 33 | Instruction decoding |
| **Total** | **391** | |

## Known Limitations

1. **Closures returned from functions** (make_adder pattern) require runtime dispatch — not yet implemented
2. **String keys in hashes** — currently only integer key comparison
3. **No garbage collection** — heap is bump-allocated, never freed
4. **No string mutation** — strings are immutable (by design)
5. **Pipeline CPU I/O** — ecall timing issues in pipeline simulator
6. **Integer overflow** — 32-bit signed integers, no overflow detection

## Files

```
src/
├── monkey-codegen.js        # AST → RISC-V assembly (1271 LOC)
├── riscv-peephole.js        # Peephole optimizer (119 LOC)
├── type-infer.js            # Forward type analysis (166 LOC)
├── closure-analysis.js      # Free variable analysis (135 LOC)
├── disassembler.js          # Machine code → assembly (163 LOC)
├── monkey-riscv.js          # CLI tool (133 LOC)
├── assembler.js             # Assembly → machine code (existing)
├── cpu.js                   # RISC-V CPU emulator (existing)
├── pipeline.js              # 5-stage pipeline simulator (existing)
└── ooo.js                   # Tomasulo OOO simulator (existing)
```

Total new code: **~2000 LOC** (codegen + optimizer + type inference + closure analysis + disassembler + CLI)
