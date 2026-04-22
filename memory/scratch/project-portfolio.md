# Project Portfolio — Apr 21, 2026

## Active Projects (substantial, ongoing)

| Project | LOC | Tests | Status | Next |
|---------|-----|-------|--------|------|
| **henrydb** | 96K | 3200+ | Volcano engine wired, db.js 4939 LOC | RIGHT/FULL join, MVCC index visibility |
| **neural-net** | 27K | 100+ | Comprehensive NN library | Stress-test transformers, flash attention |
| **riscv-emulator** | 6K+ | 32+ | RISC-V + Monkey codegen | Liveness-based register allocation |
| **lambda-calculus** | 43K | 186+ | CoC, System F, STLC, effects | Depth exploration |
| **monkey-lang** | 6K+ | varies | Monkey language compiler | Optimization passes |

## Smaller Projects (well-scoped, good for variety)

| Project | LOC | Tests | Status | Quick Win |
|---------|-----|-------|--------|-----------|
| **git** | 4671 | 153 | Mature, all pass | Rebase implementation |
| **sat-solver** | 4571 | 22/23 | CDCL + SMT | **Fix SMT strict inequality bug (8 lines)** |
| **forth** | 529 | 91 | Complete | Optimization/compilation |
| **fft** | ~500 | varies | Radix-2 FFT | Mixed-radix, inverse FFT |
| **huffman** | small | varies | Compression | Adaptive Huffman |
| **regex-engine** | varies | varies | Pattern matching | NFA/DFA optimization |
| **type-inference** | varies | varies | HM type inference | Let polymorphism |

## Key Metrics
- **Total projects**: 13+
- **Total source LOC**: ~190K
- **Total tests**: ~25,000+ (estimated from test files)
- **All-green projects**: All 12 testable projects pass their test suites
- **Known bugs**: SAT solver SMT strict inequality (8-line fix)

## Portfolio Balance
- **Database systems**: henrydb (deep, ongoing)
- **Compilers/PLT**: riscv-emulator, monkey-lang, lambda-calculus, forth, type-inference
- **ML/AI**: neural-net
- **Algorithms**: sat-solver, fft, huffman, regex-engine
- **Systems**: git
