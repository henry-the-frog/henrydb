# Project Portfolio

*13 projects, 559K lines of JavaScript, 20,438 tests.*

## Major Projects

### [HenryDB](projects/henrydb/) — SQL Database
A comprehensive SQL database in JavaScript with 6 execution engines, Raft consensus, PL/SQL, and PostgreSQL wire protocol.
- **208K lines**, 8,218 tests, 369 source files
- Blog: `projects/henrydb/blog/building-henrydb.md`

### [Monkey-lang](projects/monkey-lang/) — Language Runtime
A complete programming language runtime — lexer, parser, type checker (Hindley-Milner), SSA optimizer, bytecode compiler, stack VM, mark-sweep GC, debugger.
- **199K lines**, 8,735 tests, 190 source files
- Blog: `projects/monkey-lang/blog/building-monkey-lang.md`

### [Neural-net](projects/neural-net/) — ML Framework
Neural network library: dense, CNN, RNN, LSTM, GRU, transformers, GANs, RL (DQN), autoencoders. Training with SGD, Adam, learning rate schedules.
- **46K lines**, 2,323 tests
- Tutorial: `projects/neural-net/docs/tutorial.md`

### [Lambda Calculus](projects/lambda-calculus/) — PL Theory Library
Encyclopedia of programming language theory: Church encoding, SKI, System F, abstract machines (SECD, CEK), CPS transform, HM inference, bidirectional type checking.
- **43K lines**, 469 tests, 190 modules

## Smaller Projects

| Project | Description | LOC | Tests |
|---------|------------|-----|-------|
| [riscv-emulator](projects/riscv-emulator/) | RISC-V CPU emulator with MMU, CSRs | 9K | 166 |
| [git](projects/git/) | Git implementation (objects, refs, diff) | 10K | 100 |
| [regex-engine](projects/regex-engine/) | NFA-based regex with groups, quantifiers | 1K | 113 |
| [sat-solver](projects/sat-solver/) | DPLL SAT solver (666K propagations/sec) | 3K | 12 |
| [dns-server](projects/dns-server/) | DNS resolver/server | 2K | 26 |
| [huffman](projects/huffman/) | Huffman compression/decompression | 3K | 4 |

## Quality

- **20,438 individual test cases** across all projects
- **Differential fuzzer** for HenryDB: 2000 queries, 100% pass rate
- All 10 active projects pass their test suites ✅
