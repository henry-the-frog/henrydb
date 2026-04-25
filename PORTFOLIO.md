# Project Portfolio

*13 projects, 559K lines of JavaScript, 22,535 tests.*

## Major Projects

### [HenryDB](projects/henrydb/) — SQL Database
A comprehensive SQL database in JavaScript with 6 execution engines, Raft consensus, CRDTs, PostgreSQL wire protocol, PL/SQL stored procedures, triggers, views, and full ACID transactions.
- **212K lines**, 8,982 tests, 875 test files
- 30+ SQL feature categories verified: JOINs, CTEs (recursive), window functions, GROUPING SETS, TABLESAMPLE, ALTER TABLE, FOREIGN KEYS, CHECK/NOT NULL/UNIQUE constraints, triggers, transactions/savepoints, views, JSON, UPSERT, RETURNING
- Differential fuzzer: **97.2% match** vs SQLite (6,000 queries, 15 types)
- Blog: `projects/henrydb/blog/building-henrydb.md`

### [Monkey-lang](projects/monkey-lang/) — Language Runtime
A complete programming language runtime with 49 AST node types — lexer, parser, type checker (Hindley-Milner), SSA optimizer, bytecode compiler + VM, mark-sweep GC, debugger.
- **200K lines**, 1,053 tests (**100% pass rate**)
- Language features: closures, destructuring, spread, pipe, range, ternary, try/catch/throw, switch, array comprehensions, optional chaining, enums, variadic functions, for-in, do-while
- Bytecode optimizer (DCE, peephole, jump threading) — default-on, 50% bytecode reduction
- Optimizer fuzzer: **100% match** (1,600+ random programs)
- Blog: `projects/monkey-lang/blog/building-monkey-lang.md`

### [Neural-net](projects/neural-net/) — ML Framework
Neural network library: dense, CNN, RNN, LSTM, GRU, transformers, GANs, RL (DQN), autoencoders.
- **59K lines**, 1,780 tests
- Tutorial: `projects/neural-net/docs/tutorial.md`

### [Lambda Calculus](projects/lambda-calculus/) — PL Theory Library
Encyclopedia of programming language theory: Church encoding, SKI, System F, abstract machines (SECD, CEK), CPS transform, HM inference, bidirectional type checking.
- **43K lines**, 702 tests, 190+ modules

## Smaller Projects

| Project | Description | LOC | Tests |
|---------|------------|-----|-------|
| [riscv-emulator](projects/riscv-emulator/) | RISC-V CPU emulator with MMU, CSRs | 15K | 735 |
| [git](projects/git/) | Git implementation (objects, refs, diff) | 5K | 348 |
| [fft](projects/fft/) | Fast Fourier Transform | 3K | 151 |
| [type-inference](projects/type-inference/) | Type inference engine | 2K | 148 |
| [regex-engine](projects/regex-engine/) | NFA-based regex with groups, quantifiers | 2K | 66 |
| [sat-solver](projects/sat-solver/) | DPLL SAT solver (666K propagations/sec) | 3K | 12 |
| [dns-server](projects/dns-server/) | DNS resolver/server | 2K | 26 |
| [forth](projects/forth/) | Forth stack machine | 2K | 91 |
| [huffman](projects/huffman/) | Huffman compression/decompression | 1K | 21 |

## Quality

- **22,535 individual test cases** across all projects
- **Differential fuzzer** for HenryDB: 6,000 queries, 97.2% pass, 15 query types
- **Optimizer fuzzer** for monkey-lang: 1,600+ programs, 100% pass
- **monkey-lang**: 1,053/1,053 tests (100% pass)
- All active projects pass their test suites ✅
- **14 bugs found and fixed** in one session (all silent wrong-answer bugs)
