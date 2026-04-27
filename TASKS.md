# TASKS.md - Project Tasks

## Active Projects

### monkey-lang
- **Status**: 1790 tests (100% pass), 5 execution backends (eval, VM, JIT, transpiler, WASM)
- **WASM**: 204 tests — classes (with inheritance), closures, HOFs, memory, globals, tables, comprehensions, try/throw, pattern matching
- **Recent**: WASM Phase 2 (classes, comprehensions, hash destructuring, try/throw, import/generator stubs), DCE in VM pipeline
- **Next**: WASM exception handling (real try/catch via WASM EH proposal), WASM GC backend exploration, NaN-boxing

### HenryDB
- **Status**: ~98% SQLite compatibility, differential fuzzer 94 tests pass
- **Recent**: Boolean/integer coercion fix (comparisons return 1/0/NULL), cross-type numeric affinity coercion
- **Next**: Close remaining fuzzer gaps, LATERAL joins, recursive CTEs with proper materialization

### neural-net
- **Status**: 1305 tests, 168 source modules, ~27K LOC
- **Covers**: Hopfield (1982) → KAN (2024), complete LLM architecture (BPE, RoPE, GQA, Flash Attention, sliding window)
- **Recent**: Vanishing gradient validation (depth 8+ with BatchNorm works), all tests pass
- **Next**: Gradient checkpointing, FSDP-style sharding, mixed precision training

### type-infer
- **Status**: Working Hindley-Milner type inference, 23 tests
- **Next**: Recursive types, polymorphic containers, integration with monkey-lang

### calc-lang
- **Status**: Working calculator with sessions, 20 tests
- **Next**: Conditionals, lambda syntax, more math functions

## Test Counts (as of 2026-04-27)

| Project    | Tests | Pass Rate |
|-----------|-------|-----------|
| monkey-lang | 1790 | 100% |
| neural-net | 1305 | 100% |
| HenryDB   | 94 (fuzzer) | 100% |
| type-infer | 23 | 100% |
| calc-lang  | 20 | 100% |
