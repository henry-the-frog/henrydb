# TASKS.md - Project Tasks

## Active Projects

### monkey-lang
- **Status**: 1149 tests (100% pass), 22K LOC, 55 VM builtins + 17 prelude HOFs
- **Recent**: Comments, templates, import/export, escape analysis, per-function SSA, prelude, benchmark, demo
- **Next**: VM callback mechanism for native HOFs, dead code elimination via SSA, module resolution for import statement

### HenryDB
- **Status**: 97.6% SQLite compatibility, 209K LOC, differential fuzzer
- **Recent**: Type affinity, MEDIAN, GROUPS/EXCLUDE, VALUES, ARRAY literals, GENERATE_SERIES aliases
- **Next**: Close remaining 2.4% fuzzer gap (long-tail type edge cases), LATERAL joins, recursive CTEs

### type-infer
- **Status**: Working Hindley-Milner type inference, 23 tests
- **Next**: Add more complex tests (recursive types, polymorphic containers), integration with monkey-lang

### calc-lang
- **Status**: Working calculator with sessions, 20 tests
- **Next**: Add conditionals, lambda syntax, more math functions

## Completed Today (Apr 25)

### Session A (8:30 AM - 2:15 PM)
- 302 tasks, 15 bugs fixed, 2 fuzzers built
- monkey-lang: 1053 tests, 100% pass
- HenryDB: ~97.2% SQLite compat

### Session B (2:15 PM - ongoing)
- 60 BUILD tasks (daily ceiling reached)
- monkey-lang: +96 tests (1053→1149), +12 VM builtins, +17 prelude HOFs
- HenryDB: 97.2→97.6% SQLite compat, +8 features
- 9 bugs fixed, 2 READMEs, 1 demo, 1 benchmark
- type-infer: +23 tests, calc-lang: +20 tests
