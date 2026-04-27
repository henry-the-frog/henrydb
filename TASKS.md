# TASKS.md - Project Tasks

## Active Projects

### monkey-lang
- **Status**: 1803 tests (100% pass), 5 execution backends
- **WASM**: 217 tests — classes (inheritance + super), closures, HOFs, comprehensions, try/catch, pattern matching. 36.3x VM, 11.4x JIT.
- **Recent (Apr 27)**: WASM Phase 2 complete (all AST nodes), type inference (knownInt + return types), exception handling, benchmark update
- **Next**: Float support (NaN-boxing), WASM closure capture type propagation, WASM binary caching

### HenryDB
- **Status**: ~99% SQLite compatibility, 117 regression tests pass
- **JSON**: json_each(), json_extract(), json_array_length(), json_type(), json_valid(), json(), json_group_array(), json_group_object()
- **TVF**: generate_series() table-valued function
- **Recent (Apr 27)**: Boolean/integer coercion, cross-type comparison, CTE column renaming, JSON functions
- **Next**: TVF cross-join support, json_tree(), json_insert/replace/remove

### neural-net
- **Status**: 1305 tests, 168 source modules, ~27K LOC
- **Covers**: Hopfield (1982) → KAN (2024), complete LLM architecture
- **Next**: Gradient checkpointing, mixed precision training

## Test Counts (as of 2026-04-27 afternoon)

| Project    | Tests | Pass Rate |
|-----------|-------|-----------|
| monkey-lang | 1803 | 100% |
| neural-net | 1305 | 100% |
| HenryDB   | 117 (regression) | 100% |

## TODO (see TODO.md for full list)
- monkey-lang: NaN-boxing, WASM closure type propagation
- HenryDB: json_tree(), UPDATE OF column, TVF cross-joins
