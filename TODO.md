# TODO.md

## Urgent
(none)

## Normal
- HenryDB: Unified cost model across all 4 execution engines (benchmark shows all engines sub-0.01ms at 5K rows — calibration may not matter until 50K+ rows)
- HenryDB: SQL-level prepared statements (`db.prepare('INSERT INTO t VALUES (?, ?)')` → parse once, bind params) — existing PreparedQueryCache only accepts AST objects
- HenryDB: OOM at 5K rows with 3 indexes (memory efficiency issue)
- monkey-lang: Test coverage gaps — do-while (0 tests), for-in (0 tests), inline map/filter (0 tests)

## Low
- monkey-lang: Lazy runtime function emission (~35ms compile time savings, 72→N functions)
- monkey-lang: NaN-boxing for typed value representation  
- monkey-lang: Module resolution for import statements
- monkey-lang: WASM GC backend (structs/arrays verified in Node.js v22, design in scratch/wasm-gc-backend-design.md)
- monkey-lang: Inline reduce/sort/forEach compilation (Phase 2-3 of HOF inlining)
- type-infer: Add recursive types and polymorphic container tests
- regex-engine: Fix empty string matching and anchor support
