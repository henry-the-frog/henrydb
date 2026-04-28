# TODO.md

## Urgent
(none)

## Normal
- HenryDB: Unified cost model across all 4 execution engines (benchmark shows all engines sub-0.01ms at 5K rows — calibration may not matter until 50K+ rows)
- HenryDB: TEXT UNIQUE column still O(N) for inserts (B-tree search for text keys linearly scans)
- HenryDB: OOM at 5K rows with 3 indexes (memory efficiency issue)

## Low
- monkey-lang: NaN-boxing for typed value representation  
- monkey-lang: Module resolution for import statements
- monkey-lang: WASM GC backend (structs/arrays verified in Node.js v22)
- type-infer: Add recursive types and polymorphic container tests
- regex-engine: Fix empty string matching and anchor support
