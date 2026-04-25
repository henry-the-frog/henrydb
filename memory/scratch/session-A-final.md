# Session A Summary (2026-04-25)

## Numbers
- **178+ tasks** in ~4.5 hours (~40 tasks/hour)
- **15 bugs found and fixed** (11 HenryDB + 4 monkey-lang)
- **315+ commits** across 3 repos
- **2 fuzzers built** from scratch
- **559,837 lines** of JavaScript verified
- **22,535 test cases** across 13 projects

## HenryDB (212K lines, 8,982 tests)
- **371 source modules** — encyclopedic database implementation
- **30+ SQL feature categories verified** (all working):
  - DML: INSERT, UPDATE, DELETE, UPSERT, RETURNING
  - DDL: CREATE/ALTER/DROP TABLE, INDEX, VIEW, TRIGGER, PROCEDURE
  - Queries: JOIN (5 types), CTE (recursive), window (6 types), GROUPING SETS, TABLESAMPLE
  - Constraints: CHECK, NOT NULL, UNIQUE, FOREIGN KEY, PRIMARY KEY
  - Transactions: BEGIN/COMMIT/ROLLBACK/SAVEPOINT
  - Functions: 17+ (string, math, date, CAST, IIF, NULLIF, IFNULL)
  - JSON: JSON_EXTRACT, JSON_ARRAY, JSON_OBJECT, ->>, ->
  - Set ops: UNION, INTERSECT, EXCEPT
  - Advanced: CROSS APPLY, EXISTS, DISTINCT, CASE/WHEN, COALESCE
- **97.2% fuzzer match** vs SQLite (6000 queries, 15 types, 12 seeds)
- **Data structures**: 30+ (B+tree, LSM, skip list, Bloom, R-tree, HyperLogLog...)
- **Distributed**: Raft (10 tests), CRDTs (13), Gossip (7), 2PC (9), 2PL (10)
- **Engines**: Row store, Column store, Vectorized, Volcano, Adaptive
- **Server**: PostgreSQL wire protocol (72 test files)

## Monkey-lang (200K lines, 1,053 tests)
- **49 AST node types** — complete modern language
- **1053/1053 tests (100%)** — fixed 33 failures to 0
- **Bytecode optimizer** default-on: DCE, peephole, jump threading (50% bytecode reduction)
- **Optimizer fuzzer**: 100% (1600+ random programs)
- **Language features**: closures, destructuring, spread, pipe, range, ternary, try/catch/throw, switch, array comprehensions, optional chaining, enums, variadic, for-in, do-while, array slicing
- **Standard library**: 26+ built-ins (map, filter, reduce, sort, reverse, flatten, split, join...)
- **Compilation pipeline**: lexer → parser → AST → (evaluator | compiler → VM)
- **Analysis**: SSA, escape analysis, constant propagation, dead code elimination
- **Type checker**: Hindley-Milner (Algorithm W), 82 tests
- **Runtime**: mark-sweep GC (50 tests), debugger (25 tests), V8-style hidden classes
- **VM**: 3.8x faster than evaluator on deep recursion

## Bugs Fixed
1-9: HenryDB (string truncation, WAL, EXPLAIN, LIMIT, booleans, REGEXP, SUM/AVG, INSERT count, ORDER BY)
10: Window SUM text concatenation
11: JSON arrow doubled $.prefix
12: Constant substitution ignoring set statements (28 test failures!)
13: Optimizer jump target remapping
14: Constant substitution unconditional
15: Evaluator crash on recursive hash evaluation

## Key Insights
1. **Build the fuzzer day one** — 6 minutes of random queries beat thousands of hand-crafted tests
2. **Optimization soundness requires write-tracking** — any pass caching values must check for mutations
3. **Silent wrong answers are worse than crashes** — all 15 bugs were wrong results, not errors
4. **Type class ordering matters** — SQLite's NULL < INT < TEXT < BLOB affects 100+ comparisons
5. **The build cap forces depth** — 20-BUILD limit led to exploration that found real bugs
