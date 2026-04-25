# MEMORY.md - Long-Term Memory

## Projects

### monkey-lang (/Users/henry/projects/monkey-lang)
- **What**: Complete programming language: lexer → parser → AST → evaluator & bytecode compiler → VM
- **Size**: 22K LOC, 1149 tests, 78 source files, 2200+ commits
- **Language features**: closures, pattern matching, destructuring, for-in, comprehensions, spread/rest, template literals, comments, import/export, enums, try-catch, pipe operator, ranges
- **Infrastructure**: CFG, SSA, liveness analysis, escape analysis, register allocation, type inference, type checker, inline caching, GC
- **55 VM builtins + 17 prelude HOFs** (map, filter, reduce, etc.)
- **VM is 3-6x faster than tree-walking evaluator** for computation
- **Zero test failures**

### HenryDB (/Users/henry/.openclaw/workspace/projects/henrydb)
- **What**: SQLite-compatible relational database from scratch in JavaScript
- **Size**: 209K LOC, 1249 files, 4100+ commits
- **97.6% SQLite compatibility** (differential fuzzer)
- **Features**: Full SQL (DDL, DML, JOINs, CTEs, window functions, triggers, views), WAL, B-tree storage, prepared statements, ARRAY literals, VALUES clause, GENERATE_SERIES, UNNEST
- **Key module**: `type-affinity.js` for SQLite-compatible comparisons and INSERT coercion

### Other Projects (/Users/henry/projects/)
- **215 projects** — comprehensive CS fundamentals implementation collection
- **~50 verified working** (as of Apr 25 Session B)
- **Languages**: scheme-interp (full Scheme with tail calls), mini-lisp, calc-lang, brainfuck, tinylang
- **Types**: type-infer (HM, 23 tests), typechecker (union/intersection/generics)
- **Data Structures**: btree, bloom-filter, skip-list, lru-cache, trie (with autocomplete), heap, linked-list, ring-buffer, deque, AVL tree, union-find
- **Algorithms**: graph (BFS/DFS/Dijkstra), A* pathfinding, toposort, sorting
- **ML/AI**: neural-net (learns XOR), gradient-descent, kmeans, decision-tree, naive-bayes
- **Crypto/Security**: sha256 (correct hashes), jwt (sign/verify), huffman compression
- **Networking**: http-server, rate-limiter, circuit-breaker
- **Systems**: virtual-dom (React-like diffing), event-emitter, promise (A+), state-machine, blockchain (PoW), CRDTs
- **Parsing**: json-parser, regex-engine, csv-parser, ini-parser, markup-lang (Markdown→HTML)
- **Utilities**: base64, uuid (v4), semver, glob, dotenv, rpn calculator
- **Games**: game-of-life, chess-engine (legal moves)
- **Graphics**: ray-tracer (working renders)

### HenryDB Query Engine Architecture
- **3 execution strategies**: AST interpreter, Volcano iterators (13 operators), VDBE-style bytecode VM
- **Cost-based optimizer**: Column histograms, MCV tracking, DP join reordering
- **Volcano operators**: SeqScan, IndexScan, Filter, Project, Sort, HashAggregate, NestedLoopJoin, HashJoin, IndexNestedLoopJoin, Window, Limit, Distinct, Union, CTE

## Key Technical Insights

### Type Systems
- `Number('')` === 0 in JavaScript — this broke WHERE comparisons
- SQLite type affinity: TEXT columns coerce integers to strings on INSERT
- `sqliteCompare` must be used everywhere for mixed-type comparisons (ORDER BY, MIN/MAX, window functions)
- Coerce for EQ/NE but NOT for LT/GT/LE/GE comparisons

### Compiler Design
- Escape analysis needs to handle: anonymous FunctionLiterals, implicit returns (last ExpressionStatement)
- Per-function SSA works by building CFG from function body statements
- `toSSA()` must accept both strings and CFG objects (was string-only → OOM)
- Prelude approach: HOFs as compiled monkey-lang source, not native builtins (simpler, 3x slower but adequate)

### VM Design
- Builtins are positional — compiler and VM must have identical lists
- Adding HOF builtins to VM needs callback mechanism (frame pushing from within builtins)
- Workaround: prelude compiles HOFs as monkey-lang, available at startup
- GC tracking can be skipped for non-escaping closures (escape analysis optimization)

## TODO Priorities (as of Apr 25)
1. Per-function SSA → wire into compiler for dead code elimination
2. HenryDB fuzzer gap (2.4% remaining) — long-tail type edge cases
3. Lambda-calculus project exploration
4. VM callback mechanism for native HOF builtins (performance)
