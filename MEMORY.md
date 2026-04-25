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
- **~81 actively verified, ~103 importable** (as of Apr 25 Session B)
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

### HenryDB Full Architecture
- **6 execution strategies** (11K LOC): AST interpreter, Volcano iterators (13 operators), VDBE bytecode VM, vectorized (MonetDB-style), codegen (JIT), vectorized-codegen (DuckDB-style)
- **Adaptive engine**: Auto-selects strategy based on query analysis + runtime feedback (shape hashing, 3+ sample learning)
- **Cost-based optimizer**: Column histograms, MCV tracking, DP join reordering
- **WAL**: Binary format, CRC32 checksums, LSN tracking, ARIES-style redo recovery (1200 LOC)
- **MVCC**: PostgreSQL-style snapshots (xmin/xmax/activeSet), READ COMMITTED & REPEATABLE READ (3589 LOC)
- **SSI**: Serializable Snapshot Isolation (Cahill 2008, Ports&Grittner 2012) — rw-antidependency tracking (277 LOC)
- **Indexes**: B+ tree (composite keys), index scan optimizer, index advisor (5102 LOC)
- **PL/SQL**: Parser + interpreter exist (854 LOC) but NOT wired to procedure handler
- **pg-server**: Full PostgreSQL wire protocol v3 (1936 LOC) — psql-compatible
- **Known bug**: COUNT(DISTINCT) doesn't deduplicate in multi-join scenarios
- **Fuzzer**: 98.6% average, remaining failures are mixed type comparisons

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
1. Per-function SSA → dead code elimination (SSA-level annotation, ~200 LOC)
2. HenryDB fuzzer gap (1.4%) — mixed type comparisons + UNION type affinity
3. VM callback mechanism for native HOF builtins (prelude is 19x slower)
4. Wire PL/SQL to procedure handler (implementation exists)
5. Fix COUNT(DISTINCT) bug in multi-join scenarios
6. WASM compiler (4-phase design, ~4300 LOC, 2-3 weeks)
