# MEMORY.md - Long-Term Memory

## Projects

### monkey-lang (/Users/henry/projects/monkey-lang)
- **What**: Complete programming language: lexer → parser → AST → evaluator & **WASM compiler**
- **Size**: ~9K+ LOC, **2004 tests**, 41+ test files
- **WASM compiler is primary** (5570 lines vs 1483 evaluator — 3.8x larger)
- **Language features**: closures, pattern matching, destructuring, for-in, do-while, comprehensions, spread/rest, template literals, pipe operator (`|>`), ranges, classes, match expressions, try-catch
- **WASM inline HOFs**: map, filter, reduce compiled as WASM loops (no runtime call for simple callbacks)
- **WASM GC**: Confirmed working in Node.js v22! Struct types compile + instantiate (no flags). Inner WASM loop 21x faster than JS.
- **Performance**: 1000-line program compiles in 21ms (21μs/line), 0.28ms compile for simple programs
- **Type guards**: compile()/compileAndRun() reject non-string inputs to prevent OOM
- **Zero test failures**

### HenryDB (/Users/henry/.openclaw/workspace/projects/henrydb)
- **What**: SQLite-compatible relational database from scratch in JavaScript
- **Size**: 209K+ LOC, 882+ test files
- **97.6% SQLite compatibility** (differential fuzzer)
- **Features**: Full SQL (DDL, DML, JOINs, CTEs, window functions, triggers, views), WAL, B-tree storage, prepared statements, ARRAY literals, VALUES clause, GENERATE_SERIES, UNNEST
- **Prepared stmts**: Fast bind/unbind (in-place AST mutation, 2.7x faster), ? placeholders, executeMany batch
- **Compiled expressions**: WHERE AST → JS function via new Function() for fast scan filtering
- **Key optimizations (Apr 28)**: HeapFile Array→Map (88,000x for page lookup), INSERT RETURNING O(1), ON CONFLICT O(log N), FK early-return
- **Key bug**: V8 JIT cold-call latency: _evalExpr first call 846μs, hot 0.39μs (2169x diff)
- **Key module**: `type-affinity.js` for SQLite-compatible comparisons and INSERT coercion

### neural-net — Deep Learning Framework (38K LOC, 318 files)
- **Largest project** in the collection (38K LOC, bigger than monkey-lang's 22K!)
- **162 modules** with paper citations, 168 non-test files
- **Architectures**: Transformer, MicroGPT, Mamba SSM, RNN, VAE, Autoencoder, Capsule, SNN, SOM, MOE
- **Paper implementations**: Flash Attention (Dao 2022), DPO (Rafailov 2023), LoRA (Hu 2021), CLIP (Radford 2021), Constitutional AI (Bai 2022), Chinchilla (Hoffmann 2022), Speculative Decoding (Leviathan 2023), SimCLR (Chen 2020), AdaLN/DiT (Peebles 2023), Mamba SSM (Gu & Dao 2023)
- **LLM Infrastructure**: BPE tokenizer, GQA + KV-cache, continuous batching (vLLM-style), attention sinks (StreamingLLM), quantization, think tokens (DeepSeek R1)
- **Training**: Autograd, AdamW, schedulers, mixed precision, scaling laws, data augmentation, callbacks
- **This is a mini PyTorch implemented in JavaScript**

- **215 projects** — comprehensive CS fundamentals implementation collection
- **206 importable, 166 functional, ~141 actively verified** (as of Apr 25 Session B)
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
- **Games**: game-of-life, game-engine (2D with collision)
- **Graphics**: ray-tracer (3371 LOC, BVH, PBR), physics (529 LOC)

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

## 2026-04-26 (Sunday Session A)

### Major Milestone: WASM Compiler for monkey-lang
- Built a complete WASM compiler in ~300 LOC that produces 120-330x speedup
- Supports i32, i64, f64 numeric types
- Mandelbrot benchmark: 0.7ms WASM vs 231ms VM (80x60 grid)
- `--wasm-emit` flag writes .wasm binaries loadable in browsers (73 bytes for fibonacci)
- Key insight: WASM is stack-based like our bytecode VM, so compilation model maps directly

### Class Syntax Added
- Full class support: parser, compiler, inheritance (extends + super.init), method dispatch
- Methods compiled as scope-level functions (simple but collides between classes)
- 24 class tests

### PL/SQL Integrated into HenryDB
- 854 LOC parser+interpreter wired into callUserFunction (just 10 lines of integration)
- Supports: DECLARE, BEGIN/END, IF/ELSIF, WHILE, FOR, RETURN, RAISE, SELECT INTO
- Recursive functions and nested PL→PL calls work
- Auto-detects PL/SQL from DECLARE/BEGIN keywords

### VM Callback Mechanism (callClosureSync)
- Re-entrant VM execution via _frameFloor
- Native HOF builtins: 4x map, 3.9x filter, 1.5x reduce speedup

### Technical Lessons
- WASM i64 comparisons produce i32, need i64.extend_i32_s to convert back
- WASM f64 conditions need i32.wrap or f64.eq 0.0 conversion for br_if
- ShapedHash uses hidden classes with shape.transition() for new properties
- parseFloat('0') || '0' returns '0' because 0 is falsy — use !isNaN(n) ? n : result
- Always check opcode value table for collisions before adding new opcodes
- **HeapFile.pages: Array.find() is O(N) per access — use Map for O(1) lookups (88,000x improvement)**
- **In-place AST mutation + WeakMap caching = stale cache bug. Don't cache mutable objects by identity.**
- **Qualified column refs (table.col) may reference outer scope in correlated subqueries — can't safely compile them**
- **V8 JIT cold-call: first call to complex function can be 2000x slower than hot call. Use new Function() for pre-compilation.**
- **WASM GC structs work in Node.js v22 without flags. Inner loops 21x faster than JS.**

### Test Counts (Apr 28)
- monkey-lang: 2004 (41 files)
- HenryDB: 882 test files (thousands of tests)
- type-infer: 23
- Total: ~5000+
