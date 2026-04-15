# Scratch Notes Index

## Active Notes

### HenryDB
- **henrydb-persistence.md** — BufferPool bugs, MVCC+persistence interaction, pageLSN implementation path, architecture diagram. Consolidated from 3 notes. (uses: 2, created: 2026-04-09)
- **henrydb-predicate-locks.md** — SSI predicate-level lock design, tuple/page/table granularity. (uses: 0, created: 2026-04-10)
- **parser-expression-audit.md** — 7+ locations using parsePrimary instead of parseExpression; comparison RHS, BETWEEN, IN lists, etc. (uses: 1, created: 2026-04-13)
- **tokenizer-negative-numbers.md** — 1-1 tokenized as NUMBER(1),NUMBER(-1); unary vs binary minus context. (uses: 1, created: 2026-04-13)
- **savepoint-physicalization.md** — xmax=-2 in-memory markers lost on recovery; must physicalize before durable storage. (uses: 1, created: 2026-04-13)
- **ddl-wal-completeness.md** — 5 DDL bug families from positive allowlist rot; negative approach catches all DDL types. (uses: 1, created: 2026-04-13)
- **mvcc-strategies.md** — MVCC implementation comparison (Pavlo framework), version storage, GC, concurrency control. (uses: 1, created: 2026-04-10)
- **buffer-pool-research.md** — Buffer pool manager design from CMU 15-445, LRU-K, clock sweep. (uses: 0, created: 2026-04-09)
- **sqlite-architecture.md** — SQLite vs HenryDB architecture differences, query planner, bytecode. (uses: 1, created: 2026-04-09)
- **depth-testing-lessons.md** — Stress-testing methodology: 8 modules, 3 bugs found (KAN boundary, Izhikevich voltage, BETWEEN SYMMETRIC). (uses: 0, created: 2026-04-12)

### Monkey-Lang
- **monkey-let-binding-compilation.md** — Critical let-binding stack ordering bug in bytecode compiler; OpSetLocal vs OpPop order. (uses: 1, created: 2026-04-13)
- **monkey-try-catch-impl.md** — try/catch/throw/finally implementation: 3 opcodes, handler stack unwinding, evaluator MonkeyThrown pattern. (uses: 0, created: 2026-04-15)

### Query Compilation & Optimization
- **copy-and-patch-compilation.md** — Copy-and-patch technique, comparison with HenryDB closure approach, sea-of-nodes verdict. (uses: 1, created: 2026-04-08)
- **query-compilation.md** — HyPer push-based model, Volcano vs push, query compilation strategies. (uses: 1, created: 2026-04-07)

### Language & Compiler Theory
- **cdcl-sat-solver.md** — CDCL SAT solver design, literal negation bug, watched literals, VSIDS, SMT. (uses: 2, created: 2026-04-06)
- **thompsons-nfa.md** — Thompson's NFA construction, linear-time regex matching. (uses: 1, created: 2026-04-07)
- **algorithm-w.md** — Hindley-Milner type inference, unification, generalization. (uses: 1, created: 2026-04-07)
- **forth-compilation.md** — Forth compile vs interpret mode, IMMEDIATE words, threaded code. (uses: 1, created: 2026-04-07)

### Systems
- **riscv-architecture.md** — RISC-V instruction encoding, pipeline hazards, branch prediction, Sv32, Tomasulo. (uses: 1, created: 2026-04-07)
- **cpython-trace-fitness.md** — Mark Shannon's trace quality model for CPython JIT. (uses: 1, created: 2026-04-07)
### Monkey-lang WASM
- **wasm-strip-imports-bug.md** — Never scan raw bytecode by value matching; track instruction positions at emit time. (uses: 1, created: 2026-04-15)
- **inline-caching-design.md** — IC for monkey-lang hash lookups: shapes, monomorphic cache, JIT integration plan. (uses: 0, created: 2026-04-15)
