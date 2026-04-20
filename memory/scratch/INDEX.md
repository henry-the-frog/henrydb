# Scratch Notes Index

## Active Notes

### HenryDB
- **henrydb-persistence.md** — BufferPool bugs, MVCC+persistence interaction, pageLSN implementation path, architecture diagram. Consolidated from 3 notes. (uses: 2, created: 2026-04-09)
- **henrydb-predicate-locks.md** — SSI predicate-level lock design, tuple/page/table granularity. (uses: 0, created: 2026-04-10)
- **parser-expression-audit.md** — 7+ locations using parsePrimary instead of parseExpression; comparison RHS, BETWEEN, IN lists, etc. (uses: 1, created: 2026-04-13)
- **tokenizer-negative-numbers.md** — 1-1 tokenized as NUMBER(1),NUMBER(-1); unary vs binary minus context. (uses: 1, created: 2026-04-13)
- **savepoint-physicalization.md** — xmax=-2 in-memory markers lost on recovery; must physicalize before durable storage. (uses: 1, created: 2026-04-13)
- **ddl-wal-completeness.md** — 5 DDL bug families from positive allowlist rot; ALTER TABLE WAL fix (4 bugs, 2026-04-17), crash recovery architecture (3 phases). (uses: 2, created: 2026-04-13, updated: 2026-04-17)
- **bug-patterns-2026-04-17.md** — Analysis of 14 bugs from depth day: 5 categories (layer boundary, recovery gaps, non-atomic checkpoint, parser context, incomplete features). DDL lifecycle test insight. (uses: 1, created: 2026-04-17)
- **ddl-lifecycle-harness.md** — Design for DDL lifecycle test generator: 9 DDL types × 7 phases = 63 tests from ~200 lines. High-ROI infrastructure. (uses: 0, created: 2026-04-17)
- **alter-table-backfill-bug.md** — Root cause: ALTER TABLE ADD COLUMN backfill uses page-level updateTuple() instead of MVCC-aware path. Causes duplicate tuples after UPDATE + checkpoint. (uses: 0, created: 2026-04-17)
- **mvcc-strategies.md** — MVCC implementation comparison (Pavlo framework), version storage, GC, concurrency control. (uses: 1, created: 2026-04-10)
- **buffer-pool-research.md** — Buffer pool manager design from CMU 15-445, LRU-K, clock sweep. (uses: 0, created: 2026-04-09)
- **sqlite-architecture.md** — SQLite vs HenryDB architecture differences, query planner, bytecode. (uses: 1, created: 2026-04-09)
- **depth-testing-lessons.md** — Stress-testing methodology: 8 modules, 3 bugs found (KAN boundary, Izhikevich voltage, BETWEEN SYMMETRIC). (uses: 0, created: 2026-04-12)

### Monkey-Lang
- **monkey-let-binding-compilation.md** — Critical let-binding stack ordering bug in bytecode compiler; OpSetLocal vs OpPop order. (uses: 1, created: 2026-04-13)
- **monkey-try-catch-impl.md** — try/catch/throw/finally implementation: 3 opcodes, handler stack unwinding, evaluator MonkeyThrown pattern. (uses: 0, created: 2026-04-15)

### RISC-V Emulator
- **riscv-register-allocation.md** — Linear sequential allocation (s1-s11, overflow to stack). No liveness analysis. Good enough for Monkey. (uses: 0, created: 2026-04-18)

### HenryDB (new 2026-04-19)
- **query-optimizer-research.md** — Cost model, join ordering (DP/greedy), subquery decorrelation, sort avoidance, merge join strategies. Decision: join ordering already implemented, focus on decorrelation next. (uses: 1, created: 2026-04-19)

### HenryDB (new 2026-04-18)
- **hot-chains.md** — HOT chain implementation, PG comparison (same-page vs Map-based), integration points, MVCC interaction. (uses: 1, created: 2026-04-18)
- **stored-procedures-design.md** — Phased approach: SQL scalar → JS → table-returning → procedures. All phases 1-3 implemented. (uses: 1, created: 2026-04-18)
- **mvcc-visibility-comparison.md** — PG vs HenryDB MVCC deep comparison. CORRECTED: HenryDB already has snap.activeSet. Real gaps: CLOG, row-level xmax locking, frozen tuples. (uses: 1, created: 2026-04-18)
- **serialization-bugs.md** — Escaped quotes dead code, view format mismatch, ident function parsing. Pattern: roundtrip tests catch seam bugs. (uses: 1, created: 2026-04-18)
- **mvcc-persistence-bugs.md** — 3-bug chain: version maps not saved, committedTxns not saved, getter-only property silently failed. Pattern: try-catch + property-order deps. (uses: 1, created: 2026-04-18)

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

## 2026-04-17: Neural-Net Gradient Verification Round 2
- **KANLayer**: Out-of-range input gradient bug. B-spline basis derivative computed at clamped position when input outside grid range should give 0 (spline is constant). Fix: only compute for in-range.
- **MoE**: Expert backward used stale caches. Same expert reused for multiple batch samples loses activations. Fix: re-run forward before backward.
- **CapsuleLayer**: backward() applied weight updates inline (hardcoded lr=0.01). Fix: separate update() method.
- **NeuralODELayer**: Adjoint never updated during backward walk — returned dOutput unchanged. Fix: accumulate adjoint += h * df/dy * adjoint at each step.
- **Pattern**: Modules with complex architectures (routing, ODEs, capsules) are where backward bugs hide.
