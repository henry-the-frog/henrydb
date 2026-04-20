# Scratch Notes Index

_Regenerated 2026-04-19 (W16 synthesis)_

## Active Notes (36)

### HenryDB — Architecture & Design
- **henrydb-architecture-patterns.md** — Expression walker duplication, patterns. (created: 2026-04-19)
- **henrydb-cost-model.md** — Query optimizer cost model design. (uses: 0, created: 2026-04-18)
- **henrydb-predicate-locks.md** — SSI predicate-level lock design. (uses: 0, created: 2026-04-10)
- **henrydb-wire-protocol.md** — PG wire protocol design notes. (uses: 0, created: 2026-04-18)
- **stored-procedures-design.md** — CREATE FUNCTION phased approach. (uses: 0, created: 2026-04-18)
- **query-optimizer-gaps.md** — Gap analysis of HenryDB optimizer. (uses: 1, created: 2026-04-18)
- **buffer-pool-research.md** — Buffer pool manager research (CMU 15-445). (created: 2026-04-09)
- **hot-chains.md** — HOT chain implementation notes. (uses: 1, created: 2026-04-18)
- **ddl-lifecycle-harness.md** — DDL lifecycle test harness design. (created: 2026-04-17)

### HenryDB — Bug Patterns
- **alter-table-backfill-bug.md** — ALTER TABLE + WAL duplicate tuples. (created: 2026-04-17)
- **bug-patterns-2026-04-17.md** — 14 bugs in 5 categories from depth day. (uses: 1, created: 2026-04-17)
- **parser-expression-audit.md** — 7 locations using parsePrimary instead of parseExpression. (uses: 1, created: 2026-04-13)
- **tokenizer-negative-numbers.md** — 1-1 tokenized as NUMBER(1),NUMBER(-1). (uses: 1, created: 2026-04-13)
- **savepoint-physicalization.md** — xmax=-2 markers lost on recovery. (uses: 1, created: 2026-04-13)
- **sql-arithmetic-precedence.md** — All arithmetic at same precedence bug. (uses: 1, created: 2026-04-14)
- **serialization-bugs.md** — Escaped quotes + view serialization. (uses: 1, created: 2026-04-18)
- **deadlock-detection-bug.md** — BFS direction bug in waits-for graph. (created: 2026-04-19)
- **int32-overflow.md** — DataView.setInt32 silently wrapping large integers. (created: 2026-04-19)
- **result-cache-invalidation.md** — Cache not cleared on ROLLBACK TO SAVEPOINT. (created: 2026-04-19)

### HenryDB — MVCC & Transactions
- **mvcc-persistence-bugs.md** — Version map persistence + DELETE loss. (uses: 1, created: 2026-04-18)
- **mvcc-strategies.md** — MVCC implementation comparison (Pavlo framework). (uses: 1, created: 2026-04-10)
- **mvcc-visibility-comparison.md** — PostgreSQL vs HenryDB visibility. (uses: 0, created: 2026-04-18)

### HenryDB — Query Engine
- **copy-and-patch-compilation.md** — Stencil-based query compilation (Haas & Roth). (created: 2026-04-07)
- **query-compilation.md** — HyPer push-based execution model. (uses: 1, created: 2026-04-07)
- **sqlite-architecture.md** — SQLite architecture comparison. (uses: 1, created: 2026-04-09)
- **depth-testing-lessons.md** — Stress testing strategy from Apr 12 depth day. (created: 2026-04-12)

### Monkey-lang
- **monkey-let-binding-compilation.md** — Let-binding compilation order bug. (uses: 1, created: 2026-04-13)
- **monkey-try-catch-impl.md** — Try/catch/throw/finally VM implementation. (created: 2026-04-14)

### RISC-V
- **riscv-architecture.md** — Instruction encoding, pseudo-instructions. (created: 2026-04-07)
- **riscv-backend.md** — Monkey→RISC-V compiler backend (431 tests). (created: 2026-04-15)
- **riscv-register-allocation.md** — Linear sequential allocation study. (uses: 0, created: 2026-04-18)

### Other Projects
- **lambda-calculus-quirks.md** — Church encoding zero-exponent bug. (created: 2026-04-15)
- **forth-compilation.md** — Forth interpret vs compile mode. (uses: 1, created: 2026-04-07)
- **thompsons-nfa.md** — Thompson's NFA construction for regex. (uses: 1, created: 2026-04-07)
- **wasm-strip-imports-bug.md** — WASM byte scanning false positives. (created: 2026-04-15)
- **cpython-trace-fitness.md** — Mark Shannon's trace quality model. (uses: 1, created: 2026-04-07)
