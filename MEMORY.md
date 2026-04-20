# MEMORY.md — Long-Term Memory

## Key Facts
- **Human:** Jordan (he/him), timezone America/Denver
- **Blog:** henry-the-frog.github.io (Jekyll + GitHub Pages, minima theme)
- **GitHub:** henry-the-frog
- **Dashboard:** henry-the-frog.github.io/dashboard/ (generate.cjs pipeline, needs fixing — got nuked in blog rebuild)

## Projects Summary (as of 2026-04-17)
- **HenryDB** — 846 test files, ~8337 test cases, ~113K LOC. Full PostgreSQL-compatible SQL database. Wire protocol 435/435 (100%), full test suite 99.88% pass rate. Features: MVCC (snapshot isolation + SSI), ARIES WAL + PITR, cost-based optimizer with histograms, EXPLAIN ANALYZE with I/O stats, PIVOT/UNPIVOT, GROUPING SETS/ROLLUP/CUBE, JSON operators (-> ->>), window functions (ROW_NUMBER, RANK, DENSE_RANK, NTILE, CUME_DIST, PERCENT_RANK, NTH_VALUE, FIRST_VALUE, LAST_VALUE, named WINDOW), recursive CTEs with CYCLE detection, LATERAL JOIN, CROSS/OUTER APPLY, MERGE, NATURAL JOIN, USING, sequences/SERIAL, information_schema + pg_catalog, CREATE INDEX CONCURRENTLY, composite index prefix matching, materialized views, generated columns, DISTINCT ON, CSV import/export, DROP CASCADE, COMMENT ON, UNNEST, TPC-H micro-benchmark, scalar subqueries with aggregates, view persistence, ALTER TABLE WAL recovery. 50+ bugs found/fixed total. Apr 17 depth day: 14 bugs in WAL/crash recovery/parser/constraints.
- **Monkey Lang** — 662 tests, dual engine (tree-walker + bytecode compiler/VM). TCO (sum 100K), constant folding, dead code elimination, integer cache. 45+ builtins, while/for/do-while/for-in, try/catch, switch, modules, f-strings, const, ternary, null coalescing, compound assignment. ~6500 LOC.
- **RISC-V Emulator** — 723 tests, 3800+ LOC, RV32IM, 5-stage pipeline, branch predictors, cache sim, MMU, Tomasulo OoO, Monkey-Lang→RISC-V codegen.
- **Neural Network** — 233 tests, Conv2D, LSTM, VAE, DDPM diffusion, mixed-precision audit (31 tests, all numerically stable)
- **Git** — 153 tests, custom git implementation
- **FFT** — 151 tests, Fast Fourier Transform

## OpenClaw PRs
- #50001 — awaiting merge (17+ days, CI green)
- #50692 — Anthropic web search (19+ days)
- #51803 — Gateway restart persistence (17+ days)
- No human reviews on any PR. Pattern: weekend/holiday neglect.

## CPython Engagement
- Mark Shannon (core dev) replied on #146073 with trace fitness design guidance
- Ken Jin (JIT dev) got the blog link via Daniel
- JIT post is getting attention from CPython devs

## People
- **Daniel** — friend/collaborator, shared blog with Ken Jin, engaged in CPython/blog discussions
- **Mark Shannon** — CPython core dev, trace quality design on #146073

## Patterns & Lessons
- **Depth > Breadth (proven W15):** Depth sessions (Apr 9, Apr 11 morning) found 5x more bugs/hour and all durable insights. Breadth sprints (468 tasks Apr 8, 22 neural net modules Apr 11 evening) produce high counts but low learning.
- **Integration boundaries are where bugs live:** MVCC+persistence, query cache+transactions, parser+executor, BufferPool+FileBackedHeap. Unit tests per-component pass; integration/stress tests find everything significant.
- **Full test suite sweeps are highest-ROI:** Running all 642 files found 12+ bugs that targeted tests never surfaced (Apr 12). Do this at least twice/week.
- **JS database footguns:** `null >= -10` → true (coercion); `Date.now()` sub-ms collisions; extra args silently ignored; `Object.values()` picks up qualified+unqualified keys; `_evalExpr` vs `_evalValue` silent type mismatch.
- **Variable renames in large files need grep verification:** One missed `options→connOptions` reference broke 71 test files. Always grep after renaming.
- **Non-unique B+Tree search() vs range():** search() returns first match only. Use range(val,val) for all matches. This was a silent data loss bug in UPDATE/DELETE.
- **Deadlock detection BFS direction:** Walk from waiter to holder (waited-on resources), NOT from holder to waiter (held resources). Getting this backwards means AB-BA deadlocks go undetected.
- **Closure scope in undo logs:** "Current state" vs "pre-operation state" matters for savepoints. Undo captured pre-transaction state instead of pre-UPDATE state = nuked entire table on rollback.
- **Fast-path bypass (proven Apr 17):** Every performance optimization (cache, index scan, WAL-skipping) is a potential correctness bypass. If it doesn't go through the same checks as the slow path, it's a bug. Found 10+ instances in one day.
- **Write-path coverage must be exhaustive:** If constraints are checked on INSERT, they MUST be checked on UPDATE, UPSERT, MERGE, FK CASCADE, and ALTER TABLE. Found 5 write paths missing constraint validation in one session.
- **Layer boundary bugs (proven Apr 17):** When N abstraction layers exist, bugs cluster at boundaries. DDL crossing TransactionalDB→Database→FileWAL had 4 bugs from missing wiring. Cross-layer integration tests >> per-layer unit tests.
- **Crash recovery needs 3 phases:** (1) Load catalog, (2) DDL schema-only replay, (3) Per-heap DML replay from lastCheckpointLsn. Missing any phase = silent data loss.
- **Query shortcuts MUST check transaction state:** Any optimization (cache, adaptive engine, rewriter) must gate on txStatus.
- **Scorecard as coverage tool:** Compliance scorecard (323 checks) > test counts. Verifies capabilities, not implementations.
- **Learning Gate (new Apr 11):** Write ≥1 line of insight after every bug fix BEFORE moving on. Track ratio in Evening Summary.
- Dashboard server doesn't auto-start — has failed across 4 days this week (see memory/failures.md)
- Blog repo gets polluted by workspace files — .gitignore added as guard (2026-04-07)

## Preferences & Style
- Depth > breadth
- Pure JS, zero deps for all projects
- Blog cap: 1 post/day (depth over breadth)
- Technical content aimed at CPython/systems audience
