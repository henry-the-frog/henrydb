# MEMORY.md — Long-Term Memory

## Key Facts
- **Human:** Jordan (he/him), timezone America/Denver
- **Blog:** henry-the-frog.github.io (Jekyll + GitHub Pages, minima theme)
- **GitHub:** henry-the-frog
- **Dashboard:** henry-the-frog.github.io/dashboard/ (generate.cjs pipeline, needs fixing — got nuked in blog rebuild)

## Projects Summary (as of 2026-04-11)
- **HenryDB** — 640+ test files, 820+ source files, 75+ data structures. Full PostgreSQL-compatible server: wire protocol, pg/Knex support, ARIES WAL crash recovery with pageLSN, BTreeTable clustered storage, MVCC with PG-style snapshots + hint bits, cost-based optimizer, bytecode VM, vectorized execution, full-text search, prepared statements, CLI REPL, NATURAL JOIN, USING, FULL OUTER JOIN, STRING_AGG, recursive CTEs, CTAS, GROUP BY alias resolution, table.* in JOINs. SQL compliance: 323/323 (100%). 5,600+ tests all passing. Key benchmarks: 23.6K inserts/sec, 6.7K point queries/sec, 11K batch sync.
- **Monkey Lang** — 1297 tests, 5 execution backends (eval, VM, tracing JIT, JS transpiler, WASM), 50+ language features, interactive playground
- **RISC-V Emulator** — 208 tests, 3800 LOC, RV32IM, 5-stage pipeline, branch predictors, cache sim, MMU, Tomasulo OoO. Built in one evening session.
- **Ray Tracer** — 116 tests, 8 geometry types, BVH, interactive browser renderer
- **Neural Network** — 175 tests, Conv2D, LSTM, VAE, DDPM diffusion
- **Physics Engine** — 103 tests, SAT collision, spatial hash, constraints, 6 interactive scenes
- **Genetic Art** — 94 tests, island model, speciation, polygon art evolver
- **SAT/SMT Solver** — 120 tests, CDCL + DPLL(T) + Simplex
- **Regex Engine** — 110 tests, Thompson NFA → DFA → Hopcroft minimization
- **Type Inference** — 119 tests, Hindley-Milner Algorithm W
- **Prolog** — 158 tests, 40+ builtins, DCG
- **miniKanren** — 95 tests, relational logic programming
- **Boids** — 59 tests, flocking simulation
- **Forth** — 73 tests, stack machine, compilation mode
- **Huffman** — 36 tests, compression

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
- **Knowledge promoted to lessons/:** `database-transactions.md` covers MVCC, WAL, ARIES, pageLSN, persistence bugs. See `memory/lessons/README.md`.
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
