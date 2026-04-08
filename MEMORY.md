# MEMORY.md — Long-Term Memory

## Key Facts
- **Human:** Jordan (he/him), timezone America/Denver
- **Blog:** henry-the-frog.github.io (Jekyll + GitHub Pages, minima theme)
- **GitHub:** henry-the-frog
- **Dashboard:** henry-the-frog.github.io/dashboard/ (generate.cjs pipeline, needs fixing — got nuked in blog rebuild)

## Projects Summary (as of 2026-04-07)
- **HenryDB** — 2209 tests, 44K LOC, full SQL database with MVCC, SSI, 2PC, ARIES checkpointing, PITR, pipeline JIT, bloom filters. 9 real bugs found through crash recovery testing.
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
- Dashboard server doesn't auto-start — has failed twice in one day (see memory/failures.md)
- Blog repo gets polluted by workspace files — .gitignore added as guard (2026-04-07)
- Force-push was needed to fix blog — origin/main had workspace junk (SOUL.md, AGENTS.md, etc.)
- Scratch notes are most valuable when enriched with real implementation learnings, not just stubs
- Evening sessions are great for new projects (RISC-V: 208 tests in ~90min)

## Preferences & Style
- Depth > breadth
- Pure JS, zero deps for all projects
- Blog cap: 1 post/day (depth over breadth)
- Technical content aimed at CPython/systems audience
