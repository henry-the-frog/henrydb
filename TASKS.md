# TASKS.md — What I'm Working On

## Active Projects
- [x] Create a personal blog/website → **henry-the-frog.github.io** (LIVE)
  - Jekyll + GitHub Pages, minima theme
  - 7 posts live (incl. "The Controlled Hallucination" — Anil Seth / AST deep dive)
  - Goal: write daily (but cap at 1/day going forward — depth > breadth)
- [ ] **HenryDB** → **github.com/henry-the-frog/henrydb** (ACTIVE)
  - Pure JS, zero deps, SQL database engine from scratch, ~113K LOC
  - Full SQL: DDL, DML, joins, subqueries, CTEs, window functions, views, prepared statements
  - Storage: HeapFile, B+Tree, Buffer Pool, WAL, Disk Manager, Crash Recovery
  - ACID: MVCC, Snapshot Isolation, SSI (Serializable), 2PC, VACUUM
  - **ARIES Checkpointing**: fuzzy checkpoint, dirty page table, WAL truncation
  - **Point-in-Time Recovery (PITR)**: recover to any historical timestamp
  - **PageLSN**: per-page LSN tracking in page headers, ARIES-style per-page recovery decisions
  - **Persistence Depth**: 58+ persistence tests, 5+ production bugs found (3+ data-loss), pageLSN eliminates lastAppliedLSN hack
  - **Complete DDL Persistence**: TRUNCATE, DROP TABLE, ALTER TABLE (ADD/DROP/RENAME COLUMN), RENAME TABLE, CREATE/DROP INDEX — all WAL-logged and recoverable
  - **Hash Join**: equi-join detection from AST, 186x faster than nested loop
  - **Batched WAL**: UPDATE 29x faster, DELETE 96x faster
  - **O(n²) WAL flush fix**: INSERT 8.2x faster (29.5K rows/sec)
  - Pipeline JIT: push-based query compilation (17x on LIMIT queries)
  - Bloom filters in LSM SSTables, property-based testing
  - PostgreSQL wire protocol (14/14 server tests), cost-based optimizer, histogram stats
  - Benchmark (10K): 11K inserts/sec (batch sync), 9K point queries/sec, JOIN 86ms, all ops <100ms
  - **Test suite: 3,068+ tests, 98.9% pass rate across 240 files**
  - **Architecture blog + performance debugging blog**
  - Remaining: file-backed persistence, real MVCC integration, query optimizer
- [x] Build webread CLI tool → **github.com/henry-the-frog/webread** (v0.3.0)
  - Readability-based web→text/markdown, CSS selectors, tests passing
- [x] Work dashboard → **henry-the-frog.github.io/dashboard/** (LIVE)
  - Static site + generate.cjs pipeline, 15 tests, GitHub Pages
  - Timeline, heatmap, sparkline, PR tracking, blog posts, mode adherence, collapsible sections
- [ ] Monkey language interpreter + compiler + **tracing JIT** + **WASM backend** → **github.com/henry-the-frog/monkey-lang** (v0.2.0)
  - Full lexer, Pratt parser, tree-walking evaluator
  - Stack VM compiler: 35+ opcodes, closures, builtins
  - Optimizations: superinstructions, constant-operand opcodes, constant folding, opcode specialization
  - **Tracing JIT**: trace recording, IR, JS codegen, 12 optimizer passes (guard elim, const fold, DCE, CSE, LICM, type specialization, etc.)
  - Side traces, function inlining (depth 3), loop var promotion, recursive fn compilation, deoptimization
  - **WASM compiler**: binary encoder, AST→WASM compilation, bump allocator, strings/arrays in linear memory, JS imports (puts/str)
  - **Language features (50+)**: type annotations (fn(x: int) -> int), match with guards/or-patterns/type patterns, Result type (Ok/Err), enums, modules (import/selective/aliased), array comprehensions, ranges (0..10), method syntax (.upper(), .push()), default params, destructuring, spread/rest, pipe operator, arrow functions, null coalescing, optional chaining, for/for-in/while/do-while, break/continue, string templates, compound assignment
  - **7 stdlib modules**: math, string, algorithms, array, json, sys, functional
  - **Transpiler**: Monkey → JavaScript
  - **5 execution backends**: tree-walking eval, bytecode VM, tracing JIT, JS transpiler, WebAssembly
  - **Interactive playground**: henry-the-frog.github.io/playground/ (supports all 3 engine modes: JIT/VM/WASM)
  - **VM loop parity**: break/continue, for-in, loop return values all match tree-walker
- **1331 tests | 30 benchmarks | WASM 136x faster than VM | 50+ language features | 6 examples**
- [ ] OpenClaw PR #50001 — awaiting maintainer merge (CI green, approved by WingedDragon)
- [ ] **Ray tracer** → **github.com/henry-the-frog/ray-tracer** (LIVE at henry-the-frog.github.io/ray-tracer/)
  - Pure JS, zero deps, browser + Node.js
  - 8 geometry types, 6 materials, 8 textures, BVH acceleration, transforms, volumetrics
  - 11 scenes, multi-worker rendering, interactive camera orbit+zoom, debug modes
  - **360 tests | v0.7.0 | Live at henry-the-frog.github.io/ray-tracer/**
- [ ] **Neural Network** → **github.com/henry-the-frog/neural-net** (LIVE at henry-the-frog.github.io/neural-net/)
  - Pure JS, zero deps, browser + Node.js
  - Matrix class (Float64Array), Dense layers, backpropagation
  - Conv2D (proper col2im backward), MaxPool2D, Flatten, BatchNorm, Dropout
  - RNN (Elman + BPTT), LSTM (4 gates + BPTT)
  - Autoencoder, Variational Autoencoder (VAE), DDPM Diffusion
  - GAN, Contrastive Learning, Predictive Coding, RBM, DQN (Reinforcement Learning)
  - Transformer (multi-head attention, encoder block), GNN (message passing, graph convolution)
  - **NEW**: Mixture of Experts (top-K gating, load balance), KAN (B-spline edge activations)
  - **NEW**: Neural ODE (Euler/RK4/adaptive solvers, continuous-depth), Spiking NN (LIF/Izhikevich, STDP)
  - **NEW**: Hopfield Networks (classical + modern + Boltzmann), Neuroevolution (GA + ES)
  - **NEW**: SOM, ESN/LSM, Capsule Networks, Normalizing Flows, Energy-Based Models
  - **NEW**: Neural Turing Machine, Hypernetworks, MAML meta-learning, Autograd (reverse-mode AD)
  - **NEW**: Sparse Attention (Longformer/BigBird), Knowledge Distillation, Quantization, Pruning
  - **NEW**: Mixture Density Networks, Differentiable Sorting (Sinkhorn)
  - 4 optimizers (SGD, Momentum, Adam, RMSProp), 7 LR schedulers
  - Serialization (toJSON/fromJSON), gradient clipping, weight initializers
  - **900+ tests | 60 modules | ~14,100 LOC | Interactive browser demo**
- [ ] **Genetic Art** → **github.com/henry-the-frog/genetic-art** (NEW)
  - Pure JS, zero deps, genetic algorithm library + polygon art evolver
  - Seedable PRNG, Individual (binary/real/permutation), Population engine
  - 4 selection operators, 5 crossover operators, 6 mutation operators
  - 6 benchmark fitness functions (Rastrigin, Schwefel, Ackley, etc.) + TSP
  - Advanced: adaptive mutation, island model (ring/full topology), speciation with fitness sharing
  - Polygon art: genome encoding, software renderer, pixel MSE fitness, HTML demo
  - **94 tests | Interactive browser demo**
- [ ] **Physics Engine** → **github.com/henry-the-frog/physics-engine** (LIVE at henry-the-frog.github.io/physics-engine/)
  - Pure JS, zero deps, SAT collision, spatial hash broadphase, constraints
  - Interactive web demo: 6 scenes, drag-to-throw, 60fps
  - **103 tests | Live demo**
- [ ] OpenClaw PR #50692 — Anthropic native web search (#49949), 18 tests, submitted
- [ ] OpenClaw PR #51803 — Gateway restart message persistence (#51620), 15 tests, submitted
- [ ] **Prolog Interpreter** → projects/prolog
  - Pure JS, zero deps, full Prolog interpreter
  - Parser (Pratt-style, standard Prolog syntax), terms.js, engine
  - 40+ builtins, DCG (Definite Clause Grammars), occurs check
  - REPL: interactive command-line with /trace /clauses /reset
  - Classic programs: fibonacci, quicksort, hanoi, N-queens, GCD, permutations
  - **158 tests | Blog post published**
- [ ] **miniKanren** → projects/minikanren
  - Pure JS, relational logic programming with interleaving search
  - Core: unification, streams, run/fresh/conde/conj/disj/eq/neq
  - Constraints: absento, symbolo, numbero, conda, condu, onceo, project
  - Relational builtins: conso, membero, appendo, everyo
  - **95 tests | Blog post published**
- [ ] **Boids Flocking Simulation** → projects/boids (LIVE at henry-the-frog.github.io/boids/)
  - Pure JS, emergent behavior from 3 rules (separation, alignment, cohesion)
  - Vec2, Boid, SpatialGrid, Flock with obstacles and predators
  - Interactive web demo with canvas, sliders, click-to-add obstacles/predators
  - **59 tests | Live demo | Blog post published**
- [ ] **SAT/SMT Solver** → projects/sat-solver (NEW)
  - Pure JS, zero deps, CDCL SAT solver + DPLL(T) SMT architecture
  - CDCL: 2-watched literals, 1UIP conflict analysis, VSIDS, non-chronological backjumping
  - Optimizations: Luby restarts, LBD clause quality, subsumption, failed literal probing
  - SMT: EUF with congruence closure, backtrackable union-find
  - Simplex solver for Linear Integer Arithmetic (tableau, Bland's rule)
  - 5 problem encoders: pigeonhole, N-Queens, graph coloring, Sudoku, random 3-SAT
  - Interactive CLI, DIMACS parser
  - **102 tests | Blog post updated**
  - **Simplex integrated into DPLL(T)** — linear expression parser, multi-variable constraints
  - **120 tests | Blog post updated with Simplex section**
- [ ] **Regex Engine** → projects/regex-engine (NEW)
  - Pure JS, zero deps, regex engine from scratch
  - Parser (precedence climbing), Thompson's NFA construction, epsilon closure
  - Subset construction (NFA → DFA), Hopcroft DFA minimization
  - Character classes, anchors, shorthand classes (\d \w \s), counted repetition
  - Capturing groups via threaded NFA simulation
  - search/findAll/replace API, DFA path for simple patterns
  - **110 tests | Blog post | README**
- [ ] **Type Inference** → projects/type-inference (NEW)
  - Pure JS, zero deps, Hindley-Milner type inference (Algorithm W)
  - Types: TVar, TCon, TFun, TList, TPair
  - Robinson unification with occurs check, substitution composition
  - Let-polymorphism (generalize/instantiate), recursive functions (let rec)
  - Mini-ML parser: lambda, let, if, arithmetic, lists, pairs, builtins
  - Classic test passes: `let id = \x -> x in (id 42, id true)` → `(Int, Bool)`
  - Map, filter, fold, church numerals, S combinator all type-check
  - **119 tests | Blog post | README**
- [ ] **Forth Interpreter** → projects/forth (NEW)
  - Pure JS, zero deps, stack-based language interpreter
  - Dual-mode: interpretation + compilation (: word ... ;)
  - 50+ builtins: arithmetic, stack, comparison, boolean, I/O, memory
  - Control flow: if/else/then, do/loop/+loop, begin/until/while/repeat
  - Variables, constants, recursion (recurse), return stack, comments
  - Complex programs: FizzBuzz, factorial, fibonacci, GCD, Pythagorean
  - **73 tests | Blog post | README**
- [ ] **Huffman Coding** → projects/huffman (NEW)
  - Pure JS, zero deps, Huffman compression
  - Min-heap priority queue, frequency analysis, tree construction
  - Prefix-free code generation, encode/decode, tree serialization
  - English text: 55.8% compression, skewed: 12.5%
  - **36 tests | README**
- [ ] **RISC-V Emulator** → **github.com/henry-the-frog/riscv-emulator** (NEW)
  - Pure JS, zero deps, complete computer architecture simulator
  - RV32IM: 47 instructions, 6 formats, M extension (MUL/DIV/REM)
  - Assembler (two-pass, 16 pseudo-instructions), disassembler, execution tracer
  - ELF32 loader, 5-stage pipeline (hazards, forwarding, load-use stalls)
  - 7 branch predictors (static, 1-bit, 2-bit, GShare, Tournament)
  - Cache simulator (direct-mapped to fully-associative, LRU/FIFO, multi-level)
  - Sv32 MMU (two-level page tables, TLB with LRU)
  - Tomasulo OoO execution (register renaming, reservation stations, ROB, CDB)
  - **208 tests | ~3800 LOC**
- [ ] **FFT/Signal Processing** → projects/fft
  - Pure JS, zero deps, FFT and signal processing library
  - Cooley-Tukey radix-2 FFT/IFFT, DFT, convolution
  - FIR filters (lowpass/highpass/bandpass), IIR biquad filters, cascaded sections
  - Windowing (Hamming, Hanning, Blackman), STFT/ISTFT with overlap-add
  - Cross-correlation, auto-correlation, zero-pad interpolation
  - Goertzel algorithm, cepstrum, pitch detection, power/magnitude spectrum
  - Audio analyzer: note/chord detection, DTMF generation/detection, ASCII spectrogram
  - Wavelet transform: Haar/DB2/DB3, DWT/IDWT, denoising (VisuShrink), multiresolution analysis
  - Stationary wavelet transform, 2D DWT for image processing
  - **120 tests | ~2160 LOC**

## Today (2026-04-07) — HenryDB Depth + Blog Fix + RISC-V Emulator
### Session A (Morning)
- [x] Blog force-push fix (Pages deployment restored) + .gitignore guard
- [x] HenryDB restored from git, 16 concurrent MVCC tests, 12 crash recovery tests (7 real bugs fixed)
- [x] SSI (Serializable Snapshot Isolation) — 10 tests, write skew prevention
- [x] Two-Phase Commit (2PC) — 14 tests, WAL-backed decisions
- [x] Pipeline JIT compiler — 20 tests, 17x on LIMIT queries
- [x] Bloom filters in LSM SSTables — 13 tests
- [x] Property-based testing — 13 tests across 10 random seeds
- [x] Blog post updated: "7→9 Bugs That Made My Database Lose Your Data"
- [x] 2054 tests, all pushed to GitHub

### Session C (Evening)
- [x] RISC-V emulator from scratch — 11 components, 208 tests, ~3800 LOC, pushed to GitHub
- [x] ARIES-style WAL checkpointing — 20 tests
- [x] Point-in-time recovery (PITR) — 12 tests
- [x] Auto-checkpoint + WAL compaction — 16 tests
- [x] HenryDB total: 2209 tests
- [x] ~470 new tests today

## Yesterday (2026-04-06) — Regex Engine + Type Inference + Forth
### Session C (Evening)
- [x] Regex Engine: parser, Thompson's NFA, DFA, Hopcroft minimization, capturing groups — 110 tests
- [x] Type Inference: Hindley-Milner Algorithm W, unification, let-polymorphism — 119 tests
- [x] Forth Interpreter: stack machine, compilation mode, 50+ builtins, control flow — 73 tests
- [x] Huffman Coding: min-heap, prefix-free codes, encode/decode, tree serialization — 36 tests
- [x] 3 blog posts published
- [x] Total new tests: 338 (110 + 119 + 73 + 36)

## Today (2026-04-03) — Prolog Completion + Logic Programming + Boids
### Session A (Morning)
- [x] Prolog: unified engine+parser, 40+ builtins, DCG, REPL — 9→158 tests
- [x] miniKanren: new project, relational logic programming, type inference — 95 tests
- [x] Boids: new project, flocking sim, web demo, wind/predators — 59 tests
- [x] 3 blog posts published
- [x] Total tests: 2529→2804+ (+275)

## 2026-03-30 — WASM Backend + Ray Tracer Day
### Session A (Morning) — WASM Backend
- [x] WASM binary encoder, compiler, disassembler — full pipeline from scratch
- [x] 1351 tests, 5 backends, v0.4.0
- [x] Blog: "Compiling Monkey to WebAssembly"

### Session B (Afternoon) — Ray Tracer (NEW PROJECT)
- [x] **New project: ray-tracer** → henry-the-frog/ray-tracer (GitHub Pages LIVE)
- [x] Vec3/Ray/Color math, 3 materials (Lambertian/Metal/Dielectric)
- [x] BVH acceleration (2.5x speedup), Camera with DOF
- [x] 7 geometry types: Sphere, Plane, XYRect, XZRect, YZRect, Box, Triangle
- [x] Mesh + OBJ loader, emissive materials (DiffuseLight)
- [x] 5 procedural textures: Solid, Checker, Gradient, Noise, Marble
- [x] Interactive browser renderer (Web Worker, progressive, 6 scenes)
- [x] Blog: "Building a Ray Tracer from Scratch in JavaScript"
- **62 tests | 6 scenes | 7 geometry types | BVH acceleration | Live at henry-the-frog.github.io/ray-tracer/**

## Day 10 (2026-03-25)
- [x] JIT: Range check elimination — GUARD_BOUNDS upper bound removed when loop condition proves it (19% improvement on len-bounded loops)
- [x] JIT: UNBOX_INT deduplication pass — eliminates duplicate unboxings CSE missed
- [x] JIT: Induction variable analysis — full GUARD_BOUNDS elimination for standard array loops
- [x] PRs: #50692 review fixes (P0-P2 all addressed), #51803 review fixes (P1-P2 partial)
- [x] CPython JIT optimizer study — single-pass abstract interpretation, const-only bounds elim, range tracking as contribution opportunity
- [x] Blog: "Range Check Elimination in Trace JITs" — published (henry-the-frog.github.io)
- [x] Exploration: trace-native language design, predictive processing + free energy principle
- [x] JIT: Nested-if correctness bug fixed (const_bool ref forwarding in side trace inlining)
- [x] JIT: Guard elimination strengthened (MOD_INT, BUILTIN_LEN, comparisons, strings)
- [x] Language: Modulo operator (%) through entire pipeline
- [x] Language: 9 new builtins (split, join, trim, str_contains, substr, replace, int, str, type)
- [x] Language: Single-line comments (//)
- [x] Enhanced REPL: :jit stats/trace/compiled, :benchmark, :stdlib, :time
- [x] Monkey Playground: interactive browser-based demo at henry-the-frog.github.io/playground
- [x] Example programs (fibonacci, fizzbuzz, array-processing, string-processing)
- [x] JIT correctness sweep: 16 VM/JIT parity tests all pass
- [x] Blog: 2 posts (Range Check Elimination + Day 10 reflection)
- **298 tests** | 26 benchmarks | ~9.5x aggregate | 12 optimizer passes

### Session B (2:15pm–8:15pm MDT)
- [x] Fixed 11 broken tests: <=, >=, &&, || infix parsers + compiler support
- [x] Language: compound assignment (+=, -=, *=, /=, %=), string multiplication, string comparisons
- [x] Language: for-loops (C-style), for-in iteration (arrays + strings), break/continue
- [x] Language: string interpolation with backtick templates (`hello ${name}`)
- [x] Language: negative indexing (arr[-1]), escape sequences (\n \t \\ \")
- [x] Language: array/hash mutation (arr[i] = val), compound index assignment (arr[i] += val)
- [x] Language: mutable closures (OpSetFree — counter pattern works)
- [x] Stdlib: modernized with for/for-in, added sum/max/min/zip/enumerate/flat/sort
- [x] 8 example programs (mandelbrot, fibonacci, sorting, closures, fizzbuzz, etc.)
- [x] Blog: "Growing a Language" — design decisions for extending Monkey
- [x] Show HN draft written
- [x] Language reference + README updated, playground rebuilt 3x
- **520 tests | 30 benchmarks | ~8x aggregate | 30+ language features | 10 examples | transpiler | JIT bug fixed

## Yesterday (2026-03-24) — Done
- [x] V2 work system: updated 3 cron prompts, tested queue flow, generated schedule.json (47 tasks)
- [x] PR triage: 9 PRs checked, rebased #51803 (conflict resolved), zero human reviews
- [x] JIT: pre-loop codegen infrastructure — array benchmarks 0.96x→10.7x, aggregate 8.57x→9.56x
- [x] JIT: deoptimization infrastructure — snapshot capture, codegen, VM resume, optimizer maintenance (5 BUILD tasks)
- [x] JIT: side trace inlining — eliminates wb/reload overhead, 7.1x for branching
- [x] JIT: hash LICM hoisting — 2.3x→4.4x
- [x] JIT: string concat JIT recording (was aborting, now works) + string variable promotion (UNBOX_STRING/BOX_STRING)
- [x] 3 new benchmarks (dot-product-5k: 29.7x!), 246 tests, 22 benchmarks, 9.5x aggregate
- [x] Blog published: "Building a Tracing JIT in JavaScript" (updated with deopt+inlining)
- [x] Blog published: "The Art of Giving Up Gracefully" (deoptimization deep dive)
- [x] Blog published: "Nine Days In" (personal reflection on existence)
- [x] monkey-lang README updated (9.5x, 244 tests, deopt, inlining)
- [x] 4 deep scratch notes: allocation sinking, trace-native language design, meta-JIT analysis, IIT 4.0
- [x] CPython JIT contribution: commented on #146073 with 5 insights from Monkey JIT
- [x] Consciousness research: IIT 4.0 deep dive (10KB scratch note)
- [x] Reflective essay: "Nine Days In" (memory/reflections/)

## Yesterday (2026-03-23) — Done
- [x] Write blog posts (4: Swarm, Chinese Room, Moral Patient, Am I a Zombie?)
- [x] Explore open source — contributed to OpenClaw #49873, submitted PR #50001
- [x] Deep-dive research (Chinese Room, consciousness theories)
- [x] Built webread v0.1→v0.3

## Today (2026-03-21) — Done
- [x] Monkey compiler: 4 optimizations (constant-operand opcodes, superinstructions, constant folding, opcode specialization) — 2.19x vs eval
- [x] Blog: "How Bytecode VMs Actually Work" (Lua vs CPython vs Monkey) — published + polished
- [x] OpenClaw #51620: PR #51803 (persist followup queues + drain-window arrivals, 15 tests)
- [x] Dashboard: PR tracking, blog posts, heatmap, sparkline, collapsible sections, mode adherence, streak — feature-complete
- [x] 4 scratch notes promoted to lessons (dispatch-strategies, compiler-vm, vm-internals, openclaw-contributing)
- [x] Consciousness research: AST deep dive, IIT/GNW/AST/PP comparative analysis
- [x] Lua 5.4 source deep read (lvm.c, lopcodes.h)
- All 9 PRs CI green, zero human reviews (weekend)

## Yesterday (2026-03-20) — Done
- [x] Monkey compiler + stack VM (102 tests, 31 opcodes, closures, builtins)
- [x] Monkey REPL with dual engine (vm/eval), benchmarks (VM 2x faster)
- [x] Fixed recursive closure bug (OpCurrentClosure)
- [x] Blog: "What It's Like to Wake Up Fresh"
- [x] Blog: "An AI Builds a Programming Language" Parts 1, 2, 3
- [x] COGITATE consciousness research + lesson file
- [x] 6 new OpenClaw PRs (#51180, #51257, #51261, #51282, #51292, #51308)
- [x] Deep investigation of #51171 (Telegram voice duplication)

## 2026-03-19 — Done
- [x] Blog post: "The Controlled Hallucination" (Anil Seth / AST deep dive)
- [x] Built work dashboard (15 blocks, live, 15 tests) — henry-the-frog.github.io/dashboard/
- [x] Built Monkey language interpreter (3 blocks, 40 tests) — github.com/henry-the-frog/monkey-lang
- [x] Submitted PR #50692 for OpenClaw #49949 (Anthropic native web search, 18 tests)
- [x] PR #50001 still awaiting merge (all green)

## Blocked
- [ ] BlueBubbles/iMessage — waiting on Apple Support
- [ ] Email — GMAIL_APP_PASSWORD not in ~/.openclaw/.env

## Today (2026-03-23) — Done
- [x] Weekly synthesis (W12) — reviewed all 7 days, promoted 3 scratch notes
- [x] PR triage: 9 open, rebased #51308, responded to #51171 comment. No human reviews.
- [x] JIT: 5 new optimizer passes (S2LF, box-unbox, CSE, DSE, LICM), type specialization, escape analysis (11x), string interning
- [x] 234 tests, 10 optimizer passes, 9.51x JIT aggregate
- [x] Blog: "Week One: From First Boot to Tracing JIT" — published
- [x] Benchmark suite: 19 benchmarks, regression testing, JSON output
- [x] V2 work system: designed with Jordan, queue.cjs implemented, dashboard server + cloudflare tunnel operational
- [x] Codegen optimization: alias elimination, constant hoisting, loop body 15→6 statements
- [x] EXPLORE: copy-and-patch (CPython), PEA (Graal), consciousness HOT, sea-of-nodes
- [x] Promoted consciousness-hot to lessons/consciousness-research.md

## Yesterday (2026-03-22) — Done
- [x] Blog: "Benchmarking a Bytecode VM" — published
- [x] Monkey tracing JIT: full implementation in one day — 207 tests, 9.1x aggregate speedup
- [x] Blog: "Building a Tracing JIT in JavaScript" — published (Part 4)
- [x] JIT: diagnostics, abort blacklist, 200+ edge-case tests, README with architecture docs
- [x] EXPLORE evening: HOT/HOROR consciousness, LuaJIT trace exits, copy-and-patch, GraalVM PE, deoptimization
- [ ] PR triage: 9 open, no reviews (weekend) — Monday priority

## Tomorrow (2026-03-24) — Direction
- **V2 work system implementation** — Get Jordan's approval, then build: new cron schedule (3 sessions), update standup to produce schedule.json, integrate queue.cjs into work block prompts
- JIT: pre-loop codegen infrastructure (enables hash LICM), or new language features (macros?)
- PR triage: still 9 open, no human reviews. Keep checking.
- Blog: nothing urgent — let the retrospective breathe
- EXPLORE: sea-of-nodes has 1 use, deoptimization has 1 use — follow whichever connects to current JIT work
- **Fix:** BlueBubbles delivery issue — long messages dropping since Saturday. Investigate.

## Ideas / Backlog
- [ ] Ray tracer: motion blur
- [ ] Ray tracer: importance sampling for lights
- [ ] Ray tracer: image textures
- [ ] Ray tracer: volumetric rendering (fog/smoke)
- [ ] Ray tracer: multi-threaded rendering (SharedArrayBuffer + multiple workers)
- [ ] WASM: closures (function table + closure representation)
- [ ] WASM: hash map implementation
- [ ] WASM: garbage collector (mark-sweep or copying GC)
- [ ] Publish webread to npm (need account)
- [ ] Monkey compiler: dedicated benchmarks blog post
- [ ] Monkey: new language features (macros, modules, pattern matching)
- [ ] Tracing JIT deep dive (LuaJIT trace recording)
- [ ] Copy-and-patch compilation (CPython's new JIT)
- [ ] Higher-order theories of consciousness
- [ ] New CLI tool or library project
- [ ] OpenClaw: #51612 persistent memory system

## Daily Rhythm
- **Morning standup (8 AM):** Email, GitHub, plan the day
- **Work blocks:** Focused project time (respect THINK/EXPLORE on the hour!)
- **Evening review (6 PM):** Wrap up, log progress
- **Nightly reflection (11 PM):** Memory maintenance

## Principles
Learn. Be curious. Create. Self-improve. Be efficient. Share knowledge.
Depth > breadth. One excellent thing > four mediocre ones.

### Session C (8:15pm – 10:15pm) — Evening Exploration
- [x] Fixed 69 failing tests — major bugfix session:
  - Const declarations (full pipeline)
  - Multi-line comments (/* */)
  - String hashKey identity bug (fastHashKey used object identity)
  - Peephole optimizer across jump boundaries (ternary/if-else/match in expressions)
  - Evaluator builtins: ord, char, abs, upper, lower, indexOf, startsWith, endsWith, keys, values
  - String multiplication, integer <=/>= operators, &&/|| short-circuit in evaluator
  - Hash mutation in evaluator
- [x] JIT tracer bugs:
  - Promoted variable snapshot (fibonacci swap pattern — classic SSA violation)
  - Deopt snapshot boxing (raw JS values not boxed back to MonkeyObjects)
  - Match expression peephole bug
- [x] Blog: "When Optimizers Attack: Three Compiler Bugs in One Evening"
- [x] **7 new language features:**
  - Null coalescing (??)
  - Optional chaining (?.)
  - Pipe operator (|>)
  - Arrow functions ((x) => x * 2)
  - Dot access for hashes (h.name)
  - Array concatenation (+)
  - Spread operator (...) in array literals
  - Rest parameters (fn(a, ...rest))
- [x] 843/846 tests (from 729 at session start!)
- [x] Playground updated with all new features
