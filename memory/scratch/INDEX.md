# Scratch Notes Index

## Session C (2026-04-27 evening) — New Files
- `wasm-closure-bugs.md` — 3 closure bugs: self-ref with multi-capture, sibling shared state, recursive+mutable crash. Root cause: copy-based captures, need box/cell pattern.

## Session A (2026-04-27 morning) — New Files
- `wasm-int-ptr-confusion.md` — Critical bug: integers colliding with data segment pointers (fix: offset 65536)
- `wasm-gc-research.md` — Node.js v22 supports WASM GC structs/arrays, stringref not yet available
- `wasm-host-import-cost.md` — Host import crossing is dominant WASM bottleneck, knownInt inference 8x speedup

## Session A (2026-04-26 morning) — New Files
- `session-a-learnings-apr26.md` — Key learnings (VM callbacks, WASM, classes, SQLite affinity, SSA, DCE)
- `runtime-dispatch-plan.md` — Implementation plan for OpMethodCall runtime dispatch (~100 LOC)
- `ssa-builder-improvement.md` — Plan to refactor SSA builder from string-based to structured expressions

## Session B (2026-04-25 afternoon) — New Files
- `session-b-learnings.md` — Key learnings (type systems, SSA, prelude pattern, bugs)
- `session-b-depth-findings.md` — Depth exploration findings
- `henrydb-query-engine.md` — 3 execution strategies (AST, Volcano, VDBE VM), cost-based optimizer
- `monkey-lang-optimizations.md` — Full optimization pipeline (4 levels, 8 passes)
- `monkey-lang-dce-analysis.md` — Dead code elimination strategy (SSA-level annotation)
- `vm-vs-interp-comparison.md` — monkey-lang VM vs lisp interpreter comparison
- `henrydb-architecture.md` — Updated architecture notes

## Session A (2026-04-25 morning) — Files
- `session-A-final.md` — Complete session summary (302 tasks, 15 bugs, metrics)
- `henrydb-feature-verification.md` — 36+ SQL feature categories verified
- `monkey-lang-feature-verification.md` — 49 AST nodes, all features working
- `monkey-const-subst-bug.md` — Constant substitution mutation bug (28 failures)
- `diff-fuzzer-results.md` — Differential fuzzer: 97.2% across 6000 queries

## Key Reference Files
- `henrydb-architecture.md` — 6 execution engines, adaptive layer
- `henrydb-5-execution-engines.md` — Engine comparison
- `henrydb-query-engine.md` — **NEW**: 3 strategies (AST, Volcano, VDBE), optimizer
- `monkey-lang-optimizations.md` — **NEW**: Full 4-level optimization pipeline
- `monkey-lang-dce-analysis.md` — **NEW**: DCE strategy analysis
- `monkey-ssa-pipeline-investigation.md` — SSA/optimizer pipeline
- `project-dashboard.md` — Cross-project stats
- `project-portfolio.md` — Portfolio overview

## Total: ~105 scratch files covering HenryDB, monkey-lang, and related research

## Session A (2026-04-28 morning) — New Files
- `wasm-closure-bugs.md` — Updated: RESOLVED. All 5 bugs fixed via box/cell pattern. 14 regression tests.
- `wasm-gc-explore-apr28.md` — NEW: WASM GC structs/arrays verified in Node.js v22, binary encoding notes
- `wasm-gc-backend-design.md` — NEW: 5-phase design for WASM GC backend (12-18 hours est.)

## Session C (2026-04-28 evening) — New Files
- `nan-boxing-research.md` — NaN-boxing research: IEEE 754, LuaJIT/SpiderMonkey approaches, SMI-like unboxing for JS host. Option B (raw numbers on stack) recommended.
- `superinstruction-analysis.md` — Opcode sequence analysis for superinstructions. Top candidates: OpIncrementLocal (saves 3 dispatches), OpAddSetLocal. Key finding: V8 JIT negates dispatch reduction (<0.5% benefit).
