# Scratch Notes Index

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
