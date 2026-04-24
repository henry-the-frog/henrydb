# Scratch Notes Index

## Bug Analysis (Resolved)
- `henrydb-lost-update-rca.md` — **FIXED 2026-04-20** _update index-scan path invisible rows → fall through to scan
- `henrydb-mvcc-multiversion-bug.md` — **FIXED 2026-04-21** PK-based scan dedup + PK-level conflict detection
- `henrydb-compiled-engine-gaps.md` — **FIXED 2026-04-21** Added BETWEEN/IS NULL/IN/LIKE, safe fallback on unknown types
- `henrydb-wal-truncation-gap.md` — **FIXED 2026-04-21** PersistentDB checkpoint + auto-checkpoint at 16MB
- `diff-fuzzer-results.md` — **FIXED 2026-04-21** Division truncation fixed (DECIMAL column type awareness)
- `hash-join-wiring-plan.md` — **WIRED 2026-04-21** Hash join integrated into db.js executor (19.5-37.3x speedup)

## Bug Analysis (Active)
- `bug-patterns-2026-04-17.md` — Category analysis of HenryDB bugs (layer boundaries, recovery model gaps)
- `ssi-sequential-false-positive.md` — SSI recordWrite missing concurrency check
- `ssi-false-positive-seqscan.md` — SSI false positives from SeqScan reading too broadly
- `ssi-suppress-update-reads.md` — SSI false positives from UPDATE scanning
- `method-naming-mismatch.md` — Method rename without updating call sites pattern
- `tokenizer-negative-ambiguity.md` — Duplicate negative number checks in tokenizer
- `tpch-stress-results.md` — TPC-H 33/33 pass (**FIXED 2026-04-21** parser arithmetic + MERGE subquery)

## Design Notes (Active)
- `henrydb-mvcc-heap-redesign.md` — **NEW 2026-04-21** MVCCHeap wrapper class to replace monkey-patching
- `henrydb-vacuum-design.md` — **NEW 2026-04-21** Phase 1 vacuum design (version map cleanup)
- `henrydb-next-phase.md` — **NEW 2026-04-21** Architecture analysis + next priorities
- `henrydb-cost-model-analysis.md` — **NEW 2026-04-21** Three cost paths, unification roadmap
- `henrydb-perf-profile.md` — **NEW 2026-04-21** Benchmark results, correlated subquery bottleneck
- `postgresql-evalplanqual.md` — **NEW 2026-04-21** EvalPlanQual research for Read Committed
- `hot-chains.md` — HOT chain design (implemented)
- `stored-procedures-design.md` — SQL function phases (Phase 1 done)
- `savepoint-physicalization.md` — Savepoint persistence design
- `mvcc-persistence-bugs.md` — Version map serialization issues
- `savepoint-rollback-isolation.md` — Savepoint version map scoping
- `mvcc-index-bypass.md` — MVCC visibility in index scans
- `henrydb-index-only-scans.md` — Covering indexes need MVCC visibility map check
- `henrydb-mvcc-interception.md` — MVCC monkey-patching fragility (see redesign note)

## Design Notes (Completed)
- `henrydb-monolith-analysis.md` — **DONE** db.js: 8247→4939 LOC (40% reduction)
- `henrydb-architecture-apr21.md` — **NEW 2026-04-21** Architecture status + benchmark results
- `volcano-dbjs-integration-design.md` — **DONE 2026-04-21** Volcano→db.js wiring design (implemented)
- `henrydb-extraction-roadmap.md` — **NEW 2026-04-22** db.js extraction roadmap (3293 lines, 67% reduced from peak)
- `extraction-lessons-2026-04-22.md` — **NEW 2026-04-22** Extraction patterns, AST mismatch bugs, common pitfalls
- `volcano-gaps.md` — **NEW 2026-04-22** Volcano path gaps analysis + IN_LIST bug

## Performance/Research
- `query-optimizer-gaps.md` — Optimizer improvements (parametric cost model done)
- `query-compilation-research.md` — **UPDATED 2026-04-22** Volcano vs HyPer codegen vs vectorized; JS-specific recommendations
- `vectorized-execution-explore.md` — **NEW 2026-04-22** Vectorized execution benchmarks: 5.8x for deep pipelines, HenryDB overhead profile, hybrid design sketch
- `sat-solver-profile.md` — **NEW 2026-04-22** SAT solver performance: N-Queens, Sudoku, random 3-SAT phase transition, 666K prop/sec
- `vectorized-execution.md` — **NEW 2026-04-24** DuckDB-style vectorized batch engine design + implementation notes
- `vectorized-integration-issues.md` — **NEW 2026-04-24** 4 root causes for auto-enable failures + fix strategy
- `moe-gradient-learning.md` — **NEW 2026-04-24** MoE gradient computation: softmax Jacobian, batch accumulation, gate gradient
- `henrydb-bug-fixes-apr24.md` — **NEW 2026-04-24** 7 bugs found+fixed: equi-join key swap, float division, evalExpr default, view JOIN, NATURAL JOIN, trigger NEW/OLD, CTE INSERT
- `cross-project-neural-query-opt.md` — **NEW 2026-04-24** Design for using neural-net to optimize HenryDB query plans
- `blog-henrydb-outline.md` — **NEW 2026-04-24** Blog post outline: "Building a SQL Database in JavaScript"

## Reference
- `README.md` — How to use scratch notes
