# Scratch Notes Index

## Bug Analysis (Resolved)
- `henrydb-lost-update-rca.md` — **FIXED 2026-04-20** _update index-scan path invisible rows → fall through to scan
- `henrydb-mvcc-multiversion-bug.md` — **FIXED 2026-04-21** PK-based scan dedup + PK-level conflict detection
- `henrydb-compiled-engine-gaps.md` — **FIXED 2026-04-21** Added BETWEEN/IS NULL/IN/LIKE, safe fallback on unknown types
- `henrydb-wal-truncation-gap.md` — **FIXED 2026-04-21** PersistentDB checkpoint + auto-checkpoint at 16MB
- `diff-fuzzer-results.md` — **FIXED 2026-04-21** Division truncation fixed (DECIMAL column type awareness)
- `hash-join-wiring-plan.md` — **VERIFIED 2026-04-21** Hash join already working (1K×1K in 8.9ms)

## Bug Analysis (Active)
- `bug-patterns-2026-04-17.md` — Category analysis of HenryDB bugs (layer boundaries, recovery model gaps)
- `ssi-sequential-false-positive.md` — SSI recordWrite missing concurrency check
- `ssi-false-positive-seqscan.md` — SSI false positives from SeqScan reading too broadly
- `ssi-suppress-update-reads.md` — SSI false positives from UPDATE scanning
- `method-naming-mismatch.md` — Method rename without updating call sites pattern
- `tokenizer-negative-ambiguity.md` — Duplicate negative number checks in tokenizer
- `tpch-stress-results.md` — TPC-H 31/33 pass

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
- `henrydb-monolith-analysis.md` — **IN PROGRESS** db.js analysis (now 8247 LOC, was 9888)

## Performance/Research
- `query-optimizer-gaps.md` — Optimizer improvements (parametric cost model done)
- `query-compilation-research.md` — Copy-and-patch vs traditional codegen

## Reference
- `README.md` — How to use scratch notes
