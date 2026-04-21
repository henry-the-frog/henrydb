# Scratch Notes Index

## Bug Analysis
- `bug-patterns-2026-04-17.md` — Category analysis of HenryDB bugs (layer boundaries, recovery model gaps)
- `ssi-sequential-false-positive.md` — SSI recordWrite missing concurrency check
- `ssi-false-positive-seqscan.md` — SSI false positives from SeqScan reading too broadly
- `ssi-suppress-update-reads.md` — SSI false positives from UPDATE scanning
- `method-naming-mismatch.md` — **NEW 2026-04-20** Method rename without updating call sites pattern
- `tokenizer-negative-ambiguity.md` — **NEW 2026-04-20** Duplicate negative number checks in tokenizer

## Design Notes
- `hot-chains.md` — HOT chain design (already implemented in page.js)
- `stored-procedures-design.md` — SQL function phases (Phase 1 done: scalar functions)
- `savepoint-physicalization.md` — Savepoint persistence design
- `mvcc-persistence-bugs.md` — Version map serialization issues
- `savepoint-rollback-isolation.md` — Savepoint version map scoping
- `mvcc-index-bypass.md` — MVCC visibility in index scans

## Performance
- `query-optimizer-gaps.md` — Optimizer improvement opportunities (parametric cost model done)
- `query-compilation-research.md` — Copy-and-patch vs traditional codegen (vectorized > both for JS)

## Reference
- `README.md` — How to use scratch notes (uses counter, tags, lifecycle)
- `tpch-stress-results.md` — **NEW 2026-04-20** TPC-H 31/33 pass, hash join dead code, parser bugs
- `diff-fuzzer-results.md` — **NEW 2026-04-20** Division truncation, NULL IS NULL broken, CAST no-op
- `hash-join-wiring-plan.md` — **NEW 2026-04-20** 30-line fix: add hash join to _executeJoinWithRows
- `henrydb-monolith-analysis.md` — **NEW 2026-04-20** db.js 9844-line analysis: module boundaries, extraction order, duplicate _analyzeTable bug
- `henrydb-wal-truncation-gap.md` — **NEW 2026-04-20** WAL truncation: TransactionalDB OK, PersistentDB missing checkpoint/truncation entirely
- `henrydb-mvcc-interception.md` — **NEW 2026-04-20** MVCC via heap monkey-patching: 4 intercepted methods, fragility analysis, recommendations
- `henrydb-compiled-engine-gaps.md` — **NEW 2026-04-20** Compiled engine only handles 4 expr types. Silent null on unknown → correctness bug (returns all rows)
- `henrydb-lost-update-rca.md` — **NEW 2026-04-20** Root cause: _update index-scan path returns invisible MVCC rows as null → 0 rows updated. Fix: fall through to scan when index returns invisible rows.
