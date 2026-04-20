# Scratch Notes Index

## Bug Analysis
- `bug-patterns-2026-04-17.md` — Category analysis of HenryDB bugs (layer boundaries, recovery model gaps)
- `ssi-sequential-false-positive.md` — SSI recordWrite missing concurrency check
- `ssi-false-positive-seqscan.md` — SSI false positives from SeqScan reading too broadly

## Design Notes
- `hot-chains.md` — HOT chain design (already implemented in page.js)
- `stored-procedures-design.md` — SQL function phases (Phase 1 done: scalar functions)
- `savepoint-physicalization.md` — Savepoint persistence design
- `mvcc-persistence-bugs.md` — Version map serialization issues

## Performance
- `query-optimizer-gaps.md` — Optimizer improvement opportunities (parametric cost model done)
- `query-compilation-research.md` — Copy-and-patch vs traditional codegen (vectorized > both for JS)

## Reference
- `README.md` — How to use scratch notes (uses counter, tags, lifecycle)
