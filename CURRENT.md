# CURRENT.md — Session Status

## Status: session-ended
## Session: 2026-04-20 Session B3 (7:19 PM - 8:00 PM MDT)
## Project: henrydb (deep architecture exploration)
## Completed: 2026-04-21T01:55:00Z

### Tasks Completed: 22 (T1-T22)
All THINK/EXPLORE/MAINTAIN — 0 BUILDs (at ceiling 58/60)

### Critical Finding
**MVCC Lost Update Bug**: _update() and _delete() index-scan paths use `heap.get(RID)` which returns null for MVCC-invisible rows, then `usedIndex=true` prevents fallback to full table scan → 0 rows affected. _select() is NOT affected because it uses `findByPK()` with scan fallback. Fix: set `usedIndex=false` when all index results are invisible.

### Key Explorations
1. db.js monolith: 9844 LOC, 142 methods, 8 extractable domains
2. WAL: TransactionalDB correct, PersistentDB missing checkpoint/truncation
3. MVCC: heap monkey-patching with 5 fragility risks
4. Compiled engine: 4 expr types, silent null = latent correctness bug
5. LOC census: 200K total (75K source, 125K tests)
6. Parser: 82+ SQL features
7. Volcano: 17 operators, 11 wired, 6 unwired
8. PG wire: 26/26 tests pass
9. Cost model: histogram-accurate but dual-model divergence
10. Concurrency: write skew caught (SSI), lost update NOT caught
11. Module survey: all tested modules pass (raft, RBAC, PL/SQL, etc.)

### Tomorrow
1. P0: Fix MVCC lost update bug (simple: usedIndex=false fallback)
2. Depth work or new project for variety
