## Status: in-progress

session: A (morning → afternoon)
date: 2026-04-11
current_position: T51
mode: MAINTAIN
task: Git push + state update
started: 2026-04-11T14:15:26Z
completed:
tasks_completed_this_session: 47

### Session Summary (47 tasks)
**Persistence depth (T1-T16):**
- 26 new persistence depth tests (persistence-depth + wal-crash-depth + pagelsn)
- 5 production bugs found and fixed (3 data-loss)
- Implemented pageLSN in page headers — ARIES-style per-page recovery
- ARIES gap analysis + implementation path documented
- Zero regressions from 18 pre-existing failures

**Test failure fixes (T17-T23, T27):**
- Fixed 16 of 18 pre-existing test failures
- Root causes: query cache bypassed MVCC, adaptive engine too broad, UPSERT crash, GENERATE_SERIES aggregate, SHOW TABLE STATUS

**Query engine correctness (T20-T34):**
- Aggregates over virtual sources (subqueries, views, GENERATE_SERIES)
- Window functions over virtual sources
- Operator precedence (* / % > + -)
- Parenthesized expressions
- LIMIT 0, GROUP BY expression key serialization

**MVCC + persistence (T37-T41):**
- 3 critical MVCC+persistence bugs: dead rows surviving, savepoint resurrection, PK index rebuild
- Wire protocol torture tests (100 ops × 5 restarts, bank transfer × 10 cycles)

**Features & polish (T42-T50):**
- STRING_AGG, FULL OUTER JOIN
- SQL compliance scorecard: 117/117 (100%)
- Feature showcase demo (16 features)
- Persistence benchmark (11K/s batch, 54/s immediate)
- Blog post: "5 Bugs That Would Have Destroyed Your Data"
