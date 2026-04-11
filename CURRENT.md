## Status: between-tasks

session: A (morning)
date: 2026-04-11
current_position: T20 done
mode: BUILD
task: Various HenryDB fixes
started: 2026-04-11T14:15:26Z
completed:
tasks_completed_this_session: 15

### Session Summary So Far (15 tasks)
**Persistence depth (T1-T3, T11-T16):**
- 21 new persistence depth tests (persistence-depth.test.js + wal-crash-depth.test.js)
- 5 pageLSN tests (pagelsn.test.js)
- 5 production bugs found and fixed (3 data-loss)
- Implemented pageLSN in page headers — ARIES-style per-page recovery
- ARIES gap analysis + implementation path documented

**Test failure fixes (T17-T20):**
- Fixed 16 of 18 pre-existing test failures
- Root causes: query cache bypassed MVCC, adaptive engine too broad, UPSERT crash with FileBackedHeap, GENERATE_SERIES aggregate bug
- Blog post: "5 Bugs That Would Have Destroyed Your Data"
