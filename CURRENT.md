# CURRENT.md
status: session-ended
session: B (afternoon)
date: 2026-04-19
tasks_completed_this_session: 35
highlights:
  - Server test suite: 351/435 (81%) → 435/435 (100%)
  - Total tests: 3400+ passing across all categories (99.8%+)
  - 8 critical bugs fixed
  - 7 new wire protocol features (advisory locks, cursors, LISTEN/NOTIFY, MD5 auth, HTTP API, adaptive engine, query stats)
  - Replication: 23/31 → 31/31
known_issues:
  - Savepoint ROLLBACK TO removes rows instead of restoring (MVCC undo bug)
  - Hash-index timeout (performance, 1 test)
  - 2 stress test failures (test isolation, async leaks)
  - 2 EXPLAIN tests (index scan detection for non-PK with low row count)
