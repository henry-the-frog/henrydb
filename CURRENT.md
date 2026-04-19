# CURRENT.md
status: session-ended
session: B (afternoon)
date: 2026-04-19
tasks_completed_this_session: 37
highlights:
  - Server test suite: 351/435 (81%) → 435/435 (100%) — 84 tests fixed
  - Total tests verified: 5,000+ across 65+ categories
  - Overall pass rate: 99.5%+
  - 8 critical bugs fixed (MVCC, OID map, lock release, LIKE, JSON, INSERT mapping, COPY, replication)
  - 7 new wire protocol features (advisory locks, cursors, LISTEN/NOTIFY, MD5 auth, HTTP API, adaptive engine, query stats)
  - Replication: 23/31 → 31/31
remaining_failures:
  - Savepoint ROLLBACK TO (3 tests, MVCC undo bug)
  - Correlated subquery aggregate in WHERE (1 test)
  - Error handling depth (3 tests, parser lenience)
  - Hash-index timeout (1 test, performance)
  - Stress test isolation (2 tests, test infrastructure)
  - EXPLAIN index detection for small tables (2 tests)
