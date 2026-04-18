# CURRENT.md

## Status: session-ended
## Session: C (evening)
## Date: 2026-04-17
## Time: 8:15 PM - 10:00 PM MDT

## Summary
Deep HenryDB persistence session. Built DDL lifecycle test harness (79 tests, 11 DDL types × 7+ phases). Found and fixed 8 bugs in persistence layer. Investigated and fixed a pre-existing crash recovery bug.

## Key Changes
- `transactional-db.js` — catalog save/load for triggers, sequences, matviews; WAL DDL logging; checkpoint after ALTER/matview
- `file-wal.js` — hybrid recovery for uncommitted transactions; per-page LSN preservation
- `file-backed-heap.js` — BufferedPage.updateTuple, FileBackedHeap.updateInPlace, FileBackedHeap.truncate
- `db.js` — ALTER TABLE updateInPlace path, REFRESH matview truncate, removed dead code (76 lines)
- `cost-model.js` — empty table estimation fix (0 as falsy)
- New test files: `ddl-lifecycle.test.js` (79 tests), `cost-model-accuracy.test.js` (22 tests)

## Next
- Secondary index + MVCC snapshot after UPDATE (HOT chains)
- Fix file-wal.test.js double-logging issue
