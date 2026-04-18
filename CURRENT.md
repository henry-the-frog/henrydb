# CURRENT.md

## Status: session-ended
## Session: C (evening)
## Date: 2026-04-17
## Time: 8:15 PM - 9:45 PM MDT

## Tasks Completed (Session C)
- T300 THINK: DDL lifecycle harness design review
- T301 PLAN: DDL lifecycle harness plan
- T308 BUILD: DDL lifecycle test harness — 70 tests, 4 bugs fixed (triggers, sequences, WAL DDL logging)
- T309 BUILD: ALTER TABLE backfill duplicate tuples fix (BufferedPage.updateTuple, FileBackedHeap.updateInPlace, post-ALTER checkpoint, per-page LSN recovery)
- T314 BUILD: Cost model accuracy tests (22 tests, empty table bug fixed)
- T319 BUILD: Materialized view persistence + REFRESH fix
- T326 BUILD: Committed rows lost during recovery with uncommitted tx
- T328 BUILD: Generated column DDL lifecycle spec (79 total harness tests)
- T330 BUILD: Dead code removal (76 lines of unreachable _alterTable)
- T331 EXPLORE: Full regression sweep — 25/25 generated column/trigger/view tests pass

## Bugs Fixed (8 total)
1. Triggers not persisted in TransactionalDatabase catalog
2. Sequences not persisted in TransactionalDatabase catalog  
3. CREATE TABLE/VIEW/DROP/TRIGGER/SEQUENCE not WAL-logged for stale catalog recovery
4. ALTER TABLE backfill created duplicate rows (WAL + in-place modification conflict)
5. Per-page LSN recovery destroyed checkpointed pages
6. Materialized view persistence lost on restart
7. REFRESH MATERIALIZED VIEW duplicated data
8. Committed rows lost with uncommitted tx during recovery

## Test Results
- DDL lifecycle harness: 79/79 pass (11 DDL types × 7+ phases)
- Cost model accuracy: 22/22 pass
- Persistence/crash suite: 430+ tests across 55+ files — 0 regressions
- Pre-existing failures: 6 (4 AUTOINCREMENT, 1 adversarial, 1 file-wal double-logging)

## Next Session Focus
- Secondary index + MVCC snapshot after UPDATE (HOT chains)
- Fix file-wal.test.js double-logging issue
