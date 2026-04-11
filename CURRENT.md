## Status: session-ended

session: C (evening)
date: 2026-04-10
ended: 2026-04-11T03:37:00Z

### Session C Summary

**Tasks completed:** 24 (T215-T238)
**Commits:** 30
**Tests added:** ~80+
**Blog posts:** 2

#### Key Achievements

**Critical Bug Fix:**
- Query cache not invalidated on ROLLBACK/COMMIT — the stealthiest bug yet. Transaction engine was correct, bug was in the cache layer 3 levels removed from the actual rollback code.

**New Features:**
- Savepoints (SAVEPOINT, ROLLBACK TO, RELEASE)
- WAL checkpoint + truncation + auto-checkpoint
- SELECT FOR UPDATE / FOR SHARE (row-level locking)
- Table change notifications (CDC via LISTEN table_changes)
- Advisory locks (pg_advisory_lock/unlock/try)
- JSON functions (json_build_object, json_build_array, row_to_json, to_json, json_agg)
- EXCLUDED.val fix for UPSERT

**Performance:**
- COPY FROM STDIN: 37x faster (direct heap insertion)
- WAL deferred writes: 7.6x TPS improvement
- TPC-B benchmark: 36 TPS (wire protocol) → 127 TPS (deferred WAL)
- Profiled commit bottleneck: synchronous writeSync (18ms), NOT JSON serialization

**Validation of existing features:**
- EXPLAIN ANALYZE, prepared statements, LISTEN/NOTIFY, cursors, COPY TO, crash recovery, VACUUM, information_schema, pg_stat_activity

#### State of HenryDB
- 148K lines of JavaScript
- 5130 test cases
- 579 test files
- PostgreSQL wire protocol compatible
- Full MVCC with SSI
- WAL with checkpoint and auto-checkpoint
- TPC-B verified: 36-127 TPS depending on sync mode
