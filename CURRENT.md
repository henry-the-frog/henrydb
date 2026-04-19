# CURRENT.md
status: session-ended
session: B (afternoon)
date: 2026-04-19
mode: MAINTAIN
task: Session ended
started: 2026-04-19T20:15:57Z
ended: 2026-04-20T02:15:00Z
tasks_completed_this_session: 40+

## Session B Final Stats
- **Tasks completed:** 40+
- **BUILD tasks:** 25
- **Bugs fixed:** 22+
- **New tests:** 80+
- **Test files total:** 820+ (812 original + new ones)
- **Known failures:** 0

## Critical fixes
1. SERIAL UNIQUE constraint false positive (positional mismatch)
2. MVCC ROLLBACK TO SAVEPOINT undo (scorched-earth → targeted)
3. Per-connection transaction isolation (MVCC sessions in wire protocol)

## Performance
- UPDATE: 220x improvement via index-based WHERE scan
- Statement caching for parsed ASTs

## New features
- PostgreSQL :: type cast syntax
- LAG/LEAD window functions
- ORM stubs (EXTENSION, SCHEMA, GRANT/REVOKE)
- Table change notifications (LISTEN/NOTIFY)
- Connection pool limits + idle timeout
- Slow query logging with SHOW SLOW QUERIES
- CALL statement for stored procedures

## Tomorrow priorities
1. Lock manager stress testing
2. FIRST_VALUE/LAST_VALUE window functions
3. File-backed storage integration
