# CURRENT.md
status: in-progress
session: B (afternoon)
date: 2026-04-19
mode: MAINTAIN
task: Mid-session git push + knowledge capture
current_position: T142
started: 2026-04-19T20:15:57Z
tasks_completed_this_session: 8

## Session B accomplishments so far
- Fixed CRITICAL SERIAL UNIQUE constraint false positive
- Fixed correlated subquery comparison in WHERE (parser)
- Fixed composite PK case sensitivity and serialization
- Fixed EXPLAIN small table index preference
- Fixed column existence validation in SELECT/WHERE
- Fixed _orderValues double NEXTVAL resolution
- Fixed ON CONFLICT with UNIQUE (not just PK) columns
- Added CALL statement, RETURN UDF bodies
- Fixed DESCRIBE/pg_attribute/SHOW TABLES metadata
- Fixed pg-client/pg-integration type assertions
- Created 23 new targeted tests (18 critical-paths + 5 concurrent)
- Full 810-file test suite scan completed

## Remaining known failures
- savepoints.test.js: 3 (MVCC undo depth issue)
- orm-compat.test.js: 6 (missing CREATE EXTENSION/SCHEMA/GRANT)
- table-changes-explore.test.js: 4 (notifications not wired)
- update-rollback-stress.test.js: 1 (isolation)
- server-knex.test.js: ~13 (pool timeout + null deref)
