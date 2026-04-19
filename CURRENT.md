# CURRENT.md
status: in-progress
session: B (afternoon)
date: 2026-04-19
mode: BUILD
task: Continuing after MAINTAIN
current_position: T156+
started: 2026-04-19T20:15:57Z
tasks_completed_this_session: 17

## Session B accomplishments
- **18+ bugs fixed** including 3 CRITICAL
- **23 new tests written**  
- **812 test files scanned**
- **Remaining failures: ~4** (table-changes-explore only)

## Key fixes
- CRITICAL: SERIAL UNIQUE constraint false positive
- CRITICAL: MVCC ROLLBACK TO SAVEPOINT undo
- CRITICAL: Per-connection transaction isolation
- Correlated subquery WHERE comparison parser
- Composite PK case sensitivity + serialization
- ON CONFLICT with UNIQUE columns
- Sequence double-increment prevention
- Knex/ORM compatibility
- Column validation, CALL/RETURN UDF, metadata queries
