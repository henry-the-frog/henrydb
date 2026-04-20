# Database WAL — Lessons

_Promoted from scratch/ddl-wal-completeness.md (uses: 2, Apr 13→17)_

## The Completeness Pattern
Every DDL operation needs BOTH a write path (WAL logging) AND a read path (replay handling). Missing either = silent data corruption after crash.

## Crash Recovery Architecture (3 phases)
1. **Load catalog** → recreate tables from stored CREATE TABLE SQL
2. **DDL schema-only replay** → ALTER TABLE etc. from WAL (don't modify heap — avoid double-apply)
3. **Per-heap DML recovery** → INSERT/UPDATE/DELETE replay from lastCheckpointLsn

## Key Rules
- `txId=0` records (DDL auto-committed) must be treated as always-committed
- RENAME TABLE needs: physical file rename + DiskManager association + heap rekeying + table object wiring
- FileWAL.truncate() must reset _fileSize or post-truncate records unreadable
- DDL replay must be schema-only to avoid double-applying changes
- Per-heap recovery reads from lastCheckpointLsn, not LSN 0

## The ALTER TABLE Bug Chain (4 interacting bugs, Apr 17)
1. FileWAL missing `logDDL()` method entirely
2. Inner Database's `this.wal` was a no-op — DDL logging checked a never-defined method
3. DDL records with txId=0 treated as "uncommitted" → silently dropped during recovery
4. RENAME TABLE left physical file at old name → recovery opened new empty file

## Prevention: Negative Pattern > Positive Pattern
- **Bad:** `if (sql.startsWith('CREATE TABLE') || sql.startsWith('CREATE INDEX'))` — rots when new DDL added
- **Good:** `if (sql.startsWith('CREATE '))` — catches all future CREATE types automatically
- Allowlists rot. Negative/catch-all approaches are more resilient.
