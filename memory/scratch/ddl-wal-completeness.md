# DDL WAL Completeness Pattern

uses: 2
created: 2026-04-13
updated: 2026-04-17
tags: henrydb, persistence, wal, ddl, bug-pattern

## The Pattern

Every DDL operation needs BOTH a write path (WAL logging) AND a read path (replay handling). Missing either creates silent data corruption:

| DDL Operation | Write Path | Read Path | Bugs Found |
|---|---|---|---|
| CREATE TABLE | ✅ | ✅ | (original) |
| CREATE INDEX | ✅ | ✅ | (original) |
| CREATE VIEW | ✅ | ✅ | Bug #14 |
| CREATE TRIGGER | ✅ | ✅ | Bug #15 |
| CREATE MATERIALIZED VIEW | ✅ | ✅ | (found with views) |
| DROP TABLE | ✅ | ✅ | Bug #16 |
| TRUNCATE TABLE | ✅ | ✅ | Bug #15 (Session B) |
| ALTER TABLE (all variants) | ✅ | ✅ | Bug #17, fixed 2026-04-17 |
| DROP INDEX | ✅ | ✅ | (found with indexes) |

## 2026-04-17 Fix: ALTER TABLE WAL Recovery (4 bugs)

The ALTER TABLE fix was deeper than expected — 4 separate bugs interacted:

1. **FileWAL missing logDDL**: The file-backed WAL class had no `logDDL()` method, so DDL records were never written. Fixed by adding the method.

2. **Inner Database had no WAL route**: TransactionalDatabase stores the WAL on `this._wal`, but the inner Database's `this.wal` was a no-op. DDL logging in db.js checked `this.wal.logDDL` which was always undefined. Fixed by patching `logDDL` onto the inner Database's existing no-op WAL.

3. **DDL records treated as uncommitted**: DDL records use `txId=0` (auto-committed), but `recoverFromFileWAL` saw `txId=0` as an "active uncommitted transaction" and triggered full redo mode which only replays committed txn records — DDL records (not associated with any committed txn) were silently dropped. Fixed by excluding `txId=0` from the uncommitted check.

4. **RENAME TABLE orphaned heap files**: After `ALTER TABLE x RENAME TO y`, the physical `.db` file stayed at `x.db`. On recovery, the catalog created table `y` which opened `y.db` (a new empty file), losing all data from `x.db`. Fixed by renaming the physical file and updating internal maps during ALTER TABLE RENAME.

### Additional fixes in same change:
- Catalog (`_createSqls`) now updated after ALTER TABLE to reflect current schema
- `_reconstructCreateSQL` generates CREATE TABLE from current schema with DEFAULT clauses
- ADD_COLUMN now properly extracts column info from object AST (parser returns `{ name, type, default }` not separate fields)
- Added `matchesHeap` helper for table name aliasing during recovery (maps old→new names from DDL records)

### Crash Recovery Architecture Insight (2026-04-17)

The crash recovery flow in TransactionalDatabase has 3 phases:
1. **Load catalog** → recreate tables from CREATE TABLE SQL
2. **DDL replay** → schema-only replay of ALTER TABLE from WAL (no heap modification!)
3. **Per-heap DML recovery** → INSERT/UPDATE/DELETE replay from lastCheckpointLsn

Key learnings:
- DDL replay must be **schema-only** (don't modify heap data) to avoid double-applying changes
- Per-heap recovery must read from **lastCheckpointLsn**, not LSN 0
- txId=0 records (DDL auto-committed ops) must be treated as always-committed
- RENAME TABLE needs heap rekeying + DiskManager association + table object wiring
- FileWAL.truncate() must reset _fileSize or post-truncate records become unreadable
