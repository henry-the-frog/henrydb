# Fuzzer-Found Bugs (2026-04-25)

## Bug 1: INSERT with fewer values than columns
**Severity**: Medium — data integrity risk
**Reproduction**: 
```sql
CREATE TABLE t (a INT, b INT, c INT, d TEXT);
INSERT INTO t VALUES (1, 2);  -- Only 2 values for 4 columns
SELECT * FROM t;  -- Returns {a:1, b:2, c:null, d:null}
```
**Expected**: Error "table t has 4 columns but 2 values were supplied"
**Actual**: Silently fills missing columns with NULL
**Fix location**: `src/insert-row.js` — add value count validation before insertion
**Note**: This is actually intentional in some databases (MySQL allows it). But SQLite and PostgreSQL require exact match or explicit column names. Standard SQL requires explicit column list when not providing all values.

## Bug 2: ORDER BY non-existent column silently ignored
**Severity**: Low — confusing behavior but no data corruption
**Reproduction**:
```sql
CREATE TABLE t (a INT, b INT);
INSERT INTO t VALUES (1, 10);
SELECT * FROM t ORDER BY nonexistent ASC;  -- Returns rows in original order
```
**Expected**: Error "no such column: nonexistent"
**Actual**: Silently ignores the ORDER BY
**Fix location**: `src/select-inner.js` — validate ORDER BY columns exist in the result set or source tables
**Note**: This is related to the general column validation gap. HenryDB is permissive about column references.

## Fuzzer Stats
- **200 iterations, seed=42**: 94% pass rate
- All failures were row count mismatches caused by Bug 1 (INSERT permissiveness)
- No value-level comparison failures in correctly-populated tables
- HenryDB is MORE permissive than SQLite overall (accepts queries SQLite rejects)
