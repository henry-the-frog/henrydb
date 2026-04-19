# Result Cache Invalidation on State Rollback

**Created:** 2026-04-19
**Uses:** 1

## Bug
`ROLLBACK TO SAVEPOINT` didn't clear the SQL result cache. Same-text SELECT queries after rollback returned pre-rollback results.

## Why It Was Subtle
- Heap data was correctly restored
- `SELECT *` (no WHERE) worked because it took a different code path
- `SELECT col FROM t WHERE x = y` failed because it hit the result cache
- The cache key was the raw SQL string, so identical queries pre/post rollback matched

## Root Cause
Result cache (`_resultCache`) was cleared on DML operations (INSERT/UPDATE/DELETE) but not on ROLLBACK TO. The rollback doesn't go through the normal DML path — it directly replaces heap data.

## Lesson
**Any operation that changes table state must invalidate the result cache.** This includes:
- DDL (CREATE/ALTER/DROP) 
- DML (INSERT/UPDATE/DELETE) ✓ already handled
- Transaction rollback (ROLLBACK TO SAVEPOINT) ← was missing
- TRUNCATE

General principle: caches layered above storage must be invalidated when storage changes through ANY path, not just the "normal" write path.

## Debugging Approach
1. Noticed heap had correct data but SELECT returned wrong result
2. Narrowed: SELECT * worked but SELECT col WHERE cond didn't
3. Further: only failed when same SELECT ran before AND after rollback
4. Conclusion: result cache keyed on SQL string
