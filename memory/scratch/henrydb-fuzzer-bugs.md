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

## Fuzzer Run 2026-04-26 (Session A)

### Stats
- **1000 iterations, seed=42**: 99.3% pass rate (993/1000)
- **500 iterations, random seed**: 98.4% pass rate (492/500)
- Significant improvement from 94% (earlier run)

### Remaining Failure Patterns (all type-affinity related)

**Pattern 1: Mixed-type comparison in WHERE** (2 failures)
```sql
SELECT * FROM t1 WHERE c > -39  -- c has mixed TEXT/INT values
-- HenryDB returns 30, SQLite returns 27
```
Root cause: SQLite uses type affinity rules where TEXT values sort after all numeric values. HenryDB likely compares them differently.

**Pattern 2: Subquery AVG comparison** (1 failure)
```sql
SELECT * FROM t1 WHERE c <= (SELECT AVG(c) FROM t1) LIMIT 6
-- HenryDB returns 0 rows, SQLite returns 2
```
Root cause: AVG returns REAL in SQLite. The comparison between column values and the AVG result may use different type coercion.

**Pattern 3: ORDER BY with mixed types** (2 failures)
```sql
SELECT MIN(b) as min_val, b, c FROM t1 ORDER BY a DESC
-- Different row values in output
```
Root cause: Ordering of mixed TEXT/NUMERIC values differs from SQLite's affinity-based sorting.

**Pattern 4: UNION with mixed-type data** (2 failures)
```sql
SELECT c FROM t1 WHERE c > 'hello' UNION ALL SELECT c FROM t1 WHERE c < 55
-- Row count difference
```
Root cause: Each UNION branch evaluates WHERE with different type interpretation. SQLite's type affinity for comparisons differs from HenryDB.

### Assessment
All 7 failures trace back to **type affinity in comparisons**. The `sqliteCompare` function (added earlier today) handles WHERE/ORDER BY type affinity, but:
1. It may not be wired into all comparison paths (UNION dedup, window functions)
2. AVG/aggregate comparison with column values may not trigger affinity logic
3. The core issue is SQLite's specific rule: TEXT values compare as greater than all NUMERIC values

### Recommendation
Single fix: ensure `sqliteCompare` is used in ALL comparison paths — not just basic WHERE. Priority areas:
1. UNION DISTINCT deduplication comparison
2. Window function ORDER BY 
3. Subquery result comparison
