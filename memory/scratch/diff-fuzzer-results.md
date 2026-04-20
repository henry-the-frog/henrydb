# Differential Fuzzer Results (Session B, Apr 20)

## Test: 48 queries, HenryDB vs SQLite

### Real Bugs Found

#### 1. Division Always Truncates (CRITICAL)
- `SELECT 10.0 / 3` → HenryDB: 3, SQLite: 3.33
- `SELECT 7.0 / 2` → HenryDB: 3, SQLite: 3.5
- Even with floating-point operands, division uses integer truncation
- **Impact**: Every query with division returns wrong results
- **Root cause**: Likely `Math.trunc()` or `|0` in expression evaluator

#### 2. NULL IS NULL Doesn't Evaluate (SIGNIFICANT)
- `SELECT NULL IS NULL` → HenryDB: row with column "NULL" = "NULL"
- Expected: 1 (true) or true
- IS operator not being parsed/evaluated as a comparison
- **Impact**: NULL checks in WHERE/CASE don't work correctly

#### 3. CAST Numeric Types Don't Convert (MODERATE)
- `CAST(10 AS REAL)` → returns 10 (integer), not 10.0
- `CAST(10 AS INTEGER)` → returns 10 (already integer, no-op)
- `CAST(3.14 AS INTEGER)` → returns 3 (works for float→int only)
- **Impact**: Queries relying on CAST for type promotion in division

#### 4. Multi-arg MAX/MIN Not Supported (MINOR)
- SQLite supports `MAX(1,2,3)` as scalar function
- HenryDB only supports `MAX(col)` as aggregate
- Not standard SQL — SQLite extension

### False Positives (comparison artifact)
- JOIN + GROUP BY: actually correct, fuzzer compared first column only
- HAVING: works correctly, column naming differs
- NULL comparisons: SQLite returns empty for `NULL = NULL`, HenryDB returns NULL — both acceptable

### Summary
Division truncation is the most impactful bug. Every TPC-H query that computes `price * (1 - discount)` returns wrong numbers.
