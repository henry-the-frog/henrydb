# HenryDB SQLite Compatibility Analysis

## Current State
- **98.6% average** across 5 fuzzer runs (100 queries each)
- Remaining 1.4% failures are all type-related

## Failure Categories (from fuzzer analysis)

### 1. Mixed Type Comparisons in WHERE (most common)
- `WHERE b < 'hello'` when `b` contains integers
- SQLite: integers are less than strings (type affinity rules)
- HenryDB: some comparisons use JS semantics

### 2. UNION with Mixed Types
- `SELECT b FROM t2 WHERE b > 'foo' UNION SELECT b FROM t2 WHERE b < 79`
- SQLite: UNION deduplication uses type-aware comparison
- HenryDB: may use different comparison for dedup

### 3. Empty String Edge Cases
- `WHERE b = ''` — empty string has different type affinity than other strings
- SQLite: empty string is TEXT type, compared as text
- HenryDB: may coerce or compare differently

## What Would Get to 99%+

### Fix 1: Consistent Type Affinity in WHERE (est. +0.5%)
- Apply `sqliteCompare` in ALL WHERE clause comparisons
- Currently patched for some operators but not all paths
- Needs: audit every comparison site in expression-evaluator.js

### Fix 2: UNION Deduplication (est. +0.3%)
- Use `sqliteCompare` for UNION duplicate detection
- Currently may use `===` or `JSON.stringify`

### Fix 3: Empty String Handling (est. +0.2%)
- Empty string should always be TEXT type
- Never coerce to 0 or false

## Theoretical Maximum: ~99.5%

The remaining 0.5% would be extremely edge cases:
- Collation-dependent string ordering (NOCASE, BINARY)
- Expression affinity propagation (e.g., `CAST(x AS TEXT)` affects affinity)
- Type affinity in CASE/COALESCE expressions
- NULL handling in complex expressions
- Platform-specific float precision

## What Would Require Fundamental Changes

### To reach 100%:
1. **Full SQLite type affinity system**: Every expression must compute its type affinity using SQLite's rules (not JS)
2. **Collation support**: BINARY, NOCASE, RTRIM
3. **NUMERIC affinity storage**: Store "real" numbers differently from "text that looks like a number"
4. **Exact float representation**: SQLite uses C `double`; JS has some differences at the edges

### Not achievable in JS:
- Exact byte-level compatibility for BLOB comparisons
- C-specific floating point edge cases
- SQLite's specific LIKE optimization (compiled regex)
- Some obscure date/time edge cases tied to C strftime

## Recommendation
Focus on fixes 1-3 to reach ~99.5%. The remaining 0.5% provides diminishing returns. The type affinity system is already in place (type-affinity.js); it just needs to be used consistently across all comparison paths.
