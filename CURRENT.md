# CURRENT.md — Session Status

## Status: session-ended
## Last session: 2026-04-20 Session A (8:15 AM - 2:00 PM MDT)
## Project: henrydb

### What happened:
- ~100 tasks completed (T100-T205)
- 25+ new SQL features, 50+ new functions
- 7 critical/significant bug fixes
- Tests: 3866 → 4143 (+277), ZERO real failures
- Codebase: 94K lines (42K source, 52K test), 172 modules
- 151 SQL functions, 46 statement types
- Performance: 12K inserts/sec, 10K PK lookups/sec
- TPC-H Q1/Q3/Q4/Q5/Q13 all pass

### Critical bugs fixed:
1. _tryVectorizedExecution naming mismatch
2. _executeAst naming mismatch (5 call sites)
3. Tokenizer negative number ambiguity
4. Correlated subquery outer scope resolution
5. FILTER clause GROUP BY path
6. COMMENT ON parser missing
7. DEALLOCATE ALL keyword parsing

### New features added:
MERGE, GROUPING SETS, CTE column lists, ARRAY support (6 functions),
FILTER clause, DATE_TRUNC/EXTRACT/AGE/DATE_ADD/DATE_SUB/TO_CHAR/DATE_PART,
STDDEV/VARIANCE/MEDIAN, PERCENT_RANK/CUME_DIST/NTH_VALUE,
INITCAP/TRANSLATE/CHR/ASCII/MD5/ENCODE/DECODE,
REGEXP_MATCHES/REGEXP_COUNT/SPLIT_PART/POSITION/STRPOS,
MOD/SIGN/TRUNC/PI/EXP/LN/LOG10/SIN/COS/TAN/ASIN/ACOS/ATAN/CBRT/GCD/LCM,
SHOW TABLES/COLUMNS/FUNCTIONS, EXPLAIN FORMAT JSON, COMMENT ON,
DEFAULT CURRENT_TIMESTAMP, predicate pushdown, ROLLUP/CUBE,
prepared statements, cursors, COPY TO CSV, DISTINCT ON, DROP FUNCTION

### Next priorities:
- Split db.js (7K+ lines, duplicate methods)
- WAL truncation
- MVCC visibility function in HeapFile API
