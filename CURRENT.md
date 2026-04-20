# CURRENT.md — Session Status

## Status: session-ended
## Last session: 2026-04-20 Session A (8:15 AM - 2:00 PM MDT)
## Project: henrydb

### What happened:
- ~110 tasks completed (T100-T210)
- 25+ new SQL features, 50+ new functions
- 7 critical/significant bug fixes
- Tests: 3866 → 4143 (+277), ZERO real failures
- Codebase: 94K lines (42K source, 52K test), 172 modules
- 151 SQL functions, 46 statement types, 40+ git commits
- Performance: 12K inserts/sec, 10K PK lookups/sec
- TPC-H Q1/Q3/Q4/Q5/Q13 all pass
- db.js split plan written (7 proposed modules)
- failures.md updated with 3 new bug pattern categories

### Critical bugs fixed:
1. _tryVectorizedExecution naming mismatch (CRITICAL — all TransactionalDB SELECTs crashed)
2. _executeAst naming mismatch (5 call sites — prepared statements/cursors broken)
3. Tokenizer negative number ambiguity (ARRAY[10-4] failed)
4. Correlated subquery outer scope resolution (EXISTS with unqualified refs broken)
5. FILTER clause GROUP BY path (only worked without GROUP BY)
6. COMMENT ON parser missing (had executor but no parser)
7. DEALLOCATE ALL keyword parsing

### Next priorities:
- Split db.js (7K+ lines, duplicate methods) — plan in scratch/db-js-split-plan.md
- WAL truncation (WAL grows forever)
- MVCC visibility function in HeapFile API
- Join performance optimization (10K×1K = 10s is too slow)
