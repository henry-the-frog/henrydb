# HenryDB Changelog

## 2026-04-10 — Test Stabilization & SQL Correctness

### Bug Fixes (14 bugs fixed)
- **ARIES Recovery**: Fixed export alias typo (`ARIESRecoveryManager` → `ARIESRecovery`)
- **Recovery Manager**: Fixed hardcoded LogTypes constants (COMMIT was 5, should be 3 from LogTypes)
- **WAL Integration**: Fixed wrong COMMIT type constant in test (4 → 5)
- **Raft**: `getLeader()` now returns leader with highest term (was returning first/partitioned leader)
- **Raft**: Added public `startElection()` API, made `_startElection` resilient to missing cluster
- **ARIES Recovery**: Added self-contained convenience API (`begin`/`write`/`commit`/`checkpoint`/`crashAndRecover`) with return value
- **Adaptive Engine**: Fixed `SELECT *` — `_applyProjectAndLimit` didn't recognize `{type: 'star'}` AST node
- **Query Cache**: `get()` was returning `{result, timestamp}` wrapper instead of just `result`
- **Server**: Extended query protocol (Bind) wasn't invalidating query cache after mutations
- **Server**: Added `QueryCache.getStats()` method, query log for `pg_stat_slow_queries`
- **SQL Parser**: Added parenthesized expression support in `parsePrimary()` — fixes TPC-H queries
- **SQL Engine**: `SUM()` over empty set now returns `NULL` (SQL standard, was returning 0)
- **SQL Engine**: `BETWEEN` with `NULL` now returns `false` (was coercing NULL to 0 via JS semantics)
- **SQL Engine**: Fixed NULL ordering in ORDER BY — 4 code paths updated (NULL is smallest, matching SQLite)
- **SQL Engine**: GROUP BY output no longer leaks canonical aggregate names (e.g., `COUNT(*)` alongside alias `cnt`)
- **Set Operations**: UNION/INTERSECT/EXCEPT now remap right SELECT's column names to match left's

### New Features
- **SQL Correctness Fuzzer**: Differential testing against SQLite
  - 16 query patterns: SELECT, aggregate, GROUP BY, JOIN, compound WHERE, DISTINCT, expressions, NULL tests, HAVING, multi-column GROUP BY, IN, BETWEEN, IN subquery, EXISTS, scalar subquery, UNION/INTERSECT/EXCEPT
  - **10,800 random queries — 100% match with SQLite**
  - Found 3 real bugs through fuzzing (SUM empty set, BETWEEN NULL, NULL ordering)

### Test Results
- **5,567+ unit tests — all passing (was ~20 failing)**
- **10,800 fuzzer queries — 100% SQLite match**
- Key test files fixed: aries-recovery, recovery, wal-integration, raft, integration-stress, server-integration, server-index, server-knex, server-prepared-cache, server-slow-query, server-etl, server-migrations, server-stress, example-app, regression-tests, misc, projection
