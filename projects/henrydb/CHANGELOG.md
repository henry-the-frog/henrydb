# Changelog

## v1.1.0 (2026-04-11) — The 115-Task Saturday

The biggest single-day update in HenryDB history. 115+ tasks, 250+ tests, 30+ bugs fixed, SQL compliance from 134 to 282 checks.

### New SQL Features
- **Recursive CTEs** (`WITH RECURSIVE`) — factorial, fibonacci, tree traversal, graph reachability, mandelbrot set
- **CREATE TABLE AS SELECT** (CTAS) — create tables from query results with schema inference
- **FULL OUTER JOIN** — complete outer join support
- **NATURAL JOIN** — automatic join on shared column names
- **JOIN USING** — join on named columns without table qualification
- **STRING_AGG** — string aggregation with separator
- **SUBSTR** — alias for SUBSTRING (PostgreSQL compatibility)
- **EXP** — exponential math function
- **SIMILAR TO** — SQL standard regex matching
- **BETWEEN SYMMETRIC** — auto-swaps reversed bounds
- **EXCEPT ALL / INTERSECT ALL** — bag semantics for set operations
- **LIMIT ALL** — return all rows (no limit)
- **FETCH FIRST N ROWS ONLY** — SQL:2008 standard paging syntax
- **ORDER BY NULLS FIRST/LAST** — control NULL sort position
- **UNIQUE column constraint** — enforce uniqueness in CREATE TABLE
- **GROUP BY alias** — resolve SELECT aliases in GROUP BY
- **table.* in JOINs** — qualified star expansion

### Bug Fixes (Critical)
- **Literal parsing:** Numbers/strings in SELECT were parsed as column references (`SELECT 42` → column `42`)
- **Duplicate expr names:** Multiple unnamed expressions shared the key `expr` (second overwrites first)
- **Recursive CTE column loss:** Multi-column recursive CTEs lost columns after first iteration
- **INSERT INTO SELECT mapping:** GROUP BY added extra keys, breaking positional column mapping
- **NULLS FIRST/LAST with DESC:** Direction negation was applied to NULL comparison, swapping semantics
- **pageLSN not persisted:** Recovery skipped already-applied changes but didn't track per-page
- **Dead rows survived restart:** Old UPDATE versions not compacted during close/reopen
- **Savepoint-rolled-back rows resurrected:** WAL replay recreated rows that should be gone
- **PK indexes not rebuilt after recovery:** Point queries failed after WAL replay
- **CTAS property mismatch:** Used wrong AST property names (ast.name vs ast.table)

### Tests
- 250+ new tests across 12 test files
- 100-query SQL fuzzer (random query generation)
- Integrated e-commerce analytics scenario (14 tests)
- Recursive CTE suite (8 tests: factorial, fibonacci, tree, graph, powers, strings)
- Ultimate SQL stress tests (10 tests combining CTEs + windows + subqueries + aggregates)
- Optimizer correctness tests (20 tests comparing optimized vs unoptimized results)
- Wire protocol end-to-end verification (every feature through pg client)
- Persistence + restart verification for recursive CTEs

### Documentation
- 4 blog posts published to GitHub Pages
- Interactive CLI (`node henrydb-cli.js`)
- Performance benchmarks in README
- SQL compliance scorecard: 282/282 (100%)
- Feature showcase with 10+ example queries

### Infrastructure
- `package.json` with proper bin entries and exports
- Blog pushed to GitHub Pages

## v1.0.0 (2026-04-10) — Initial Release

- SQL parser (recursive descent, 150+ keywords)
- In-memory and persistent storage (WAL + fsync)
- MVCC transactions (snapshots, hint bits)
- PostgreSQL wire protocol (pg, psql, Knex compatible)
- B+ Tree indexes with cost-based optimizer
- Window functions (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, NTILE, FIRST_VALUE)
- CTEs, subqueries, GENERATE_SERIES
- Full-text search with GIN indexes
- EXPLAIN and EXPLAIN ANALYZE
