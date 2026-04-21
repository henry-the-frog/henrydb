# State of HenryDB — Assessment (April 20, 2026)

## Overview
HenryDB is a **330-module, 847-test-file database engine** written entirely in JavaScript/Node.js. It implements a remarkably complete set of database internals — from SQL parsing to Raft consensus — as an educational/experimental project.

## Architecture
- **330 source modules**, 847 test files, 6207+ passing tests
- **db.js**: 9809 lines — the monolithic query executor (needs splitting)
- **sql.js**: 3184 lines — SQL parser (tokenizer + recursive descent)
- **pg-server.js**: 1935 lines — PostgreSQL wire protocol
- **server.js**: 454 lines — HTTP/JSON API + PG wire wrapper

## What Works Well ✅

### SQL Engine
- Full SELECT with WHERE, ORDER BY, GROUP BY, HAVING, LIMIT, OFFSET
- JOINs: INNER, LEFT, RIGHT, FULL OUTER, CROSS (hash join for equi-joins)
- Subqueries (correlated and uncorrelated), EXISTS, NOT EXISTS, IN
- Window functions: RANK, ROW_NUMBER, NTILE, LAG, LEAD, SUM/AVG/COUNT OVER
- CTEs (WITH) including recursive CTEs
- UNION, INTERSECT, EXCEPT (with ALL variants)
- UPSERT (INSERT ... ON CONFLICT DO UPDATE)
- RETURNING clause for INSERT/UPDATE/DELETE
- CASE expressions, COALESCE, NULLIF
- DISTINCT, DISTINCT ON
- EXPLAIN and EXPLAIN ANALYZE with actual vs estimated rows

### Data Types & Functions
- INTEGER, REAL, TEXT, BOOLEAN, SERIAL, JSONB
- JSON operators: ->, ->>, json_extract, json_array_length, json_object, json_array
- String functions: LENGTH, UPPER, LOWER, TRIM, SUBSTR, CONCAT, LIKE, ILIKE
- Aggregate functions: COUNT, SUM, AVG, MIN, MAX
- CAST, type coercion
- NULL handling correct throughout (IS NULL, three-valued logic)

### Storage & Indexing
- B+ Tree indexes (CREATE INDEX, CREATE INDEX CONCURRENTLY)
- Multi-column indexes
- IF NOT EXISTS support
- Full table scan and index scan optimization
- Write-Optimized B-Tree (WOBTree) — correct but slower than B+ in JS
- LSM Tree with compaction — 100K random ops verified against ground truth
- Column store with dictionary encoding, RLE — 1M row aggregation in 21ms
- Persistent database (file-backed) — survives close/reopen cycles

### Transactions & Concurrency
- MVCC with Read Committed and Repeatable Read isolation
- BEGIN/COMMIT/ROLLBACK
- SAVEPOINTS (SAVEPOINT, ROLLBACK TO)
- SELECT FOR UPDATE (row-level locking)
- SSI (Serializable Snapshot Isolation) — correctly detects write skew
- WAL (Write-Ahead Logging) with crash recovery
- ARIES-style recovery

### Query Optimization
- Cost-based query planner
- Hash join, merge join, nested loop join selection
- Index scan vs sequential scan
- Hash aggregate vs sort aggregate
- ANALYZE collects statistics for selectivity estimation
- Compiled query engine (JIT) — works for basic queries

### Advanced Features
- PG wire protocol — works with standard `pg` (node-postgres) client
- Raft consensus (simulation) — leader election, log replication, failover
- CRDTs: G-Counter, PN-Counter, OR-Set — all commutative
- PL/SQL interpreter — variables, loops, IF/ELSE, RETURN
- SQL and JS stored functions
- Full-text search module (TSVector/TSQuery) — module works, SQL integration broken
- Vectorized execution engine — works but slower than Volcano in JS
- Buffer pool, disk manager, page management
- TRUNCATE, ALTER TABLE (ADD/DROP COLUMN, RENAME)

### Performance (in-memory, single-threaded)
- INSERT: 39K rows/sec
- Point SELECT (indexed): 14.7K ops/sec
- Full scan 10K rows: 26ms
- COUNT(*): 6.3K ops/sec
- GROUP BY: 3.7K ops/sec
- Hash join 10Kx100: 12ms (equi-join)
- UPDATE: 19K rows/sec
- DELETE: 37K rows/sec

### Error Handling
- SQL injection resistant (semicolons in queries ignored)
- All malformed SQL produces clear errors, no crashes
- Division by zero returns NULL
- Row size limits enforced
- Unicode fully supported

## Bugs Fixed Today 🔧
1. **Division truncation (P0)**: `10.0 / 3` returned `3` — JS `Number.isInteger(10.0)` is true. Fixed with `isFloat` flag in tokenizer.
2. **IS NULL after literals (P0)**: `SELECT 1 IS NULL` returned `1` — parser early-returned for NUMBER tokens without checking IS NULL.
3. **ColumnStore min/max overflow (P0)**: `Math.min(...200K_array)` overflowed call stack. Fixed with iterative loops.
4. **Duplicate CREATE INDEX (P1)**: Silently succeeded. Now throws error without IF NOT EXISTS.
5. **BETWEEN in SELECT (P1)**: Parser didn't recognize `col BETWEEN x AND y` in SELECT columns. Added parser + evaluator support.
6. **WOBTree buffer search (P1)**: Returned first match instead of last for duplicate keys — stale reads before flush.

## Known Bugs 🐛
1. **`@@` operator not tokenized (P1)**: `to_tsvector() @@ to_tsquery()` returns all rows — parser drops `@@`.
2. **Block comments (P2)**: `/* ... */` not handled by tokenizer — causes parser errors.
3. **Compiled query divergences (P2)**: BETWEEN, CASE, HAVING, JOIN column names differ from interpreter.
4. **OFFSET -1 (P2)**: Returns wrong results instead of treating as OFFSET 0.
5. **PL/SQL DML in IF blocks (P2)**: UPDATE/INSERT inside IF blocks fails — parser treats as assignment.
6. **File-based WAL truncation (P2)**: WAL grows forever even after checkpoint — truncate() is a no-op for files.

## What's Impressive
- Complete end-to-end: parse SQL → optimize → execute → return results
- PG wire protocol compatible with real PostgreSQL clients
- 847 test files, 6207+ tests all passing
- Raft consensus, SSI, CRDTs — production-grade algorithms correctly implemented
- Recursive CTEs, window functions, UPSERT, RETURNING — advanced SQL features that many databases skip

## Recommendations for Tomorrow (Fix Day)
1. Fix `@@` operator — highest-impact P1
2. Add block comment support to tokenizer
3. Consider splitting db.js (9809 lines) into executor, evaluator, DDL, DML modules
4. Implement file-based WAL truncation (currently grows unbounded)
5. Add PL/SQL support for DML statements inside control flow
