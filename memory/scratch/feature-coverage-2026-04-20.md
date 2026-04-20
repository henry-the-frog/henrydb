# HenryDB Feature Coverage Report (Session B, Apr 20 2026)

## Executive Summary
94K LOC, 172 modules, 4204/4208 tests pass. Wide feature surface with narrow depth. Storage layer is solid; SQL layer has correctness bugs; integration layers are broken.

## SQL Features — Tested & Working ✅
- SELECT (columns, *, expressions, aliases)
- INSERT (single, multi-row, INSERT INTO SELECT, RETURNING)
- UPDATE (SET, subquery, correlated, UPDATE FROM, RETURNING)
- DELETE (WHERE, subquery, RETURNING)
- CREATE/DROP TABLE, ALTER TABLE (ADD/DROP/RENAME COLUMN, RENAME TABLE)
- CREATE/DROP INDEX (B-tree, expression, partial, multi-column)
- CREATE/DROP VIEW (including nested views)
- CREATE/DROP FUNCTION (PostgreSQL syntax)
- MERGE (table USING only)
- COPY TO/FROM (CSV with HEADER)
- PREPARE/EXECUTE
- EXPLAIN/EXPLAIN ANALYZE
- BEGIN/COMMIT/ROLLBACK/SAVEPOINT
- SHOW TABLES/COLUMNS
- TRUNCATE
- LISTEN/NOTIFY
- COMMENT ON TABLE/COLUMN
- VACUUM/ANALYZE/CHECKPOINT
- CREATE TABLE AS SELECT
- DECLARE CURSOR/FETCH/CLOSE
- CREATE SEQUENCE/nextval/currval/setval
- INSERT ON CONFLICT (DO UPDATE/DO NOTHING)
- INTERSECT/EXCEPT (+ ALL variants)

## SQL Features — Working with Known Bugs ⚠️
- **Division**: 10.0/3=3 (truncates)
- **CASE WHEN**: Always true for non-comparison conditions
- **NULL IS NULL in SELECT**: Returns string "NULL"
- **SUM(empty)**: Returns 0 not NULL
- **LIMIT 0**: Returns all rows
- **NATURAL JOIN**: Acts as cross join
- **FULL OUTER JOIN**: Drops unmatched right rows
- **VIEW-TABLE JOIN**: Drops table-side columns
- **INSERT FROM CTE**: Silent no-op
- **EXCLUDED in UPSERT**: Returns NULL
- **Trigger NEW/OLD**: Returns NULL
- **UNIQUE constraint**: Parsed but not enforced
- **Multi-column PK**: Parsed but not enforced
- **SERIAL**: Column created but no auto-increment
- **BIGINT**: Truncated to 32-bit
- **Index after rollback**: PK lookup fails
- **Multi-statement**: Only first statement executes

## SQL Features — Parsed but Not Executed ❌
- FETCH FIRST/NEXT (no limit applied)
- TABLESAMPLE (no sampling)
- ROWS/RANGE BETWEEN (frame spec ignored)
- MATERIALIZED VIEW (acts as regular view)
- FTS to_tsvector/@@ (no filtering)
- GIN index syntax
- DO blocks (PL/pgSQL)
- TEMP TABLE
- SET/GUC parameters
- DEFAULT VALUES insert
- CREATE EXTENSION
- DOUBLE PRECISION, VARCHAR(N), CHAR(N) types

## Aggregate Functions — 13/16 Working
✅ COUNT, SUM, AVG, MIN, MAX, STDDEV, VARIANCE, GROUP_CONCAT, COUNT DISTINCT, SUM DISTINCT, FILTER
⚠️ ARRAY_AGG, STRING_AGG (return undefined), BOOL_AND, BOOL_OR (return undefined)
❌ BIT_AND, BIT_OR, EVERY

## Window Functions — 8/10 Working
✅ ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, NTH_VALUE, PERCENT_RANK, CUME_DIST
❌ ROWS/RANGE BETWEEN frame specification
❌ FIRST_VALUE/LAST_VALUE with custom frame

## String Functions — 25/26 Working
✅ All except OVERLAY keyword syntax

## Math Functions — All Working
✅ ABS, CEIL, FLOOR, ROUND, TRUNC, MOD, SIGN, POWER, SQRT, CBRT, LN, LOG, EXP, PI, RANDOM

## Date Functions — Partial
✅ EXTRACT, DATE_TRUNC, NOW, CURRENT_TIMESTAMP
❌ TO_CHAR (stub), TO_DATE, TO_NUMBER, TO_TIMESTAMP

## JSON Functions — 5/10
✅ JSON_EXTRACT, JSON_SET, JSON_TYPE, JSON_ARRAY_LENGTH, JSONB_EXTRACT_PATH
❌ JSON_ARRAY, JSON_OBJECT, JSON_VALID, JSON_KEYS

## Standalone Modules — Tested
| Module | Status | Notes |
|--------|--------|-------|
| B+ Tree | ✅ SOLID | 4M ins/sec, 7.7M get/sec, range, delete |
| HeapFile | ✅ SOLID | 435K ins/sec, slotted pages, 98.9% util |
| WAL | ✅ SOLID | Through TransactionalDatabase (direct: serialize bug) |
| MVCC | ✅ SOLID | Snapshot isolation, no dirty/phantom reads |
| CoW B-Tree | ✅ SOLID | Correct isolation, 100x overhead |
| ART | ✅ SOLID | Radix tree, prefix search |
| SkipList | ✅ SOLID | Set/get/delete/range |
| BloomFilter | ✅ SOLID | 0% FP on 1K items |
| HyperLogLog | ✅ SOLID | 4.1% error |
| IntervalTree | ✅ SOLID | Range/point queries |
| KDTree | ✅ SOLID | Range/nearest queries |
| RTree | ✅ SOLID | Range/NN (not integrated) |
| VEBTree | ✅ SOLID | O(log log U) operations |
| WaveletTree | ✅ SOLID | Access/rank/select |
| ConsistentHash | ✅ SOLID | Even distribution |
| CuckooHash | ✅ SOLID | Set/get/has/delete |
| ExtendibleHash | ✅ SOLID | Dynamic resizing |
| LinearHash | ✅ SOLID | Dynamic resizing |
| LSM Tree | ⚠️ BUGGY | Put/get/delete work, no range scan |
| HTTP Server | ✅ SOLID | REST API functional |
| PG Protocol | ⚠️ PARTIAL | Messages encoded, no TCP server |
| Volcano Executor | ⚠️ BUGGY | 18 operators, HashJoin column bug |
| Vectorized Engine | ⚠️ PARTIAL | 1.6-1.8x speedup, HashJoin broken |
| QueryCodeGen | ⚠️ BUGGY | Generates JS but drops GROUP BY |
| CompiledQuery | ⚠️ PARTIAL | Bridge exists, not clearly active |
| ColumnStore | ❌ BROKEN | Column lookup fails after insert |
| GraceHashJoin | ❌ BROKEN | API expects key arrays |
| SortMergeJoin | ❌ BROKEN | Returns undefined |
| ThetaJoin | ❌ BROKEN | API mismatch |
| DistributedKV | ❌ BROKEN | Get returns null |
| Raft | ❌ BROKEN | Election crashes |
| 2PC | ❌ BROKEN | Abort crashes |
| TDigest | ❌ BROKEN | Returns input not value |
| CountMinSketch | ❌ BROKEN | Returns Infinity |

## Performance Summary
| Operation | Speed |
|-----------|-------|
| Single INSERT | 12.9K/sec |
| PK lookup | 10K/sec |
| Index lookup | 14.7K/sec (134x vs scan) |
| Full scan (10K) | 16ms |
| GROUP BY (10K) | 23ms |
| JOIN (1K×2K) | 2.5s (NL) / 222ms (Volcano HashJoin — but buggy) |
| Memory per row | 822 bytes |
| B+ tree insert | 4M/sec |
| HeapFile insert | 435K/sec |
