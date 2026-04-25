# HenryDB Architecture Notes

## Module Structure (209K LOC total)

### Core (13K LOC)
- `sql.js` (6K) — Full SQL parser: DDL, DML, expressions, window specs
- `db.js` (4.5K) — Database class, transaction handling, trigger execution
- `expression-evaluator.js` (1.2K) — Expression evaluation mixin
- `select-inner.js` (700) — Core SELECT execution
- `insert-row.js` (200) — INSERT with constraint validation

### Query Execution
- `select-inner.js` — Main SELECT pipeline (FROM, WHERE, ORDER BY, LIMIT)
- `group-by-executor.js` — GROUP BY with HAVING
- `cte.js` — Common Table Expressions (WITH clause)
- `window-functions.js` — Window function evaluator (RANK, ROW_NUMBER, etc.)
- `join-executor.js` — JOIN execution (INNER, LEFT, RIGHT, FULL, CROSS)

### Optimization
- `pushdown.js` — Predicate pushdown
- `selectivity.js` — Selectivity estimation for query planning
- `compiled-query.js` — Compiled query plans
- `vectorized-bridge.js` — Vectorized GROUP BY execution
- `volcano.js` — Volcano-model iterator operators

### Storage Engine
- `btree.js` — B-tree implementation for indexes
- `page.js` — Page management (4KB pages)
- `disk-manager.js` — Disk I/O
- `wal.js` — Write-Ahead Logging for durability
- `page-cache.js` — LRU page cache

### Type System
- `type-affinity.js` — SQLite-compatible type affinity and comparison
- `percentile.js` — MEDIAN and PERCENTILE_CONT/DISC

### Prepared Statements
- `prepared-stmts-ast.js` — AST-level parameter binding
- `prepared-stmts.js` — String-level EXECUTE handler

### Standard Library
- `sql-functions.js` — 50+ scalar SQL functions (COALESCE, SUBSTR, DATE, etc.)

## Key Design Decisions
1. **SQLite compatibility** over SQL standard compliance
2. **Type affinity on INSERT** — coerce values based on column type
3. **sqliteCompare** everywhere — consistent mixed-type ordering
4. **Expression evaluation as mixin** — methods installed on Database.prototype
5. **Virtual rows for VALUES/GENERATE_SERIES** — convert to __subquery internally
