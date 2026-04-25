# HenryDB

A SQLite-compatible relational database written from scratch in JavaScript. Implements SQL parsing, query execution, indexing, persistence, transactions, and window functions.

## Stats
- **209,000+ lines** of JavaScript
- **1,249 source files**
- **4,100+ commits**
- **97.5% SQLite compatibility** (differential fuzzer, 10 seeds × 200 iterations)

## SQL Support

### DDL
- `CREATE TABLE` with constraints (PRIMARY KEY, NOT NULL, UNIQUE, CHECK, DEFAULT, FOREIGN KEY)
- `CREATE INDEX` / `CREATE UNIQUE INDEX`
- `CREATE VIEW` / `CREATE TEMP TABLE`
- `CREATE TRIGGER` (BEFORE/AFTER/INSTEAD OF, INSERT/UPDATE/DELETE)
- `ALTER TABLE` (ADD COLUMN, DROP COLUMN, RENAME)
- `DROP TABLE/INDEX/VIEW/TRIGGER`

### DML
- `SELECT` with full expression evaluation
- `INSERT INTO ... VALUES` / `INSERT INTO ... SELECT`
- `UPDATE ... SET ... WHERE`
- `DELETE FROM ... WHERE`
- `UPSERT` (INSERT ... ON CONFLICT DO UPDATE/NOTHING)
- `MERGE` / `REPLACE`
- `EXPLAIN QUERY PLAN`

### Queries
- **JOINs**: INNER, LEFT, RIGHT, FULL OUTER, CROSS, NATURAL
- **Subqueries**: Scalar, correlated, EXISTS, IN
- **CTEs**: `WITH name AS (SELECT ...)` — multiple and chained
- **Set operations**: UNION, UNION ALL, INTERSECT, EXCEPT
- **Window functions**: ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE
- **Window frames**: ROWS, RANGE, GROUPS with BETWEEN/UNBOUNDED/CURRENT ROW and EXCLUDE
- **GROUP BY** with HAVING
- **ORDER BY** with ASC/DESC, NULLS FIRST/LAST
- **LIMIT** / **OFFSET**
- **DISTINCT** / **DISTINCT ON**
- **VALUES** as table source: `SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)`
- **GENERATE_SERIES** / **UNNEST** table functions

### Aggregate Functions
`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `MEDIAN`, `PERCENTILE_CONT`, `PERCENTILE_DISC`,
`GROUP_CONCAT`, `STRING_AGG`, `JSON_AGG`, `ARRAY_AGG`, `BOOL_AND`, `BOOL_OR`,
`STDDEV`, `VARIANCE`, `CORR`, `COVAR_POP`, `REGR_SLOPE`, `REGR_R2`

### Scalar Functions
`COALESCE`, `NULLIF`, `IFNULL`, `IIF`, `CAST`, `TYPEOF`, `ABS`, `ROUND`,
`SUBSTR`, `LENGTH`, `UPPER`, `LOWER`, `TRIM`, `LTRIM`, `RTRIM`, `REPLACE`,
`INSTR`, `PRINTF`, `DATE`, `TIME`, `DATETIME`, `STRFTIME`, `RANDOM`, `HEX`,
`UNICODE`, `QUOTE`, `ZEROBLOB`, `JSON_EXTRACT`, `JSON_ARRAY`, `JSON_OBJECT`

### Transactions
- `BEGIN` / `COMMIT` / `ROLLBACK`
- `SAVEPOINT` / `RELEASE` / `ROLLBACK TO`
- WAL (Write-Ahead Logging)
- Crash recovery

### Storage
- B-tree based page storage
- WAL for durability
- Page cache with LRU eviction
- Disk manager with configurable page size
- Vacuum / auto-vacuum

### Type System
- SQLite-compatible type affinity (TEXT, INTEGER, REAL, NUMERIC, BLOB)
- Coercion on INSERT based on column type
- SQLite-compatible comparison ordering (NULL < numbers < text < blob)

### Optimization
- Query plan optimization
- Index utilization (B-tree, hash, covering)
- Predicate pushdown
- Index recommendation (`RECOMMEND INDEXES`)
- Selectivity estimation
- Vectorized GROUP BY execution
- Compiled query plans

### Prepared Statements
- `PREPARE name AS SELECT ...`
- `EXECUTE name(params)`
- `db.execute(sql, [params])` API with AST-level parameter binding
- Parameter count validation

## API

```javascript
import { Database } from './src/db.js';

const db = new Database();

// DDL
db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)');
db.execute('CREATE INDEX idx_age ON users(age)');

// DML with parameters
db.execute('INSERT INTO users VALUES ($1, $2, $3)', [1, 'alice', 30]);

// Queries
const result = db.execute('SELECT * FROM users WHERE age > $1', [25]);
console.log(result.rows);

// Window functions
db.execute(`
  SELECT name, age,
    RANK() OVER (ORDER BY age DESC) as rank,
    AVG(age) OVER (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as moving_avg
  FROM users
`);

// CTE
db.execute(`
  WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (ORDER BY age) as rn
    FROM users
  )
  SELECT * FROM ranked WHERE rn <= 3
`);
```

## Architecture

```
SQL String → Parser → AST → Query Planner → Executor
                                    ↓
                              Index Selection
                              Predicate Pushdown
                              Vectorized Execution
                                    ↓
                              Storage Engine
                              (B-tree, Pages, WAL)
```
