# HenryDB 🐸

A fully-featured SQL database engine written from scratch in JavaScript. No dependencies. **93K lines** (42K source, 51K tests), **4100+ tests passing**.

## Features

### DDL (Data Definition Language)
- `CREATE TABLE` with column types, constraints, defaults
- `ALTER TABLE ADD COLUMN` / `DROP COLUMN` / `RENAME COLUMN` / `ALTER TYPE`
- `DROP TABLE` / `TRUNCATE TABLE`
- `CREATE INDEX` / `CREATE UNIQUE INDEX` / `DROP INDEX`
- Expression indexes: `CREATE INDEX idx ON t (LOWER(name))`
- Generated columns: `GENERATED ALWAYS AS (expr) STORED`
- `CREATE TABLE AS SELECT` (CTAS) / `CREATE TEMP TABLE`
- `CREATE VIEW` / `DROP VIEW`
- `CREATE TRIGGER` (BEFORE/AFTER INSERT/UPDATE/DELETE)
- `CREATE SEQUENCE` / `DROP SEQUENCE`
- `CREATE SCHEMA` / `SET search_path`
- `COMMENT ON TABLE/COLUMN/FUNCTION/INDEX`

### DML (Data Manipulation Language)
- `INSERT INTO ... VALUES` (multi-row)
- `INSERT INTO ... SELECT`
- `INSERT ... ON CONFLICT DO UPDATE / DO NOTHING` (upsert)
- `INSERT OR REPLACE` / `INSERT OR IGNORE`
- `INSERT ... RETURNING *`
- `UPDATE ... SET` with expressions
- `UPDATE ... FROM` (PostgreSQL-style multi-table)
- `UPDATE ... RETURNING *`
- `DELETE FROM ... WHERE`
- `DELETE ... USING` (multi-table delete)
- `DELETE ... RETURNING *`
- `MERGE INTO ... USING ... ON ... WHEN MATCHED/NOT MATCHED` (SQL:2003)
- `COPY ... TO STDOUT` (CSV export)

### Queries
- `SELECT` with column aliases, expressions, unary minus
- `WHERE` with complex conditions
- `ORDER BY` (multi-column, ASC/DESC, NULLS FIRST/LAST, expressions)
- `LIMIT` / `OFFSET`
- `GROUP BY` / `HAVING`
- `GROUP BY ROLLUP(...)` / `GROUP BY CUBE(...)`
- `GROUP BY GROUPING SETS ((a, b), (a), ())`
- `DISTINCT` / `DISTINCT ON`
- `JOIN` (INNER, LEFT, RIGHT, FULL, CROSS)
- `LATERAL JOIN` (comma syntax + explicit LATERAL)
- `UNION` / `UNION ALL` / `INTERSECT` / `INTERSECT ALL` / `EXCEPT` / `EXCEPT ALL`
- Subqueries (scalar, IN, EXISTS, correlated)
- Common Table Expressions (`WITH` / recursive `WITH RECURSIVE name(cols) AS (...)`)
- Window functions: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`, `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`, `PERCENT_RANK`, `CUME_DIST`, `SUM`, `AVG`, `COUNT`, `MIN`, `MAX`
- `CASE WHEN ... THEN ... ELSE ... END`
- `VALUES (1, 'a'), (2, 'b')` as standalone query
- `ARRAY[1, 2, 3]` constructor

### Constraints
- `PRIMARY KEY`
- `NOT NULL`
- `UNIQUE` (column + expression)
- `CHECK` (column-level and table-level)
- `DEFAULT` values
- `FOREIGN KEY ... REFERENCES ... ON DELETE CASCADE/SET NULL/RESTRICT`

### Functions
- **String**: `UPPER`, `LOWER`, `LENGTH`, `CONCAT`, `CONCAT_WS`, `SUBSTRING`, `REPLACE`, `TRIM`, `LTRIM`, `RTRIM`, `LPAD`, `RPAD`, `LEFT`, `RIGHT`, `REVERSE`, `REPEAT`, `INSTR`, `PRINTF`, `INITCAP`, `TRANSLATE`, `CHR`, `ASCII`, `MD5`, `ENCODE`, `DECODE`, `POSITION`, `SPLIT_PART`
- **Math**: `ABS`, `ROUND`, `CEIL`, `FLOOR`, `POWER`, `SQRT`, `LOG`, `GREATEST`, `LEAST`, `MOD`, `SIGN`, `TRUNC`, `PI`, `EXP`, `LN`, `LOG10`, `LOG2`, `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `ATAN2`, `CBRT`, `GCD`, `LCM`, `DEGREES`, `RADIANS`
- **Null**: `COALESCE`, `NULLIF`, `IFNULL`, `IIF`
- **Type**: `TYPEOF`, `CAST(... AS type)`
- **Date/Time**: `NOW()`, `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `DATE_TRUNC`, `EXTRACT(field FROM date)`, `DATE_PART`, `AGE`, `DATE_ADD`, `DATE_SUB`, `TO_CHAR`, `STRFTIME`
- **Array**: `ARRAY_LENGTH`, `ARRAY_APPEND`, `ARRAY_REMOVE`, `ARRAY_CAT`, `ARRAY_POSITION`
- **JSON**: `JSON_EXTRACT`, `JSON_SET`, `JSON_ARRAY_LENGTH`, `JSON_TYPE`, `JSON_OBJECT`, `JSON_ARRAY`
- **Regex**: `REGEXP_REPLACE`, `REGEXP_MATCH`, `REGEXP_MATCHES`, `REGEXP_COUNT`
- **Aggregates**: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `ARRAY_AGG`, `JSON_AGG`, `STRING_AGG`, `GROUP_CONCAT`, `BOOL_AND`, `BOOL_OR`, `STDDEV`, `STDDEV_POP`, `VARIANCE`, `VAR_POP`, `MEDIAN`
- **Aggregate FILTER**: `COUNT(*) FILTER (WHERE condition)`
- **User-defined**: `CREATE FUNCTION name(params) RETURNS type AS $$ body $$`
- `generate_series(start, stop)` table-valued function

### Pattern Matching
- `LIKE` (case-sensitive) / `ILIKE` (case-insensitive)
- `SIMILAR TO` (regex)
- `BETWEEN` / `NOT BETWEEN`
- `IN` / `NOT IN`
- `IS NULL` / `IS NOT NULL`
- `IS DISTINCT FROM`

### Advanced
- `EXPLAIN` / `EXPLAIN ANALYZE` / `EXPLAIN (FORMAT JSON)` / `EXPLAIN COMPILED`
- `SHOW TABLES` / `SHOW COLUMNS FROM` / `SHOW FUNCTIONS`
- `PREPARE` / `EXECUTE` / `DEALLOCATE` (prepared statements)
- `DECLARE CURSOR` / `FETCH` / `CLOSE` (cursors)
- `VACUUM` (garbage collection)
- Transactions (`BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `ROLLBACK TO`)
- Serializable Snapshot Isolation (SSI) for full ACID compliance
- B-tree indexes with cost-based query planner optimization
- Three execution engines: Volcano (iterative), Pipeline Compiler (JIT), Vectorized (columnar batches)
- Auto-vectorization for tables with 500+ rows (2x speedup)
- Predicate pushdown through joins
- Correlated EXISTS → batch hash semi-join decorrelation
- WAL (Write-Ahead Log) for crash recovery with ALTER TABLE persistence
- PostgreSQL wire protocol (`PgServer` for psql/JDBC clients)
- Full serialization/deserialization (save/restore entire database)
- `information_schema.tables` / `information_schema.columns`
- Inverted index for full-text search
- R-tree for spatial indexing
- TDigest for approximate percentiles

## Usage

```javascript
import { Database } from './src/db.js';

const db = new Database();

// Create tables with constraints and generated columns
db.execute(`CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT UNIQUE,
  score REAL DEFAULT 0,
  grade TEXT GENERATED ALWAYS AS (
    CASE WHEN score >= 90 THEN 'A'
         WHEN score >= 80 THEN 'B'
         ELSE 'C' END
  ) STORED
)`);

// Insert with RETURNING
db.execute("INSERT INTO users (id, name, email, score) VALUES (1, 'Alice', 'alice@example.com', 95) RETURNING *");
// → [{ id: 1, name: 'Alice', email: 'alice@example.com', score: 95, grade: 'A' }]

// Recursive CTE: Fibonacci sequence
db.execute(`
  WITH RECURSIVE fib(n, a, b) AS (
    SELECT 1, 0, 1
    UNION ALL
    SELECT n + 1, b, a + b FROM fib WHERE n < 10
  )
  SELECT n, a AS fibonacci FROM fib
`);

// Window functions + FILTER clause
db.execute(`
  SELECT region,
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE status = 'active') AS active,
    SUM(amount) OVER (PARTITION BY region ORDER BY date) AS running_total
  FROM orders
  GROUP BY region
`);

// MERGE (upsert alternative)
db.execute(`
  MERGE INTO inventory t USING shipment s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET qty = t.qty + s.qty
  WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.qty)
`);

// Array operations
db.execute('SELECT ARRAY[1, 2, 3] AS arr, ARRAY_LENGTH(ARRAY[1, 2, 3]) AS len');

// Date functions
db.execute("SELECT DATE_TRUNC('month', '2024-03-15') AS month_start, EXTRACT(YEAR FROM '2024-03-15') AS year");

// Persistence
import { TransactionalDatabase } from './src/transactional-db.js';
const tdb = TransactionalDatabase.open('./mydb');  // file-backed with WAL
tdb.execute('CREATE TABLE ...');
tdb.close();  // data persisted
```

## Tests

```bash
node --test src/*.test.js        # Run all 4100+ tests
node --test src/sql.test.js      # Run specific test file
```

## Architecture

- **Parser** (`sql.js`): Hand-written recursive descent SQL parser with 100+ keyword support
- **Planner** (`planner.js`): Cost-based query optimizer with join method selection (nested loop, hash join, merge join), index selection, and predicate pushdown
- **Vectorized Engine** (`vectorized.js`): Columnar batch execution with selection vectors, auto-applied for large tables
- **MVCC** (`ssi.js`): Serializable Snapshot Isolation with write-write conflict detection, phantom prevention
- **Storage** (`db.js`): Heap storage, B-tree indexes, WAL, constraint enforcement
- **Network** (`server.js`): PostgreSQL wire protocol for external client access
- **All in JavaScript**: No native modules, no dependencies, runs anywhere Node does

## Stats

| Metric | Value |
|--------|-------|
| Source lines | ~42,000 |
| Test lines | ~52,000 |
| Tests passing | 4,143+ |
| Source modules | 172 |
| SQL functions | 151 |
| Statement types | 46 |
| Dependencies | 0 |

## License

MIT
