# HenryDB 🐸

A fully-featured SQL database engine written from scratch in JavaScript. No dependencies. Single file.

## Features

### DDL (Data Definition Language)
- `CREATE TABLE` with column types, constraints, defaults
- `ALTER TABLE ADD COLUMN` / `DROP COLUMN` / `RENAME COLUMN`
- `DROP TABLE` / `TRUNCATE TABLE`
- `CREATE INDEX` / `CREATE UNIQUE INDEX`
- Expression indexes: `CREATE INDEX idx ON t (LOWER(name))`
- Generated columns: `GENERATED ALWAYS AS (expr) STORED`
- `CREATE TABLE AS SELECT` (CTAS)
- `CREATE VIEW` / `DROP VIEW`
- `CREATE TRIGGER` (BEFORE/AFTER INSERT/UPDATE/DELETE)

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

### Queries
- `SELECT` with column aliases, expressions
- `WHERE` with complex conditions
- `ORDER BY` (multi-column, ASC/DESC, expressions)
- `LIMIT` / `OFFSET`
- `GROUP BY` / `HAVING`
- `DISTINCT` / `DISTINCT ON`
- `JOIN` (INNER, LEFT, RIGHT, FULL, CROSS)
- `LATERAL JOIN`
- `UNION` / `UNION ALL` / `INTERSECT` / `INTERSECT ALL` / `EXCEPT` / `EXCEPT ALL`
- Subqueries (scalar, IN, EXISTS, correlated)
- Common Table Expressions (WITH / recursive WITH)
- Window functions (ROW_NUMBER, RANK, DENSE_RANK, SUM, AVG, etc.)
- `CASE WHEN ... THEN ... ELSE ... END`
- `VALUES (1, 'a'), (2, 'b')` as standalone query

### Constraints
- `PRIMARY KEY`
- `NOT NULL`
- `UNIQUE` (column + expression)
- `CHECK` (column-level and table-level)
- `DEFAULT` values
- `FOREIGN KEY ... REFERENCES ... ON DELETE CASCADE/SET NULL/RESTRICT`

### Functions
- String: `UPPER`, `LOWER`, `LENGTH`, `CONCAT`, `SUBSTRING`, `REPLACE`, `TRIM`, `LTRIM`, `RTRIM`, `INSTR`, `PRINTF`
- Math: `ABS`, `ROUND`, `CEIL`, `FLOOR`
- Null: `COALESCE`, `NULLIF`, `IFNULL`, `IIF`
- Type: `TYPEOF`, `CAST`
- Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `GROUP_CONCAT`

### Pattern Matching
- `LIKE` (case-sensitive)
- `ILIKE` (case-insensitive)
- `BETWEEN`
- `IN` / `NOT IN`

### Advanced
- `EXPLAIN` / `EXPLAIN ANALYZE` (query plan + actual timing)
- Transactions (`BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`)
- B-tree indexes with query planner optimization
- WAL (Write-Ahead Log) for crash recovery
- Full serialization/deserialization (save/restore entire database)
- Inverted index for full-text search

## Usage

```javascript
import { Database } from './src/db.js';

const db = new Database();

// Create tables
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

// Insert data
db.execute("INSERT INTO users (id, name, email, score) VALUES (1, 'Alice', 'alice@example.com', 95) RETURNING *");
// → [{ id: 1, name: 'Alice', email: 'alice@example.com', score: 95, grade: 'A' }]

// Query with CTE + window function
db.execute(`
  WITH ranked AS (
    SELECT name, score, ROW_NUMBER() OVER (ORDER BY score DESC) AS rank
    FROM users
  )
  SELECT * FROM ranked WHERE rank <= 10
`);

// Persistence
const snapshot = db.save();       // serialize to JSON
const db2 = Database.fromSerialized(snapshot);  // restore
```

## Tests

```bash
npm test
```

## Architecture

- **Parser** (`sql.js`): Hand-written recursive descent SQL parser
- **Planner** (`planner.js`): Cost-based query optimizer with index selection
- **Engine** (`db.js`): Heap storage, B-tree indexes, WAL, constraint enforcement
- **All in JavaScript**: No native modules, no dependencies, runs anywhere Node does

## License

MIT
