#!/usr/bin/env node
// demo.js — HenryDB Feature Demo
// Run: node demo.js

import { Database } from './src/db.js';

const db = new Database();
const log = (msg) => console.log(`\n${'='.repeat(60)}\n  ${msg}\n${'='.repeat(60)}`);
const show = (r) => { for (const row of r.rows) console.log('  ', JSON.stringify(row)); };

log('1. DDL — CREATE TABLE with constraints');
db.execute(`CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT UNIQUE,
  age INTEGER CHECK (age >= 0 AND age <= 150),
  data JSONB
)`);
console.log('  ✓ Table created with SERIAL, NOT NULL, UNIQUE, CHECK, JSONB');

log('2. DML — INSERT, UPDATE, DELETE with RETURNING');
db.execute("INSERT INTO users (name, email, age, data) VALUES ('Alice', 'alice@example.com', 30, '{\"role\": \"admin\", \"tags\": [\"dev\", \"ops\"]}')");
db.execute("INSERT INTO users (name, email, age, data) VALUES ('Bob', 'bob@example.com', 25, '{\"role\": \"user\", \"tags\": [\"dev\"]}')");
db.execute("INSERT INTO users (name, email, age, data) VALUES ('Charlie', 'charlie@example.com', 35, '{\"role\": \"admin\", \"tags\": [\"mgmt\"]}')");
const ret = db.execute("INSERT INTO users (name, email, age, data) VALUES ('Diana', 'diana@example.com', 28, '{\"role\": \"user\"}') RETURNING id, name");
console.log('  RETURNING:', JSON.stringify(ret.rows));

log('3. Indexes');
db.execute('CREATE INDEX idx_age ON users (age)');
db.execute('CREATE INDEX CONCURRENTLY idx_email ON users (email)');
console.log('  ✓ B-tree index + concurrent index created');

log('4. Transactions — ACID');
db.execute('BEGIN');
db.execute("UPDATE users SET age = 31 WHERE name = 'Alice'");
db.execute('SAVEPOINT sp1');
db.execute("UPDATE users SET age = 99 WHERE name = 'Alice'");
db.execute('ROLLBACK TO sp1');  // age stays 31, not 99
db.execute('COMMIT');
const alice = db.execute("SELECT age FROM users WHERE name = 'Alice'");
console.log('  Alice age after savepoint rollback:', alice.rows[0].age, '(should be 31)');

log('5. Window Functions');
show(db.execute(`
  SELECT name, age, 
         RANK() OVER (ORDER BY age DESC) as age_rank,
         NTILE(2) OVER (ORDER BY age) as half
  FROM users ORDER BY age DESC
`));

log('6. CTEs (Common Table Expressions)');
show(db.execute(`
  WITH admins AS (
    SELECT * FROM users WHERE data->>'role' = 'admin'
  )
  SELECT name, age FROM admins ORDER BY age
`));

log('7. JSON Operations');
show(db.execute(`
  SELECT name, 
         data->>'role' as role,
         data->'tags'->0 as first_tag,
         json_array_length(data->'tags') as tag_count
  FROM users WHERE data->>'role' = 'admin'
`));

log('8. Full-Text Search (@@)');
db.execute('CREATE TABLE docs (id SERIAL, title TEXT, body TEXT)');
db.execute("INSERT INTO docs (title, body) VALUES ('DB Guide', 'Learn about databases and SQL queries')");
db.execute("INSERT INTO docs (title, body) VALUES ('Cooking', 'How to cook pasta and pizza')");
db.execute("INSERT INTO docs (title, body) VALUES ('SQL Tips', 'Advanced SQL techniques for database optimization')");
show(db.execute("SELECT title FROM docs WHERE to_tsvector(body) @@ to_tsquery('database')"));

log('9. Aggregation + HAVING');
show(db.execute(`
  SELECT data->>'role' as role, COUNT(*) as cnt, AVG(age) as avg_age
  FROM users GROUP BY data->>'role'
  HAVING COUNT(*) >= 1
  ORDER BY cnt DESC
`));

log('10. Subqueries');
show(db.execute(`
  SELECT name, age FROM users 
  WHERE age > (SELECT AVG(age) FROM users)
  ORDER BY age DESC
`));

log('11. UPSERT (INSERT ON CONFLICT)');
db.execute("INSERT INTO users (id, name, email, age) VALUES (1, 'Alice Updated', 'alice@example.com', 31) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name");
show(db.execute("SELECT id, name FROM users WHERE id = 1"));

log('12. Stored Functions');
db.execute(`CREATE FUNCTION double_val(x INTEGER) RETURNS INTEGER AS $$
SELECT x * 2;
$$ LANGUAGE sql`);
show(db.execute('SELECT name, double_val(age) as double_age FROM users ORDER BY age'));

log('13. EXPLAIN ANALYZE');
show(db.execute('EXPLAIN ANALYZE SELECT * FROM users WHERE age > 28'));

log('14. SQL Comments');
show(db.execute(`
  -- This is a line comment
  SELECT name, age /* inline comment */ FROM users
  WHERE age > 25
  ORDER BY age
`));

log('15. BETWEEN in SELECT');
show(db.execute(`
  SELECT name, age, age BETWEEN 25 AND 30 as in_range
  FROM users ORDER BY age
`));

console.log('\n' + '='.repeat(60));
console.log('  HenryDB Demo Complete!');
console.log('  330 modules • 847 test files • 6207+ tests');
console.log('='.repeat(60));
