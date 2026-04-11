// sql-compliance-scorecard.js — Check PostgreSQL SQL feature compliance
// Run: node sql-compliance-scorecard.js

import { Database } from './src/db.js';

const checks = [];
let passed = 0, failed = 0, errors = 0;

function check(category, name, fn) {
  try {
    const result = fn();
    if (result === true || result === undefined) {
      checks.push({ category, name, status: '✅' });
      passed++;
    } else {
      checks.push({ category, name, status: '❌', detail: `Expected truthy, got: ${result}` });
      failed++;
    }
  } catch (e) {
    checks.push({ category, name, status: '💥', detail: e.message.slice(0, 80) });
    errors++;
  }
}

const db = new Database();
db.execute('CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT, age INT, score INT)');
db.execute("INSERT INTO t1 VALUES (1, 'Alice', 30, 90)");
db.execute("INSERT INTO t1 VALUES (2, 'Bob', 25, 80)");
db.execute("INSERT INTO t1 VALUES (3, 'Charlie', NULL, NULL)");
db.execute('CREATE TABLE t2 (id INT PRIMARY KEY, t1_id INT, amount INT)');
db.execute('INSERT INTO t2 VALUES (1, 1, 100)');
db.execute('INSERT INTO t2 VALUES (2, 1, 200)');
db.execute('INSERT INTO t2 VALUES (3, 2, 150)');

// --- DDL ---
check('DDL', 'CREATE TABLE', () => { db.execute('CREATE TABLE ddl_test (id INT PRIMARY KEY)'); return true; });
check('DDL', 'CREATE TABLE IF NOT EXISTS', () => { db.execute('CREATE TABLE IF NOT EXISTS ddl_test (id INT)'); return true; });
check('DDL', 'DROP TABLE', () => { db.execute('DROP TABLE ddl_test'); return true; });
check('DDL', 'CREATE INDEX', () => { db.execute('CREATE INDEX idx_name ON t1 (name)'); return true; });
check('DDL', 'CREATE VIEW', () => { db.execute('CREATE VIEW v1 AS SELECT * FROM t1 WHERE age > 20'); return true; });
check('DDL', 'ALTER TABLE ADD COLUMN', () => { db.execute('ALTER TABLE t1 ADD COLUMN email TEXT'); return true; });
check('DDL', 'ALTER TABLE DROP COLUMN', () => { db.execute('ALTER TABLE t1 DROP COLUMN email'); return true; });

// --- DML ---
check('DML', 'INSERT', () => db.execute('INSERT INTO t1 VALUES (99, \'Test\', 20, 50)').count >= 0);
check('DML', 'INSERT RETURNING', () => db.execute("INSERT INTO t1 VALUES (98, 'Ret', 21, 51) RETURNING *").rows.length === 1);
check('DML', 'UPDATE', () => { db.execute('UPDATE t1 SET age = 31 WHERE id = 1'); return true; });
check('DML', 'DELETE', () => { db.execute('DELETE FROM t1 WHERE id >= 98'); return true; });
check('DML', 'UPSERT (ON CONFLICT)', () => { db.execute("INSERT INTO t1 VALUES (1, 'Alice', 30, 90) ON CONFLICT (id) DO UPDATE SET age = 30"); return true; });
check('DML', 'TRUNCATE', () => { db.execute('CREATE TABLE trunc (id INT)'); db.execute('INSERT INTO trunc VALUES (1)'); db.execute('TRUNCATE trunc'); return db.execute('SELECT COUNT(*) as c FROM trunc').rows[0].c === 0; });

// --- SELECT ---
check('SELECT', 'Basic SELECT *', () => db.execute('SELECT * FROM t1').rows.length > 0);
check('SELECT', 'Column aliases', () => db.execute('SELECT name as n FROM t1 WHERE id = 1').rows[0].n === 'Alice');
check('SELECT', 'WHERE =', () => db.execute('SELECT * FROM t1 WHERE id = 1').rows.length === 1);
check('SELECT', 'WHERE !=', () => db.execute('SELECT * FROM t1 WHERE id != 1').rows.length > 0);
check('SELECT', 'WHERE <, >, <=, >=', () => db.execute('SELECT * FROM t1 WHERE id > 1').rows.length > 0);
check('SELECT', 'WHERE AND', () => db.execute('SELECT * FROM t1 WHERE id > 0 AND age > 20').rows.length > 0);
check('SELECT', 'WHERE OR', () => db.execute('SELECT * FROM t1 WHERE id = 1 OR id = 2').rows.length === 2);
check('SELECT', 'WHERE NOT', () => db.execute('SELECT * FROM t1 WHERE NOT id = 1').rows.length > 0);
check('SELECT', 'WHERE IN', () => db.execute('SELECT * FROM t1 WHERE id IN (1, 2)').rows.length === 2);
check('SELECT', 'WHERE NOT IN', () => db.execute('SELECT * FROM t1 WHERE id NOT IN (1, 2)').rows.length > 0);
check('SELECT', 'WHERE BETWEEN', () => db.execute('SELECT * FROM t1 WHERE id BETWEEN 1 AND 2').rows.length === 2);
check('SELECT', 'WHERE LIKE', () => db.execute("SELECT * FROM t1 WHERE name LIKE 'A%'").rows.length >= 1);
check('SELECT', 'WHERE IS NULL', () => db.execute('SELECT * FROM t1 WHERE age IS NULL').rows.length >= 1);
check('SELECT', 'WHERE IS NOT NULL', () => db.execute('SELECT * FROM t1 WHERE age IS NOT NULL').rows.length >= 1);
check('SELECT', 'WHERE EXISTS', () => db.execute('SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t1_id = t1.id)').rows.length > 0);
check('SELECT', 'DISTINCT', () => db.execute('SELECT DISTINCT age FROM t1').rows.length <= 3);
check('SELECT', 'ORDER BY ASC', () => { const r = db.execute('SELECT id FROM t1 ORDER BY id'); return r.rows[0].id <= r.rows[1].id; });
check('SELECT', 'ORDER BY DESC', () => { const r = db.execute('SELECT id FROM t1 ORDER BY id DESC'); return r.rows[0].id >= r.rows[1].id; });
check('SELECT', 'LIMIT', () => db.execute('SELECT * FROM t1 LIMIT 1').rows.length === 1);
check('SELECT', 'LIMIT 0', () => db.execute('SELECT * FROM t1 LIMIT 0').rows.length === 0);
check('SELECT', 'OFFSET', () => db.execute('SELECT * FROM t1 ORDER BY id OFFSET 1').rows[0].id === 2);

// --- JOINs ---
check('JOIN', 'INNER JOIN', () => db.execute('SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id').rows.length > 0);
check('JOIN', 'LEFT JOIN', () => db.execute('SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id').rows.length >= 3);
check('JOIN', 'RIGHT JOIN', () => db.execute('SELECT * FROM t2 RIGHT JOIN t1 ON t1.id = t2.t1_id').rows.length >= 3);
check('JOIN', 'CROSS JOIN', () => db.execute('SELECT * FROM t1 CROSS JOIN t2').rows.length > 0);
check('JOIN', 'Self-join', () => db.execute('SELECT a.name, b.name FROM t1 a JOIN t1 b ON a.age = b.age AND a.id < b.id').rows.length >= 0);

// --- Aggregates ---
check('AGG', 'COUNT(*)', () => db.execute('SELECT COUNT(*) as c FROM t1').rows[0].c > 0);
check('AGG', 'COUNT(column)', () => typeof db.execute('SELECT COUNT(age) as c FROM t1').rows[0].c === 'number');
check('AGG', 'COUNT(DISTINCT)', () => typeof db.execute('SELECT COUNT(DISTINCT age) as c FROM t1').rows[0].c === 'number');
check('AGG', 'SUM', () => db.execute('SELECT SUM(score) as s FROM t1').rows[0].s > 0);
check('AGG', 'AVG', () => db.execute('SELECT AVG(score) as a FROM t1').rows[0].a > 0);
check('AGG', 'MIN', () => typeof db.execute('SELECT MIN(age) as m FROM t1').rows[0].m === 'number');
check('AGG', 'MAX', () => typeof db.execute('SELECT MAX(age) as m FROM t1').rows[0].m === 'number');
check('AGG', 'GROUP BY', () => db.execute('SELECT age, COUNT(*) FROM t1 GROUP BY age').rows.length > 0);
check('AGG', 'HAVING', () => db.execute('SELECT age, COUNT(*) as c FROM t1 GROUP BY age HAVING COUNT(*) > 0').rows.length > 0);

// --- Window Functions ---
check('WINDOW', 'ROW_NUMBER', () => { const r = db.execute('SELECT ROW_NUMBER() OVER (ORDER BY id) as rn FROM t1'); return r.rows.some(row => row.rn === 1); });
check('WINDOW', 'RANK', () => db.execute('SELECT RANK() OVER (ORDER BY age) as r FROM t1 WHERE age IS NOT NULL').rows.length > 0);
check('WINDOW', 'SUM OVER', () => db.execute('SELECT SUM(score) OVER () as total FROM t1 WHERE score IS NOT NULL').rows[0].total > 0);
check('WINDOW', 'Running SUM', () => db.execute('SELECT SUM(score) OVER (ORDER BY id) as running FROM t1 WHERE score IS NOT NULL').rows.length > 0);
check('WINDOW', 'PARTITION BY', () => db.execute('SELECT id, age, ROW_NUMBER() OVER (PARTITION BY age ORDER BY id) as rn FROM t1 WHERE age IS NOT NULL').rows.length > 0);

// --- Subqueries ---
check('SUBQUERY', 'Scalar subquery', () => db.execute('SELECT (SELECT COUNT(*) FROM t2) as cnt').rows[0].cnt === 3);
check('SUBQUERY', 'FROM subquery', () => db.execute('SELECT * FROM (SELECT * FROM t1 WHERE id <= 2) sq').rows.length === 2);
check('SUBQUERY', 'IN subquery', () => db.execute('SELECT * FROM t1 WHERE id IN (SELECT t1_id FROM t2)').rows.length > 0);

// --- CTEs ---
check('CTE', 'WITH clause', () => db.execute('WITH cte AS (SELECT * FROM t1) SELECT COUNT(*) as c FROM cte').rows[0].c > 0);
check('CTE', 'Multiple CTEs', () => db.execute('WITH a AS (SELECT * FROM t1), b AS (SELECT * FROM t2) SELECT COUNT(*) as c FROM a').rows[0].c > 0);

// --- Expressions ---
check('EXPR', 'Arithmetic (+, -, *, /)', () => db.execute('SELECT 2 + 3 as result').rows[0].result === 5);
check('EXPR', 'String concatenation ||', () => db.execute("SELECT 'hello' || ' ' || 'world' as r").rows[0].r === 'hello world');
check('EXPR', 'CASE WHEN', () => db.execute("SELECT CASE WHEN 1 > 0 THEN 'yes' ELSE 'no' END as r").rows[0].r === 'yes');
check('EXPR', 'COALESCE', () => db.execute('SELECT COALESCE(NULL, NULL, 42) as r').rows[0].r === 42);
check('EXPR', 'CAST', () => db.execute("SELECT CAST(42 AS TEXT) as r").rows[0].r === '42');

// --- GENERATE_SERIES ---
check('GEN', 'GENERATE_SERIES basic', () => db.execute('SELECT * FROM GENERATE_SERIES(1, 5)').rows.length === 5);
check('GEN', 'with aggregate', () => db.execute('SELECT SUM(value) FROM GENERATE_SERIES(1, 10)').rows.length === 1);
check('GEN', 'with GROUP BY', () => db.execute('SELECT value % 3, COUNT(*) FROM GENERATE_SERIES(1, 30) GROUP BY value % 3').rows.length === 3);
check('GEN', 'with window', () => db.execute('SELECT value, ROW_NUMBER() OVER (ORDER BY value) as rn FROM GENERATE_SERIES(1, 5)').rows[0].rn === 1);

// --- Set Operations ---
check('SET', 'UNION', () => db.execute('SELECT id FROM t1 WHERE id <= 2 UNION SELECT id FROM t1 WHERE id >= 2').rows.length === 3);
check('SET', 'UNION ALL', () => db.execute('SELECT id FROM t1 WHERE id <= 2 UNION ALL SELECT id FROM t1 WHERE id >= 2').rows.length > 3);

// --- Other ---
check('OTHER', 'EXPLAIN', () => db.execute('EXPLAIN SELECT * FROM t1').rows.length > 0);
check('OTHER', 'SHOW TABLES', () => db.execute('SHOW TABLES').rows.length > 0);
check('OTHER', 'DESCRIBE', () => db.execute('DESCRIBE t1').rows.length > 0);
check('OTHER', 'VACUUM', () => { db.execute('VACUUM'); return true; });
check('OTHER', 'ANALYZE', () => { db.execute('ANALYZE t1'); return true; });

// --- Report ---
console.log('\n=== HenryDB SQL Compliance Scorecard ===\n');

const categories = {};
for (const c of checks) {
  if (!categories[c.category]) categories[c.category] = { pass: 0, fail: 0, error: 0 };
  if (c.status === '✅') categories[c.category].pass++;
  else if (c.status === '❌') categories[c.category].fail++;
  else categories[c.category].error++;
}

for (const [cat, stats] of Object.entries(categories)) {
  const total = stats.pass + stats.fail + stats.error;
  const pct = ((stats.pass / total) * 100).toFixed(0);
  console.log(`${cat.padEnd(10)} ${stats.pass}/${total} (${pct}%)${stats.fail ? ` — ${stats.fail} fail` : ''}${stats.error ? ` — ${stats.error} error` : ''}`);
}

console.log(`\n${'TOTAL'.padEnd(10)} ${passed}/${passed + failed + errors} (${((passed / (passed + failed + errors)) * 100).toFixed(0)}%)`);
if (failed + errors > 0) {
  console.log('\nFailed/Error:');
  for (const c of checks) {
    if (c.status !== '✅') {
      console.log(`  ${c.status} ${c.category}/${c.name}${c.detail ? ': ' + c.detail : ''}`);
    }
  }
}
