// feature-coverage.test.js — One test per major HenryDB feature
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('SQL Feature Coverage', () => {
  let db;

  function fresh() {
    db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT, dept TEXT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30, 'eng')");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25, 'sales')");
    db.execute("INSERT INTO users VALUES (3, 'Carol', 35, 'eng')");
    db.execute("INSERT INTO users VALUES (4, 'Dave', 28, 'hr')");
    db.execute("INSERT INTO users VALUES (5, 'Eve', 32, 'sales')");
    return db;
  }

  it('SELECT *', () => { const r = fresh().execute('SELECT * FROM users'); assert.strictEqual(r.rows.length, 5); });
  it('WHERE', () => { const r = fresh().execute("SELECT * FROM users WHERE dept = 'eng'"); assert.strictEqual(r.rows.length, 2); });
  it('ORDER BY', () => { const r = fresh().execute('SELECT name FROM users ORDER BY age DESC'); assert.strictEqual(r.rows[0].name, 'Carol'); });
  it('LIMIT', () => { const r = fresh().execute('SELECT * FROM users LIMIT 3'); assert.strictEqual(r.rows.length, 3); });
  it('OFFSET', () => { const r = fresh().execute('SELECT * FROM users ORDER BY id LIMIT 2 OFFSET 3'); assert.strictEqual(r.rows[0].id, 4); });
  it('DISTINCT', () => { const r = fresh().execute('SELECT DISTINCT dept FROM users'); assert.strictEqual(r.rows.length, 3); });
  it('COUNT', () => { const r = fresh().execute('SELECT COUNT(*) as c FROM users'); assert.strictEqual(r.rows[0].c, 5); });
  it('SUM', () => { const r = fresh().execute('SELECT SUM(age) as s FROM users'); assert.strictEqual(r.rows[0].s, 150); });
  it('AVG', () => { const r = fresh().execute('SELECT AVG(age) as a FROM users'); assert.strictEqual(r.rows[0].a, 30); });
  it('MIN/MAX', () => { const r = fresh().execute('SELECT MIN(age) as mn, MAX(age) as mx FROM users'); assert.strictEqual(r.rows[0].mn, 25); assert.strictEqual(r.rows[0].mx, 35); });
  it('GROUP BY', () => { const r = fresh().execute('SELECT dept, COUNT(*) as c FROM users GROUP BY dept ORDER BY c DESC'); assert.strictEqual(r.rows[0].c, 2); });
  it('HAVING', () => { const r = fresh().execute('SELECT dept, COUNT(*) as c FROM users GROUP BY dept HAVING COUNT(*) > 1'); assert.strictEqual(r.rows.length, 2); });
  it('LIKE', () => { const r = fresh().execute("SELECT * FROM users WHERE name LIKE 'A%'"); assert.strictEqual(r.rows.length, 1); });
  it('BETWEEN', () => { const r = fresh().execute('SELECT * FROM users WHERE age BETWEEN 28 AND 32'); assert.strictEqual(r.rows.length, 3); });
  it('IN', () => { const r = fresh().execute("SELECT * FROM users WHERE dept IN ('eng', 'hr')"); assert.strictEqual(r.rows.length, 3); });
  it('IS NULL', () => { fresh().execute('INSERT INTO users VALUES (6, NULL, 40, NULL)'); const r = db.execute('SELECT * FROM users WHERE name IS NULL'); assert.strictEqual(r.rows.length, 1); });
  it('COALESCE', () => { fresh().execute('INSERT INTO users VALUES (6, NULL, 40, NULL)'); const r = db.execute("SELECT COALESCE(name, 'unknown') as n FROM users WHERE id = 6"); assert.strictEqual(r.rows[0].n, 'unknown'); });
  it('CASE WHEN', () => { const r = fresh().execute("SELECT name, CASE WHEN age > 30 THEN 'senior' ELSE 'junior' END as level FROM users WHERE id = 1"); assert.strictEqual(r.rows[0].level, 'junior'); });
  
  it('JOIN', () => {
    const db = fresh();
    db.execute('CREATE TABLE depts (id INT PRIMARY KEY, dname TEXT)');
    db.execute("INSERT INTO depts VALUES (1, 'Engineering')");
    const r = db.execute("SELECT u.name, d.dname FROM users u JOIN depts d ON u.dept = 'eng' AND d.id = 1");
    assert.ok(r.rows.length > 0);
  });

  it('LEFT JOIN', () => {
    const db = fresh();
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)');
    db.execute('INSERT INTO orders VALUES (1, 1, 100)');
    const r = db.execute('SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.id');
    assert.strictEqual(r.rows.length, 5);
    assert.strictEqual(r.rows[0].amount, 100);
    assert.strictEqual(r.rows[1].amount, null);
  });

  it('SUBQUERY', () => { const r = fresh().execute('SELECT name FROM users WHERE age > (SELECT AVG(age) FROM users)'); assert.ok(r.rows.length > 0); });
  it('EXISTS', () => { const r = fresh().execute("SELECT name FROM users WHERE EXISTS (SELECT 1 FROM users WHERE dept = 'eng')"); assert.ok(r.rows.length > 0); });
  
  it('INSERT', () => { fresh().execute("INSERT INTO users VALUES (6, 'Frank', 27, 'marketing')"); const r = db.execute('SELECT COUNT(*) as c FROM users'); assert.strictEqual(r.rows[0].c, 6); });
  it('UPDATE', () => { fresh().execute('UPDATE users SET age = 99 WHERE id = 1'); const r = db.execute('SELECT age FROM users WHERE id = 1'); assert.strictEqual(r.rows[0].age, 99); });
  it('DELETE', () => { fresh().execute('DELETE FROM users WHERE id = 5'); const r = db.execute('SELECT COUNT(*) as c FROM users'); assert.strictEqual(r.rows[0].c, 4); });
  it('TRUNCATE', () => { fresh().execute('TRUNCATE TABLE users'); const r = db.execute('SELECT COUNT(*) as c FROM users'); assert.strictEqual(r.rows[0].c, 0); });
  
  it('CREATE INDEX', () => { fresh().execute('CREATE INDEX idx ON users (age)'); assert.ok(true); });
  it('EXPLAIN', () => { const r = fresh().execute('EXPLAIN SELECT * FROM users WHERE id = 1'); assert.strictEqual(r.type, 'PLAN'); });
  it('VACUUM', () => { const r = fresh().execute('VACUUM'); assert.strictEqual(r.type, 'OK'); });
  it('ANALYZE', () => { const r = fresh().execute('ANALYZE users'); assert.strictEqual(r.type, 'ANALYZE'); });
  it('SHOW TABLES', () => { const r = fresh().execute('SHOW TABLES'); assert.ok(r.rows.length >= 1); });
  it('DESCRIBE', () => { const r = fresh().execute('DESCRIBE users'); assert.strictEqual(r.rows.length, 4); });
  it('BEGIN/COMMIT', () => { fresh().execute('BEGIN'); db.execute("INSERT INTO users VALUES (6, 'X', 1, 'x')"); db.execute('COMMIT'); const r = db.execute('SELECT COUNT(*) as c FROM users'); assert.strictEqual(r.rows[0].c, 6); });
  
  it('UNION', () => {
    const db = fresh();
    const r = db.execute("SELECT name FROM users WHERE dept = 'eng' UNION SELECT name FROM users WHERE dept = 'hr'");
    assert.strictEqual(r.rows.length, 3);
  });

  it('CTE', () => {
    const r = fresh().execute('WITH young AS (SELECT * FROM users WHERE age < 30) SELECT COUNT(*) as c FROM young');
    assert.strictEqual(r.rows[0].c, 2);
  });

  it('WINDOW FUNCTION', () => {
    const r = fresh().execute('SELECT name, ROW_NUMBER() OVER (ORDER BY age) as rn FROM users');
    assert.strictEqual(r.rows.length, 5);
  });

  it('RETURNING', () => {
    const r = fresh().execute("INSERT INTO users VALUES (6, 'Zoe', 29, 'ops') RETURNING id, name");
    assert.strictEqual(r.rows[0].name, 'Zoe');
  });

  it('CAST', () => {
    const r = fresh().execute("SELECT CAST(age AS TEXT) as age_str FROM users WHERE id = 1");
    assert.strictEqual(typeof r.rows[0].age_str, 'string');
  });

  it('arithmetic expressions', () => {
    const r = fresh().execute('SELECT age * 2 as doubled FROM users WHERE id = 1');
    assert.strictEqual(r.rows[0].doubled, 60);
  });

  it('string concatenation', () => {
    const r = fresh().execute("SELECT name || ' (' || dept || ')' as label FROM users WHERE id = 1");
    assert.strictEqual(r.rows[0].label, 'Alice (eng)');
  });

  it('NULLIF', () => {
    const r = fresh().execute("SELECT NULLIF(dept, 'eng') as d FROM users WHERE id = 1");
    assert.strictEqual(r.rows[0].d, null);
  });
});
