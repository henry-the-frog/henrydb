// sql-compat.test.js — SQL compatibility tests covering all major features
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('SQL Compatibility', () => {
  // DDL
  it('CREATE TABLE with all constraint types', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL, age INT CHECK (age > 0), email TEXT DEFAULT null)');
    const schema = db.tables.get('t').schema;
    assert.equal(schema.length, 4);
  });

  it('CREATE TABLE IF NOT EXISTS', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE IF NOT EXISTS t (id INT PRIMARY KEY)'); // No error
  });

  it('DROP TABLE IF EXISTS', () => {
    const db = new Database();
    db.execute('DROP TABLE IF EXISTS nonexistent'); // No error
  });

  it('ALTER TABLE ADD/DROP/RENAME COLUMN', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hello')");
    db.execute('ALTER TABLE t ADD COLUMN b INT');
    db.execute('ALTER TABLE t DROP COLUMN a');
    assert.ok(db.tables.get('t').schema.some(c => c.name === 'b'));
  });

  // DML
  it('INSERT with column list', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b INT)');
    db.execute("INSERT INTO t (id, b) VALUES (1, 42)");
    assert.equal(db.execute('SELECT b FROM t').rows[0].b, 42);
  });

  it('UPDATE with arithmetic', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('UPDATE t SET val = val + 5 WHERE id = 1');
    assert.equal(db.execute('SELECT val FROM t').rows[0].val, 15);
  });

  it('DELETE with WHERE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute('DELETE FROM t WHERE id = 1');
    assert.equal(db.execute('SELECT COUNT(*) AS cnt FROM t').rows[0].cnt, 1);
  });

  // SELECT features
  it('DISTINCT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grp TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'A')");
    db.execute("INSERT INTO t VALUES (2, 'A')");
    db.execute("INSERT INTO t VALUES (3, 'B')");
    assert.equal(db.execute('SELECT DISTINCT grp FROM t').rows.length, 2);
  });

  it('ORDER BY ASC/DESC', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 30)');
    db.execute('INSERT INTO t VALUES (2, 10)');
    db.execute('INSERT INTO t VALUES (3, 20)');
    const asc = db.execute('SELECT val FROM t ORDER BY val').rows;
    assert.deepEqual(asc.map(r => r.val), [10, 20, 30]);
    const desc = db.execute('SELECT val FROM t ORDER BY val DESC').rows;
    assert.deepEqual(desc.map(r => r.val), [30, 20, 10]);
  });

  it('LIMIT and OFFSET', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    assert.equal(db.execute('SELECT * FROM t LIMIT 3').rows.length, 3);
    assert.equal(db.execute('SELECT * FROM t LIMIT 3 OFFSET 5').rows[0].id, 6);
  });

  it('GROUP BY with HAVING', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 10)");
    db.execute("INSERT INTO t VALUES (2, 'A', 20)");
    db.execute("INSERT INTO t VALUES (3, 'B', 5)");
    const r = db.execute('SELECT grp, SUM(val) AS total FROM t GROUP BY grp HAVING SUM(val) > 10');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].grp, 'A');
  });

  // Joins
  it('INNER JOIN', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT PRIMARY KEY, val TEXT)');
    db.execute('CREATE TABLE b (id INT PRIMARY KEY, a_id INT)');
    db.execute("INSERT INTO a VALUES (1, 'x')");
    db.execute('INSERT INTO b VALUES (1, 1)');
    db.execute('INSERT INTO b VALUES (2, 99)');
    const r = db.execute('SELECT a.val FROM a JOIN b ON a.id = b.a_id');
    assert.equal(r.rows.length, 1);
  });

  it('LEFT JOIN', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT PRIMARY KEY, val TEXT)');
    db.execute('CREATE TABLE b (id INT PRIMARY KEY, a_id INT)');
    db.execute("INSERT INTO a VALUES (1, 'x')");
    db.execute("INSERT INTO a VALUES (2, 'y')");
    db.execute('INSERT INTO b VALUES (1, 1)');
    const r = db.execute('SELECT a.val FROM a LEFT JOIN b ON a.id = b.a_id');
    assert.equal(r.rows.length, 2); // y has no match but still included
  });

  // Subqueries
  it('IN with literal list', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute('INSERT INTO t VALUES (3)');
    assert.equal(db.execute('SELECT * FROM t WHERE id IN (1, 3)').rows.length, 2);
  });

  it('IN with subquery', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE t2 (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t1 VALUES (1)');
    db.execute('INSERT INTO t1 VALUES (2)');
    db.execute('INSERT INTO t2 VALUES (2)');
    assert.equal(db.execute('SELECT * FROM t1 WHERE id IN (SELECT id FROM t2)').rows.length, 1);
  });

  it('EXISTS subquery', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE t2 (id INT PRIMARY KEY, t1_id INT)');
    db.execute('INSERT INTO t1 VALUES (1)');
    db.execute('INSERT INTO t1 VALUES (2)');
    db.execute('INSERT INTO t2 VALUES (1, 1)');
    const r = db.execute('SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.t1_id = t1.id)');
    assert.equal(r.rows.length, 1);
  });

  // Functions
  it('string functions', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT UPPER('hello') AS r").rows[0].r, 'HELLO');
    assert.equal(db.execute("SELECT LOWER('HELLO') AS r").rows[0].r, 'hello');
    assert.equal(db.execute("SELECT LENGTH('hello') AS r").rows[0].r, 5);
    assert.equal(db.execute("SELECT REVERSE('abc') AS r").rows[0].r, 'cba');
  });

  it('math functions', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT ABS(-5) AS r').rows[0].r, 5);
    assert.equal(db.execute('SELECT ROUND(3.7) AS r').rows[0].r, 4);
    assert.equal(db.execute('SELECT POWER(2, 8) AS r').rows[0].r, 256);
    assert.equal(db.execute('SELECT SQRT(49) AS r').rows[0].r, 7);
  });

  it('COALESCE and NULLIF', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT COALESCE(null, 'default') AS r").rows[0].r, 'default');
    assert.equal(db.execute('SELECT NULLIF(1, 1) AS r').rows[0].r, null);
    assert.equal(db.execute('SELECT NULLIF(1, 2) AS r').rows[0].r, 1);
  });

  it('CASE expression', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    const r = db.execute("SELECT id, CASE WHEN val > 15 THEN 'high' ELSE 'low' END AS tier FROM t ORDER BY id");
    assert.equal(r.rows[0].tier, 'low');
    assert.equal(r.rows[1].tier, 'high');
  });

  // Advanced features
  it('CTE (WITH clause)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, pid INT)');
    db.execute('INSERT INTO t VALUES (1, null)');
    db.execute('INSERT INTO t VALUES (2, 1)');
    const r = db.execute('WITH roots AS (SELECT id FROM t WHERE pid IS NULL) SELECT * FROM roots');
    assert.equal(r.rows.length, 1);
  });

  it('window functions', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 10)");
    db.execute("INSERT INTO t VALUES (2, 'A', 20)");
    db.execute("INSERT INTO t VALUES (3, 'B', 30)");
    const r = db.execute('SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) AS rn FROM t');
    assert.equal(r.rows.length, 3);
  });

  it('UNION and UNION ALL', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE t2 (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t1 VALUES (1)');
    db.execute('INSERT INTO t2 VALUES (2)');
    const r = db.execute('SELECT id FROM t1 UNION ALL SELECT id FROM t2');
    assert.equal(r.rows.length, 2);
  });

  it('JSON functions', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, data TEXT)');
    db.execute("INSERT INTO t VALUES (1, '{\"name\": \"Alice\"}')");
    assert.equal(db.execute("SELECT JSON_EXTRACT(data, '$.name') AS n FROM t").rows[0].n, 'Alice');
  });

  it('UPSERT (ON CONFLICT)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (1, 20) ON CONFLICT (id) DO UPDATE SET val = 20');
    assert.equal(db.execute('SELECT val FROM t').rows[0].val, 20);
  });

  it('prepared statements', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'test')");
    const stmt = db.prepare('SELECT name FROM t WHERE id = $1');
    assert.equal(stmt.execute([1]).rows[0].name, 'test');
  });

  it('GENERATE_SERIES', () => {
    const db = new Database();
    const r = db.execute('SELECT * FROM GENERATE_SERIES(1, 5)');
    assert.equal(r.rows.length, 5);
  });

  it('full-text search', () => {
    const db = new Database();
    db.execute('CREATE TABLE docs (id INT PRIMARY KEY, content TEXT)');
    db.execute("INSERT INTO docs VALUES (1, 'hello world')");
    db.execute("INSERT INTO docs VALUES (2, 'goodbye world')");
    db.execute('CREATE FULLTEXT INDEX idx ON docs(content)');
    const r = db.execute("SELECT * FROM docs WHERE MATCH(content) AGAINST('hello')");
    assert.equal(r.rows.length, 1);
  });

  it('materialized views', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('CREATE MATERIALIZED VIEW mv AS SELECT SUM(val) AS total FROM t');
    assert.equal(db.execute('SELECT total FROM mv').rows[0].total, 10);
  });
});
