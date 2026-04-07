// edge-cases.test.js — Edge case and regression tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Edge Cases', () => {
  it('empty table operations', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    assert.equal(db.execute('SELECT * FROM t').rows.length, 0);
    assert.equal(db.execute('SELECT COUNT(*) AS cnt FROM t').rows[0].cnt, 0);
    db.execute('DELETE FROM t');
    db.execute('UPDATE t SET id = 1');
  });

  it('single row operations', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 42)');
    assert.equal(db.execute('SELECT MIN(val) AS m FROM t').rows[0].m, 42);
    assert.equal(db.execute('SELECT MAX(val) AS m FROM t').rows[0].m, 42);
    assert.equal(db.execute('SELECT AVG(val) AS a FROM t').rows[0].a, 42);
  });

  it('NULL in aggregates', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, null)');
    db.execute('INSERT INTO t VALUES (2, 10)');
    db.execute('INSERT INTO t VALUES (3, null)');
    // COUNT(*) counts all rows, COUNT(val) counts non-null
    assert.equal(db.execute('SELECT COUNT(*) AS all_count FROM t').rows[0].all_count, 3);
  });

  it('large string values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, data TEXT)');
    const bigStr = 'x'.repeat(500);
    db.execute(`INSERT INTO t VALUES (1, '${bigStr}')`);
    assert.equal(db.execute('SELECT LENGTH(data) AS len FROM t').rows[0].len, 500);
  });

  it('negative numbers', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, -100)');
    db.execute('INSERT INTO t VALUES (2, -50)');
    assert.equal(db.execute('SELECT SUM(val) AS s FROM t').rows[0].s, -150);
    assert.equal(db.execute('SELECT ABS(val) AS a FROM t WHERE id = 1').rows[0].a, 100);
  });

  it('zero values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 0)');
    assert.equal(db.execute('SELECT val FROM t WHERE val = 0').rows.length, 1);
    assert.equal(db.execute('SELECT val FROM t WHERE val > 0').rows.length, 0);
  });

  it('special characters in strings', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hello-world_123')");
    assert.equal(db.execute('SELECT name FROM t').rows[0].name, 'hello-world_123');
  });

  it('multiple concurrent tables', () => {
    const db = new Database();
    for (let i = 0; i < 10; i++) {
      db.execute(`CREATE TABLE t${i} (id INT PRIMARY KEY)`);
      db.execute(`INSERT INTO t${i} VALUES (${i})`);
    }
    assert.equal(db.tables.size, 10);
    assert.equal(db.execute('SELECT id FROM t5').rows[0].id, 5);
  });

  it('table with many columns', () => {
    const db = new Database();
    const cols = Array.from({ length: 20 }, (_, i) => `c${i} INT`);
    db.execute(`CREATE TABLE wide (id INT PRIMARY KEY, ${cols.join(', ')})`);
    const vals = Array.from({ length: 21 }, (_, i) => i);
    db.execute(`INSERT INTO wide VALUES (${vals.join(', ')})`);
    assert.equal(db.execute('SELECT c10 FROM wide').rows[0].c10, 11);
  });

  it('chained operations', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('UPDATE t SET val = val * 2 WHERE id = 1');
    db.execute('UPDATE t SET val = val + 5 WHERE id = 1');
    assert.equal(db.execute('SELECT val FROM t').rows[0].val, 25);
  });
});

describe('SQL Feature Combinations', () => {
  it('CTE + JOIN + GROUP BY + HAVING + ORDER BY + LIMIT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT PRIMARY KEY, grp TEXT)');
    db.execute('CREATE TABLE t2 (id INT PRIMARY KEY, t1_id INT, val INT)');
    for (let i = 0; i < 20; i++) db.execute(`INSERT INTO t1 VALUES (${i}, 'g${i % 3}')`);
    for (let i = 0; i < 50; i++) db.execute(`INSERT INTO t2 VALUES (${i}, ${i % 20}, ${i * 10})`);
    
    const r = db.execute(`WITH grouped AS (SELECT t1.grp, SUM(t2.val) AS total FROM t1 JOIN t2 ON t1.id = t2.t1_id GROUP BY t1.grp HAVING SUM(t2.val) > 1000) SELECT * FROM grouped ORDER BY total DESC LIMIT 2`);
    assert.ok(r.rows.length <= 2);
    if (r.rows.length === 2) {
      assert.ok(r.rows[0].total >= r.rows[1].total);
    }
  });

  it('subquery in WHERE + aggregation', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (id INT PRIMARY KEY, amount INT)');
    for (let i = 0; i < 20; i++) db.execute(`INSERT INTO sales VALUES (${i}, ${(i + 1) * 50})`);
    
    const r = db.execute('SELECT COUNT(*) AS above_avg FROM sales WHERE amount > (SELECT AVG(amount) FROM sales)');
    assert.ok(r.rows[0].above_avg > 0);
    assert.ok(r.rows[0].above_avg < 20);
  });

  it('UNION + ORDER BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT PRIMARY KEY, val INT)');
    db.execute('CREATE TABLE t2 (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t1 VALUES (1, 10)');
    db.execute('INSERT INTO t2 VALUES (2, 20)');
    
    const r = db.execute('SELECT id, val FROM t1 UNION ALL SELECT id, val FROM t2');
    assert.equal(r.rows.length, 2);
  });

  it('window function + JOIN', () => {
    const db = new Database();
    db.execute('CREATE TABLE dept (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE emp (id INT PRIMARY KEY, dept_id INT, salary INT)');
    db.execute("INSERT INTO dept VALUES (1, 'Engineering')");
    db.execute("INSERT INTO dept VALUES (2, 'Sales')");
    db.execute('INSERT INTO emp VALUES (1, 1, 100)');
    db.execute('INSERT INTO emp VALUES (2, 1, 120)');
    db.execute('INSERT INTO emp VALUES (3, 2, 90)');
    
    const r = db.execute('SELECT e.id, d.name, e.salary, ROW_NUMBER() OVER (PARTITION BY d.name ORDER BY e.salary DESC) AS rank FROM emp e JOIN dept d ON e.dept_id = d.id');
    assert.equal(r.rows.length, 3);
  });

  it('prepared statement with JOIN', () => {
    const db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, total INT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    db.execute('INSERT INTO orders VALUES (1, 1, 100)');
    
    const stmt = db.prepare('SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = $1');
    const r = stmt.execute([1]);
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'Alice');
  });
});
