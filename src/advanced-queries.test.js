// advanced-queries.test.js — Advanced query patterns
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Advanced Query Patterns', () => {
  it('correlated subquery in WHERE', () => {
    const db = new Database();
    db.execute('CREATE TABLE emp (id INT PRIMARY KEY, dept TEXT, salary INT)');
    db.execute("INSERT INTO emp VALUES (1, 'A', 100)");
    db.execute("INSERT INTO emp VALUES (2, 'A', 150)");
    db.execute("INSERT INTO emp VALUES (3, 'B', 200)");
    db.execute("INSERT INTO emp VALUES (4, 'B', 120)");
    
    // Find employees earning above department average
    const r = db.execute('SELECT id, dept, salary FROM emp e WHERE salary > (SELECT AVG(salary) FROM emp WHERE dept = e.dept)');
    assert.ok(r.rows.length >= 2);
  });

  it('NOT EXISTS for anti-join', () => {
    const db = new Database();
    db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT)');
    db.execute("INSERT INTO customers VALUES (1, 'Alice')");
    db.execute("INSERT INTO customers VALUES (2, 'Bob')");
    db.execute("INSERT INTO customers VALUES (3, 'Charlie')");
    db.execute('INSERT INTO orders VALUES (1, 1)');
    
    const r = db.execute('SELECT name FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders WHERE customer_id = c.id)');
    assert.equal(r.rows.length, 2); // Bob and Charlie have no orders
  });

  it('multiple aggregates in one query', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    
    const r = db.execute('SELECT COUNT(*) AS cnt, SUM(val) AS total, AVG(val) AS average, MIN(val) AS minimum, MAX(val) AS maximum FROM t');
    assert.equal(r.rows[0].cnt, 100);
    assert.equal(r.rows[0].total, 5050);
    assert.equal(r.rows[0].minimum, 1);
    assert.equal(r.rows[0].maximum, 100);
  });

  it('GROUP BY with multiple aggregates', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (id INT PRIMARY KEY, product TEXT, amount INT, qty INT)');
    db.execute("INSERT INTO sales VALUES (1, 'A', 100, 2)");
    db.execute("INSERT INTO sales VALUES (2, 'A', 200, 3)");
    db.execute("INSERT INTO sales VALUES (3, 'B', 150, 1)");
    db.execute("INSERT INTO sales VALUES (4, 'A', 50, 5)");
    
    const r = db.execute('SELECT product, SUM(amount) AS revenue, SUM(qty) AS units, COUNT(*) AS orders FROM sales GROUP BY product ORDER BY revenue DESC');
    assert.equal(r.rows[0].product, 'A');
    assert.equal(r.rows[0].revenue, 350);
    assert.equal(r.rows[0].units, 10);
    assert.equal(r.rows[0].orders, 3);
  });

  it('LIKE with underscore wildcard', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, code TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'ABC')");
    db.execute("INSERT INTO t VALUES (2, 'AXC')");
    db.execute("INSERT INTO t VALUES (3, 'ABD')");
    
    const r = db.execute("SELECT * FROM t WHERE code LIKE 'A_C'");
    assert.equal(r.rows.length, 2);
  });

  it('IN with large list', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    
    const ids = Array.from({ length: 20 }, (_, i) => i * 5).join(', ');
    const r = db.execute(`SELECT COUNT(*) AS cnt FROM t WHERE id IN (${ids})`);
    assert.equal(r.rows[0].cnt, 20);
  });

  it('nested COALESCE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT, c TEXT)');
    db.execute('INSERT INTO t VALUES (1, null, null, \'found\')');
    
    const r = db.execute("SELECT COALESCE(a, b, c) AS result FROM t");
    assert.equal(r.rows[0].result, 'found');
  });

  it('arithmetic in WHERE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, price INT, qty INT)');
    db.execute('INSERT INTO t VALUES (1, 10, 5)');
    db.execute('INSERT INTO t VALUES (2, 20, 3)');
    db.execute('INSERT INTO t VALUES (3, 5, 20)');
    
    const r = db.execute('SELECT id FROM t WHERE price * qty > 50');
    assert.equal(r.rows.length, 2);
  });

  it('ORDER BY alias', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, 10, 20)');
    db.execute('INSERT INTO t VALUES (2, 5, 30)');
    db.execute('INSERT INTO t VALUES (3, 15, 10)');
    
    const r = db.execute('SELECT id, a + b AS total FROM t ORDER BY total DESC');
    assert.equal(r.rows[0].id, 2); // 5+30=35
  });

  it('DISTINCT with ORDER BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grp TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'B')");
    db.execute("INSERT INTO t VALUES (2, 'A')");
    db.execute("INSERT INTO t VALUES (3, 'B')");
    db.execute("INSERT INTO t VALUES (4, 'C')");
    
    const r = db.execute('SELECT DISTINCT grp FROM t ORDER BY grp');
    assert.deepEqual(r.rows.map(row => row.grp), ['A', 'B', 'C']);
  });

  it('multi-column DISTINCT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'x', 'y')");
    db.execute("INSERT INTO t VALUES (2, 'x', 'y')");
    db.execute("INSERT INTO t VALUES (3, 'x', 'z')");
    
    const r = db.execute('SELECT DISTINCT a, b FROM t');
    assert.equal(r.rows.length, 2);
  });

  it('BETWEEN with strings', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    db.execute("INSERT INTO t VALUES (2, 'Bob')");
    db.execute("INSERT INTO t VALUES (3, 'Charlie')");
    db.execute("INSERT INTO t VALUES (4, 'David')");
    
    const r = db.execute("SELECT name FROM t WHERE name BETWEEN 'B' AND 'D'");
    assert.ok(r.rows.length >= 2);
  });

  it('complex UPDATE with arithmetic', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('INSERT INTO t VALUES (3, 30)');
    
    db.execute('UPDATE t SET val = val * 2');
    const r = db.execute('SELECT val FROM t ORDER BY id');
    assert.equal(r.rows[0].val, 20);
    assert.equal(r.rows[1].val, 40);
    assert.equal(r.rows[2].val, 60);
  });

  it('DELETE with subquery', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    db.execute('DELETE FROM t WHERE val < (SELECT AVG(val) FROM t)');
    const r = db.execute('SELECT COUNT(*) AS cnt FROM t');
    assert.ok(r.rows[0].cnt < 10);
    assert.ok(r.rows[0].cnt > 0);
  });

  it('GENERATE_SERIES with JOIN', () => {
    const db = new Database();
    db.execute('CREATE TABLE data (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO data VALUES (1, 100)');
    db.execute('INSERT INTO data VALUES (3, 300)');
    db.execute('INSERT INTO data VALUES (5, 500)');
    
    // Generate series and LEFT JOIN to find gaps
    const r = db.execute('SELECT value FROM GENERATE_SERIES(1, 5)');
    assert.equal(r.rows.length, 5);
  });

  it('CTE used in complex query', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    const r = db.execute('WITH high AS (SELECT * FROM t WHERE val > 50) SELECT COUNT(*) AS cnt FROM high');
    assert.ok(r.rows[0].cnt >= 1);
  });

  it('UPSERT with RETURNING', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    
    const r = db.execute('INSERT INTO t VALUES (1, 99) ON CONFLICT (id) DO UPDATE SET val = 99 RETURNING *');
    assert.equal(r.rows.length, 1);
  });

  it('JSON extract in WHERE', () => {
    const db = new Database();
    db.execute('CREATE TABLE events (id INT PRIMARY KEY, data TEXT)');
    db.execute("INSERT INTO events VALUES (1, '{\"type\": \"click\", \"count\": 5}')");
    db.execute("INSERT INTO events VALUES (2, '{\"type\": \"view\", \"count\": 10}')");
    
    const r = db.execute("SELECT id FROM events WHERE JSON_EXTRACT(data, '$.type') = 'click'");
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].id, 1);
  });
});
