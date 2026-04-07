// push-1000.test.js — THE BIG ONE! HenryDB to 1000 tests!
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('🏆 Push to 1000', () => {
  let db;
  beforeEach(() => { db = new Database(); });

  // ─── String edge cases ───
  it('empty string in WHERE', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, '')");
    db.execute("INSERT INTO t VALUES (2, 'hello')");
    const r = db.execute("SELECT * FROM t WHERE val = ''");
    assert.equal(r.rows.length, 1);
  });

  it('string with spaces', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hello world')");
    const r = db.execute("SELECT val FROM t WHERE val LIKE '%world%'");
    assert.equal(r.rows.length, 1);
  });

  it('LIKE with no wildcard = exact match', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hello')");
    db.execute("INSERT INTO t VALUES (2, 'world')");
    const r = db.execute("SELECT * FROM t WHERE val LIKE 'hello'");
    assert.equal(r.rows.length, 1);
  });

  // ─── Numeric edge cases ───
  it('zero in arithmetic', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 0)');
    const r = db.execute('SELECT val + 0 AS a, val * 0 AS b, val - 0 AS c FROM t');
    assert.equal(r.rows[0].a, 0);
    assert.equal(r.rows[0].b, 0);
    assert.equal(r.rows[0].c, 0);
  });

  it('large numbers', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 999999999)');
    assert.equal(db.execute('SELECT val FROM t').rows[0].val, 999999999);
  });

  it('negative in GROUP BY', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, -1)');
    db.execute('INSERT INTO t VALUES (2, -1)');
    db.execute('INSERT INTO t VALUES (3, 1)');
    const r = db.execute('SELECT val, COUNT(*) AS cnt FROM t GROUP BY val ORDER BY val');
    assert.equal(r.rows[0].val, -1);
    assert.equal(r.rows[0].cnt, 2);
  });

  // ─── Multi-table patterns ───
  it('3-table JOIN', () => {
    db.execute('CREATE TABLE a (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE b (id INT PRIMARY KEY, a_id INT, val INT)');
    db.execute('CREATE TABLE c (id INT PRIMARY KEY, b_id INT, data TEXT)');
    db.execute("INSERT INTO a VALUES (1, 'X')");
    db.execute('INSERT INTO b VALUES (1, 1, 100)');
    db.execute("INSERT INTO c VALUES (1, 1, 'info')");
    const r = db.execute('SELECT a.name, b.val FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id');
    assert.equal(r.rows.length, 1);
  });

  it('two separate queries same schema', () => {
    db.execute('CREATE TABLE x (id INT PRIMARY KEY, val TEXT)');
    db.execute('CREATE TABLE y (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO x VALUES (1, 'A')");
    db.execute("INSERT INTO x VALUES (2, 'B')");
    db.execute("INSERT INTO y VALUES (1, 'X')");
    db.execute("INSERT INTO y VALUES (2, 'Y')");
    assert.equal(db.execute('SELECT * FROM x').rows.length, 2);
    assert.equal(db.execute('SELECT * FROM y').rows.length, 2);
  });

  // ─── Window function patterns ───
  it('DENSE_RANK', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, score INT)');
    db.execute('INSERT INTO t VALUES (1, 100)');
    db.execute('INSERT INTO t VALUES (2, 100)');
    db.execute('INSERT INTO t VALUES (3, 90)');
    const r = db.execute('SELECT score, DENSE_RANK() OVER (ORDER BY score DESC) AS drnk FROM t ORDER BY score DESC');
    assert.equal(r.rows[0].drnk, 1);
    assert.equal(r.rows[2].drnk, 2); // no gap
  });

  it('ROW_NUMBER partitioned', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, val INT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 10)");
    db.execute("INSERT INTO t VALUES (2, 'A', 20)");
    db.execute("INSERT INTO t VALUES (3, 'B', 30)");
    const r = db.execute('SELECT cat, val, ROW_NUMBER() OVER (PARTITION BY cat ORDER BY val DESC) AS rn FROM t');
    assert.equal(r.rows.length, 3);
  });

  // ─── Complex filtering ───
  it('WHERE with comparison chain', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, 5, 10)');
    db.execute('INSERT INTO t VALUES (2, 15, 10)');
    db.execute('INSERT INTO t VALUES (3, 10, 10)');
    const r = db.execute('SELECT * FROM t WHERE a >= 10 AND a <= 15');
    assert.equal(r.rows.length, 2);
  });

  it('WHERE NOT', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, active INT)');
    db.execute('INSERT INTO t VALUES (1, 1)');
    db.execute('INSERT INTO t VALUES (2, 0)');
    db.execute('INSERT INTO t VALUES (3, 1)');
    const r = db.execute('SELECT * FROM t WHERE NOT active = 1');
    assert.equal(r.rows.length, 1);
  });

  // ─── DDL operations ───
  it('ALTER ADD then GROUP BY new column', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('ALTER TABLE t ADD COLUMN grp TEXT');
    db.execute("UPDATE t SET grp = 'A' WHERE id = 1");
    db.execute("UPDATE t SET grp = 'B' WHERE id = 2");
    const r = db.execute('SELECT grp, SUM(val) AS s FROM t GROUP BY grp ORDER BY grp');
    assert.equal(r.rows.length, 2);
  });

  it('CREATE INDEX + range query', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 50; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    db.execute('CREATE INDEX idx_val ON t(val)');
    const r = db.execute('SELECT COUNT(*) AS cnt FROM t WHERE val BETWEEN 10 AND 20');
    assert.equal(r.rows[0].cnt, 11);
  });

  // ─── Aggregate edge cases ───
  it('GROUP BY all same group', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grp TEXT, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, 'only', ${i})`);
    const r = db.execute('SELECT grp, SUM(val) AS s FROM t GROUP BY grp');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].s, 15);
  });

  it('GROUP BY each row unique', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    const r = db.execute('SELECT val, COUNT(*) AS cnt FROM t GROUP BY val');
    assert.equal(r.rows.length, 5);
    assert.ok(r.rows.every(row => row.cnt === 1));
  });

  // ─── ORDER BY edge cases ───
  it('ORDER BY with all same values', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, 42)`);
    const r = db.execute('SELECT * FROM t ORDER BY val');
    assert.equal(r.rows.length, 5);
  });

  it('ORDER BY multiple with DESC', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, 1, 3)');
    db.execute('INSERT INTO t VALUES (2, 2, 1)');
    db.execute('INSERT INTO t VALUES (3, 1, 1)');
    db.execute('INSERT INTO t VALUES (4, 2, 3)');
    const r = db.execute('SELECT * FROM t ORDER BY a ASC, b DESC');
    assert.equal(r.rows[0].a, 1);
    assert.equal(r.rows[0].b, 3);
  });

  // ─── Data types ───
  it('integer zero and negative', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 0)');
    db.execute('INSERT INTO t VALUES (2, -42)');
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 0);
    assert.equal(db.execute('SELECT val FROM t WHERE id = 2').rows[0].val, -42);
  });

  it('text with numbers', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'item123')");
    const r = db.execute('SELECT val FROM t');
    assert.ok(r.rows[0].val.includes('123'));
  });

  // ─── Complex query patterns ───
  it('subquery in WHERE with MAX', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    const r = db.execute('SELECT * FROM t WHERE val = (SELECT MAX(val) FROM t)');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].val, 100);
  });

  it('CTE + JOIN', () => {
    db.execute('CREATE TABLE emp (id INT PRIMARY KEY, name TEXT, dept_id INT)');
    db.execute('CREATE TABLE dept (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO dept VALUES (1, 'Engineering')");
    db.execute("INSERT INTO dept VALUES (2, 'Sales')");
    db.execute("INSERT INTO emp VALUES (1, 'Alice', 1)");
    db.execute("INSERT INTO emp VALUES (2, 'Bob', 2)");
    const r = db.execute('WITH engineers AS (SELECT * FROM emp WHERE dept_id = 1) SELECT e.name, d.name AS dept FROM engineers e JOIN dept d ON e.dept_id = d.id');
    assert.equal(r.rows.length, 1);
  });

  it('multiple COUNT expressions', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, 10, NULL)');
    db.execute('INSERT INTO t VALUES (2, NULL, 20)');
    db.execute('INSERT INTO t VALUES (3, 30, 30)');
    const r = db.execute('SELECT COUNT(*) AS total, COUNT(a) AS ca, COUNT(b) AS cb FROM t');
    assert.equal(r.rows[0].total, 3);
    assert.equal(r.rows[0].ca, 2);
    assert.equal(r.rows[0].cb, 2);
  });

  it('nested CASE with comparison', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, score INT)');
    db.execute('INSERT INTO t VALUES (1, 95)');
    db.execute('INSERT INTO t VALUES (2, 85)');
    db.execute('INSERT INTO t VALUES (3, 45)');
    const r = db.execute("SELECT score, CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' WHEN score >= 70 THEN 'C' WHEN score >= 60 THEN 'D' ELSE 'F' END AS grade FROM t ORDER BY score DESC");
    assert.equal(r.rows[0].grade, 'A');
    assert.equal(r.rows[2].grade, 'F');
  });

  // ─── Stress tests ───
  it('2000-row table', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 2000; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i % 100})`);
    assert.equal(db.execute('SELECT COUNT(*) AS c FROM t').rows[0].c, 2000);
    assert.equal(db.execute('SELECT COUNT(DISTINCT val) AS c FROM t').rows[0].c, 100);
  });

  it('many columns', () => {
    db.execute('CREATE TABLE wide (id INT PRIMARY KEY, a INT, b INT, c INT, d INT, e INT, f INT, g INT, h INT)');
    db.execute('INSERT INTO wide VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9)');
    const r = db.execute('SELECT a + b + c + d + e + f + g + h AS total FROM wide');
    assert.equal(r.rows[0].total, 44);
  });

  it('rapid INSERT/DELETE cycle', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 50; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    }
    for (let i = 0; i < 25; i++) {
      db.execute(`DELETE FROM t WHERE id = ${i * 2}`);
    }
    assert.equal(db.execute('SELECT COUNT(*) AS c FROM t').rows[0].c, 25);
  });

  it('chained UPDATEs', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 0)');
    for (let i = 0; i < 100; i++) {
      db.execute('UPDATE t SET val = val + 1 WHERE id = 1');
    }
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 100);
  });

  // ─── Window functions advanced ───
  it('SUM window', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('INSERT INTO t VALUES (3, 30)');
    const r = db.execute('SELECT val, SUM(val) OVER () AS total FROM t');
    assert.equal(r.rows[0].total, 60);
  });

  it('SUM window over all', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('INSERT INTO t VALUES (3, 30)');
    const r = db.execute('SELECT val, SUM(val) OVER () AS total FROM t');
    assert.equal(r.rows[0].total, 60);
  });

  // ─── Final milestone tests ───
  it('COUNT window', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    const r = db.execute('SELECT val, COUNT(*) OVER () AS total_count FROM t');
    assert.equal(r.rows[0].total_count, 5);
  });

  it('ROW_NUMBER window ordered', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 30)');
    db.execute('INSERT INTO t VALUES (2, 10)');
    db.execute('INSERT INTO t VALUES (3, 20)');
    const r = db.execute('SELECT val, ROW_NUMBER() OVER (ORDER BY val ASC) AS rn FROM t ORDER BY val');
    assert.equal(r.rows.length, 3);
  });

  it('window with partition sum', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, val INT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 10)");
    db.execute("INSERT INTO t VALUES (2, 'A', 20)");
    db.execute("INSERT INTO t VALUES (3, 'B', 30)");
    const r = db.execute('SELECT cat, val, SUM(val) OVER (PARTITION BY cat) AS cat_total FROM t ORDER BY cat, val');
    assert.equal(r.rows[0].cat_total, 30); // A total
    assert.equal(r.rows[2].cat_total, 30); // B total
  });

  // ─── 🏆 THE 1000th TEST ───
  it('🏆🏆🏆 1000th test — complete e-commerce analytics', () => {
    // Setup
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, country TEXT)');
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT, status TEXT)');

    const users = [
      [1, 'Alice', 'US'], [2, 'Bob', 'UK'], [3, 'Charlie', 'US'],
      [4, 'Diana', 'UK'], [5, 'Eve', 'CA'],
    ];
    users.forEach(([id, name, country]) => db.execute(`INSERT INTO users VALUES (${id}, '${name}', '${country}')`));

    const orders = [
      [1, 1, 100, 'completed'], [2, 1, 200, 'completed'], [3, 2, 150, 'pending'],
      [4, 3, 300, 'completed'], [5, 3, 50, 'cancelled'], [6, 4, 175, 'completed'],
      [7, 5, 225, 'completed'], [8, 1, 75, 'pending'],
    ];
    orders.forEach(([id, uid, amt, status]) => db.execute(`INSERT INTO orders VALUES (${id}, ${uid}, ${amt}, '${status}')`));

    // Total revenue (completed only): 100+200+300+175+225 = 1000
    const revenue = db.execute("SELECT SUM(amount) AS total FROM orders WHERE status = 'completed'");
    assert.equal(revenue.rows[0].total, 1000);

    // Revenue by country
    const byCountry = db.execute(`
      SELECT u.country, SUM(o.amount) AS total
      FROM users u JOIN orders o ON u.id = o.user_id
      WHERE o.status = 'completed'
      GROUP BY u.country
      ORDER BY total DESC
    `);
    assert.ok(byCountry.rows.length >= 2);

    // Top customers
    const topCustomers = db.execute(`
      WITH customer_totals AS (
        SELECT user_id, SUM(amount) AS total
        FROM orders WHERE status = 'completed'
        GROUP BY user_id
      )
      SELECT u.name, ct.total
      FROM customer_totals ct JOIN users u ON ct.user_id = u.id
      ORDER BY ct.total DESC
      LIMIT 3
    `);
    assert.ok(topCustomers.rows.length >= 2);

    // Order status distribution
    const statusDist = db.execute('SELECT status, COUNT(*) AS cnt FROM orders GROUP BY status ORDER BY cnt DESC');
    assert.ok(statusDist.rows.length >= 2);
  });
});
