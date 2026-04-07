// push-1050.test.js — Beyond 1000!
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Beyond 1000', () => {
  let db;
  beforeEach(() => { db = new Database(); });

  it('nested WHERE with multiple conditions', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b TEXT, c INT)');
    db.execute("INSERT INTO t VALUES (1, 10, 'x', 100)");
    db.execute("INSERT INTO t VALUES (2, 20, 'y', 200)");
    db.execute("INSERT INTO t VALUES (3, 10, 'x', 300)");
    const r = db.execute("SELECT * FROM t WHERE a = 10 AND b = 'x' AND c > 150");
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].id, 3);
  });

  it('GROUP BY with multiple columns', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT, val INT)');
    db.execute("INSERT INTO t VALUES (1, 'X', 'A', 10)");
    db.execute("INSERT INTO t VALUES (2, 'X', 'A', 20)");
    db.execute("INSERT INTO t VALUES (3, 'X', 'B', 30)");
    db.execute("INSERT INTO t VALUES (4, 'Y', 'A', 40)");
    const r = db.execute('SELECT a, b, SUM(val) AS s FROM t GROUP BY a, b ORDER BY s');
    assert.equal(r.rows.length, 3);
  });

  it('SELECT with arithmetic alias', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, price INT, qty INT)');
    db.execute('INSERT INTO t VALUES (1, 10, 5)');
    db.execute('INSERT INTO t VALUES (2, 20, 3)');
    const r = db.execute('SELECT id, price + qty AS combined FROM t ORDER BY id');
    assert.equal(r.rows[0].combined, 15);
    assert.equal(r.rows[1].combined, 23);
  });

  it('DELETE then INSERT maintains separate IDs', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    db.execute("INSERT INTO t VALUES (2, 'b')");
    db.execute('DELETE FROM t WHERE id = 1');
    db.execute("INSERT INTO t VALUES (3, 'c')");
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].id, 2);
    assert.equal(r.rows[1].id, 3);
  });

  it('UPDATE with arithmetic in SET', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 100)');
    db.execute('UPDATE t SET val = val / 2 WHERE id = 1');
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 50);
  });

  it('multiple DISTINCT queries', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, sub TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 'X')");
    db.execute("INSERT INTO t VALUES (2, 'A', 'Y')");
    db.execute("INSERT INTO t VALUES (3, 'B', 'X')");
    assert.equal(db.execute('SELECT DISTINCT cat FROM t').rows.length, 2);
    assert.equal(db.execute('SELECT DISTINCT sub FROM t').rows.length, 2);
  });

  it('ORDER BY with NULL sorting', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 30)');
    db.execute('INSERT INTO t VALUES (2, NULL)');
    db.execute('INSERT INTO t VALUES (3, 10)');
    const r = db.execute('SELECT * FROM t ORDER BY val');
    assert.equal(r.rows.length, 3);
  });

  it('complex CTE query', () => {
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, product TEXT, amount INT)');
    for (let i = 1; i <= 20; i++) {
      db.execute(`INSERT INTO orders VALUES (${i}, 'P${i % 3}', ${i * 10})`);
    }
    const r = db.execute(`
      WITH product_totals AS (
        SELECT product, SUM(amount) AS total, COUNT(*) AS cnt
        FROM orders GROUP BY product
      )
      SELECT * FROM product_totals ORDER BY total DESC
    `);
    assert.equal(r.rows.length, 3);
  });

  it('window RANK on ties', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 50)');
    db.execute('INSERT INTO t VALUES (2, 50)');
    db.execute('INSERT INTO t VALUES (3, 100)');
    db.execute('INSERT INTO t VALUES (4, 25)');
    const r = db.execute('SELECT val, RANK() OVER (ORDER BY val DESC) AS rnk FROM t ORDER BY val DESC');
    assert.equal(r.rows[0].rnk, 1); // 100
    assert.equal(r.rows[1].rnk, 2); // 50
    assert.equal(r.rows[2].rnk, 2); // 50 tie
    assert.equal(r.rows[3].rnk, 4); // 25
  });

  it('large JOIN', () => {
    db.execute('CREATE TABLE a (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE b (id INT PRIMARY KEY, a_id INT, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO a VALUES (${i}, 'item${i}')`);
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO b VALUES (${i}, ${(i % 10) + 1}, ${i})`);
    const r = db.execute('SELECT a.name, COUNT(*) AS cnt FROM a JOIN b ON a.id = b.a_id GROUP BY a.name ORDER BY cnt DESC');
    assert.equal(r.rows.length, 10);
  });

  it('multiple conditions with OR', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, status TEXT, priority INT)');
    db.execute("INSERT INTO t VALUES (1, 'open', 1)");
    db.execute("INSERT INTO t VALUES (2, 'closed', 1)");
    db.execute("INSERT INTO t VALUES (3, 'open', 3)");
    db.execute("INSERT INTO t VALUES (4, 'closed', 3)");
    const r = db.execute("SELECT * FROM t WHERE status = 'open' OR priority = 1");
    assert.equal(r.rows.length, 3);
  });

  it('IN subquery filter', () => {
    db.execute('CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE child (id INT PRIMARY KEY, parent_id INT)');
    db.execute("INSERT INTO parent VALUES (1, 'A')");
    db.execute("INSERT INTO parent VALUES (2, 'B')");
    db.execute('INSERT INTO child VALUES (1, 1)');
    const r = db.execute('SELECT name FROM parent WHERE id IN (SELECT parent_id FROM child)');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'A');
  });

  it('NOT IN subquery filter', () => {
    db.execute('CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE child (id INT PRIMARY KEY, parent_id INT)');
    db.execute("INSERT INTO parent VALUES (1, 'A')");
    db.execute("INSERT INTO parent VALUES (2, 'B')");
    db.execute('INSERT INTO child VALUES (1, 1)');
    const r = db.execute('SELECT name FROM parent WHERE id NOT IN (SELECT parent_id FROM child)');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'B');
  });

  it('subquery AVG in WHERE', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    const r = db.execute('SELECT * FROM t WHERE val >= (SELECT AVG(val) FROM t) ORDER BY val');
    assert.equal(r.rows.length, 5); // 60, 70, 80, 90, 100
  });

  it('multiple table schemas', () => {
    for (let i = 1; i <= 5; i++) {
      db.execute(`CREATE TABLE t${i} (id INT PRIMARY KEY, val INT)`);
      db.execute(`INSERT INTO t${i} VALUES (1, ${i * 10})`);
    }
    assert.equal(db.execute('SELECT val FROM t1').rows[0].val, 10);
    assert.equal(db.execute('SELECT val FROM t5').rows[0].val, 50);
  });

  it('10-column table', () => {
    db.execute('CREATE TABLE wide (id INT PRIMARY KEY, c1 INT, c2 INT, c3 INT, c4 INT, c5 INT, c6 INT, c7 INT, c8 INT, c9 INT)');
    db.execute('INSERT INTO wide VALUES (1, 1, 2, 3, 4, 5, 6, 7, 8, 9)');
    const r = db.execute('SELECT c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 AS total FROM wide');
    assert.equal(r.rows[0].total, 45);
  });

  it('BETWEEN with ORDER BY', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 20; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    const r = db.execute('SELECT val FROM t WHERE val BETWEEN 5 AND 15 ORDER BY val DESC');
    assert.equal(r.rows.length, 11);
    assert.equal(r.rows[0].val, 15);
  });

  it('CASE in SELECT with NULL', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    db.execute('INSERT INTO t VALUES (2, 0)');
    db.execute('INSERT INTO t VALUES (3, 42)');
    const r = db.execute("SELECT CASE WHEN val IS NULL THEN 'null' WHEN val = 0 THEN 'zero' ELSE 'value' END AS label FROM t ORDER BY id");
    assert.equal(r.rows[0].label, 'null');
    assert.equal(r.rows[1].label, 'zero');
    assert.equal(r.rows[2].label, 'value');
  });

  it('chain: INSERT → UPDATE → SELECT → DELETE → SELECT', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('UPDATE t SET val = 20 WHERE id = 1');
    assert.equal(db.execute('SELECT val FROM t').rows[0].val, 20);
    db.execute('DELETE FROM t WHERE id = 1');
    assert.equal(db.execute('SELECT COUNT(*) AS c FROM t').rows[0].c, 0);
  });

  it('window SUM over partition', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, dept TEXT, salary INT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 100)");
    db.execute("INSERT INTO t VALUES (2, 'A', 200)");
    db.execute("INSERT INTO t VALUES (3, 'B', 300)");
    const r = db.execute('SELECT dept, salary, SUM(salary) OVER (PARTITION BY dept) AS dept_total FROM t ORDER BY dept, salary');
    assert.equal(r.rows[0].dept_total, 300); // A total
    assert.equal(r.rows[2].dept_total, 300); // B total
  });

  it('3000-row stress', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 3000; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i % 100})`);
    assert.equal(db.execute('SELECT COUNT(*) AS c FROM t').rows[0].c, 3000);
    assert.equal(db.execute('SELECT COUNT(DISTINCT val) AS c FROM t').rows[0].c, 100);
    assert.equal(db.execute('SELECT SUM(val) AS s FROM t WHERE val < 10').rows[0].s, 30 * 45); // 30 each of 0..9
  });

  it('mixed aggregations', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grp TEXT, val INT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, '${i % 5 === 0 ? "A" : "B"}', ${i})`);
    const r = db.execute('SELECT grp, COUNT(*) AS cnt, SUM(val) AS s, AVG(val) AS a, MIN(val) AS mn, MAX(val) AS mx FROM t GROUP BY grp ORDER BY grp');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].cnt, 20); // A: multiples of 5
    assert.equal(r.rows[1].cnt, 80); // B: rest
  });

  it('ROW_NUMBER over large dataset', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO t VALUES (${i}, ${51 - i})`);
    const r = db.execute('SELECT val, ROW_NUMBER() OVER (ORDER BY val) AS rn FROM t ORDER BY val LIMIT 5');
    assert.equal(r.rows.length, 5);
    assert.equal(r.rows[0].val, 1);
  });

  it('CTE + window function combo', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, dept TEXT, salary INT)');
    db.execute("INSERT INTO t VALUES (1, 'Eng', 100)");
    db.execute("INSERT INTO t VALUES (2, 'Eng', 200)");
    db.execute("INSERT INTO t VALUES (3, 'Sales', 150)");
    const r = db.execute(`
      WITH ranked AS (
        SELECT dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
        FROM t
      )
      SELECT dept, salary FROM ranked WHERE rn = 1
    `);
    assert.equal(r.rows.length, 2);
  });

  it('LEFT JOIN with aggregation', () => {
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE posts (id INT PRIMARY KEY, user_id INT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    db.execute("INSERT INTO users VALUES (2, 'Bob')");
    db.execute('INSERT INTO posts VALUES (1, 1)');
    db.execute('INSERT INTO posts VALUES (2, 1)');
    db.execute('INSERT INTO posts VALUES (3, 1)');
    const r = db.execute('SELECT u.name, COUNT(p.id) AS post_count FROM users u LEFT JOIN posts p ON u.id = p.user_id GROUP BY u.name ORDER BY post_count DESC');
    assert.equal(r.rows.length, 2);
  });

  it('INSERT with column subset', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, val INT)');
    db.execute("INSERT INTO t (id, name) VALUES (1, 'test')");
    const r = db.execute('SELECT * FROM t');
    assert.equal(r.rows[0].name, 'test');
    assert.ok(r.rows[0].val === null || r.rows[0].val === undefined);
  });

  it('subquery MIN in WHERE', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, dept TEXT, salary INT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 100)");
    db.execute("INSERT INTO t VALUES (2, 'A', 200)");
    db.execute("INSERT INTO t VALUES (3, 'A', 300)");
    const r = db.execute('SELECT * FROM t WHERE salary > (SELECT MIN(salary) FROM t)');
    assert.equal(r.rows.length, 2);
  });

  it('DENSE_RANK window', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 100)');
    db.execute('INSERT INTO t VALUES (2, 100)');
    db.execute('INSERT INTO t VALUES (3, 90)');
    db.execute('INSERT INTO t VALUES (4, 80)');
    const r = db.execute('SELECT val, DENSE_RANK() OVER (ORDER BY val DESC) AS dr FROM t ORDER BY val DESC');
    assert.equal(r.rows[0].dr, 1);
    assert.equal(r.rows[1].dr, 1);
    assert.equal(r.rows[2].dr, 2);
  });

  it('🎯 1050th test — final analytics', () => {
    db.execute('CREATE TABLE metrics (id INT PRIMARY KEY, day INT, category TEXT, value INT)');
    for (let d = 1; d <= 7; d++) {
      for (const cat of ['A', 'B', 'C']) {
        db.execute(`INSERT INTO metrics VALUES (${(d-1)*3 + ['A','B','C'].indexOf(cat) + 1}, ${d}, '${cat}', ${d * 10 + ['A','B','C'].indexOf(cat)})`);
      }
    }
    
    // Total per category
    const perCat = db.execute('SELECT category, SUM(value) AS total FROM metrics GROUP BY category ORDER BY total DESC');
    assert.equal(perCat.rows.length, 3);
    
    // Ranked by day
    const ranked = db.execute(`
      WITH daily AS (SELECT day, SUM(value) AS total FROM metrics GROUP BY day)
      SELECT day, total, ROW_NUMBER() OVER (ORDER BY total DESC) AS rn FROM daily
    `);
    assert.equal(ranked.rows.length, 7);
  });
});
