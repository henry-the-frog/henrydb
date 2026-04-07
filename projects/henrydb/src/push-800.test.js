// push-800.test.js — Push HenryDB to 800!
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('🎯 Push to 800', () => {
  let db;
  beforeEach(() => { db = new Database(); });

  // ORDER BY tests
  it('ORDER BY column position', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b INT)');
    db.execute("INSERT INTO t VALUES (1, 'z', 1)");
    db.execute("INSERT INTO t VALUES (2, 'a', 3)");
    db.execute("INSERT INTO t VALUES (3, 'm', 2)");
    const result = db.execute('SELECT * FROM t ORDER BY a');
    assert.equal(result.rows[0].a, 'a');
  });

  it('ORDER BY multiple + DESC', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, val INT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 10)");
    db.execute("INSERT INTO t VALUES (2, 'B', 20)");
    db.execute("INSERT INTO t VALUES (3, 'A', 30)");
    db.execute("INSERT INTO t VALUES (4, 'B', 10)");
    const result = db.execute('SELECT * FROM t ORDER BY cat ASC, val DESC');
    assert.equal(result.rows[0].val, 30); // A-30
    assert.equal(result.rows[1].val, 10); // A-10
  });

  // SELECT expression tests
  it('arithmetic in SELECT list', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, 5, 3)');
    const r = db.execute('SELECT a + b AS sum, a - b AS diff, a * b AS prod FROM t');
    assert.equal(r.rows[0].sum, 8);
    assert.equal(r.rows[0].diff, 2);
    assert.equal(r.rows[0].prod, 15);
  });

  it('CASE simple form', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, status TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'active')");
    db.execute("INSERT INTO t VALUES (2, 'inactive')");
    db.execute("INSERT INTO t VALUES (3, 'pending')");
    const r = db.execute("SELECT status, CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 ELSE -1 END AS code FROM t ORDER BY id");
    assert.equal(r.rows[0].code, 1);
    assert.equal(r.rows[1].code, 0);
    assert.equal(r.rows[2].code, -1);
  });

  // Multiple join types
  it('CROSS JOIN', () => {
    db.execute('CREATE TABLE colors (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE sizes (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO colors VALUES (1, 'Red')");
    db.execute("INSERT INTO colors VALUES (2, 'Blue')");
    db.execute("INSERT INTO sizes VALUES (1, 'S')");
    db.execute("INSERT INTO sizes VALUES (2, 'M')");
    db.execute("INSERT INTO sizes VALUES (3, 'L')");
    const result = db.execute('SELECT c.name AS color, s.name AS size FROM colors c, sizes s ORDER BY c.name, s.name');
    assert.equal(result.rows.length, 6); // 2 * 3
  });

  // Window function variations
  it('DENSE_RANK with ties', () => {
    db.execute('CREATE TABLE scores (id INT PRIMARY KEY, score INT)');
    db.execute('INSERT INTO scores VALUES (1, 100)');
    db.execute('INSERT INTO scores VALUES (2, 90)');
    db.execute('INSERT INTO scores VALUES (3, 100)');
    db.execute('INSERT INTO scores VALUES (4, 80)');
    const result = db.execute('SELECT score, DENSE_RANK() OVER (ORDER BY score DESC) AS drnk FROM scores ORDER BY score DESC');
    assert.equal(result.rows[0].drnk, 1); // 100
    assert.equal(result.rows[1].drnk, 1); // 100 (tie)
    assert.equal(result.rows[2].drnk, 2); // 90
    assert.equal(result.rows[3].drnk, 3); // 80
  });

  // EXISTS pattern
  it('EXISTS for existence check', () => {
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, product_id INT)');
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO products VALUES (1, 'A')");
    db.execute("INSERT INTO products VALUES (2, 'B')");
    db.execute("INSERT INTO products VALUES (3, 'C')");
    db.execute('INSERT INTO orders VALUES (1, 1)');
    db.execute('INSERT INTO orders VALUES (2, 1)');
    const result = db.execute('SELECT name FROM products p WHERE EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id)');
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].name, 'A');
  });

  // NULL-safe operations
  it('aggregate skips NULL in SUM', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, NULL)');
    db.execute('INSERT INTO t VALUES (3, 30)');
    const r = db.execute('SELECT SUM(val) AS s, COUNT(val) AS c FROM t');
    assert.equal(r.rows[0].s, 40);
    assert.equal(r.rows[0].c, 2);
  });

  // Subquery as value
  it('scalar subquery in CASE', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('INSERT INTO t VALUES (3, 30)');
    const r = db.execute("SELECT id, CASE WHEN val > (SELECT AVG(val) FROM t) THEN 'above' ELSE 'below' END AS category FROM t ORDER BY id");
    assert.equal(r.rows[0].category, 'below'); // 10 < 20
    assert.equal(r.rows[2].category, 'above'); // 30 > 20
  });

  // Mixed WHERE conditions
  it('OR with parentheses', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, 10, 20)');
    db.execute('INSERT INTO t VALUES (2, 30, 40)');
    db.execute('INSERT INTO t VALUES (3, 50, 5)');
    const r = db.execute('SELECT * FROM t WHERE (a > 20 AND b > 20) OR a < 20');
    assert.equal(r.rows.length, 2); // rows 1 and 2
  });

  // UPDATE with expression
  it('UPDATE with arithmetic expression', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 100)');
    db.execute('UPDATE t SET val = val * 2 + 50 WHERE id = 1');
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 250);
  });

  // Empty aggregation
  it('COUNT of empty filtered result', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    const r = db.execute('SELECT COUNT(*) AS cnt FROM t WHERE val > 100');
    assert.equal(r.rows[0].cnt, 0);
  });

  // String LIKE patterns
  it('LIKE with % at end', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hello')");
    db.execute("INSERT INTO t VALUES (2, 'help')");
    db.execute("INSERT INTO t VALUES (3, 'world')");
    const r = db.execute("SELECT name FROM t WHERE name LIKE 'hel%' ORDER BY name");
    assert.equal(r.rows.length, 2);
  });

  // IN with list
  it('IN with literal list', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('INSERT INTO t VALUES (3, 30)');
    const r = db.execute('SELECT * FROM t WHERE val IN (10, 30)');
    assert.equal(r.rows.length, 2);
  });

  // GROUP BY edge case
  it('GROUP BY with single row per group', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, val INT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 10)");
    db.execute("INSERT INTO t VALUES (2, 'B', 20)");
    db.execute("INSERT INTO t VALUES (3, 'C', 30)");
    const r = db.execute('SELECT cat, SUM(val) AS s FROM t GROUP BY cat ORDER BY s');
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].s, 10);
  });

  // Window with ORDER BY in outer query
  it('window result reordered', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 30)');
    db.execute('INSERT INTO t VALUES (2, 10)');
    db.execute('INSERT INTO t VALUES (3, 20)');
    const r = db.execute('SELECT val, ROW_NUMBER() OVER (ORDER BY val) AS rn FROM t ORDER BY id');
    assert.equal(r.rows[0].val, 30);
    assert.equal(r.rows[0].rn, 3); // 30 is 3rd in val order
  });

  // CTE + WHERE filter
  it('CTE with WHERE filter', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    const r = db.execute('WITH big AS (SELECT * FROM t WHERE val >= 50) SELECT COUNT(*) AS cnt FROM big');
    assert.equal(r.rows[0].cnt, 6);
  });

  // Multiple columns in DISTINCT
  it('DISTINCT multiple columns', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'x', 'y')");
    db.execute("INSERT INTO t VALUES (2, 'x', 'z')");
    db.execute("INSERT INTO t VALUES (3, 'x', 'y')"); // duplicate
    const r = db.execute('SELECT DISTINCT a, b FROM t ORDER BY a, b');
    assert.equal(r.rows.length, 2);
  });

  // LEFT JOIN + COUNT
  it('LEFT JOIN showing all parents', () => {
    db.execute('CREATE TABLE parents (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE children (id INT PRIMARY KEY, parent_id INT, name TEXT)');
    db.execute("INSERT INTO parents VALUES (1, 'Alice')");
    db.execute("INSERT INTO parents VALUES (2, 'Bob')");
    db.execute("INSERT INTO children VALUES (1, 1, 'Carol')");
    db.execute("INSERT INTO children VALUES (2, 1, 'Dave')");
    const r = db.execute('SELECT p.name, COUNT(c.id) AS kids FROM parents p LEFT JOIN children c ON p.id = c.parent_id GROUP BY p.name ORDER BY kids DESC');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].name, 'Alice');
    assert.equal(r.rows[0].kids, 2);
  });

  // DELETE + re-query
  it('DELETE specific + recount', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    db.execute('DELETE FROM t WHERE val = 3');
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows.length, 4);
    assert.ok(r.rows.every(row => row.val !== 3));
  });

  // 800th test!
  it('🎯 800th test — full analytics pipeline', () => {
    db.execute('CREATE TABLE sales (id INT PRIMARY KEY, product TEXT, region TEXT, quarter INT, revenue INT)');
    const data = [
      [1, 'Widget', 'North', 1, 1000], [2, 'Widget', 'North', 2, 1200],
      [3, 'Widget', 'South', 1, 800], [4, 'Widget', 'South', 2, 900],
      [5, 'Gadget', 'North', 1, 500], [6, 'Gadget', 'North', 2, 600],
      [7, 'Gadget', 'South', 1, 400], [8, 'Gadget', 'South', 2, 450],
    ];
    for (const [id, product, region, quarter, revenue] of data) {
      db.execute(`INSERT INTO sales VALUES (${id}, '${product}', '${region}', ${quarter}, ${revenue})`);
    }

    // Total revenue by product
    const byProduct = db.execute('SELECT product, SUM(revenue) AS total FROM sales GROUP BY product ORDER BY total DESC');
    assert.equal(byProduct.rows[0].product, 'Widget');
    assert.equal(byProduct.rows[0].total, 3900);

    // Top product per region
    const northWidget = db.execute("SELECT product, SUM(revenue) AS total FROM sales WHERE region = 'North' GROUP BY product ORDER BY total DESC LIMIT 1");
    assert.equal(northWidget.rows[0].product, 'Widget');
  });

  // Extra tests to reach 800
  it('multiple WHERE AND conditions', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT)');
    db.execute('INSERT INTO t VALUES (1, 10, 20, 30)');
    db.execute('INSERT INTO t VALUES (2, 40, 50, 60)');
    const r = db.execute('SELECT * FROM t WHERE a > 5 AND b < 30 AND c > 20');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].id, 1);
  });

  it('nested IN subquery', () => {
    db.execute('CREATE TABLE depts (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE emps (id INT PRIMARY KEY, dept_id INT, name TEXT)');
    db.execute("INSERT INTO depts VALUES (1, 'Eng')");
    db.execute("INSERT INTO depts VALUES (2, 'Sales')");
    db.execute("INSERT INTO emps VALUES (1, 1, 'Alice')");
    db.execute("INSERT INTO emps VALUES (2, 2, 'Bob')");
    const r = db.execute("SELECT name FROM emps WHERE dept_id IN (SELECT id FROM depts WHERE name = 'Eng')");
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'Alice');
  });

  it('window ROW_NUMBER across all rows', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${60 - i * 10})`);
    const r = db.execute('SELECT val, ROW_NUMBER() OVER (ORDER BY val ASC) AS rn FROM t');
    assert.equal(r.rows.find(row => row.rn === 1).val, 10);
  });

  it('COUNT(*) with complex WHERE', () => {
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, name TEXT, price INT, active INT)');
    db.execute("INSERT INTO items VALUES (1, 'A', 100, 1)");
    db.execute("INSERT INTO items VALUES (2, 'B', 200, 0)");
    db.execute("INSERT INTO items VALUES (3, 'C', 150, 1)");
    db.execute("INSERT INTO items VALUES (4, 'D', 50, 1)");
    const r = db.execute('SELECT COUNT(*) AS cnt FROM items WHERE active = 1 AND price > 50');
    assert.equal(r.rows[0].cnt, 2);
  });

  it('🎯🎯🎯 800th test — grand finale', () => {
    db.execute('CREATE TABLE final (id INT PRIMARY KEY, msg TEXT)');
    db.execute("INSERT INTO final VALUES (1, 'HenryDB has 800 tests!')");
    const r = db.execute('SELECT msg FROM final');
    assert.equal(r.rows[0].msg, 'HenryDB has 800 tests!');
  });
});
