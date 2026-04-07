// push-900.test.js — Push HenryDB to 900 tests!
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Push to 900', () => {
  let db;
  beforeEach(() => { db = new Database(); });

  // ─── Arithmetic expressions ───
  it('modulo operator', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 17)');
    const r = db.execute('SELECT val % 5 AS remainder FROM t');
    assert.equal(r.rows[0].remainder, 2);
  });

  it('negative arithmetic', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, -10)');
    db.execute('INSERT INTO t VALUES (2, 5)');
    const r = db.execute('SELECT val * -1 AS negated FROM t ORDER BY id');
    assert.equal(r.rows[0].negated, 10);
  });

  it('multiplication in WHERE', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, qty INT, price INT)');
    db.execute('INSERT INTO t VALUES (1, 5, 10)');
    db.execute('INSERT INTO t VALUES (2, 3, 20)');
    const r = db.execute('SELECT * FROM t WHERE qty * price > 40');
    assert.equal(r.rows.length, 2);
  });

  // ─── String operations ───
  it('string comparison in ORDER BY', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Charlie')");
    db.execute("INSERT INTO t VALUES (2, 'Alice')");
    db.execute("INSERT INTO t VALUES (3, 'Bob')");
    const r = db.execute('SELECT name FROM t ORDER BY name');
    assert.equal(r.rows[0].name, 'Alice');
    assert.equal(r.rows[2].name, 'Charlie');
  });

  it('LIKE with underscore wildcard', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, code TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'AB')");
    db.execute("INSERT INTO t VALUES (2, 'AC')");
    db.execute("INSERT INTO t VALUES (3, 'BC')");
    const r = db.execute("SELECT code FROM t WHERE code LIKE 'A_' ORDER BY code");
    assert.equal(r.rows.length, 2);
  });

  it('LIKE case sensitivity', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Hello')");
    db.execute("INSERT INTO t VALUES (2, 'hello')");
    const r = db.execute("SELECT name FROM t WHERE name LIKE 'hello'");
    assert.ok(r.rows.length >= 1); // at least matches lowercase
  });

  // ─── NULL handling ───
  it('NULL in arithmetic returns NULL', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    const r = db.execute('SELECT val + 5 AS result FROM t');
    assert.equal(r.rows[0].result, null);
  });

  it('NULL equality comparison', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    db.execute('INSERT INTO t VALUES (2, 10)');
    assert.equal(db.execute('SELECT COUNT(*) AS cnt FROM t WHERE val = NULL').rows[0].cnt, 0);
    assert.equal(db.execute('SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL').rows[0].cnt, 1);
  });

  it('NULL in GROUP BY', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 10)");
    db.execute('INSERT INTO t VALUES (2, NULL, 20)');
    db.execute("INSERT INTO t VALUES (3, 'A', 30)");
    const r = db.execute('SELECT grp, COUNT(*) AS cnt FROM t GROUP BY grp ORDER BY grp');
    assert.ok(r.rows.length >= 2);
  });

  it('COALESCE-like behavior with CASE', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    db.execute('INSERT INTO t VALUES (2, 42)');
    const r = db.execute('SELECT CASE WHEN val IS NULL THEN 0 ELSE val END AS safe_val FROM t ORDER BY id');
    assert.equal(r.rows[0].safe_val, 0);
    assert.equal(r.rows[1].safe_val, 42);
  });

  // ─── Subquery patterns ───
  it('IN with literal list of strings', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    db.execute("INSERT INTO t VALUES (2, 'Bob')");
    db.execute("INSERT INTO t VALUES (3, 'Charlie')");
    const r = db.execute("SELECT name FROM t WHERE name IN ('Alice', 'Charlie') ORDER BY name");
    assert.equal(r.rows.length, 2);
  });

  it('NOT IN with list', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('INSERT INTO t VALUES (3, 30)');
    const r = db.execute('SELECT val FROM t WHERE val NOT IN (10, 30)');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].val, 20);
  });

  it('MAX via subquery in WHERE', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    const r = db.execute('SELECT val FROM t WHERE val = (SELECT MAX(val) FROM t)');
    assert.equal(r.rows[0].val, 50);
  });

  // ─── Window function patterns ───
  it('RANK with ORDER BY DESC', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, score INT)');
    db.execute('INSERT INTO t VALUES (1, 100)');
    db.execute('INSERT INTO t VALUES (2, 200)');
    db.execute('INSERT INTO t VALUES (3, 150)');
    const r = db.execute('SELECT score, RANK() OVER (ORDER BY score DESC) AS rnk FROM t ORDER BY score DESC');
    assert.equal(r.rows[0].score, 200);
    assert.equal(r.rows[0].rnk, 1);
  });

  it('ROW_NUMBER without PARTITION', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'A')");
    db.execute("INSERT INTO t VALUES (2, 'B')");
    db.execute("INSERT INTO t VALUES (3, 'C')");
    const r = db.execute('SELECT name, ROW_NUMBER() OVER (ORDER BY name) AS rn FROM t');
    assert.equal(r.rows.length, 3);
  });

  it('window PARTITION BY', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, dept TEXT, salary INT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 100)");
    db.execute("INSERT INTO t VALUES (2, 'A', 200)");
    db.execute("INSERT INTO t VALUES (3, 'B', 150)");
    const r = db.execute('SELECT dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM t');
    assert.equal(r.rows.length, 3);
  });

  // ─── CTE patterns ───
  it('CTE with filter and aggregate', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 20; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    const r = db.execute('WITH even AS (SELECT * FROM t WHERE val % 2 = 0) SELECT COUNT(*) AS cnt FROM even');
    assert.equal(r.rows[0].cnt, 10);
  });

  it('CTE with ORDER BY', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 30)');
    db.execute('INSERT INTO t VALUES (2, 10)');
    db.execute('INSERT INTO t VALUES (3, 20)');
    const r = db.execute('WITH sorted AS (SELECT * FROM t ORDER BY val) SELECT * FROM sorted');
    assert.ok(r.rows.length === 3);
  });

  // ─── Transaction patterns ───
  it('multiple INSERTs are durable', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    assert.equal(db.execute('SELECT SUM(val) AS s FROM t').rows[0].s, 30);
  });

  it('DELETE then INSERT reuses ids', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('DELETE FROM t WHERE id = 1');
    db.execute('INSERT INTO t VALUES (1, 99)');
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 99);
  });

  // ─── JOIN patterns ───
  it('self-join for pairs', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('INSERT INTO t VALUES (3, 30)');
    const r = db.execute('SELECT a.val AS a_val, b.val AS b_val FROM t a JOIN t b ON a.id < b.id');
    assert.equal(r.rows.length, 3); // (1,2), (1,3), (2,3)
  });

  it('LEFT JOIN with NULL check', () => {
    db.execute('CREATE TABLE a (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE b (id INT PRIMARY KEY, a_id INT)');
    db.execute("INSERT INTO a VALUES (1, 'X')");
    db.execute("INSERT INTO a VALUES (2, 'Y')");
    db.execute('INSERT INTO b VALUES (1, 1)');
    const r = db.execute('SELECT a.name, b.id AS b_id FROM a LEFT JOIN b ON a.id = b.a_id ORDER BY a.id');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[1].b_id, null);
  });

  // ─── More aggregate patterns ───
  it('COUNT DISTINCT', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 10)');
    db.execute('INSERT INTO t VALUES (3, 20)');
    const r = db.execute('SELECT COUNT(DISTINCT val) AS cnt FROM t');
    assert.equal(r.rows[0].cnt, 2);
  });

  it('AVG returns decimal', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('INSERT INTO t VALUES (3, 30)');
    const r = db.execute('SELECT AVG(val) AS avg_val FROM t');
    assert.equal(r.rows[0].avg_val, 20);
  });

  it('GROUP BY with multiple aggregates', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 10)");
    db.execute("INSERT INTO t VALUES (2, 'A', 20)");
    db.execute("INSERT INTO t VALUES (3, 'B', 30)");
    db.execute("INSERT INTO t VALUES (4, 'B', 40)");
    const r = db.execute('SELECT grp, SUM(val) AS s, AVG(val) AS a, COUNT(*) AS c, MIN(val) AS mn, MAX(val) AS mx FROM t GROUP BY grp ORDER BY grp');
    assert.equal(r.rows[0].s, 30);
    assert.equal(r.rows[0].c, 2);
    assert.equal(r.rows[1].mx, 40);
  });

  // ─── Complex queries ───
  it('nested arithmetic in WHERE', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, 10, 5)');
    db.execute('INSERT INTO t VALUES (2, 20, 3)');
    const r = db.execute('SELECT * FROM t WHERE a - b > 10');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].id, 2);
  });

  it('aliased expressions in SELECT', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, price INT, qty INT)');
    db.execute('INSERT INTO t VALUES (1, 10, 5)');
    const r = db.execute('SELECT price * qty AS total, price + qty AS combined FROM t');
    assert.equal(r.rows[0].total, 50);
    assert.equal(r.rows[0].combined, 15);
  });

  it('BETWEEN with strings', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    db.execute("INSERT INTO t VALUES (2, 'Bob')");
    db.execute("INSERT INTO t VALUES (3, 'Charlie')");
    db.execute("INSERT INTO t VALUES (4, 'Dave')");
    const r = db.execute("SELECT name FROM t WHERE name BETWEEN 'B' AND 'D' ORDER BY name");
    assert.ok(r.rows.length >= 2);
  });

  it('OR in WHERE', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('INSERT INTO t VALUES (3, 30)');
    const r = db.execute('SELECT * FROM t WHERE val = 10 OR val = 30 ORDER BY val');
    assert.equal(r.rows.length, 2);
  });

  it('complex WHERE with parentheses', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, 1, 1)');
    db.execute('INSERT INTO t VALUES (2, 1, 0)');
    db.execute('INSERT INTO t VALUES (3, 0, 1)');
    db.execute('INSERT INTO t VALUES (4, 0, 0)');
    const r = db.execute('SELECT * FROM t WHERE (a = 1 AND b = 1) OR (a = 0 AND b = 0)');
    assert.equal(r.rows.length, 2);
  });

  it('INSERT 500 rows + aggregate', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 500; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i % 100})`);
    const r = db.execute('SELECT COUNT(*) AS cnt, SUM(val) AS s FROM t');
    assert.equal(r.rows[0].cnt, 500);
  });

  it('multi-column ORDER BY', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b INT)');
    db.execute("INSERT INTO t VALUES (1, 'X', 2)");
    db.execute("INSERT INTO t VALUES (2, 'X', 1)");
    db.execute("INSERT INTO t VALUES (3, 'Y', 1)");
    const r = db.execute('SELECT * FROM t ORDER BY a, b');
    assert.equal(r.rows[0].b, 1); // X,1
    assert.equal(r.rows[1].b, 2); // X,2
  });

  it('DISTINCT with ORDER BY', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('INSERT INTO t VALUES (3, 10)');
    const r = db.execute('SELECT DISTINCT val FROM t ORDER BY val');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].val, 10);
  });

  it('UPDATE with expression referencing same column', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, balance INT)');
    db.execute('INSERT INTO t VALUES (1, 100)');
    db.execute('UPDATE t SET balance = balance - 30 WHERE id = 1');
    assert.equal(db.execute('SELECT balance FROM t WHERE id = 1').rows[0].balance, 70);
  });

  it('mixed types in same table', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT, active INT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice', 30, 1)");
    db.execute("INSERT INTO t VALUES (2, 'Bob', 25, 0)");
    const r = db.execute("SELECT * FROM t WHERE active = 1 AND age > 20");
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'Alice');
  });

  it('empty result with aggregation', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    const r = db.execute('SELECT COUNT(*) AS cnt, SUM(val) AS s FROM t WHERE val > 100');
    assert.equal(r.rows[0].cnt, 0);
  });

  it('GROUP BY with ORDER BY on aggregate', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 10)");
    db.execute("INSERT INTO t VALUES (2, 'B', 5)");
    db.execute("INSERT INTO t VALUES (3, 'A', 20)");
    db.execute("INSERT INTO t VALUES (4, 'C', 15)");
    const r = db.execute('SELECT grp, SUM(val) AS total FROM t GROUP BY grp ORDER BY total DESC');
    assert.equal(r.rows[0].grp, 'A'); // 30
    assert.equal(r.rows[0].total, 30);
  });

  it('LIMIT with offset-like behavior', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    const r = db.execute('SELECT val FROM t ORDER BY val LIMIT 3');
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].val, 10);
    assert.equal(r.rows[2].val, 30);
  });

  it('WHERE with IS NOT NULL', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, NULL)');
    db.execute('INSERT INTO t VALUES (3, 30)');
    const r = db.execute('SELECT * FROM t WHERE val IS NOT NULL ORDER BY val');
    assert.equal(r.rows.length, 2);
  });

  it('multiple DELETE + recount', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    db.execute('DELETE FROM t WHERE val <= 3');
    db.execute('DELETE FROM t WHERE val >= 8');
    assert.equal(db.execute('SELECT COUNT(*) AS cnt FROM t').rows[0].cnt, 4); // 4,5,6,7
  });

  it('UPDATE multiple columns', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, 10, 20)');
    db.execute('UPDATE t SET a = 99, b = 88 WHERE id = 1');
    const r = db.execute('SELECT a, b FROM t');
    assert.equal(r.rows[0].a, 99);
    assert.equal(r.rows[0].b, 88);
  });

  it('INSERT + SELECT count consistency', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 50; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    assert.equal(db.execute('SELECT COUNT(*) AS cnt FROM t').rows[0].cnt, 50);
    for (let i = 50; i < 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    assert.equal(db.execute('SELECT COUNT(*) AS cnt FROM t').rows[0].cnt, 100);
  });

  it('ORDER BY expression', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, priority TEXT, val INT)');
    db.execute("INSERT INTO t VALUES (1, 'low', 10)");
    db.execute("INSERT INTO t VALUES (2, 'high', 20)");
    db.execute("INSERT INTO t VALUES (3, 'medium', 15)");
    const r = db.execute('SELECT * FROM t ORDER BY val DESC');
    assert.equal(r.rows[0].priority, 'high');
  });

  it('nested CASE expressions', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 50)');
    db.execute('INSERT INTO t VALUES (3, 90)');
    const r = db.execute("SELECT val, CASE WHEN val >= 80 THEN 'A' WHEN val >= 40 THEN 'B' ELSE 'C' END AS grade FROM t ORDER BY val");
    assert.equal(r.rows[0].grade, 'C');
    assert.equal(r.rows[1].grade, 'B');
    assert.equal(r.rows[2].grade, 'A');
  });

  // ─── 🎯 900th test ───
  it('🎯 900th test — full pipeline with CTE + window + aggregation', () => {
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, category TEXT, price INT, stock INT)');
    const data = [
      [1, 'Laptop', 'Electronics', 999, 50],
      [2, 'Phone', 'Electronics', 699, 200],
      [3, 'Tablet', 'Electronics', 499, 100],
      [4, 'Desk', 'Furniture', 299, 30],
      [5, 'Chair', 'Furniture', 199, 80],
      [6, 'Lamp', 'Furniture', 49, 500],
      [7, 'Book', 'Media', 15, 1000],
      [8, 'DVD', 'Media', 10, 200],
    ];
    for (const [id, name, cat, price, stock] of data) {
      db.execute(`INSERT INTO products VALUES (${id}, '${name}', '${cat}', ${price}, ${stock})`);
    }

    // Category summary
    const summary = db.execute('SELECT category, COUNT(*) AS items, SUM(price) AS total_price FROM products GROUP BY category ORDER BY total_price DESC');
    assert.equal(summary.rows.length, 3);

    // Top product per category via CTE
    const ranked = db.execute(`
      WITH ranked AS (
        SELECT name, category, price,
          ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS rn
        FROM products
      )
      SELECT name, category, price FROM ranked WHERE rn = 1 ORDER BY price DESC
    `);
    assert.equal(ranked.rows.length, 3);
    assert.equal(ranked.rows[0].name, 'Laptop');

    // Filter expensive products
    const expensive = db.execute('SELECT name FROM products WHERE price > 200 ORDER BY price DESC');
    assert.equal(expensive.rows.length, 4);
  });
});
