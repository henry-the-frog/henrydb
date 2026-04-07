// final-600.test.js — Final push to 600 tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Reaching 600 Tests', () => {
  it('full CRUD cycle with verification', () => {
    const db = new Database();
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, name TEXT, qty INT)');
    // Create
    db.execute("INSERT INTO items VALUES (1, 'Apple', 50)");
    assert.equal(db.execute('SELECT COUNT(*) AS c FROM items').rows[0].c, 1);
    // Read
    assert.equal(db.execute('SELECT name FROM items WHERE id = 1').rows[0].name, 'Apple');
    // Update
    db.execute('UPDATE items SET qty = qty - 5 WHERE id = 1');
    assert.equal(db.execute('SELECT qty FROM items WHERE id = 1').rows[0].qty, 45);
    // Delete
    db.execute('DELETE FROM items WHERE id = 1');
    assert.equal(db.execute('SELECT COUNT(*) AS c FROM items').rows[0].c, 0);
  });

  it('complex WHERE with parentheses', () => {
    const db = new Database();
    db.execute('CREATE TABLE p (id INT PRIMARY KEY, a INT, b INT, c INT)');
    db.execute('INSERT INTO p VALUES (1, 1, 2, 3)');
    db.execute('INSERT INTO p VALUES (2, 4, 5, 6)');
    db.execute('INSERT INTO p VALUES (3, 7, 8, 9)');
    const r = db.execute('SELECT * FROM p WHERE (a > 3 AND b < 9) OR c = 3');
    assert.equal(r.rows.length, 3); // id=1 (c=3), id=2 (a>3, b<9), id=3 (a>3, b<9)
  });

  it('ORDER BY multiple columns mixed direction', () => {
    const db = new Database();
    db.execute('CREATE TABLE s (id INT PRIMARY KEY, cat TEXT, val INT)');
    db.execute("INSERT INTO s VALUES (1, 'A', 30)");
    db.execute("INSERT INTO s VALUES (2, 'B', 10)");
    db.execute("INSERT INTO s VALUES (3, 'A', 10)");
    db.execute("INSERT INTO s VALUES (4, 'B', 30)");
    const r = db.execute('SELECT * FROM s ORDER BY cat ASC, val DESC');
    assert.equal(r.rows[0].cat, 'A');
    assert.equal(r.rows[0].val, 30);
    assert.equal(r.rows[1].cat, 'A');
    assert.equal(r.rows[1].val, 10);
  });

  it('window function preserves all columns', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, dept TEXT, salary INT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 100)");
    db.execute("INSERT INTO t VALUES (2, 'A', 200)");
    db.execute("INSERT INTO t VALUES (3, 'B', 150)");
    const r = db.execute('SELECT id, dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM t');
    assert.equal(r.rows.length, 3);
    assert.ok(r.rows.every(row => row.id && row.dept && row.salary && row.rn));
  });

  it('aggregate without GROUP BY returns single row', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('INSERT INTO t VALUES (3, 30)');
    const r = db.execute('SELECT COUNT(*) AS cnt, SUM(val) AS sum, AVG(val) AS avg, MIN(val) AS min, MAX(val) AS max FROM t');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].cnt, 3);
    assert.equal(r.rows[0].sum, 60);
    assert.equal(r.rows[0].avg, 20);
    assert.equal(r.rows[0].min, 10);
    assert.equal(r.rows[0].max, 30);
  });

  it('multi-row INSERT + aggregate', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)');
    const r = db.execute('SELECT SUM(val) AS total FROM t');
    assert.equal(r.rows[0].total, 150);
  });

  it('UNION with different column aliases', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT PRIMARY KEY, val INT)');
    db.execute('CREATE TABLE b (id INT PRIMARY KEY, num INT)');
    db.execute('INSERT INTO a VALUES (1, 10)');
    db.execute('INSERT INTO b VALUES (1, 20)');
    const r = db.execute('SELECT val AS result FROM a UNION ALL SELECT num AS result FROM b');
    assert.equal(r.rows.length, 2);
  });

  it('CTE with aggregate', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, val INT)');
    db.execute("INSERT INTO t VALUES (1, 'X', 10)");
    db.execute("INSERT INTO t VALUES (2, 'Y', 20)");
    db.execute("INSERT INTO t VALUES (3, 'X', 30)");
    const r = db.execute('WITH totals AS (SELECT cat, SUM(val) AS total FROM t GROUP BY cat) SELECT * FROM totals ORDER BY total DESC');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].cat, 'X');
  });

  it('VIEW + aggregate', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, val INT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 10)");
    db.execute("INSERT INTO t VALUES (2, 'A', 20)");
    db.execute("INSERT INTO t VALUES (3, 'B', 30)");
    db.execute("CREATE VIEW cat_a AS SELECT * FROM t WHERE cat = 'A'");
    const r = db.execute('SELECT SUM(val) AS total FROM cat_a');
    assert.equal(r.rows[0].total, 30);
  });

  it('EXPLAIN for indexed query shows INDEX_SCAN', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, email TEXT)');
    db.execute('CREATE INDEX idx ON t (email)');
    const plan = db.execute("EXPLAIN SELECT * FROM t WHERE email = 'test@test.com'");
    assert.ok(plan.plan.some(p => p.operation === 'INDEX_SCAN'));
  });

  it('DESCRIBE after multiple operations', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('ALTER TABLE t ADD name TEXT');
    db.execute('ALTER TABLE t ADD age INT DEFAULT 0');
    db.execute('CREATE INDEX idx_val ON t (val)');
    const r = db.execute('DESCRIBE t');
    assert.equal(r.rows.length, 4);
    const val = r.rows.find(c => c.column_name === 'val');
    assert.ok(val); // val column exists
  });

  it('SHOW TABLES counts correctly', () => {
    const db = new Database();
    for (let i = 0; i < 5; i++) db.execute(`CREATE TABLE t${i} (id INT PRIMARY KEY)`);
    const r = db.execute('SHOW TABLES');
    assert.equal(r.rows.length, 5);
  });

  it('BETWEEN with strings', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    db.execute("INSERT INTO t VALUES (2, 'Bob')");
    db.execute("INSERT INTO t VALUES (3, 'Charlie')");
    db.execute("INSERT INTO t VALUES (4, 'David')");
    const r = db.execute("SELECT * FROM t WHERE name BETWEEN 'B' AND 'D'");
    assert.ok(r.rows.length >= 2); // Bob, Charlie
  });

  it('NOT with complex expression', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('INSERT INTO t VALUES (3, 30)');
    const r = db.execute('SELECT * FROM t WHERE NOT (val >= 15 AND val <= 25)');
    assert.equal(r.rows.length, 2); // 10 and 30
  });

  it('COALESCE with multiple NULLs', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT)');
    db.execute('INSERT INTO t VALUES (1, NULL, NULL, 42)');
    const r = db.execute('SELECT COALESCE(a, b, c) AS result FROM t');
    assert.equal(r.rows[0].result, 42);
  });

  it('CASE with IN', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 1)');
    db.execute('INSERT INTO t VALUES (2, 2)');
    db.execute('INSERT INTO t VALUES (3, 3)');
    const r = db.execute("SELECT id, CASE WHEN val IN (1, 3) THEN 'odd' ELSE 'even' END AS parity FROM t ORDER BY id");
    assert.equal(r.rows[0].parity, 'odd');
    assert.equal(r.rows[1].parity, 'even');
    assert.equal(r.rows[2].parity, 'odd');
  });

  it('nested functions: LENGTH(UPPER(TRIM(val)))', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, '  hello  ')");
    const r = db.execute('SELECT LENGTH(UPPER(TRIM(val))) AS result FROM t');
    assert.equal(r.rows[0].result, 5);
  });

  it('100-row INSERT + GROUP BY + HAVING + ORDER + LIMIT', () => {
    const db = new Database();
    db.execute('CREATE TABLE data (id INT PRIMARY KEY, cat TEXT, val INT)');
    for (let i = 1; i <= 100; i++) {
      db.execute(`INSERT INTO data VALUES (${i}, 'C${i % 10}', ${i})`);
    }
    const r = db.execute('SELECT cat, SUM(val) AS total FROM data GROUP BY cat HAVING total > 400 ORDER BY total DESC LIMIT 3');
    assert.equal(r.rows.length, 3);
    assert.ok(r.rows[0].total >= r.rows[1].total);
    assert.ok(r.rows[1].total >= r.rows[2].total);
  });
});
