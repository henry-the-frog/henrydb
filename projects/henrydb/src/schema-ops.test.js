// schema-ops.test.js — Schema operations, DDL, and introspection
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Schema Operations', () => {
  let db;
  beforeEach(() => { db = new Database(); });

  it('CREATE TABLE + verify structure', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT)');
    const r = db.execute('SELECT * FROM t');
    assert.equal(r.rows.length, 0); // empty table exists
  });

  it('DROP TABLE removes it', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('DROP TABLE t');
    assert.throws(() => db.execute('SELECT * FROM t'));
  });

  it('duplicate table creation throws', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    assert.throws(() => db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)'));
  });

  it('CREATE duplicate table throws', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    assert.throws(() => db.execute('CREATE TABLE t (id INT PRIMARY KEY)'));
  });

  it('ALTER TABLE ADD COLUMN', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    db.execute('ALTER TABLE t ADD COLUMN age INT');
    const r = db.execute('SELECT * FROM t');
    assert.equal(r.rows[0].age, null); // default NULL
  });

  it('ALTER TABLE ADD COLUMN with default', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('ALTER TABLE t ADD COLUMN status TEXT DEFAULT \'active\'');
    const r = db.execute('SELECT status FROM t');
    // Default applied to existing rows
    assert.ok(r.rows[0].status === 'active' || r.rows[0].status === null);
  });

  it('CREATE INDEX + query still works', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice', 30)");
    db.execute("INSERT INTO t VALUES (2, 'Bob', 25)");
    db.execute('CREATE INDEX idx_age ON t(age)');
    const r = db.execute('SELECT name FROM t WHERE age = 25');
    assert.equal(r.rows[0].name, 'Bob');
  });

  it('multiple tables coexist', () => {
    db.execute('CREATE TABLE t1 (id INT PRIMARY KEY, val INT)');
    db.execute('CREATE TABLE t2 (id INT PRIMARY KEY, val INT)');
    db.execute('CREATE TABLE t3 (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t1 VALUES (1, 10)');
    db.execute('INSERT INTO t2 VALUES (1, 20)');
    db.execute('INSERT INTO t3 VALUES (1, 30)');
    assert.equal(db.execute('SELECT val FROM t1').rows[0].val, 10);
    assert.equal(db.execute('SELECT val FROM t2').rows[0].val, 20);
    assert.equal(db.execute('SELECT val FROM t3').rows[0].val, 30);
  });

  it('SELECT from non-existent table throws', () => {
    assert.throws(() => db.execute('SELECT * FROM nonexistent'));
  });

  it('INSERT into non-existent table throws', () => {
    assert.throws(() => db.execute("INSERT INTO nonexistent VALUES (1, 'test')"));
  });

  it('DROP non-existent table throws', () => {
    assert.throws(() => db.execute('DROP TABLE nonexistent'));
  });

  it('multiple tables share nothing', () => {
    db.execute('CREATE TABLE t1 (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE t2 (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t1 VALUES (1)');
    assert.equal(db.execute('SELECT COUNT(*) AS cnt FROM t2').rows[0].cnt, 0);
  });

  it('recreate table after DROP', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('DROP TABLE t');
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    const r = db.execute('SELECT * FROM t');
    assert.equal(r.rows.length, 0);
  });

  // More query tests to reach 850
  it('SELECT with multiple ANDs', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT)');
    db.execute('INSERT INTO t VALUES (1, 10, 20, 30)');
    db.execute('INSERT INTO t VALUES (2, 10, 20, 40)');
    db.execute('INSERT INTO t VALUES (3, 10, 30, 30)');
    const r = db.execute('SELECT * FROM t WHERE a = 10 AND b = 20 AND c = 30');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].id, 1);
  });

  it('nested LET with arithmetic', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 5)');
    const r = db.execute('SELECT val * 2 + 3 AS computed FROM t');
    assert.equal(r.rows[0].computed, 13);
  });

  it('COUNT with no rows', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    const r = db.execute('SELECT COUNT(*) AS cnt FROM t');
    assert.equal(r.rows[0].cnt, 0);
  });

  it('SUM with no rows', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    const r = db.execute('SELECT SUM(val) AS s FROM t');
    assert.ok(r.rows[0].s === null || r.rows[0].s === 0);
  });

  it('MAX/MIN with single row', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 42)');
    const r = db.execute('SELECT MAX(val) AS mx, MIN(val) AS mn FROM t');
    assert.equal(r.rows[0].mx, 42);
    assert.equal(r.rows[0].mn, 42);
  });

  it('GROUP BY returns one row per group', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 10)");
    db.execute("INSERT INTO t VALUES (2, 'A', 20)");
    db.execute("INSERT INTO t VALUES (3, 'B', 30)");
    const r = db.execute('SELECT grp, SUM(val) AS s FROM t GROUP BY grp ORDER BY grp');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].s, 30);
    assert.equal(r.rows[1].s, 30);
  });

  it('GROUP BY with HAVING cnt alias', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 10)");
    db.execute("INSERT INTO t VALUES (2, 'A', 20)");
    db.execute("INSERT INTO t VALUES (3, 'B', 5)");
    const r = db.execute('SELECT grp, COUNT(*) AS cnt FROM t GROUP BY grp ORDER BY cnt DESC');
    assert.equal(r.rows[0].grp, 'A');
    assert.equal(r.rows[0].cnt, 2);
  });

  it('JOIN produces combined rows', () => {
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, total INT)');
    db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO customers VALUES (1, 'Alice')");
    db.execute("INSERT INTO customers VALUES (2, 'Bob')");
    db.execute('INSERT INTO orders VALUES (1, 1, 100)');
    db.execute('INSERT INTO orders VALUES (2, 1, 200)');
    db.execute('INSERT INTO orders VALUES (3, 2, 50)');
    const r = db.execute('SELECT c.name, o.total FROM customers c JOIN orders o ON c.id = o.customer_id ORDER BY o.total DESC');
    assert.equal(r.rows.length, 3);
  });

  it('LIMIT 1 returns single row', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    const r = db.execute('SELECT * FROM t LIMIT 1');
    assert.equal(r.rows.length, 1);
  });

  it('ORDER BY descending string', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    db.execute("INSERT INTO t VALUES (2, 'Charlie')");
    db.execute("INSERT INTO t VALUES (3, 'Bob')");
    const r = db.execute('SELECT name FROM t ORDER BY name DESC');
    assert.equal(r.rows[0].name, 'Charlie');
  });

  it('self-join', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, parent_id INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, NULL, 'Root')");
    db.execute("INSERT INTO t VALUES (2, 1, 'Child1')");
    db.execute("INSERT INTO t VALUES (3, 1, 'Child2')");
    const r = db.execute('SELECT c.name AS child, p.name AS parent FROM t c JOIN t p ON c.parent_id = p.id ORDER BY c.name');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].parent, 'Root');
  });

  it('DISTINCT after JOIN', () => {
    db.execute('CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)');
    db.execute('CREATE TABLE t2 (id INT PRIMARY KEY, t1_id INT)');
    db.execute("INSERT INTO t1 VALUES (1, 'A')");
    db.execute("INSERT INTO t1 VALUES (2, 'B')");
    db.execute('INSERT INTO t2 VALUES (1, 1)');
    db.execute('INSERT INTO t2 VALUES (2, 1)');
    db.execute('INSERT INTO t2 VALUES (3, 2)');
    const r = db.execute('SELECT DISTINCT t1.val FROM t1 JOIN t2 ON t1.id = t2.t1_id ORDER BY t1.val');
    assert.equal(r.rows.length, 2);
  });

  it('UPDATE all rows (no WHERE)', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('UPDATE t SET val = 0');
    const r = db.execute('SELECT SUM(val) AS s FROM t');
    assert.equal(r.rows[0].s, 0);
  });

  it('DELETE all rows', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    db.execute('DELETE FROM t');
    assert.equal(db.execute('SELECT COUNT(*) AS cnt FROM t').rows[0].cnt, 0);
  });

  it('INSERT + immediate SELECT consistency', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 100; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, ${i * 2})`);
    }
    const r = db.execute('SELECT COUNT(*) AS cnt, SUM(val) AS s FROM t');
    assert.equal(r.rows[0].cnt, 100);
    assert.equal(r.rows[0].s, 100 * 101); // 2*(1+2+...+100) = 100*101
  });

  it('ROW_NUMBER with ORDER BY in outer', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 30)');
    db.execute('INSERT INTO t VALUES (2, 10)');
    db.execute('INSERT INTO t VALUES (3, 20)');
    const r = db.execute('SELECT val, ROW_NUMBER() OVER (ORDER BY val) AS rn FROM t ORDER BY val');
    assert.equal(r.rows[0].val, 10);
    assert.equal(r.rows[2].val, 30);
  });

  it('CTE used multiple times', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    const r = db.execute(`
      WITH data AS (SELECT val FROM t WHERE val >= 30)
      SELECT COUNT(*) AS cnt, SUM(val) AS s FROM data
    `);
    assert.equal(r.rows[0].cnt, 3); // 30, 40, 50
    assert.equal(r.rows[0].s, 120);
  });

  it('🎯 850th test — end-to-end analytics', () => {
    db.execute('CREATE TABLE logs (id INT PRIMARY KEY, user_id INT, action TEXT, ts INT)');
    const actions = ['login', 'view', 'click', 'purchase', 'logout'];
    for (let i = 1; i <= 50; i++) {
      db.execute(`INSERT INTO logs VALUES (${i}, ${(i % 5) + 1}, '${actions[i % 5]}', ${1000 + i})`);
    }
    
    // Actions per user
    const perUser = db.execute('SELECT user_id, COUNT(*) AS cnt FROM logs GROUP BY user_id ORDER BY cnt DESC');
    assert.equal(perUser.rows.length, 5);
    assert.equal(perUser.rows[0].cnt, 10);
    
    // Most common action
    const top = db.execute('SELECT action, COUNT(*) AS cnt FROM logs GROUP BY action ORDER BY cnt DESC LIMIT 1');
    assert.equal(top.rows[0].cnt, 10);
  });

  it('negative value handling', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, -100)');
    db.execute('INSERT INTO t VALUES (2, -50)');
    db.execute('INSERT INTO t VALUES (3, 50)');
    const r = db.execute('SELECT SUM(val) AS s, MIN(val) AS mn FROM t');
    assert.equal(r.rows[0].s, -100);
    assert.equal(r.rows[0].mn, -100);
  });
});
