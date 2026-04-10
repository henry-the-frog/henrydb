// edge-cases.test.js — Tests for SQL edge cases and correctness
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function setup() {
  const db = new Database();
  db.execute('CREATE TABLE t1 (id INT, val TEXT)');
  db.execute('CREATE TABLE t2 (id INT, ref_id INT, data TEXT)');
  db.execute("INSERT INTO t1 VALUES (1, 'a')");
  db.execute("INSERT INTO t1 VALUES (2, 'b')");
  db.execute("INSERT INTO t1 VALUES (3, NULL)");
  db.execute("INSERT INTO t2 VALUES (1, 1, 'x')");
  db.execute("INSERT INTO t2 VALUES (2, NULL, 'y')");
  db.execute("INSERT INTO t2 VALUES (3, 2, 'z')");
  return db;
}

describe('SQL Edge Cases', () => {
  it('NULL in JOIN key is excluded', () => {
    const db = setup();
    const r = db.execute('SELECT t1.val, t2.data FROM t1 JOIN t2 ON t1.id = t2.ref_id ORDER BY t1.id');
    assert.equal(r.rows.length, 2);
  });

  it('LEFT JOIN preserves NULLs', () => {
    const db = setup();
    const r = db.execute('SELECT t1.val, t2.data FROM t1 LEFT JOIN t2 ON t1.id = t2.ref_id ORDER BY t1.id');
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[2].data, null);
  });

  it('COALESCE with NULL', () => {
    const db = setup();
    const r = db.execute("SELECT COALESCE(val, 'unknown') as result FROM t1 ORDER BY id");
    assert.equal(r.rows[2].result, 'unknown');
  });

  it('BETWEEN', () => {
    const db = setup();
    const r = db.execute('SELECT id FROM t1 WHERE id BETWEEN 1 AND 2 ORDER BY id');
    assert.equal(r.rows.length, 2);
  });

  it('LIKE pattern matching', () => {
    const db = new Database();
    db.execute('CREATE TABLE s (name TEXT)');
    db.execute("INSERT INTO s VALUES ('Alice')");
    db.execute("INSERT INTO s VALUES ('Bob')");
    db.execute("INSERT INTO s VALUES ('Alex')");
    const r = db.execute("SELECT name FROM s WHERE name LIKE 'Al%' ORDER BY name");
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].name, 'Alex');
  });

  it('CAST to TEXT', () => {
    const db = setup();
    const r = db.execute('SELECT CAST(id AS TEXT) as txt FROM t1 WHERE id IS NOT NULL ORDER BY id');
    assert.equal(r.rows[0].txt, '1');
  });

  it('UPDATE with COALESCE expression', () => {
    const db = setup();
    db.execute("UPDATE t1 SET val = COALESCE(val, 'default') WHERE val IS NULL");
    const r = db.execute('SELECT val FROM t1 WHERE id = 3');
    assert.equal(r.rows[0].val, 'default');
  });

  it('DELETE with IN subquery', () => {
    const db = setup();
    db.execute('DELETE FROM t2 WHERE ref_id IN (SELECT id FROM t1 WHERE id > 1)');
    const r = db.execute('SELECT * FROM t2 ORDER BY id');
    assert.equal(r.rows.length, 2); // Only ref_id=1 and ref_id=NULL survive
  });

  it('NOT IN', () => {
    const db = setup();
    const r = db.execute('SELECT id FROM t1 WHERE id NOT IN (1, 3) ORDER BY id');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].id, 2);
  });

  it('OR condition', () => {
    const db = setup();
    const r = db.execute('SELECT id FROM t1 WHERE id = 1 OR id = 3 ORDER BY id');
    assert.equal(r.rows.length, 2);
  });

  it('Complex WHERE with AND/OR', () => {
    const db = setup();
    const r = db.execute('SELECT id FROM t1 WHERE (id > 1 AND val IS NOT NULL) OR id = 1 ORDER BY id');
    assert.equal(r.rows.length, 2);
  });

  it('IN subquery with GROUP BY aggregate', () => {
    const db = new Database();
    db.execute('CREATE TABLE g (cat TEXT, val INT)');
    db.execute("INSERT INTO g VALUES ('a', 10)");
    db.execute("INSERT INTO g VALUES ('a', 20)");
    db.execute("INSERT INTO g VALUES ('b', 30)");
    const r = db.execute('SELECT * FROM g WHERE val IN (SELECT MAX(val) FROM g GROUP BY cat) ORDER BY val');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].val, 20);
    assert.equal(r.rows[1].val, 30);
  });

  it('Recursive CTE (counting)', () => {
    const db = new Database();
    const r = db.execute('WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 5) SELECT * FROM cnt');
    assert.equal(r.rows.length, 5);
    assert.equal(r.rows[0].x, 1);
    assert.equal(r.rows[4].x, 5);
  });

  it('Multi GROUP BY columns', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (product TEXT, region TEXT, amount INT)');
    db.execute("INSERT INTO sales VALUES ('A', 'N', 100)");
    db.execute("INSERT INTO sales VALUES ('A', 'S', 200)");
    db.execute("INSERT INTO sales VALUES ('B', 'N', 150)");
    const r = db.execute('SELECT product, region, SUM(amount) as total FROM sales GROUP BY product, region ORDER BY product, region');
    assert.equal(r.rows.length, 3);
  });

  it('COUNT DISTINCT', () => {
    const db = new Database();
    db.execute('CREATE TABLE d (x INT)');
    db.execute('INSERT INTO d VALUES (1)');
    db.execute('INSERT INTO d VALUES (2)');
    db.execute('INSERT INTO d VALUES (1)');
    db.execute('INSERT INTO d VALUES (3)');
    const r = db.execute('SELECT COUNT(DISTINCT x) as cnt FROM d');
    assert.equal(r.rows[0].cnt, 3);
  });

  it('Expression aliases in SELECT', () => {
    const db = new Database();
    db.execute('CREATE TABLE n (x INT)');
    db.execute('INSERT INTO n VALUES (10)');
    const r = db.execute('SELECT x * 2 as doubled, x + 5 as plus_five FROM n');
    assert.equal(r.rows[0].doubled, 20);
    assert.equal(r.rows[0].plus_five, 15);
  });

  it('Empty table aggregation', () => {
    const db = new Database();
    db.execute('CREATE TABLE empty (x INT)');
    const r = db.execute('SELECT COUNT(*) as cnt, SUM(x) as total, AVG(x) as avg FROM empty');
    assert.equal(r.rows[0].cnt, 0);
    assert.equal(r.rows[0].total, null);
  });

  it('Escaped single quotes in strings', () => {
    const db = new Database();
    db.execute('CREATE TABLE q (s TEXT)');
    db.execute("INSERT INTO q VALUES ('it''s a test')");
    const r = db.execute('SELECT s FROM q');
    assert.equal(r.rows[0].s, "it's a test");
  });
});
