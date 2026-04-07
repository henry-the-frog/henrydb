// final-push.test.js — Final tests to reach 1600
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Final Push to 1600', () => {
  it('LOWER in WHERE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'ALICE')");
    const r = db.execute("SELECT * FROM t WHERE LOWER(name) = 'alice'");
    assert.equal(r.rows.length, 1);
  });

  it('UPPER in SELECT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'alice')");
    assert.equal(db.execute('SELECT UPPER(name) AS u FROM t').rows[0].u, 'ALICE');
  });

  it('LENGTH function', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, s TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hello')");
    assert.equal(db.execute('SELECT LENGTH(s) AS len FROM t').rows[0].len, 5);
  });

  it('REPLACE function', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, s TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hello world')");
    assert.equal(db.execute("SELECT REPLACE(s, 'world', 'earth') AS r FROM t").rows[0].r, 'hello earth');
  });

  it('TRIM function', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, s TEXT)');
    db.execute("INSERT INTO t VALUES (1, '  hello  ')");
    assert.equal(db.execute('SELECT TRIM(s) AS r FROM t').rows[0].r, 'hello');
  });

  it('ROUND function', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 7)');
    const r = db.execute('SELECT ROUND(3.7) AS rounded');
    assert.equal(r.rows[0].rounded, 4);
  });

  it('IIF function', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, -5)');
    const r = db.execute("SELECT id, IIF(val > 0, 'positive', 'negative') AS sign FROM t ORDER BY id");
    assert.equal(r.rows[0].sign, 'positive');
    assert.equal(r.rows[1].sign, 'negative');
  });

  it('GROUP_CONCAT with multiple values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grp TEXT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 'Alice')");
    db.execute("INSERT INTO t VALUES (2, 'A', 'Amy')");
    db.execute("INSERT INTO t VALUES (3, 'B', 'Bob')");
    const r = db.execute('SELECT grp, GROUP_CONCAT(name) AS names FROM t GROUP BY grp ORDER BY grp');
    assert.ok(r.rows[0].names.includes('Alice'));
    assert.ok(r.rows[0].names.includes('Amy'));
  });

  it('CAST to INT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, s TEXT)');
    db.execute("INSERT INTO t VALUES (1, '42')");
    const r = db.execute('SELECT CAST(s AS INT) AS num FROM t');
    assert.equal(r.rows[0].num, 42);
  });

  it('multiple aggregate functions with GROUP BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE scores (id INT PRIMARY KEY, subject TEXT, score INT)');
    db.execute("INSERT INTO scores VALUES (1, 'Math', 90)");
    db.execute("INSERT INTO scores VALUES (2, 'Math', 85)");
    db.execute("INSERT INTO scores VALUES (3, 'Science', 95)");
    db.execute("INSERT INTO scores VALUES (4, 'Science', 80)");
    
    const r = db.execute('SELECT subject, MIN(score) AS min_s, MAX(score) AS max_s, AVG(score) AS avg_s FROM scores GROUP BY subject ORDER BY subject');
    assert.equal(r.rows[0].subject, 'Math');
    assert.equal(r.rows[0].min_s, 85);
    assert.equal(r.rows[0].max_s, 90);
  });

  it('CREATE INDEX after data exists', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 50; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 2})`);
    
    db.execute('CREATE INDEX idx_val ON t(val)');
    const r = db.execute('SELECT * FROM t WHERE val = 50');
    assert.equal(r.rows.length, 1);
  });

  it('DROP INDEX', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('CREATE INDEX idx_val ON t(val)');
    db.execute('DROP INDEX idx_val');
    
    // Should still work, just without the index
    db.execute('INSERT INTO t VALUES (1, 10)');
    assert.equal(db.execute('SELECT val FROM t').rows[0].val, 10);
  });

  it('CREATE VIEW and query', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT, active INT)');
    db.execute('INSERT INTO t VALUES (1, 10, 1)');
    db.execute('INSERT INTO t VALUES (2, 20, 0)');
    db.execute('INSERT INTO t VALUES (3, 30, 1)');
    
    db.execute('CREATE VIEW active_items AS SELECT id, val FROM t WHERE active = 1');
    const r = db.execute('SELECT * FROM active_items ORDER BY id');
    assert.equal(r.rows.length, 2);
  });

  it('INTERSECT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE t2 (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t1 VALUES (1)');
    db.execute('INSERT INTO t1 VALUES (2)');
    db.execute('INSERT INTO t1 VALUES (3)');
    db.execute('INSERT INTO t2 VALUES (2)');
    db.execute('INSERT INTO t2 VALUES (3)');
    db.execute('INSERT INTO t2 VALUES (4)');
    
    const r = db.execute('SELECT id FROM t1 INTERSECT SELECT id FROM t2');
    assert.equal(r.rows.length, 2);
  });

  it('EXCEPT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE t2 (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t1 VALUES (1)');
    db.execute('INSERT INTO t1 VALUES (2)');
    db.execute('INSERT INTO t1 VALUES (3)');
    db.execute('INSERT INTO t2 VALUES (2)');
    
    const r = db.execute('SELECT id FROM t1 EXCEPT SELECT id FROM t2');
    assert.equal(r.rows.length, 2); // 1 and 3
  });

  it('nested subquery in SELECT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    
    const r = db.execute('SELECT id, val, (SELECT SUM(val) FROM t) AS total FROM t ORDER BY id');
    assert.equal(r.rows[0].total, 30);
    assert.equal(r.rows[1].total, 30);
  });

  it('TYPEOF function', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 42, 'hello')");
    
    const r1 = db.execute('SELECT TYPEOF(val) AS t FROM t');
    assert.ok(['number', 'integer'].includes(r1.rows[0].t));
    const r2 = db.execute('SELECT TYPEOF(name) AS t FROM t');
    assert.ok(['string', 'text'].includes(r2.rows[0].t));
  });
});
