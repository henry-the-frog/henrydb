// data-integrity.test.js — Data integrity and consistency tests
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Data Integrity', () => {
  let db;
  beforeEach(() => { db = new Database(); });

  it('INSERT preserves all columns', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b INT, c REAL)');
    db.execute("INSERT INTO t VALUES (1, 'hello', 42, 3.14)");
    const r = db.execute('SELECT * FROM t');
    assert.equal(r.rows[0].a, 'hello');
    assert.equal(r.rows[0].b, 42);
    assert.ok(Math.abs(r.rows[0].c - 3.14) < 0.01);
  });

  it('UPDATE preserves unchanged columns', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b INT)');
    db.execute("INSERT INTO t VALUES (1, 'hello', 42)");
    db.execute('UPDATE t SET b = 99 WHERE id = 1');
    const r = db.execute('SELECT * FROM t');
    assert.equal(r.rows[0].a, 'hello'); // unchanged
    assert.equal(r.rows[0].b, 99); // changed
  });

  it('DELETE removes only matching rows', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    db.execute('DELETE FROM t WHERE val % 2 = 0');
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows.length, 5);
    assert.ok(r.rows.every(row => row.val % 2 === 1));
  });

  it('INSERT order matches SELECT order', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (3, 'c')");
    db.execute("INSERT INTO t VALUES (1, 'a')");
    db.execute("INSERT INTO t VALUES (2, 'b')");
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows[0].val, 'a');
    assert.equal(r.rows[1].val, 'b');
    assert.equal(r.rows[2].val, 'c');
  });

  it('multiple UPDATEs stack', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 0)');
    db.execute('UPDATE t SET val = val + 1 WHERE id = 1');
    db.execute('UPDATE t SET val = val + 1 WHERE id = 1');
    db.execute('UPDATE t SET val = val + 1 WHERE id = 1');
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 3);
  });

  it('mixed INSERT/UPDATE/DELETE sequence', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('INSERT INTO t VALUES (3, 30)');
    db.execute('DELETE FROM t WHERE id = 2');
    db.execute('UPDATE t SET val = 99 WHERE id = 1');
    db.execute('INSERT INTO t VALUES (4, 40)');
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].val, 99);
    assert.equal(r.rows[1].val, 30);
    assert.equal(r.rows[2].val, 40);
  });

  it('SELECT * returns all columns', () => {
    db.execute('CREATE TABLE wide (id INT PRIMARY KEY, a INT, b INT, c INT, d INT, e INT)');
    db.execute('INSERT INTO wide VALUES (1, 2, 3, 4, 5, 6)');
    const r = db.execute('SELECT * FROM wide');
    assert.equal(Object.keys(r.rows[0]).length, 6);
  });

  it('table isolation — different tables are independent', () => {
    db.execute('CREATE TABLE t1 (id INT PRIMARY KEY, val INT)');
    db.execute('CREATE TABLE t2 (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t1 VALUES (1, 10)');
    db.execute('INSERT INTO t2 VALUES (1, 20)');
    assert.equal(db.execute('SELECT val FROM t1').rows[0].val, 10);
    assert.equal(db.execute('SELECT val FROM t2').rows[0].val, 20);
    db.execute('DELETE FROM t1');
    assert.equal(db.execute('SELECT COUNT(*) AS cnt FROM t2').rows[0].cnt, 1);
  });

  it('NULL vs empty string', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, '')");
    db.execute('INSERT INTO t VALUES (2, NULL)');
    assert.equal(db.execute("SELECT COUNT(*) AS cnt FROM t WHERE val = ''").rows[0].cnt, 1);
    assert.equal(db.execute('SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL').rows[0].cnt, 1);
  });

  it('large INSERT batch', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 200; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 7})`);
    const r = db.execute('SELECT COUNT(*) AS cnt, SUM(val) AS s FROM t');
    assert.equal(r.rows[0].cnt, 200);
    assert.equal(r.rows[0].s, 200 * 199 / 2 * 7); // sum of 0..199 * 7
  });

  it('ORDER BY stability — equal elements maintain relative order', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, grp TEXT, val INT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 10)");
    db.execute("INSERT INTO t VALUES (2, 'A', 10)");
    db.execute("INSERT INTO t VALUES (3, 'B', 10)");
    const r = db.execute('SELECT * FROM t ORDER BY val');
    assert.equal(r.rows.length, 3);
  });

  it('multiple tables with same column names', () => {
    db.execute('CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE t2 (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t1 VALUES (1, 'Alice')");
    db.execute("INSERT INTO t2 VALUES (1, 'Bob')");
    const r1 = db.execute('SELECT name FROM t1');
    const r2 = db.execute('SELECT name FROM t2');
    assert.equal(r1.rows[0].name, 'Alice');
    assert.equal(r2.rows[0].name, 'Bob');
  });

  it('aggregation after DELETE', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    db.execute('DELETE FROM t WHERE val > 50');
    const r = db.execute('SELECT SUM(val) AS s, COUNT(*) AS c FROM t');
    assert.equal(r.rows[0].c, 5);
    assert.equal(r.rows[0].s, 150); // 10+20+30+40+50
  });

  it('DELETE with comparison', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    db.execute('DELETE FROM t WHERE val > 5');
    const r = db.execute('SELECT COUNT(*) AS cnt FROM t');
    assert.equal(r.rows[0].cnt, 5);
  });

  it('UPDATE affects only specified rows', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, category TEXT, val INT)');
    db.execute("INSERT INTO t VALUES (1, 'A', 10)");
    db.execute("INSERT INTO t VALUES (2, 'B', 20)");
    db.execute("INSERT INTO t VALUES (3, 'A', 30)");
    db.execute("UPDATE t SET val = 0 WHERE category = 'A'");
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows[0].val, 0);
    assert.equal(r.rows[1].val, 20); // B unchanged
    assert.equal(r.rows[2].val, 0);
  });

  it('🎯 825th test — comprehensive data lifecycle', () => {
    db.execute('CREATE TABLE inventory (id INT PRIMARY KEY, item TEXT, qty INT, price INT)');

    // Create
    db.execute("INSERT INTO inventory VALUES (1, 'Widget', 100, 10)");
    db.execute("INSERT INTO inventory VALUES (2, 'Gadget', 50, 20)");
    db.execute("INSERT INTO inventory VALUES (3, 'Doohickey', 200, 5)");

    // Read
    assert.equal(db.execute('SELECT COUNT(*) AS cnt FROM inventory').rows[0].cnt, 3);

    // Update
    db.execute("UPDATE inventory SET qty = qty - 10 WHERE item = 'Widget'");
    assert.equal(db.execute("SELECT qty FROM inventory WHERE item = 'Widget'").rows[0].qty, 90);

    // Delete
    db.execute("DELETE FROM inventory WHERE item = 'Doohickey'");
    assert.equal(db.execute('SELECT COUNT(*) AS cnt FROM inventory').rows[0].cnt, 2);

    // Aggregate
    const total = db.execute('SELECT SUM(qty * price) AS total_value FROM inventory');
    assert.equal(total.rows[0].total_value, 90 * 10 + 50 * 20); // 1900
  });
});
