// aliasing-stress.test.js — Stress tests for table and column aliasing
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Aliasing stress tests', () => {
  
  it('table alias in simple query', () => {
    const db = new Database();
    db.execute('CREATE TABLE employees (id INT, name TEXT)');
    db.execute("INSERT INTO employees VALUES (1, 'Alice')");
    const r = db.execute('SELECT e.id, e.name FROM employees e');
    assert.strictEqual(r.rows[0].name, 'Alice');
  });

  it('column alias in SELECT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    db.execute('INSERT INTO t VALUES (42)');
    const r = db.execute('SELECT val as value FROM t');
    assert.strictEqual(r.rows[0].value, 42);
  });

  it('table and column aliases together', () => {
    const db = new Database();
    db.execute('CREATE TABLE employees (id INT, name TEXT)');
    db.execute("INSERT INTO employees VALUES (1, 'Alice')");
    const r = db.execute('SELECT e.id as employee_id, e.name as employee_name FROM employees e');
    assert.strictEqual(r.rows[0].employee_id, 1);
    assert.strictEqual(r.rows[0].employee_name, 'Alice');
  });

  it('alias in JOIN', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT, val TEXT)');
    db.execute('CREATE TABLE b (id INT, a_id INT)');
    db.execute("INSERT INTO a VALUES (1, 'hello')");
    db.execute('INSERT INTO b VALUES (1, 1)');
    const r = db.execute('SELECT x.val, y.id as bid FROM a x JOIN b y ON x.id = y.a_id');
    assert.strictEqual(r.rows[0].val, 'hello');
    assert.strictEqual(r.rows[0].bid, 1);
  });

  it('alias in GROUP BY reference', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (cat TEXT, val INT)');
    db.execute("INSERT INTO t VALUES ('A', 10)");
    db.execute("INSERT INTO t VALUES ('A', 20)");
    db.execute("INSERT INTO t VALUES ('B', 30)");
    const r = db.execute('SELECT cat as category, SUM(val) as total FROM t GROUP BY cat ORDER BY cat');
    assert.strictEqual(r.rows[0].category, 'A');
    assert.strictEqual(r.rows[0].total, 30);
  });

  it('alias used in ORDER BY (fixed today)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 30)');
    db.execute('INSERT INTO t VALUES (2, 10)');
    db.execute('INSERT INTO t VALUES (3, 20)');
    const r = db.execute('SELECT id, val as v FROM t ORDER BY v');
    assert.deepStrictEqual(r.rows.map(r => r.id), [2, 3, 1]);
  });

  it('alias in aggregate expression', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (amount INT)');
    [10, 20, 30].forEach(v => db.execute(`INSERT INTO t VALUES (${v})`));
    const r = db.execute('SELECT SUM(amount) as total_amount, AVG(amount) as avg_amount FROM t');
    assert.strictEqual(r.rows[0].total_amount, 60);
    assert.strictEqual(r.rows[0].avg_amount, 20);
  });

  it('same alias for different queries', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    
    const r1 = db.execute('SELECT id as x FROM t ORDER BY x');
    const r2 = db.execute('SELECT val as x FROM t ORDER BY x');
    
    assert.deepStrictEqual(r1.rows.map(r => r.x), [1, 2]);
    assert.deepStrictEqual(r2.rows.map(r => r.x), [10, 20]);
  });
});
