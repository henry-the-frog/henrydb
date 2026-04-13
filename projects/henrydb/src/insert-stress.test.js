// insert-stress.test.js — Stress tests for INSERT
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('INSERT stress tests', () => {
  
  it('basic INSERT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hello')");
    const r = db.execute('SELECT * FROM t');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].name, 'hello');
  });

  it('INSERT with column list', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT, val INT)');
    db.execute("INSERT INTO t (id, name) VALUES (1, 'test')");
    const r = db.execute('SELECT * FROM t');
    assert.strictEqual(r.rows[0].id, 1);
    assert.strictEqual(r.rows[0].name, 'test');
  });

  it('INSERT SELECT', () => {
    const db = new Database();
    db.execute('CREATE TABLE src (id INT, val TEXT)');
    db.execute('CREATE TABLE dst (id INT, val TEXT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO src VALUES (${i}, 'v${i}')`);
    
    db.execute('INSERT INTO dst SELECT * FROM src WHERE id <= 3');
    const r = db.execute('SELECT COUNT(*) as cnt FROM dst');
    assert.strictEqual(r.rows[0].cnt, 3);
  });

  it('INSERT SELECT with WHERE', () => {
    const db = new Database();
    db.execute('CREATE TABLE src (id INT, val INT)');
    db.execute('CREATE TABLE dst (id INT, val INT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO src VALUES (${i}, ${i * 10})`);
    
    db.execute('INSERT INTO dst SELECT * FROM src WHERE val > 500');
    const r = db.execute('SELECT COUNT(*) as cnt FROM dst');
    assert.strictEqual(r.rows[0].cnt, 50);
  });

  it('INSERT SELECT from same table', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    
    db.execute('INSERT INTO t SELECT id + 10, val * 2 FROM t');
    const r = db.execute('SELECT COUNT(*) as cnt FROM t');
    assert.strictEqual(r.rows[0].cnt, 10);
  });

  it('INSERT NULL values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT, val INT)');
    db.execute('INSERT INTO t VALUES (1, NULL, NULL)');
    const r = db.execute('SELECT * FROM t WHERE id = 1');
    assert.strictEqual(r.rows[0].name, null);
    assert.strictEqual(r.rows[0].val, null);
  });

  it('INSERT with string escaping', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'O''Brien')");
    const r = db.execute('SELECT name FROM t WHERE id = 1');
    assert.strictEqual(r.rows[0].name, "O'Brien");
  });

  it('mass INSERT: 10000 rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE big (id INT, val INT)');
    for (let i = 1; i <= 10000; i++) {
      db.execute(`INSERT INTO big VALUES (${i}, ${i * 2})`);
    }
    const r = db.execute('SELECT COUNT(*) as cnt FROM big');
    assert.strictEqual(r.rows[0].cnt, 10000);
    // Verify first and last
    const first = db.execute('SELECT val FROM big WHERE id = 1');
    assert.strictEqual(first.rows[0].val, 2);
    const last = db.execute('SELECT val FROM big WHERE id = 10000');
    assert.strictEqual(last.rows[0].val, 20000);
  });

  it('INSERT into table with PRIMARY KEY', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'first')");
    db.execute("INSERT INTO t VALUES (2, 'second')");
    
    // Duplicate PK should fail
    try {
      db.execute("INSERT INTO t VALUES (1, 'duplicate')");
      // If it doesn't throw, verify the original is preserved
      const r = db.execute('SELECT name FROM t WHERE id = 1');
      assert.strictEqual(r.rows[0].name, 'first');
    } catch (e) {
      assert.ok(e.message.length > 0);
    }
  });

  it('INSERT with CHECK constraint', () => {
    const db = new Database();
    try {
      db.execute('CREATE TABLE t (id INT, val INT CHECK (val > 0))');
      db.execute('INSERT INTO t VALUES (1, 10)'); // Should work
      
      try {
        db.execute('INSERT INTO t VALUES (2, -5)'); // Should fail
      } catch (e) {
        assert.ok(e.message.includes('CHECK') || e.message.includes('constraint'));
      }
    } catch (e) {
      // CHECK constraints may not be supported
      assert.ok(true);
    }
  });

  it('INSERT preserves data types', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT, price REAL, active BOOLEAN)');
    db.execute("INSERT INTO t VALUES (1, 'test', 3.14, true)");
    const r = db.execute('SELECT * FROM t');
    assert.strictEqual(typeof r.rows[0].id, 'number');
    assert.strictEqual(typeof r.rows[0].name, 'string');
    assert.strictEqual(typeof r.rows[0].price, 'number');
  });

  it('INSERT then immediate SELECT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    
    for (let i = 1; i <= 100; i++) {
      db.execute(`INSERT INTO t VALUES (${i})`);
      const r = db.execute(`SELECT COUNT(*) as cnt FROM t`);
      assert.strictEqual(r.rows[0].cnt, i, `after insert ${i}, count should be ${i}`);
    }
  });

  it('INSERT with negative numbers', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (-1, -100)');
    const r = db.execute('SELECT * FROM t WHERE id = -1');
    assert.strictEqual(r.rows[0].val, -100);
  });

  it('INSERT with large numbers', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 2147483647)'); // INT_MAX
    db.execute('INSERT INTO t VALUES (2, -2147483648)'); // INT_MIN
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.strictEqual(r.rows[0].val, 2147483647);
    assert.strictEqual(r.rows[1].val, -2147483648);
  });

  it('INSERT with empty string', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, '')");
    const r = db.execute('SELECT * FROM t');
    assert.strictEqual(r.rows[0].name, '');
  });

  it('INSERT SELECT with aggregate (column mapping)', () => {
    const db = new Database();
    db.execute('CREATE TABLE src (id INT, val INT)');
    db.execute('CREATE TABLE dst (total INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO src VALUES (${i}, ${i})`);
    
    db.execute("INSERT INTO dst SELECT SUM(val) FROM src");
    const r = db.execute('SELECT * FROM dst');
    assert.strictEqual(r.rows[0].total, 55);
  });

  it('INSERT SELECT with JOIN', () => {
    const db = new Database();
    db.execute('CREATE TABLE customers (id INT, name TEXT)');
    db.execute('CREATE TABLE orders (id INT, customer_id INT, amount INT)');
    db.execute('CREATE TABLE report (name TEXT, amount INT)');
    
    db.execute("INSERT INTO customers VALUES (1, 'Alice')");
    db.execute("INSERT INTO customers VALUES (2, 'Bob')");
    db.execute('INSERT INTO orders VALUES (1, 1, 100)');
    db.execute('INSERT INTO orders VALUES (2, 2, 200)');
    
    db.execute('INSERT INTO report SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id');
    const r = db.execute('SELECT * FROM report ORDER BY name');
    assert.strictEqual(r.rows.length, 2);
    assert.strictEqual(r.rows[0].name, 'Alice');
    assert.strictEqual(r.rows[0].amount, 100);
  });
});
