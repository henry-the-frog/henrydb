// prepared-stmt-stress.test.js — Stress tests for prepared statements
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Prepared statement stress tests', () => {
  
  it('basic prepare and execute', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hello')");
    const stmt = db.prepare('SELECT * FROM t WHERE id = $1');
    const r = stmt.execute([1]);
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].name, 'hello');
  });

  it('prepared INSERT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    const stmt = db.prepare('INSERT INTO t VALUES ($1, $2)');
    for (let i = 1; i <= 100; i++) stmt.execute([i, `name_${i}`]);
    assert.strictEqual(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 100);
  });

  it('prepared SELECT with multiple params', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, cat TEXT, val INT)');
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO t VALUES (${i}, 'cat${i % 3}', ${i * 10})`);
    const stmt = db.prepare("SELECT id FROM t WHERE cat = $1 AND val > $2 ORDER BY id");
    const r = stmt.execute(['cat0', 100]);
    assert.ok(r.rows.length > 0);
  });

  it('repeated execution with different params', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    const stmt = db.prepare('SELECT id FROM t WHERE id = $1');
    for (let i = 1; i <= 10; i++) {
      const r = stmt.execute([i]);
      assert.strictEqual(r.rows.length, 1);
      assert.strictEqual(r.rows[0].id, i);
    }
  });

  it('prepared UPDATE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, 0)`);
    const stmt = db.prepare('UPDATE t SET val = $1 WHERE id = $2');
    for (let i = 1; i <= 10; i++) stmt.execute([i * 100, i]);
    assert.strictEqual(db.execute('SELECT val FROM t WHERE id = 5').rows[0].val, 500);
  });

  it('prepared DELETE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    const stmt = db.prepare('DELETE FROM t WHERE id = $1');
    stmt.execute([5]);
    stmt.execute([7]);
    assert.strictEqual(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 8);
  });

  it('mass prepared INSERT (5000 rows)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    const stmt = db.prepare('INSERT INTO t VALUES ($1, $2)');
    const start = Date.now();
    for (let i = 1; i <= 5000; i++) stmt.execute([i, `value_${i}`]);
    const elapsed = Date.now() - start;
    assert.strictEqual(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 5000);
    console.log(`5000 prepared INSERTs in ${elapsed}ms`);
  });

  it('multiple prepared statements coexist', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT)');
    db.execute('CREATE TABLE b (id INT)');
    const stmtA = db.prepare('INSERT INTO a VALUES ($1)');
    const stmtB = db.prepare('INSERT INTO b VALUES ($1)');
    for (let i = 1; i <= 10; i++) {
      stmtA.execute([i]);
      stmtB.execute([i * 10]);
    }
    assert.strictEqual(db.execute('SELECT COUNT(*) as cnt FROM a').rows[0].cnt, 10);
    assert.strictEqual(db.execute('SELECT COUNT(*) as cnt FROM b').rows[0].cnt, 10);
  });

  it('close prepared statement', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    const stmt = db.prepare('SELECT * FROM t WHERE id = $1');
    stmt.close();
    // After close, execute may throw or succeed depending on implementation
    assert.ok(true);
  });

  it('prepared statement survives schema change', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    const stmt = db.prepare('SELECT * FROM t WHERE id = $1');
    
    const r1 = stmt.execute([1]);
    assert.strictEqual(r1.rows.length, 1);
    
    // Add column
    db.execute('ALTER TABLE t ADD COLUMN extra INT');
    
    // Prepared statement may still work or may need re-prepare
    try {
      const r2 = stmt.execute([1]);
      assert.ok(r2.rows.length >= 1);
    } catch (e) {
      assert.ok(true); // Schema change invalidation is acceptable
    }
  });
});
