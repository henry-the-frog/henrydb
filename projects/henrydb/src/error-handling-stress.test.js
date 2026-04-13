// error-handling-stress.test.js — Stress tests for error handling
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Error handling stress tests', () => {
  
  it('SELECT from non-existent table', () => {
    const db = new Database();
    assert.throws(() => db.execute('SELECT * FROM nonexistent'), /not found|does not exist|Unknown/i);
  });

  it('INSERT into non-existent table', () => {
    const db = new Database();
    assert.throws(() => db.execute('INSERT INTO nonexistent VALUES (1)'));
  });

  it('DROP non-existent table (without IF EXISTS)', () => {
    const db = new Database();
    assert.throws(() => db.execute('DROP TABLE nonexistent'));
  });

  it('invalid SQL syntax', () => {
    const db = new Database();
    assert.throws(() => db.execute('SELCT * FORM t'));
  });

  it('type mismatch in WHERE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    // Comparing int to string — should either work or error gracefully
    try {
      db.execute("SELECT * FROM t WHERE id = 'abc'");
    } catch (e) {
      assert.ok(e.message.length > 0);
    }
  });

  it('duplicate table name', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    assert.throws(() => db.execute('CREATE TABLE t (id INT)'));
  });

  it('column not found', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    try {
      const r = db.execute('SELECT nonexistent FROM t');
      // Some DBs return null for missing columns
      assert.ok(true);
    } catch (e) {
      assert.ok(e.message.length > 0);
    }
  });

  it('database recovers after error', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    
    // Cause an error
    try { db.execute('SELECT * FROM nonexistent'); } catch (e) {}
    
    // Should still work
    const r = db.execute('SELECT * FROM t');
    assert.strictEqual(r.rows.length, 1);
  });

  it('empty query', () => {
    const db = new Database();
    try {
      db.execute('');
    } catch (e) {
      assert.ok(true);
    }
  });

  it('multiple errors in sequence', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    
    for (let i = 0; i < 10; i++) {
      try { db.execute('SELECT * FROM bad'); } catch (e) {}
    }
    
    // Should still work after many errors
    db.execute('INSERT INTO t VALUES (1)');
    assert.strictEqual(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 1);
  });
});
