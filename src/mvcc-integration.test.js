// mvcc-integration.test.js — Tests for MVCC integrated into Database class
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Database MVCC Integration', () => {
  it('begin/commit/rollback work with MVCC enabled', () => {
    const db = new Database({ mvcc: true });
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT)');
    
    db.execute('BEGIN');
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    db.execute('COMMIT');
    
    const r = db.execute('SELECT * FROM users');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'Alice');
  });

  it('MVCC mode does not break regular operations', () => {
    const db = new Database({ mvcc: true });
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    
    for (let i = 1; i <= 10; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    }
    
    const r = db.execute('SELECT SUM(val) as total FROM t');
    assert.equal(r.rows[0].total, 550);
    
    db.execute('UPDATE t SET val = val + 1 WHERE id < 5');
    const r2 = db.execute('SELECT COUNT(*) as cnt FROM t WHERE val > 40');
    assert.ok(r2.rows[0].cnt > 0);
  });

  it('non-MVCC mode still works (backward compatibility)', () => {
    const db = new Database(); // no mvcc option
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('BEGIN');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('COMMIT');
    
    const r = db.execute('SELECT * FROM t');
    assert.equal(r.rows.length, 1);
  });

  it('rollback with MVCC resets transaction state', () => {
    const db = new Database({ mvcc: true });
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute("INSERT INTO t VALUES (1, 100)");
    
    db.execute('BEGIN');
    // MVCC transaction is active
    assert.ok(db._currentTx !== null);
    assert.ok(db._mvcc.activeTxns.size > 0);
    
    db.execute('ROLLBACK');
    
    // After rollback, transaction state cleared
    assert.equal(db._currentTx, null);
    assert.equal(db._currentTxId, 0);
    assert.equal(db._inTransaction, false);
    // Note: full heap-level rollback requires MVCC-aware heap integration
    // which is planned for the next phase
  });

  it('MVCC manager stats accessible', () => {
    const db = new Database({ mvcc: true });
    assert.ok(db._mvcc !== null);
    assert.ok(typeof db._mvcc.getStats === 'function');
    const stats = db._mvcc.getStats();
    assert.equal(stats.activeTxns, 0);
  });
});
