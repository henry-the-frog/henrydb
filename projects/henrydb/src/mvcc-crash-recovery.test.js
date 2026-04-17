// mvcc-crash-recovery.test.js — Tests for MVCC + crash recovery interaction
// Validates that transaction atomicity is preserved across crashes.
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { Database } from './db.js';

function tmpDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'henrydb-mvcc-crash-'));
}

function cleanup(dir) {
  try { fs.rmSync(dir, { recursive: true, force: true }); } catch {}
}

describe('MVCC + Crash Recovery', () => {
  let dir;
  beforeEach(() => { dir = tmpDir(); });
  afterEach(() => { cleanup(dir); });

  it('uncommitted transaction should NOT appear after crash recovery', () => {
    // Phase 1: Insert committed data, start uncommitted transaction, crash
    const db1 = new Database({ dataDir: dir, walSync: 'immediate' });
    db1.execute('CREATE TABLE accounts (id INT, name TEXT, balance INT)');
    db1.execute("INSERT INTO accounts VALUES (1, 'Alice', 1000)");
    db1.execute("INSERT INTO accounts VALUES (2, 'Bob', 500)");
    
    // Start transaction but DON'T commit — simulate crash
    db1.execute('BEGIN');
    db1.execute("INSERT INTO accounts VALUES (3, 'Charlie', 750)");
    db1.execute("UPDATE accounts SET balance = 0 WHERE id = 1");
    // CRASH — close without commit
    db1.close();
    
    // Phase 2: Recover
    const db2 = Database.recover(dir);
    const result = db2.execute('SELECT * FROM accounts ORDER BY id');
    
    // Should only see Alice and Bob with original balances
    assert.strictEqual(result.rows.length, 2, 'Uncommitted rows should not survive crash recovery');
    assert.strictEqual(result.rows[0].name, 'Alice');
    assert.strictEqual(result.rows[0].balance, 1000, 'Uncommitted update should not survive');
    assert.strictEqual(result.rows[1].name, 'Bob');
    assert.strictEqual(result.rows[1].balance, 500);
    db2.close();
  });

  it('committed transaction should survive crash recovery', () => {
    const db1 = new Database({ dataDir: dir, walSync: 'immediate' });
    db1.execute('CREATE TABLE t (id INT, val TEXT)');
    
    // Committed transaction
    db1.execute('BEGIN');
    db1.execute("INSERT INTO t VALUES (1, 'committed')");
    db1.execute("INSERT INTO t VALUES (2, 'committed')");
    db1.execute('COMMIT');
    
    db1.close();
    
    const db2 = Database.recover(dir);
    const result = db2.execute('SELECT * FROM t ORDER BY id');
    assert.strictEqual(result.rows.length, 2, 'Committed rows should survive');
    assert.strictEqual(result.rows[0].val, 'committed');
    assert.strictEqual(result.rows[1].val, 'committed');
    db2.close();
  });

  it('interleaved transactions: committed survives, uncommitted does not', () => {
    const db1 = new Database({ dataDir: dir, walSync: 'immediate' });
    db1.execute('CREATE TABLE orders (id INT, product TEXT, status TEXT)');
    
    // Auto-committed insert (should survive)
    db1.execute("INSERT INTO orders VALUES (1, 'Widget', 'shipped')");
    
    // Explicit committed transaction
    db1.execute('BEGIN');
    db1.execute("INSERT INTO orders VALUES (2, 'Gadget', 'pending')");
    db1.execute('COMMIT');
    
    // Uncommitted transaction (should NOT survive crash)
    db1.execute('BEGIN');
    db1.execute("INSERT INTO orders VALUES (3, 'Doohickey', 'draft')");
    db1.execute("UPDATE orders SET status = 'cancelled' WHERE id = 1");
    // CRASH
    db1.close();
    
    const db2 = Database.recover(dir);
    const result = db2.execute('SELECT * FROM orders ORDER BY id');
    
    assert.strictEqual(result.rows.length, 2, 'Only committed rows should survive');
    assert.strictEqual(result.rows[0].product, 'Widget');
    assert.strictEqual(result.rows[0].status, 'shipped', 'Uncommitted update should not survive');
    assert.strictEqual(result.rows[1].product, 'Gadget');
    db2.close();
  });

  it('rollback before close should not affect recovery', () => {
    const db1 = new Database({ dataDir: dir, walSync: 'immediate' });
    db1.execute('CREATE TABLE t (id INT)');
    db1.execute('INSERT INTO t VALUES (1)');
    
    db1.execute('BEGIN');
    db1.execute('INSERT INTO t VALUES (2)');
    db1.execute('ROLLBACK');
    
    // Clean close after rollback
    db1.close();
    
    const db2 = Database.recover(dir);
    const result = db2.execute('SELECT * FROM t');
    assert.strictEqual(result.rows.length, 1, 'Rolled back rows should not appear');
    assert.strictEqual(result.rows[0].id, 1);
    db2.close();
  });

  it('multiple committed transactions interleaved with uncommitted', () => {
    const db1 = new Database({ dataDir: dir, walSync: 'immediate' });
    db1.execute('CREATE TABLE t (id INT, val TEXT)');
    
    // Committed tx 1
    db1.execute('BEGIN');
    db1.execute("INSERT INTO t VALUES (1, 'a')");
    db1.execute('COMMIT');
    
    // Auto-committed
    db1.execute("INSERT INTO t VALUES (2, 'b')");
    
    // Committed tx 2
    db1.execute('BEGIN');
    db1.execute("INSERT INTO t VALUES (3, 'c')");
    db1.execute('COMMIT');
    
    // Uncommitted (crash)
    db1.execute('BEGIN');
    db1.execute("INSERT INTO t VALUES (4, 'd')");
    db1.execute("DELETE FROM t WHERE id = 1");
    db1.close();
    
    const db2 = Database.recover(dir);
    const result = db2.execute('SELECT * FROM t ORDER BY id');
    
    assert.strictEqual(result.rows.length, 3, 'Should have 3 committed rows');
    assert.deepStrictEqual(
      result.rows.map(r => r.id),
      [1, 2, 3],
      'All committed rows should survive, uncommitted delete should not'
    );
    db2.close();
  });

  it('crash during UPDATE should not partially apply', () => {
    const db1 = new Database({ dataDir: dir, walSync: 'immediate' });
    db1.execute('CREATE TABLE accounts (id INT, balance INT)');
    db1.execute('INSERT INTO accounts VALUES (1, 1000)');
    db1.execute('INSERT INTO accounts VALUES (2, 1000)');
    
    // Transfer: debit one account, credit another
    db1.execute('BEGIN');
    db1.execute('UPDATE accounts SET balance = 500 WHERE id = 1');
    db1.execute('UPDATE accounts SET balance = 1500 WHERE id = 2');
    // CRASH before commit
    db1.close();
    
    const db2 = Database.recover(dir);
    const result = db2.execute('SELECT * FROM accounts ORDER BY id');
    
    // Both should have original balances (atomicity)
    assert.strictEqual(result.rows[0].balance, 1000, 'Atomicity: balance should be original');
    assert.strictEqual(result.rows[1].balance, 1000, 'Atomicity: balance should be original');
    
    // Total should be preserved (conservation)
    const total = result.rows.reduce((s, r) => s + r.balance, 0);
    assert.strictEqual(total, 2000, 'Total balance should be conserved');
    db2.close();
  });

  it('committed DELETE should survive crash', () => {
    const db1 = new Database({ dataDir: dir, walSync: 'immediate' });
    db1.execute('CREATE TABLE t (id INT)');
    db1.execute('INSERT INTO t VALUES (1)');
    db1.execute('INSERT INTO t VALUES (2)');
    db1.execute('INSERT INTO t VALUES (3)');
    
    db1.execute('BEGIN');
    db1.execute('DELETE FROM t WHERE id = 2');
    db1.execute('COMMIT');
    
    db1.close();
    
    const db2 = Database.recover(dir);
    const result = db2.execute('SELECT * FROM t ORDER BY id');
    assert.strictEqual(result.rows.length, 2);
    assert.deepStrictEqual(result.rows.map(r => r.id), [1, 3]);
    db2.close();
  });

  it('crash with empty uncommitted transaction', () => {
    const db1 = new Database({ dataDir: dir, walSync: 'immediate' });
    db1.execute('CREATE TABLE t (id INT)');
    db1.execute('INSERT INTO t VALUES (1)');
    
    // Empty transaction
    db1.execute('BEGIN');
    // CRASH
    db1.close();
    
    const db2 = Database.recover(dir);
    const result = db2.execute('SELECT * FROM t');
    assert.strictEqual(result.rows.length, 1);
    db2.close();
  });

  it('double recovery should be idempotent', () => {
    const db1 = new Database({ dataDir: dir, walSync: 'immediate' });
    db1.execute('CREATE TABLE t (id INT, val TEXT)');
    db1.execute("INSERT INTO t VALUES (1, 'hello')");
    db1.close();
    
    // First recovery
    const db2 = Database.recover(dir);
    const r1 = db2.execute('SELECT * FROM t');
    assert.strictEqual(r1.rows.length, 1);
    db2.close();
    
    // Second recovery  
    const db3 = Database.recover(dir);
    const r2 = db3.execute('SELECT * FROM t');
    assert.strictEqual(r2.rows.length, 1, 'Double recovery should not duplicate rows');
    db3.close();
  });

  it('large transaction rollback on crash should not corrupt database', () => {
    const db1 = new Database({ dataDir: dir, walSync: 'immediate' });
    db1.execute('CREATE TABLE items (id INT, name TEXT)');
    
    // Insert 10 committed rows
    for (let i = 1; i <= 10; i++) {
      db1.execute(`INSERT INTO items VALUES (${i}, 'item_${i}')`);
    }
    
    // Large uncommitted transaction modifying everything
    db1.execute('BEGIN');
    for (let i = 11; i <= 30; i++) {
      db1.execute(`INSERT INTO items VALUES (${i}, 'uncommitted_${i}')`);
    }
    db1.execute("UPDATE items SET name = 'MODIFIED' WHERE id <= 5");
    // CRASH
    db1.close();
    
    const db2 = Database.recover(dir);
    const result = db2.execute('SELECT * FROM items ORDER BY id');
    
    assert.strictEqual(result.rows.length, 10, 'Should only have original 10 rows');
    // Verify no modifications from uncommitted tx
    for (let i = 0; i < 5; i++) {
      assert.strictEqual(result.rows[i].name, `item_${i + 1}`, 
        `Row ${i+1} should have original name, not MODIFIED`);
    }
    db2.close();
  });
});
