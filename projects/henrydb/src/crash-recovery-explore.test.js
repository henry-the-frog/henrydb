// crash-recovery-explore.test.js — Exploring what happens when we simulate crashes
// This is an EXPLORE task: we're not sure what we'll find.
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, existsSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { TransactionalDatabase } from './transactional-db.js';

describe('Crash Recovery Exploration', () => {

  it('committed data survives close and reopen', () => {
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-crash-'));
    
    // Phase 1: create data and commit
    const db1 = TransactionalDatabase.open(dir);
    db1.execute("CREATE TABLE users (id INT, name TEXT)");
    db1.execute("INSERT INTO users VALUES (1, 'Alice')");
    db1.execute("INSERT INTO users VALUES (2, 'Bob')");
    
    const s = db1.session();
    s.begin();
    s.execute("INSERT INTO users VALUES (3, 'Charlie')");
    s.commit();
    s.close();
    db1.close();
    
    // Phase 2: reopen and verify
    const db2 = TransactionalDatabase.open(dir);
    const result = db2.execute("SELECT * FROM users ORDER BY id");
    console.log('After reopen:', result.rows);
    
    assert.equal(result.rows.length, 3, 'All 3 rows should survive');
    assert.equal(result.rows[0].name, 'Alice');
    assert.equal(result.rows[1].name, 'Bob');
    assert.equal(result.rows[2].name, 'Charlie');
    db2.close();
    rmSync(dir, { recursive: true });
  });

  it('uncommitted transaction data is lost on crash', () => {
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-crash-'));
    
    // Phase 1: start transaction but don't commit
    const db1 = TransactionalDatabase.open(dir);
    db1.execute("CREATE TABLE orders (id INT, total INT)");
    db1.execute("INSERT INTO orders VALUES (1, 100)");
    
    const s = db1.session();
    s.begin();
    s.execute("INSERT INTO orders VALUES (2, 200)"); // NOT committed
    // Simulate crash: close without commit
    db1.close();
    
    // Phase 2: reopen
    const db2 = TransactionalDatabase.open(dir);
    const result = db2.execute("SELECT * FROM orders ORDER BY id");
    console.log('After crash (uncommitted):', result.rows);
    
    // Order 2 should NOT be there (was in uncommitted tx)
    assert.equal(result.rows.length, 1, 'Only committed row should survive');
    assert.equal(result.rows[0].total, 100);
    db2.close();
    rmSync(dir, { recursive: true });
  });

  it('UPDATE data persists after commit + reopen', () => {
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-crash-'));
    
    const db1 = TransactionalDatabase.open(dir);
    db1.execute("CREATE TABLE balance (id INT, val INT)");
    db1.execute("INSERT INTO balance VALUES (1, 1000)");
    
    const s = db1.session();
    s.begin();
    s.execute("UPDATE balance SET val = 500 WHERE id = 1");
    s.commit();
    s.close();
    db1.close();
    
    const db2 = TransactionalDatabase.open(dir);
    const result = db2.execute("SELECT val FROM balance WHERE id = 1");
    console.log('After reopen (UPDATE):', result.rows);
    
    assert.equal(result.rows[0].val, 500, 'Updated value should persist');
    db2.close();
    rmSync(dir, { recursive: true });
  });

  it('DELETE data persists after commit + reopen', () => {
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-crash-'));
    
    const db1 = TransactionalDatabase.open(dir);
    db1.execute("CREATE TABLE items (id INT, name TEXT)");
    db1.execute("INSERT INTO items VALUES (1, 'keep')");
    db1.execute("INSERT INTO items VALUES (2, 'delete_me')");
    
    const s = db1.session();
    s.begin();
    s.execute("DELETE FROM items WHERE id = 2");
    s.commit();
    s.close();
    db1.close();
    
    const db2 = TransactionalDatabase.open(dir);
    const result = db2.execute("SELECT * FROM items ORDER BY id");
    console.log('After reopen (DELETE):', result.rows);
    
    assert.equal(result.rows.length, 1, 'Deleted row should not survive');
    assert.equal(result.rows[0].name, 'keep');
    db2.close();
    rmSync(dir, { recursive: true });
  });

  it('multiple transactions across restart', () => {
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-crash-'));
    
    const db1 = TransactionalDatabase.open(dir);
    db1.execute("CREATE TABLE counter (id INT, val INT)");
    db1.execute("INSERT INTO counter VALUES (1, 0)");
    
    // 5 committed increments
    for (let i = 0; i < 5; i++) {
      const s = db1.session();
      s.begin();
      s.execute("UPDATE counter SET val = val + 10 WHERE id = 1");
      s.commit();
      s.close();
    }
    
    const before = db1.execute("SELECT val FROM counter WHERE id = 1");
    console.log('Before close:', before.rows[0].val);
    assert.equal(before.rows[0].val, 50);
    db1.close();
    
    const db2 = TransactionalDatabase.open(dir);
    const after = db2.execute("SELECT val FROM counter WHERE id = 1");
    console.log('After reopen:', after.rows[0].val);
    assert.equal(after.rows[0].val, 50, 'All increments should persist');
    db2.close();
    rmSync(dir, { recursive: true });
  });

  it('crash during transaction leaves DB consistent', () => {
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-crash-'));
    
    const db1 = TransactionalDatabase.open(dir);
    db1.execute("CREATE TABLE bank (acct INT, bal INT)");
    db1.execute("INSERT INTO bank VALUES (1, 1000)");
    db1.execute("INSERT INTO bank VALUES (2, 1000)");
    
    // Start a transfer, debit done but credit not
    const s = db1.session();
    s.begin();
    s.execute("UPDATE bank SET bal = bal - 500 WHERE acct = 1");
    // Don't do credit, don't commit — crash
    db1.close();
    
    const db2 = TransactionalDatabase.open(dir);
    const result = db2.execute("SELECT * FROM bank ORDER BY acct");
    console.log('After crash mid-transfer:', result.rows);
    
    // Both accounts should be at 1000 (uncommitted tx rolled back)
    const total = result.rows.reduce((sum, r) => sum + r.bal, 0);
    assert.equal(total, 2000, 'Total balance should be preserved (2000)');
    
    db2.close();
    rmSync(dir, { recursive: true });
  });

  it('file listing after operations', () => {
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-crash-'));
    
    const db = TransactionalDatabase.open(dir);
    db.execute("CREATE TABLE test (id INT)");
    db.execute("INSERT INTO test VALUES (1)");
    db.close();
    
    const files = readdirSync(dir);
    console.log('Files in data dir:', files);
    // Should have catalog.json, wal.log, and table data file
    assert.ok(files.includes('catalog.json'), 'Should have catalog');
    
    rmSync(dir, { recursive: true });
  });
});
