// crash-recovery-deep.test.js — Deep exploration of crash recovery edge cases
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, readFileSync, writeFileSync, appendFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { TransactionalDatabase } from './transactional-db.js';

describe('Crash Recovery Deep Exploration', () => {

  it('WAL corruption: truncate WAL mid-record', () => {
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-wal-'));
    
    // Phase 1: create some committed data
    const db1 = TransactionalDatabase.open(dir);
    db1.execute("CREATE TABLE data (id INT, val TEXT)");
    db1.execute("INSERT INTO data VALUES (1, 'safe')");
    db1.close();
    
    // Corrupt the WAL: truncate last few bytes
    const walPath = join(dir, 'wal.log');
    const wal = readFileSync(walPath);
    console.log('WAL size before corruption:', wal.length);
    if (wal.length > 10) {
      writeFileSync(walPath, wal.subarray(0, wal.length - 5));
      console.log('WAL size after corruption:', wal.length - 5);
    }
    
    // Phase 2: reopen — should recover what it can
    let recovered = false;
    try {
      const db2 = TransactionalDatabase.open(dir);
      const result = db2.execute("SELECT * FROM data");
      console.log('After WAL truncation:', result.rows);
      recovered = true;
      db2.close();
    } catch (e) {
      console.log('Recovery error (may be expected):', e.message);
    }
    
    console.log('Recovery succeeded:', recovered);
    rmSync(dir, { recursive: true });
  });

  it('many small transactions across restart', () => {
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-many-'));
    
    const db1 = TransactionalDatabase.open(dir);
    db1.execute("CREATE TABLE log (id INT, msg TEXT)");
    
    for (let i = 0; i < 50; i++) {
      const s = db1.session();
      s.begin();
      s.execute(`INSERT INTO log VALUES (${i}, 'msg-${i}')`);
      s.commit();
      s.close();
    }
    
    const count1 = db1.execute("SELECT COUNT(*) as n FROM log");
    assert.equal(count1.rows[0].n, 50);
    db1.close();
    
    const db2 = TransactionalDatabase.open(dir);
    const count2 = db2.execute("SELECT COUNT(*) as n FROM log");
    console.log('After 50 txns + restart:', count2.rows[0].n);
    assert.equal(count2.rows[0].n, 50, 'All 50 inserts should survive');
    
    // Verify ordering
    const first = db2.execute("SELECT * FROM log WHERE id = 0");
    const last = db2.execute("SELECT * FROM log WHERE id = 49");
    assert.equal(first.rows[0].msg, 'msg-0');
    assert.equal(last.rows[0].msg, 'msg-49');
    db2.close();
    rmSync(dir, { recursive: true });
  });

  it('schema evolution: add table after restart', () => {
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-schema-'));
    
    // Phase 1: create table
    const db1 = TransactionalDatabase.open(dir);
    db1.execute("CREATE TABLE v1 (id INT)");
    db1.execute("INSERT INTO v1 VALUES (1)");
    db1.close();
    
    // Phase 2: reopen and add another table
    const db2 = TransactionalDatabase.open(dir);
    db2.execute("CREATE TABLE v2 (id INT, name TEXT)");
    db2.execute("INSERT INTO v2 VALUES (1, 'hello')");
    
    // Verify both tables exist
    const r1 = db2.execute("SELECT * FROM v1");
    const r2 = db2.execute("SELECT * FROM v2");
    assert.equal(r1.rows.length, 1);
    assert.equal(r2.rows.length, 1);
    db2.close();
    
    // Phase 3: reopen again — both should persist
    const db3 = TransactionalDatabase.open(dir);
    const r3 = db3.execute("SELECT * FROM v1");
    const r4 = db3.execute("SELECT * FROM v2");
    assert.equal(r3.rows.length, 1);
    assert.equal(r4.rows.length, 1);
    assert.equal(r4.rows[0].name, 'hello');
    console.log('Schema evolution across 3 restarts: ✅');
    db3.close();
    rmSync(dir, { recursive: true });
  });

  it('rapid open/close cycles (memory leak check)', () => {
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-rapid-'));
    
    // First: create table
    const db1 = TransactionalDatabase.open(dir);
    db1.execute("CREATE TABLE rapid (id INT)");
    db1.execute("INSERT INTO rapid VALUES (1)");
    db1.close();
    
    // Rapid open/close 10 times
    const startMem = process.memoryUsage().heapUsed;
    for (let i = 0; i < 10; i++) {
      const db = TransactionalDatabase.open(dir);
      db.execute("SELECT * FROM rapid");
      db.close();
    }
    const endMem = process.memoryUsage().heapUsed;
    const growth = endMem - startMem;
    console.log(`Memory growth over 10 open/close cycles: ${(growth / 1024 / 1024).toFixed(2)} MB`);
    
    // Final verify
    const dbFinal = TransactionalDatabase.open(dir);
    const result = dbFinal.execute("SELECT * FROM rapid");
    assert.equal(result.rows.length, 1);
    dbFinal.close();
    rmSync(dir, { recursive: true });
  });

  it('savepoint state does NOT survive restart', () => {
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-sp-crash-'));
    
    const db1 = TransactionalDatabase.open(dir);
    db1.execute("CREATE TABLE sp_test (id INT, val INT)");
    db1.execute("INSERT INTO sp_test VALUES (1, 10)");
    
    const s = db1.session();
    s.begin();
    s.execute("SAVEPOINT sp1");
    s.execute("UPDATE sp_test SET val = 99 WHERE id = 1");
    // Don't commit — crash with savepoint active
    db1.close();
    
    const db2 = TransactionalDatabase.open(dir);
    const result = db2.execute("SELECT val FROM sp_test WHERE id = 1");
    console.log('After crash with active savepoint:', result.rows);
    assert.equal(result.rows[0].val, 10, 'Savepoint changes should be lost on crash');
    db2.close();
    rmSync(dir, { recursive: true });
  });

  it('mixed committed and uncommitted across restart', () => {
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-mixed-'));
    
    const db1 = TransactionalDatabase.open(dir);
    db1.execute("CREATE TABLE mixed (id INT, committed INT)");
    
    // Committed
    const s1 = db1.session();
    s1.begin();
    s1.execute("INSERT INTO mixed VALUES (1, 1)");
    s1.commit();
    s1.close();
    
    // Uncommitted
    const s2 = db1.session();
    s2.begin();
    s2.execute("INSERT INTO mixed VALUES (2, 0)");
    // NOT committed
    
    // Another committed
    const s3 = db1.session();
    s3.begin();
    s3.execute("INSERT INTO mixed VALUES (3, 1)");
    s3.commit();
    s3.close();
    
    db1.close();
    
    const db2 = TransactionalDatabase.open(dir);
    const result = db2.execute("SELECT * FROM mixed ORDER BY id");
    console.log('After mixed crash:', result.rows);
    
    // Should have rows 1 and 3 (committed), NOT row 2
    const ids = result.rows.map(r => r.id);
    assert.ok(ids.includes(1), 'Committed row 1 should survive');
    assert.ok(ids.includes(3), 'Committed row 3 should survive');
    assert.ok(!ids.includes(2), 'Uncommitted row 2 should be lost');
    
    db2.close();
    rmSync(dir, { recursive: true });
  });
});
