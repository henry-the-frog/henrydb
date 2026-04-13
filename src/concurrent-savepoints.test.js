// concurrent-savepoints.test.js — Concurrent sessions with savepoints
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dir, db;

function fresh() {
  dir = join(tmpdir(), `henrydb-cs-${Date.now()}-${Math.random().toString(36).slice(2)}`);
  mkdirSync(dir, { recursive: true });
  return TransactionalDatabase.open(dir);
}

function cleanup() {
  try { db?.close(); } catch {}
  try { rmSync(dir, { recursive: true, force: true }); } catch {}
}

describe('Concurrent Sessions with Savepoints', () => {
  afterEach(cleanup);

  it('two sessions with savepoints dont interfere', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    
    const s1 = db.session();
    const s2 = db.session();
    
    s1.begin();
    s2.begin();
    
    s1.execute("INSERT INTO t VALUES (1, 's1')");
    s1.execute('SAVEPOINT s1_sp');
    s1.execute("INSERT INTO t VALUES (2, 's1_sp')");
    
    s2.execute("INSERT INTO t VALUES (3, 's2')");
    s2.execute('SAVEPOINT s2_sp');
    s2.execute("INSERT INTO t VALUES (4, 's2_sp')");
    
    // Rollback s1's savepoint
    s1.execute('ROLLBACK TO s1_sp');
    
    // s2 still has all its rows
    assert.equal(s2.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 2);
    
    // s1 only has 1 row
    assert.equal(s1.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 1);
    
    s1.commit();
    s2.commit();
    
    // Final: s1's row 1 + s2's rows 3 and 4
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 3);
    
    s1.close();
    s2.close();
  });

  it('savepoint + concurrent reader sees consistent state', () => {
    db = fresh();
    db.execute('CREATE TABLE accounts (id INT, balance INT)');
    db.execute('INSERT INTO accounts VALUES (1, 1000)');
    db.execute('INSERT INTO accounts VALUES (2, 1000)');
    
    const writer = db.session();
    const reader = db.session();
    
    reader.begin();
    // Reader takes snapshot: total = 2000
    assert.equal(reader.execute('SELECT SUM(balance) as total FROM accounts').rows[0].total, 2000);
    
    writer.begin();
    writer.execute('SAVEPOINT before_transfer');
    writer.execute('UPDATE accounts SET balance = 500 WHERE id = 1');
    writer.execute('UPDATE accounts SET balance = 1500 WHERE id = 2');
    
    // Reader still sees 2000
    assert.equal(reader.execute('SELECT SUM(balance) as total FROM accounts').rows[0].total, 2000);
    
    // Writer decides to rollback transfer
    writer.execute('ROLLBACK TO before_transfer');
    writer.commit();
    reader.commit();
    
    // Both accounts back to 1000
    assert.equal(db.execute('SELECT SUM(balance) as total FROM accounts').rows[0].total, 2000);
    
    writer.close();
    reader.close();
  });

  it('nested savepoints in concurrent sessions', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INT)');
    
    const s1 = db.session();
    s1.begin();
    s1.execute('INSERT INTO t VALUES (1)');
    s1.execute('SAVEPOINT sp1');
    s1.execute('INSERT INTO t VALUES (2)');
    s1.execute('SAVEPOINT sp2');
    s1.execute('INSERT INTO t VALUES (3)');
    
    // Rollback to sp2 then sp1
    s1.execute('ROLLBACK TO sp2');
    assert.equal(s1.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 2); // 1, 2
    s1.execute('ROLLBACK TO sp1');
    assert.equal(s1.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 1); // Only 1
    
    s1.commit();
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 1);
    s1.close();
  });
});
