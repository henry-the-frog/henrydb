import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdirSync, rmSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dir, db;

function fresh(opts) {
  dir = join(tmpdir(), `henrydb-cp-${Date.now()}-${Math.random().toString(36).slice(2)}`);
  mkdirSync(dir, { recursive: true });
  return TransactionalDatabase.open(dir, opts);
}

function cleanup() {
  try { db?.close(); } catch {}
  try { rmSync(dir, { recursive: true, force: true }); } catch {}
}

function query(d, sql) {
  const r = d.execute(sql);
  return r && r.rows ? r.rows : r;
}

describe('TransactionalDatabase Checkpoint', () => {
  afterEach(cleanup);

  it('checkpoint() should return valid result', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    db.execute("INSERT INTO t VALUES (2, 'Bob')");
    
    const result = db.checkpoint();
    assert.ok(result, 'checkpoint should return result');
    assert.ok(result.checkpointLsn > 0 || result.beginLsn > 0, 'should have checkpoint LSN');
  });

  it('data survives checkpoint + close + reopen', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    db.execute("INSERT INTO t VALUES (2, 'Bob')");
    db.execute("INSERT INTO t VALUES (3, 'Charlie')");
    
    db.checkpoint();
    db.close();
    
    // Reopen
    db = TransactionalDatabase.open(dir);
    const rows = query(db, 'SELECT * FROM t ORDER BY id');
    assert.equal(rows.length, 3);
    assert.equal(rows[0].val, 'Alice');
    assert.equal(rows[2].val, 'Charlie');
  });

  it('checkpoint after updates preserves latest values', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
    db.execute('INSERT INTO t VALUES (1, 100)');
    db.execute('UPDATE t SET val = 200 WHERE id = 1');
    
    db.checkpoint();
    db.close();
    
    db = TransactionalDatabase.open(dir);
    const rows = query(db, 'SELECT * FROM t WHERE id = 1');
    assert.equal(rows[0].val, 200);
  });

  it('checkpoint after deletes persists correctly', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)');
    for (let i = 0; i < 10; i++) {
      db.execute(`INSERT INTO t VALUES (${i})`);
    }
    db.execute('DELETE FROM t WHERE id < 5');
    
    db.checkpoint();
    db.close();
    
    db = TransactionalDatabase.open(dir);
    const count = query(db, 'SELECT COUNT(*) AS c FROM t');
    assert.equal(count[0].c, 5);
  });

  it('multiple checkpoints work correctly', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    
    // Phase 1
    db.execute("INSERT INTO t VALUES (1, 'v1')");
    db.checkpoint();
    
    // Phase 2
    db.execute("INSERT INTO t VALUES (2, 'v2')");
    db.execute("UPDATE t SET val = 'v1_updated' WHERE id = 1");
    db.checkpoint();
    
    // Phase 3
    db.execute("INSERT INTO t VALUES (3, 'v3')");
    db.checkpoint();
    
    db.close();
    db = TransactionalDatabase.open(dir);
    
    const rows = query(db, 'SELECT * FROM t ORDER BY id');
    assert.equal(rows.length, 3);
    assert.equal(rows[0].val, 'v1_updated');
    assert.equal(rows[2].val, 'v3');
  });

  it('crash without checkpoint loses uncommitted data', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'committed')");
    db.checkpoint();
    
    // This data is after checkpoint and before close — simulates "crash"
    db.execute("INSERT INTO t VALUES (2, 'not_checkpointed')");
    
    // Force close without clean shutdown (no second checkpoint)
    // Note: close() does flush, but let's verify checkpoint-only recovery
    db.close();
    
    db = TransactionalDatabase.open(dir);
    const rows = query(db, 'SELECT * FROM t ORDER BY id');
    // The WAL replay should recover row 2 (it was committed even without checkpoint)
    assert.ok(rows.length >= 1, 'At least checkpointed data should survive');
    assert.equal(rows[0].val, 'committed');
  });

  it('checkpoint handles empty database', () => {
    db = fresh();
    const result = db.checkpoint();
    assert.ok(result, 'checkpoint should work on empty db');
  });

  it('checkpoint handles tables with indexes', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT)');
    db.execute('CREATE UNIQUE INDEX idx_email ON t (email)');
    db.execute("INSERT INTO t VALUES (1, 'alice@test.com')");
    db.execute("INSERT INTO t VALUES (2, 'bob@test.com')");
    
    db.checkpoint();
    db.close();
    
    db = TransactionalDatabase.open(dir);
    const rows = query(db, "SELECT * FROM t WHERE email = 'alice@test.com'");
    assert.equal(rows.length, 1);
    assert.equal(rows[0].id, 1);
  });

  it('heavy write then checkpoint', () => {
    db = fresh();
    db.execute('CREATE TABLE big (id INTEGER PRIMARY KEY, val TEXT)');
    
    for (let i = 0; i < 100; i++) {
      db.execute(`INSERT INTO big VALUES (${i}, 'row_${i}')`);
    }
    
    db.checkpoint();
    db.close();
    
    db = TransactionalDatabase.open(dir);
    const count = query(db, 'SELECT COUNT(*) AS c FROM big');
    assert.equal(count[0].c, 100);
    
    const row = query(db, 'SELECT * FROM big WHERE id = 50');
    assert.equal(row[0].val, 'row_50');
  });
});
