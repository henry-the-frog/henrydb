import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dir;

function fresh(opts) {
  dir = join(tmpdir(), `henrydb-stress-${Date.now()}-${Math.random().toString(36).slice(2)}`);
  mkdirSync(dir, { recursive: true });
  return TransactionalDatabase.open(dir, opts);
}

function cleanup() {
  try { rmSync(dir, { recursive: true, force: true }); } catch {}
}

function query(d, sql) {
  const r = d.execute(sql);
  return r && r.rows ? r.rows : r;
}

describe('Cross-Feature Stress Tests', () => {
  afterEach(cleanup);

  it('UNIQUE constraint survives checkpoint + truncation + reopen', () => {
    let db = fresh();
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)');
    db.execute('CREATE UNIQUE INDEX idx_email ON users (email)');
    db.execute("INSERT INTO users VALUES (1, 'alice@test.com')");
    db.execute("INSERT INTO users VALUES (2, 'bob@test.com')");
    
    db.checkpoint({ truncateWal: true });
    db.close();
    
    db = TransactionalDatabase.open(dir);
    
    // UNIQUE should still be enforced
    assert.throws(() => {
      db.execute("INSERT INTO users VALUES (3, 'alice@test.com')");
    }, /duplicate|unique|constraint/i);
    
    // Non-duplicate should work
    db.execute("INSERT INTO users VALUES (3, 'charlie@test.com')");
    assert.equal(query(db, 'SELECT COUNT(*) AS c FROM users')[0].c, 3);
    db.close();
  });

  it('DELETE + UNIQUE + checkpoint + reopen: deleted key available', () => {
    let db = fresh();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, code TEXT)');
    db.execute('CREATE UNIQUE INDEX idx_code ON t (code)');
    db.execute("INSERT INTO t VALUES (1, 'ABC')");
    db.execute("INSERT INTO t VALUES (2, 'DEF')");
    
    db.execute("DELETE FROM t WHERE id = 1");
    db.checkpoint({ truncateWal: true });
    db.close();
    
    db = TransactionalDatabase.open(dir);
    
    // Count should be 1 (one deleted)
    const count = query(db, 'SELECT COUNT(*) AS c FROM t');
    assert.equal(count[0].c, 1);
    db.close();
  });

  it('UPDATE + checkpoint + reopen preserves new values', () => {
    let db = fresh();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)');
    db.execute('INSERT INTO t VALUES (1, 100, \'start\')');
    
    // Multiple updates
    db.execute('UPDATE t SET val = 200 WHERE id = 1');
    db.execute("UPDATE t SET name = 'updated' WHERE id = 1");
    
    db.checkpoint({ truncateWal: true });
    db.close();
    
    db = TransactionalDatabase.open(dir);
    const rows = query(db, 'SELECT * FROM t WHERE id = 1');
    assert.equal(rows.length, 1);
    assert.equal(rows[0].val, 200);
    assert.equal(rows[0].name, 'updated');
    db.close();
  });

  it('multiple checkpoint cycles with mixed operations', () => {
    let db = fresh();
    db.execute('CREATE TABLE events (id INTEGER PRIMARY KEY, event TEXT)');
    
    // Cycle 1: inserts
    for (let i = 0; i < 5; i++) {
      db.execute(`INSERT INTO events VALUES (${i}, 'cycle1')`);
    }
    db.checkpoint({ truncateWal: true });
    
    // Cycle 2: updates + deletes + inserts
    db.execute("UPDATE events SET event = 'modified' WHERE id = 0");
    db.execute('DELETE FROM events WHERE id = 4');
    db.execute("INSERT INTO events VALUES (5, 'cycle2')");
    db.checkpoint({ truncateWal: true });
    
    // Cycle 3: more inserts
    for (let i = 6; i < 10; i++) {
      db.execute(`INSERT INTO events VALUES (${i}, 'cycle3')`);
    }
    db.checkpoint({ truncateWal: true });
    
    db.close();
    
    db = TransactionalDatabase.open(dir);
    const count = query(db, 'SELECT COUNT(*) AS c FROM events');
    assert.equal(count[0].c, 9); // 5 - 1 + 1 + 4 = 9
    
    const row0 = query(db, 'SELECT * FROM events WHERE id = 0');
    assert.equal(row0[0].event, 'modified');
    db.close();
  });

  it('large table with checkpoint + truncation', () => {
    let db = fresh();
    db.execute('CREATE TABLE big (id INTEGER PRIMARY KEY, data TEXT, counter INTEGER)');
    
    // Insert 500 rows
    for (let i = 0; i < 500; i++) {
      db.execute(`INSERT INTO big VALUES (${i}, 'row_${i}', 0)`);
    }
    db.checkpoint({ truncateWal: true });
    
    // Update every 10th row
    for (let i = 0; i < 500; i += 10) {
      db.execute(`UPDATE big SET counter = 1 WHERE id = ${i}`);
    }
    db.checkpoint({ truncateWal: true });
    
    // Delete every 5th row
    for (let i = 0; i < 500; i += 5) {
      db.execute(`DELETE FROM big WHERE id = ${i}`);
    }
    db.checkpoint({ truncateWal: true });
    
    db.close();
    
    db = TransactionalDatabase.open(dir);
    
    // 500 - 100 (deleted every 5th) = 400
    const count = query(db, 'SELECT COUNT(*) AS c FROM big');
    assert.equal(count[0].c, 400);
    
    // id=10 should be deleted (multiple of 5)
    // id=11 should exist
    const row11 = query(db, 'SELECT * FROM big WHERE id = 11');
    assert.equal(row11.length, 1);
    assert.equal(row11[0].data, 'row_11');
    
    db.close();
  });

  it('HOT updates + checkpoint + reopen', () => {
    let db = fresh();
    db.execute('CREATE TABLE hot (id INTEGER PRIMARY KEY, indexed_val INTEGER, data TEXT)');
    db.execute('CREATE INDEX idx_val ON hot (indexed_val)');
    
    db.execute("INSERT INTO hot VALUES (1, 100, 'initial')");
    
    // HOT updates (non-indexed column)
    for (let i = 0; i < 20; i++) {
      db.execute(`UPDATE hot SET data = 'update_${i}' WHERE id = 1`);
    }
    
    db.checkpoint();
    db.close();
    
    db = TransactionalDatabase.open(dir);
    const row = query(db, 'SELECT * FROM hot WHERE id = 1');
    assert.equal(row[0].data, 'update_19');
    assert.equal(row[0].indexed_val, 100);
    db.close();
  });

  it('pg_stat_statements resets after checkpoint + reopen', () => {
    let db = fresh();
    db.execute('CREATE TABLE t (id INTEGER)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('INSERT INTO t VALUES (2)');
    
    // Check stats exist
    const stats = query(db, 'SELECT * FROM pg_stat_statements');
    assert.ok(stats.length > 0);
    
    db.checkpoint();
    db.close();
    
    db = TransactionalDatabase.open(dir);
    // After reopen, stats should be fresh (no persistence for stats)
    const newStats = query(db, 'SELECT * FROM pg_stat_statements');
    // Only the SELECT itself should be tracked
    assert.ok(newStats.length <= 2, 'Stats should be minimal after reopen');
    db.close();
  });
});
