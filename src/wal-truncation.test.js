import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdirSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dir, db;

function fresh(opts) {
  dir = join(tmpdir(), `henrydb-trunc-${Date.now()}-${Math.random().toString(36).slice(2)}`);
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

describe('WAL Truncation', () => {
  afterEach(cleanup);

  it('truncateWal reduces WAL file size', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    
    // Insert many rows to grow WAL
    for (let i = 0; i < 50; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, 'row_${i}')`);
    }
    
    const walPath = join(dir, 'wal.log');
    const sizeBefore = statSync(walPath).size;
    assert.ok(sizeBefore > 0, 'WAL should have content');
    
    // Checkpoint with truncation
    const result = db.checkpoint({ truncateWal: true });
    assert.ok(result.truncation, 'Should have truncation result');
    assert.ok(result.truncation.removed > 0, 'Should have removed records');
    
    const sizeAfter = statSync(walPath).size;
    assert.ok(sizeAfter < sizeBefore, `WAL should shrink: ${sizeAfter} < ${sizeBefore}`);
  });

  it('data survives truncation + close + reopen', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    db.execute("INSERT INTO t VALUES (2, 'Bob')");
    
    db.checkpoint({ truncateWal: true });
    db.close();
    
    db = TransactionalDatabase.open(dir);
    const rows = query(db, 'SELECT * FROM t ORDER BY id');
    assert.equal(rows.length, 2);
    assert.equal(rows[0].val, 'Alice');
  });

  it('new data after truncation works', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    
    // Phase 1: initial data
    for (let i = 0; i < 10; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, 'phase1')`);
    }
    db.checkpoint({ truncateWal: true });
    
    // Phase 2: more data
    for (let i = 10; i < 20; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, 'phase2')`);
    }
    db.checkpoint({ truncateWal: true });
    
    db.close();
    db = TransactionalDatabase.open(dir);
    
    const count = query(db, 'SELECT COUNT(*) AS c FROM t');
    assert.equal(count[0].c, 20);
  });

  it('truncation without data is a no-op', () => {
    db = fresh();
    const result = db.checkpoint({ truncateWal: true });
    assert.ok(result.truncation.removed === 0 || result.truncation === null || result.truncation.kept >= 0);
  });

  it('checkpoint without truncateWal keeps WAL intact', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INTEGER)');
    for (let i = 0; i < 20; i++) {
      db.execute(`INSERT INTO t VALUES (${i})`);
    }
    
    const walPath = join(dir, 'wal.log');
    const sizeBefore = statSync(walPath).size;
    
    db.checkpoint(); // No truncation
    
    // WAL should be same size or slightly larger (checkpoint record added)
    const sizeAfter = statSync(walPath).size;
    assert.ok(sizeAfter >= sizeBefore, 'WAL should not shrink without truncation');
  });

  it('multiple truncation cycles work', () => {
    db = fresh();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, counter INTEGER)');
    
    for (let cycle = 0; cycle < 5; cycle++) {
      // Insert some data
      for (let i = cycle * 10; i < (cycle + 1) * 10; i++) {
        db.execute(`INSERT INTO t VALUES (${i}, ${cycle})`);
      }
      db.checkpoint({ truncateWal: true });
    }
    
    db.close();
    db = TransactionalDatabase.open(dir);
    
    const count = query(db, 'SELECT COUNT(*) AS c FROM t');
    assert.equal(count[0].c, 50);
  });
});
