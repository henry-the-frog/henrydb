// wal-crash-depth.test.js — Deep WAL crash recovery tests
// Tests: abort recovery, partial writes, interleaved transactions, WAL truncation

import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { DiskManager, PAGE_SIZE } from './disk-manager.js';
import { BufferPool } from './buffer-pool.js';
import { FileBackedHeap } from './file-backed-heap.js';
import { FileWAL, recoverFromFileWAL } from './file-wal.js';
import { PersistentDatabase } from './persistent-db.js';
import { rmSync, existsSync, mkdirSync, writeFileSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const testDir = () => join(tmpdir(), `henrydb-walcrash-${Date.now()}-${Math.random().toString(36).slice(2)}`);

function makeHeap(dir, name = 'test') {
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  const dbPath = join(dir, `${name}.db`);
  const walPath = join(dir, `${name}.wal`);
  const dm = new DiskManager(dbPath);
  const bp = new BufferPool(4);
  const wal = new FileWAL(walPath);
  const heap = new FileBackedHeap(name, dm, bp, wal);
  return { dm, bp, wal, heap, dbPath, walPath };
}

function reopenHeap(dir, name = 'test') {
  const dbPath = join(dir, `${name}.db`);
  const walPath = join(dir, `${name}.wal`);
  const dm = new DiskManager(dbPath);
  const bp = new BufferPool(4);
  const wal = new FileWAL(walPath);
  const heap = new FileBackedHeap(name, dm, bp, wal);
  return { dm, bp, wal, heap };
}

function closeAll(...parts) {
  for (const p of parts) {
    if (p.wal) try { p.wal.close(); } catch {}
    if (p.dm) try { p.dm.close(); } catch {}
  }
}

function scanAll(heap) {
  const rows = [];
  for (const { values } of heap.scan()) rows.push(values);
  return rows;
}

describe('WAL Crash Recovery: Abort Handling', () => {
  const dirs = [];
  afterEach(() => {
    for (const d of dirs) {
      try { if (existsSync(d)) rmSync(d, { recursive: true }); } catch {}
    }
    dirs.length = 0;
  });

  it('aborted transaction data not visible after recovery', () => {
    const d = testDir(); dirs.push(d);
    const { dm, bp, wal, heap } = makeHeap(d);

    // Committed txn
    const tx1 = wal.allocateTxId();
    wal.beginTransaction(tx1);
    heap._currentTxId = tx1;
    heap.insert([1, 'committed']);
    heap.insert([2, 'committed']);
    wal.appendCommit(tx1);

    // Aborted txn
    const tx2 = wal.allocateTxId();
    wal.beginTransaction(tx2);
    heap._currentTxId = tx2;
    heap.insert([3, 'aborted']);
    heap.insert([4, 'aborted']);
    wal.appendAbort(tx2);

    heap.flush();
    wal.flush();
    dm.close();

    // Reopen and recover
    const r = reopenHeap(d);
    recoverFromFileWAL(r.heap, r.wal);

    const rows = scanAll(r.heap);
    assert.strictEqual(rows.length, 2, `Expected 2 committed rows, got ${rows.length}`);
    assert.deepStrictEqual(rows.map(r => r[0]).sort(), [1, 2]);
    closeAll(r);
  });

  it('interleaved committed and aborted transactions', () => {
    const d = testDir(); dirs.push(d);
    const { dm, bp, wal, heap } = makeHeap(d);

    // tx1: commit
    const tx1 = wal.allocateTxId();
    wal.beginTransaction(tx1);
    heap._currentTxId = tx1;
    heap.insert([1, 'tx1']);

    // tx2: will abort
    const tx2 = wal.allocateTxId();
    wal.beginTransaction(tx2);
    heap._currentTxId = tx2;
    heap.insert([2, 'tx2_abort']);

    // tx1: more inserts then commit
    heap._currentTxId = tx1;
    heap.insert([3, 'tx1']);
    wal.appendCommit(tx1);

    // tx2: more inserts then abort
    heap._currentTxId = tx2;
    heap.insert([4, 'tx2_abort']);
    wal.appendAbort(tx2);

    // tx3: commit
    const tx3 = wal.allocateTxId();
    wal.beginTransaction(tx3);
    heap._currentTxId = tx3;
    heap.insert([5, 'tx3']);
    wal.appendCommit(tx3);

    heap.flush();
    wal.flush();
    dm.close();

    const r = reopenHeap(d);
    recoverFromFileWAL(r.heap, r.wal);

    const rows = scanAll(r.heap);
    const ids = rows.map(r => r[0]).sort();
    assert.deepStrictEqual(ids, [1, 3, 5], 'Only committed txn rows survive');
    closeAll(r);
  });

  it('no-commit transaction (simulated in-flight crash) excluded', () => {
    const d = testDir(); dirs.push(d);
    const { dm, bp, wal, heap } = makeHeap(d);

    // Committed txn
    const tx1 = wal.allocateTxId();
    wal.beginTransaction(tx1);
    heap._currentTxId = tx1;
    heap.insert([1, 'safe']);
    wal.appendCommit(tx1);

    // In-flight txn (no commit, no abort — process died)
    const tx2 = wal.allocateTxId();
    wal.beginTransaction(tx2);
    heap._currentTxId = tx2;
    heap.insert([2, 'inflight']);
    heap.insert([3, 'inflight']);
    // No commit or abort — simulating crash

    heap.flush();
    wal.flush();
    dm.close();

    const r = reopenHeap(d);
    recoverFromFileWAL(r.heap, r.wal);

    const rows = scanAll(r.heap);
    assert.strictEqual(rows.length, 1);
    assert.strictEqual(rows[0][0], 1);
    closeAll(r);
  });
});

describe('WAL Crash Recovery: Partial Writes', () => {
  const dirs = [];
  afterEach(() => {
    for (const d of dirs) {
      try { if (existsSync(d)) rmSync(d, { recursive: true }); } catch {}
    }
    dirs.length = 0;
  });

  it('truncated WAL file: recovers what is parseable', () => {
    const d = testDir(); dirs.push(d);
    const { dm, bp, wal, heap, walPath } = makeHeap(d);

    // Write committed data
    const tx1 = wal.allocateTxId();
    wal.beginTransaction(tx1);
    heap._currentTxId = tx1;
    for (let i = 0; i < 20; i++) {
      heap.insert([i, `row_${i}`]);
    }
    wal.appendCommit(tx1);
    heap.flush();
    wal.flush();

    // Save the WAL file size after the good data
    const goodWalSize = wal.fileSize;

    // Write more data (tx2) and flush
    const tx2 = wal.allocateTxId();
    wal.beginTransaction(tx2);
    heap._currentTxId = tx2;
    heap.insert([100, 'extra']);
    wal.appendCommit(tx2);
    wal.flush();

    wal.close();
    dm.close();

    // Truncate WAL to remove tx2 records (simulating partial write / torn page)
    const walBuf = readFileSync(walPath);
    writeFileSync(walPath, walBuf.subarray(0, goodWalSize));

    // Reopen — should recover tx1 (20 rows) but not tx2
    const r = reopenHeap(d);
    recoverFromFileWAL(r.heap, r.wal);

    const rows = scanAll(r.heap);
    assert.strictEqual(rows.length, 20, `Expected 20 rows from tx1, got ${rows.length}`);
    closeAll(r);
  });

  it('corrupted WAL bytes: recovery stops at corruption, prior records ok', () => {
    const d = testDir(); dirs.push(d);
    const { dm, bp, wal, heap, walPath } = makeHeap(d);

    // Write committed data
    const tx1 = wal.allocateTxId();
    wal.beginTransaction(tx1);
    heap._currentTxId = tx1;
    heap.insert([1, 'good']);
    heap.insert([2, 'good']);
    wal.appendCommit(tx1);
    heap.flush();
    wal.flush();

    const goodSize = wal.fileSize;

    wal.close();
    dm.close();

    // Append garbage bytes
    const walBuf = readFileSync(walPath);
    const garbage = Buffer.alloc(100, 0xFF);
    writeFileSync(walPath, Buffer.concat([walBuf, garbage]));

    // Reopen — recovery should parse the valid records and stop at garbage
    const r = reopenHeap(d);
    recoverFromFileWAL(r.heap, r.wal);

    const rows = scanAll(r.heap);
    assert.strictEqual(rows.length, 2);
    closeAll(r);
  });
});

describe('WAL Crash Recovery: Updates and Deletes', () => {
  const dirs = [];
  afterEach(() => {
    for (const d of dirs) {
      try { if (existsSync(d)) rmSync(d, { recursive: true }); } catch {}
    }
    dirs.length = 0;
  });

  it('UPDATE + crash: updated values survive recovery', () => {
    const d = testDir(); dirs.push(d);
    
    // Phase 1: insert via PersistentDatabase
    const db = PersistentDatabase.open(d, { poolSize: 4 });
    db.execute('CREATE TABLE upd (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 10; i++) {
      db.execute(`INSERT INTO upd VALUES (${i}, ${i * 10})`);
    }
    
    // Update some
    db.execute('UPDATE upd SET val = 999 WHERE id = 5');
    db.execute('UPDATE upd SET val = 888 WHERE id = 0');
    
    // Simulate crash: flush WAL, save catalog, but don't close cleanly
    db._wal.flush();
    db._saveCatalog();
    
    // Reopen
    const db2 = PersistentDatabase.open(d, { poolSize: 4 });
    const r5 = db2.execute('SELECT val FROM upd WHERE id = 5');
    assert.strictEqual(r5.rows[0].val, 999);
    
    const r0 = db2.execute('SELECT val FROM upd WHERE id = 0');
    assert.strictEqual(r0.rows[0].val, 888);
    
    const count = db2.execute('SELECT COUNT(*) as cnt FROM upd');
    assert.strictEqual(count.rows[0].cnt, 10);
    
    db2.close();
  });

  it('DELETE + crash: deleted rows stay deleted after recovery', () => {
    const d = testDir(); dirs.push(d);
    
    const db = PersistentDatabase.open(d, { poolSize: 4 });
    db.execute('CREATE TABLE del (id INT PRIMARY KEY, val TEXT)');
    for (let i = 0; i < 20; i++) {
      db.execute(`INSERT INTO del VALUES (${i}, 'row_${i}')`);
    }
    
    // Delete evens
    for (let i = 0; i < 20; i += 2) {
      db.execute(`DELETE FROM del WHERE id = ${i}`);
    }
    
    db._wal.flush();
    db._saveCatalog();
    
    const db2 = PersistentDatabase.open(d, { poolSize: 4 });
    const count = db2.execute('SELECT COUNT(*) as cnt FROM del');
    assert.strictEqual(count.rows[0].cnt, 10);
    
    // Verify only odds remain
    const rows = db2.execute('SELECT id FROM del ORDER BY id');
    for (let i = 0; i < 10; i++) {
      assert.strictEqual(rows.rows[i].id, i * 2 + 1);
    }
    
    db2.close();
  });
});

describe('WAL Recovery: Checkpoint and Truncation', () => {
  const dirs = [];
  afterEach(() => {
    for (const d of dirs) {
      try { if (existsSync(d)) rmSync(d, { recursive: true }); } catch {}
    }
    dirs.length = 0;
  });

  it('WAL checkpoint + truncate: data in page files, not WAL', () => {
    const d = testDir(); dirs.push(d);
    
    const db = PersistentDatabase.open(d, { poolSize: 8 });
    db.execute('CREATE TABLE cp (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 50; i++) {
      db.execute(`INSERT INTO cp VALUES (${i}, ${i})`);
    }
    
    // Flush all dirty pages + checkpoint + truncate WAL
    db.flush();
    db._wal.checkpoint();
    db._wal.truncate();
    
    // WAL should be empty now
    assert.strictEqual(db._wal.fileSize, 0, 'WAL should be empty after truncate');
    
    db.close();
    
    // Reopen — data should come from page files, not WAL
    const db2 = PersistentDatabase.open(d, { poolSize: 8 });
    const count = db2.execute('SELECT COUNT(*) as cnt FROM cp');
    assert.strictEqual(count.rows[0].cnt, 50);
    
    // Add more data after checkpoint
    db2.execute('INSERT INTO cp VALUES (100, 100)');
    db2.close();
    
    const db3 = PersistentDatabase.open(d, { poolSize: 8 });
    const count2 = db3.execute('SELECT COUNT(*) as cnt FROM cp');
    assert.strictEqual(count2.rows[0].cnt, 51);
    db3.close();
  });

  it('WAL grows across sessions without checkpoint', () => {
    const d = testDir(); dirs.push(d);
    
    let db = PersistentDatabase.open(d, { poolSize: 4 });
    db.execute('CREATE TABLE grow (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 10; i++) {
      db.execute(`INSERT INTO grow VALUES (${i}, ${i})`);
    }
    const walSize1 = db._wal.fileSize;
    db.close();
    
    // Second session — WAL should grow
    db = PersistentDatabase.open(d, { poolSize: 4 });
    for (let i = 10; i < 20; i++) {
      db.execute(`INSERT INTO grow VALUES (${i}, ${i})`);
    }
    db.close();
    
    // Third session — verify all data
    db = PersistentDatabase.open(d, { poolSize: 4 });
    const count = db.execute('SELECT COUNT(*) as cnt FROM grow');
    assert.strictEqual(count.rows[0].cnt, 20);
    db.close();
  });
});
