// pagelsn.test.js — Tests that pageLSN is stored in page headers and used by recovery

import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { PersistentDatabase } from './persistent-db.js';
import { DiskManager, PAGE_SIZE } from './disk-manager.js';
import { BufferPool } from './buffer-pool.js';
import { FileBackedHeap } from './file-backed-heap.js';
import { FileWAL, recoverFromFileWAL } from './file-wal.js';
import { rmSync, existsSync, mkdirSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const testDir = () => join(tmpdir(), `henrydb-pagelsn-${Date.now()}-${Math.random().toString(36).slice(2)}`);

describe('PageLSN: Page Header Storage', () => {
  const dirs = [];
  afterEach(() => {
    for (const d of dirs) {
      try { if (existsSync(d)) rmSync(d, { recursive: true }); } catch {}
    }
    dirs.length = 0;
  });

  it('pageLSN is persisted in page header bytes', () => {
    const d = testDir(); dirs.push(d);
    mkdirSync(d, { recursive: true });
    
    const dbPath = join(d, 'test.db');
    const walPath = join(d, 'test.wal');
    const dm = new DiskManager(dbPath);
    const bp = new BufferPool(4);
    const wal = new FileWAL(walPath);
    const heap = new FileBackedHeap('test', dm, bp, wal);
    
    const txId = wal.allocateTxId();
    wal.beginTransaction(txId);
    heap._currentTxId = txId;
    heap.insert([1, 'hello']);
    wal.appendCommit(txId);
    
    // Flush to disk
    heap.flush();
    wal.close();
    
    // Read the raw page and check pageLSN at offset 8
    const rawPage = dm.readPage(0);
    const view = new DataView(rawPage.buffer, rawPage.byteOffset, rawPage.byteLength);
    const pageLSN = view.getUint32(8, true);
    
    assert.ok(pageLSN > 0, `pageLSN should be > 0 after insert, got ${pageLSN}`);
    
    dm.close();
  });

  it('pageLSN increases with each modification', () => {
    const d = testDir(); dirs.push(d);
    mkdirSync(d, { recursive: true });
    
    const dbPath = join(d, 'test.db');
    const walPath = join(d, 'test.wal');
    const dm = new DiskManager(dbPath);
    const bp = new BufferPool(4);
    const wal = new FileWAL(walPath);
    const heap = new FileBackedHeap('test', dm, bp, wal);
    
    const tx1 = wal.allocateTxId();
    wal.beginTransaction(tx1);
    heap._currentTxId = tx1;
    heap.insert([1, 'first']);
    const lsn1 = heap.getPageLSN(0);
    
    heap.insert([2, 'second']);
    const lsn2 = heap.getPageLSN(0);
    wal.appendCommit(tx1);
    
    assert.ok(lsn2 > lsn1, `Second LSN (${lsn2}) should be > first (${lsn1})`);
    
    wal.close();
    dm.close();
  });

  it('recovery skips pages with up-to-date pageLSN', () => {
    const d = testDir(); dirs.push(d);
    
    // Phase 1: insert, close cleanly 
    const db = PersistentDatabase.open(d, { poolSize: 4 });
    db.execute('CREATE TABLE lsn_test (id INT PRIMARY KEY, val TEXT)');
    for (let i = 0; i < 10; i++) {
      db.execute(`INSERT INTO lsn_test VALUES (${i}, 'row_${i}')`);
    }
    db.close();
    
    // Phase 2: reopen — pageLSN should prevent unnecessary redo
    const db2 = PersistentDatabase.open(d, { poolSize: 4 });
    const count = db2.execute('SELECT COUNT(*) as cnt FROM lsn_test');
    assert.strictEqual(count.rows[0].cnt, 10, 'All 10 rows should be present');
    
    // Insert more and close
    for (let i = 10; i < 20; i++) {
      db2.execute(`INSERT INTO lsn_test VALUES (${i}, 'row_${i}')`);
    }
    db2.close();
    
    // Phase 3: reopen again — should have 20 rows, no duplicates
    const db3 = PersistentDatabase.open(d, { poolSize: 4 });
    const count2 = db3.execute('SELECT COUNT(*) as cnt FROM lsn_test');
    assert.strictEqual(count2.rows[0].cnt, 20, 'Should have exactly 20 rows, no duplicates');
    db3.close();
  });

  it('pageLSN survives checkpoint + WAL truncation', () => {
    const d = testDir(); dirs.push(d);
    
    const db = PersistentDatabase.open(d, { poolSize: 8 });
    db.execute('CREATE TABLE cp (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 30; i++) {
      db.execute(`INSERT INTO cp VALUES (${i}, ${i})`);
    }
    
    // Checkpoint + truncate WAL
    db.flush();
    db._wal.checkpoint();
    db._wal.truncate();
    db.close();
    
    // Reopen — data in page files with pageLSN, WAL empty
    const db2 = PersistentDatabase.open(d, { poolSize: 8 });
    const count = db2.execute('SELECT COUNT(*) as cnt FROM cp');
    assert.strictEqual(count.rows[0].cnt, 30);
    
    // Add more after truncation
    db2.execute('INSERT INTO cp VALUES (100, 100)');
    db2.close();
    
    // Final check
    const db3 = PersistentDatabase.open(d, { poolSize: 8 });
    const count2 = db3.execute('SELECT COUNT(*) as cnt FROM cp');
    assert.strictEqual(count2.rows[0].cnt, 31);
    db3.close();
  });

  it('pageLSN per-page: only stale pages get replayed', () => {
    const d = testDir(); dirs.push(d);
    mkdirSync(d, { recursive: true });
    
    const dbPath = join(d, 'test.db');
    const walPath = join(d, 'test.wal');
    const dm = new DiskManager(dbPath);
    const bp = new BufferPool(2); // Tiny pool — forces eviction
    const wal = new FileWAL(walPath);
    const heap = new FileBackedHeap('test', dm, bp, wal);
    
    // Insert enough to span 3+ pages
    const txId = wal.allocateTxId();
    wal.beginTransaction(txId);
    heap._currentTxId = txId;
    for (let i = 0; i < 200; i++) {
      heap.insert([i, `val_${i}_${'x'.repeat(80)}`]);
    }
    wal.appendCommit(txId);
    
    // Flush — all pages now have pageLSN
    heap.flush();
    
    // Check that multiple pages exist
    assert.ok(dm.pageCount >= 2, `Should have 2+ pages, got ${dm.pageCount}`);
    
    // Each page should have a non-zero pageLSN
    for (let i = 0; i < dm.pageCount; i++) {
      const rawPage = dm.readPage(i);
      const view = new DataView(rawPage.buffer, rawPage.byteOffset, rawPage.byteLength);
      const pageLSN = view.getUint32(8, true);
      assert.ok(pageLSN > 0, `Page ${i} should have pageLSN > 0, got ${pageLSN}`);
    }
    
    wal.close();
    dm.close();
    
    // Reopen with fresh buffer pool
    const dm2 = new DiskManager(dbPath);
    const bp2 = new BufferPool(2);
    const wal2 = new FileWAL(walPath);
    const heap2 = new FileBackedHeap('test', dm2, bp2, wal2);
    
    const result = recoverFromFileWAL(heap2, wal2);
    
    // Recovery should skip all records (pages are up to date)
    assert.strictEqual(result.redone, 0, `Should skip all records (pageLSN up to date), but redone=${result.redone}`);
    
    // Verify data integrity
    let count = 0;
    for (const { values } of heap2.scan()) {
      assert.ok(values[0] >= 0 && values[0] < 200);
      count++;
    }
    assert.strictEqual(count, 200);
    
    wal2.close();
    dm2.close();
  });
});
