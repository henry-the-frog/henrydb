// wal-integration.test.js — WAL + buffer pool write-ahead constraint tests
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { FileBackedHeap } from './file-backed-heap.js';
import { DiskManager } from './disk-manager.js';
import { BufferPool } from './buffer-pool.js';
import { WriteAheadLog } from './wal.js';
import { unlinkSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const testFile = () => join(tmpdir(), `henrydb-wal-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);

describe('WAL Integration', () => {
  const files = [];
  
  afterEach(() => {
    for (const f of files) {
      try { if (existsSync(f)) unlinkSync(f); } catch {}
    }
    files.length = 0;
  });

  it('WAL flush happens before dirty page eviction', () => {
    const f = testFile();
    files.push(f);
    
    const dm = new DiskManager(f);
    const bp = new BufferPool(4); // tiny pool
    const wal = new WriteAheadLog();
    const heap = new FileBackedHeap('test', dm, bp, wal);
    
    // Track WAL flush calls
    let walFlushedToLsn = 0;
    const origForceToLsn = wal.forceToLsn.bind(wal);
    wal.forceToLsn = (lsn) => {
      walFlushedToLsn = Math.max(walFlushedToLsn, lsn);
      origForceToLsn(lsn);
    };
    
    // Insert data and log WAL records
    wal.beginTransaction(1);
    const rid = heap.insert([1, 'data']);
    const lsn = wal.appendInsert(1, 'test', rid.pageId, rid.slotIdx, [1, 'data']);
    heap.setPageLSN(rid.pageId, lsn);
    wal.appendCommit(1); // This auto-flushes
    
    // Force eviction by filling the buffer pool
    for (let i = 0; i < 10; i++) {
      wal.beginTransaction(i + 2);
      const r = heap.insert([i, 'x'.repeat(200)]);
      const l = wal.appendInsert(i + 2, 'test', r.pageId, r.slotIdx, [i, 'x']);
      heap.setPageLSN(r.pageId, l);
      wal.appendCommit(i + 2);
    }
    
    // Evictions should have triggered WAL flush
    assert.ok(walFlushedToLsn > 0, 'WAL was flushed during eviction');
    
    heap.flush();
    dm.close();
  });

  it('write-ahead constraint: WAL flushed up to page LSN', () => {
    const f = testFile();
    files.push(f);
    
    const dm = new DiskManager(f);
    const bp = new BufferPool(8);
    const wal = new WriteAheadLog();
    const heap = new FileBackedHeap('test', dm, bp, wal);
    
    // Insert with WAL logging
    wal.beginTransaction(1);
    const rid = heap.insert([42, 'test']);
    const lsn = wal.appendInsert(1, 'test', rid.pageId, rid.slotIdx, [42, 'test']);
    heap.setPageLSN(rid.pageId, lsn);
    
    // Before commit: WAL not flushed (only commit triggers auto-flush)
    assert.strictEqual(wal.flushedLsn, 0, 'WAL not flushed before commit');
    
    // Commit: auto-flushes WAL
    wal.appendCommit(1);
    assert.ok(wal.flushedLsn > 0, 'WAL flushed after commit');
    
    // Flush pages — should trigger write-ahead check
    heap.flush();
    assert.ok(wal.flushedLsn >= lsn, 'WAL flushed up to page LSN');
    
    dm.close();
  });

  it('without WAL, eviction still works', () => {
    const f = testFile();
    files.push(f);
    
    const dm = new DiskManager(f);
    const bp = new BufferPool(4);
    const heap = new FileBackedHeap('test', dm, bp); // No WAL
    
    // Insert enough to cause eviction
    for (let i = 0; i < 20; i++) {
      heap.insert([i, 'x'.repeat(200)]);
    }
    
    // Should work without errors
    const count = [...heap.scan()].length;
    assert.ok(count > 0);
    
    heap.flush();
    dm.close();
  });

  it('multiple transactions with WAL maintain consistency', () => {
    const f = testFile();
    files.push(f);
    
    const dm = new DiskManager(f);
    const bp = new BufferPool(8);
    const wal = new WriteAheadLog();
    const heap = new FileBackedHeap('test', dm, bp, wal);
    
    // Multiple transactions
    for (let txId = 1; txId <= 10; txId++) {
      wal.beginTransaction(txId);
      const rid = heap.insert([txId, `tx_${txId}`]);
      const lsn = wal.appendInsert(txId, 'test', rid.pageId, rid.slotIdx, [txId, `tx_${txId}`]);
      heap.setPageLSN(rid.pageId, lsn);
      wal.appendCommit(txId);
    }
    
    // Verify all data
    const rows = [...heap.scan()].map(r => r.values);
    assert.strictEqual(rows.length, 10);
    
    // Verify WAL has all records
    const walRecords = wal.readFromStable();
    const commits = walRecords.filter(r => r.type === 4); // WAL_TYPES.COMMIT = 4
    assert.strictEqual(commits.length, 10);
    
    heap.flush();
    dm.close();
  });
});
