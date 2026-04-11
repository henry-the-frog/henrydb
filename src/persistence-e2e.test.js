// persistence-e2e.test.js — End-to-end persistence tests
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { FileBackedHeap } from './file-backed-heap.js';
import { DiskManager } from './disk-manager.js';
import { BufferPool } from './buffer-pool.js';
import { WriteAheadLog } from './wal.js';
import { unlinkSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const testFile = () => join(tmpdir(), `henrydb-e2e-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);

describe('Persistence End-to-End', () => {
  const files = [];
  
  afterEach(() => {
    for (const f of files) {
      try { if (existsSync(f)) unlinkSync(f); } catch {}
    }
    files.length = 0;
  });

  it('full lifecycle: create, insert, close, reopen, verify, close', () => {
    const f = testFile();
    files.push(f);
    
    // Phase 1: Create and populate
    {
      const dm = new DiskManager(f);
      const bp = new BufferPool(16);
      const heap = new FileBackedHeap('users', dm, bp);
      
      heap.insert([1, 'Alice', 30, 'alice@example.com']);
      heap.insert([2, 'Bob', 25, 'bob@example.com']);
      heap.insert([3, 'Carol', 35, 'carol@example.com']);
      heap.insert([4, 'Dave', 28, 'dave@example.com']);
      heap.insert([5, 'Eve', 22, 'eve@example.com']);
      
      heap.flush();
      dm.close();
    }
    
    // Phase 2: Reopen and verify
    {
      const dm = new DiskManager(f, { create: false });
      const bp = new BufferPool(16);
      const heap = new FileBackedHeap('users', dm, bp);
      
      const rows = [...heap.scan()].map(r => r.values);
      assert.strictEqual(rows.length, 5);
      
      // Verify specific values
      assert.strictEqual(rows[0][1], 'Alice');
      assert.strictEqual(rows[4][1], 'Eve');
      assert.strictEqual(rows[2][2], 35);
      
      // Add more data
      heap.insert([6, 'Frank', 40, 'frank@example.com']);
      heap.flush();
      dm.close();
    }
    
    // Phase 3: Verify again
    {
      const dm = new DiskManager(f, { create: false });
      const bp = new BufferPool(16);
      const heap = new FileBackedHeap('users', dm, bp);
      
      const rows = [...heap.scan()].map(r => r.values);
      assert.strictEqual(rows.length, 6);
      assert.strictEqual(rows[5][1], 'Frank');
      
      dm.close();
    }
  });

  it('survives eviction storm: more data than buffer pool can hold', () => {
    const f = testFile();
    files.push(f);
    
    const dm = new DiskManager(f);
    const bp = new BufferPool(4); // Tiny pool — forces heavy eviction
    const heap = new FileBackedHeap('big', dm, bp);
    
    // Insert 1000 rows with ~100 byte values to span many pages
    const expected = [];
    for (let i = 0; i < 1000; i++) {
      const row = [i, `name_${i}`, i * 3.14, `data_${'x'.repeat(50)}_${i}`];
      heap.insert(row);
      expected.push(row);
    }
    
    // Verify all data accessible even after evictions
    const scanned = [...heap.scan()].map(r => r.values);
    assert.strictEqual(scanned.length, 1000);
    
    // Spot check
    assert.strictEqual(scanned[0][0], 0);
    assert.strictEqual(scanned[999][0], 999);
    assert.ok(scanned[500][1].startsWith('name_'));
    
    // Flush and reopen
    heap.flush();
    dm.close();
    
    const dm2 = new DiskManager(f, { create: false });
    const bp2 = new BufferPool(4);
    const heap2 = new FileBackedHeap('big', dm2, bp2);
    
    const reopened = [...heap2.scan()].map(r => r.values);
    assert.strictEqual(reopened.length, 1000);
    assert.strictEqual(reopened[42][0], 42);
    
    dm2.close();
  });

  it('delete + persist: deleted rows stay deleted', () => {
    const f = testFile();
    files.push(f);
    
    // Insert and delete
    const dm1 = new DiskManager(f);
    const bp1 = new BufferPool(8);
    const heap1 = new FileBackedHeap('del', dm1, bp1);
    
    const rids = [];
    for (let i = 0; i < 10; i++) rids.push(heap1.insert([i]));
    
    // Delete even-numbered rows
    for (let i = 0; i < 10; i += 2) {
      heap1.delete(rids[i].pageId, rids[i].slotIdx);
    }
    
    const beforeClose = [...heap1.scan()].length;
    assert.strictEqual(beforeClose, 5, '5 rows remain after delete');
    
    heap1.flush();
    dm1.close();
    
    // Reopen and verify deletes persisted
    const dm2 = new DiskManager(f, { create: false });
    const bp2 = new BufferPool(8);
    const heap2 = new FileBackedHeap('del', dm2, bp2);
    
    const afterReopen = [...heap2.scan()].map(r => r.values);
    assert.strictEqual(afterReopen.length, 5);
    const ids = afterReopen.map(r => r[0]).sort();
    assert.deepStrictEqual(ids, [1, 3, 5, 7, 9], 'Only odd rows remain');
    
    dm2.close();
  });

  it('buffer pool stats track hits, misses, evictions', () => {
    const f = testFile();
    files.push(f);
    
    const dm = new DiskManager(f);
    const bp = new BufferPool(4);
    const heap = new FileBackedHeap('stats', dm, bp);
    
    // Insert data across multiple pages
    for (let i = 0; i < 200; i++) {
      heap.insert([i, 'x'.repeat(200)]);
    }
    
    const stats = bp.stats();
    assert.ok(stats.used > 0, 'Pool has used frames');
    assert.ok(stats.misses > 0, 'Had cache misses');
    // With pool size 4 and many pages, should have evictions
    assert.ok(stats.evictions > 0, 'Had evictions');
    assert.ok(typeof stats.hitRate === 'string' && stats.hitRate.includes('%'), 'Hit rate is a percentage string');
    
    heap.flush();
    dm.close();
  });

  it('mixed data types persist correctly', () => {
    const f = testFile();
    files.push(f);
    
    const dm = new DiskManager(f);
    const bp = new BufferPool(8);
    const heap = new FileBackedHeap('types', dm, bp);
    
    heap.insert([42, 'string', 3.14, true, null, 0, '', false]);
    heap.flush();
    dm.close();
    
    const dm2 = new DiskManager(f, { create: false });
    const bp2 = new BufferPool(8);
    const heap2 = new FileBackedHeap('types', dm2, bp2);
    
    const row = [...heap2.scan()][0].values;
    assert.strictEqual(row[0], 42);
    assert.strictEqual(row[1], 'string');
    assert.ok(Math.abs(row[2] - 3.14) < 0.001);
    assert.strictEqual(row[3], true);
    assert.strictEqual(row[4], null);
    assert.strictEqual(row[5], 0);
    assert.strictEqual(row[6], '');
    assert.strictEqual(row[7], false);
    
    dm2.close();
  });
});
