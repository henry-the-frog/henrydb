// file-backed-heap.test.js — Tests for file-backed heap
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { FileBackedHeap } from './file-backed-heap.js';
import { DiskManager } from './disk-manager.js';
import { BufferPool } from './buffer-pool.js';
import { unlinkSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const testFile = () => join(tmpdir(), `henrydb-heap-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);

describe('FileBackedHeap', () => {
  const files = [];
  
  function createHeap(name = 'test', poolSize = 16) {
    const f = testFile();
    files.push(f);
    const dm = new DiskManager(f);
    const bp = new BufferPool(poolSize);
    return { heap: new FileBackedHeap(name, dm, bp), dm, bp, file: f };
  }

  afterEach(() => {
    for (const f of files) {
      try { if (existsSync(f)) unlinkSync(f); } catch {}
    }
    files.length = 0;
  });

  it('insert and get single tuple', () => {
    const { heap, dm } = createHeap();
    const rid = heap.insert([42, 'hello', true]);
    assert.strictEqual(rid.pageId, 0);
    assert.strictEqual(rid.slotIdx, 0);
    
    const values = heap.get(rid.pageId, rid.slotIdx);
    assert.deepStrictEqual(values, [42, 'hello', true]);
    
    heap.flush();
    dm.close();
  });

  it('insert many tuples', () => {
    const { heap, dm } = createHeap();
    const rids = [];
    for (let i = 0; i < 100; i++) {
      rids.push(heap.insert([i, `row_${i}`]));
    }
    
    assert.strictEqual(heap.tupleCount, 100);
    
    // Verify each
    for (let i = 0; i < 100; i++) {
      const values = heap.get(rids[i].pageId, rids[i].slotIdx);
      assert.strictEqual(values[0], i);
      assert.strictEqual(values[1], `row_${i}`);
    }
    
    heap.flush();
    dm.close();
  });

  it('scan returns all tuples', () => {
    const { heap, dm } = createHeap();
    for (let i = 0; i < 50; i++) heap.insert([i]);
    
    const scanned = [...heap.scan()];
    assert.strictEqual(scanned.length, 50);
    
    heap.flush();
    dm.close();
  });

  it('delete removes tuple from scan', () => {
    const { heap, dm } = createHeap();
    const r1 = heap.insert([1, 'keep']);
    const r2 = heap.insert([2, 'delete']);
    const r3 = heap.insert([3, 'keep']);
    
    heap.delete(r2.pageId, r2.slotIdx);
    
    const scanned = [...heap.scan()];
    assert.strictEqual(scanned.length, 2);
    const ids = scanned.map(s => s.values[0]).sort();
    assert.deepStrictEqual(ids, [1, 3]);
    
    heap.flush();
    dm.close();
  });

  it('data persists across close/reopen', () => {
    const f = testFile();
    files.push(f);
    
    // Write
    const dm1 = new DiskManager(f);
    const bp1 = new BufferPool(16);
    const heap1 = new FileBackedHeap('test', dm1, bp1);
    heap1.insert([1, 'Alice']);
    heap1.insert([2, 'Bob']);
    heap1.insert([3, 'Carol']);
    heap1.flush();
    dm1.close();
    
    // Reopen
    const dm2 = new DiskManager(f, { create: false });
    const bp2 = new BufferPool(16);
    const heap2 = new FileBackedHeap('test', dm2, bp2);
    
    const scanned = [...heap2.scan()];
    assert.strictEqual(scanned.length, 3);
    assert.strictEqual(scanned[0].values[0], 1);
    assert.strictEqual(scanned[0].values[1], 'Alice');
    assert.strictEqual(scanned[2].values[0], 3);
    
    dm2.close();
  });

  it('handles buffer pool eviction under memory pressure', () => {
    // Pool size 4 but we use more pages
    const { heap, dm } = createHeap('test', 4);
    
    // Insert enough data to use many pages (32KB pages need ~5000 bytes per tuple to fill 4+ pages)
    const rids = [];
    for (let i = 0; i < 200; i++) {
      rids.push(heap.insert([i, 'x'.repeat(2000)])); // ~2000 bytes per tuple → ~16 per page
    }
    
    // Should work even though pool is tiny
    assert.ok(heap.pageCount > 4, `More pages (${heap.pageCount}) than pool size (4)`);
    
    // Random access — will cause evictions
    for (let i = 0; i < 50; i++) {
      const idx = Math.floor(Math.random() * rids.length);
      const values = heap.get(rids[idx].pageId, rids[idx].slotIdx);
      assert.strictEqual(values[0], idx);
    }
    
    heap.flush();
    dm.close();
  });

  it('pageCount reflects allocated pages', () => {
    const { heap, dm } = createHeap();
    assert.strictEqual(heap.pageCount, 0);
    
    heap.insert([1]);
    assert.ok(heap.pageCount >= 1);
    
    heap.flush();
    dm.close();
  });

  it('handles NULL values', () => {
    const { heap, dm } = createHeap();
    const rid = heap.insert([null, 42, null]);
    const values = heap.get(rid.pageId, rid.slotIdx);
    assert.deepStrictEqual(values, [null, 42, null]);
    
    heap.flush();
    dm.close();
  });

  it('handles empty strings', () => {
    const { heap, dm } = createHeap();
    const rid = heap.insert(['', 'hello', '']);
    const values = heap.get(rid.pageId, rid.slotIdx);
    assert.deepStrictEqual(values, ['', 'hello', '']);
    
    heap.flush();
    dm.close();
  });
});
