import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Page, encodeTuple, decodeTuple, BufferPool, HeapFile, PAGE_SIZE } from './page.js';

// ===== Page Tests =====
describe('Page', () => {
  it('starts empty', () => {
    const p = new Page(1);
    assert.equal(p.getPageId(), 1);
    assert.equal(p.getNumSlots(), 0);
    assert.equal(p.getFreeSpaceEnd(), PAGE_SIZE);
  });

  it('inserts and retrieves a tuple', () => {
    const p = new Page(0);
    const slot = p.insertTuple(new Uint8Array([1, 2, 3, 4]));
    assert.equal(slot, 0);
    const data = p.getTuple(0);
    assert.deepStrictEqual([...data], [1, 2, 3, 4]);
  });

  it('inserts multiple tuples', () => {
    const p = new Page(0);
    for (let i = 0; i < 10; i++) {
      const slot = p.insertTuple(new Uint8Array([i, i + 1]));
      assert.equal(slot, i);
    }
    assert.equal(p.getNumSlots(), 10);
    for (let i = 0; i < 10; i++) {
      const data = p.getTuple(i);
      assert.deepStrictEqual([...data], [i, i + 1]);
    }
  });

  it('returns -1 when page is full', () => {
    const p = new Page(0);
    // Fill with large tuples
    const big = new Uint8Array(500);
    let count = 0;
    while (p.insertTuple(big) >= 0) count++;
    assert.ok(count > 0 && count < 10);
    assert.equal(p.insertTuple(big), -1);
  });

  it('deletes a tuple', () => {
    const p = new Page(0);
    p.insertTuple(new Uint8Array([1, 2, 3]));
    p.insertTuple(new Uint8Array([4, 5, 6]));
    assert.ok(p.deleteTuple(0));
    assert.equal(p.getTuple(0), null);
    assert.deepStrictEqual([...p.getTuple(1)], [4, 5, 6]);
  });

  it('scans live tuples', () => {
    const p = new Page(0);
    p.insertTuple(new Uint8Array([1]));
    p.insertTuple(new Uint8Array([2]));
    p.insertTuple(new Uint8Array([3]));
    p.deleteTuple(1);
    const live = [...p.scanTuples()];
    assert.equal(live.length, 2);
    assert.deepStrictEqual([...live[0].data], [1]);
    assert.deepStrictEqual([...live[1].data], [3]);
  });

  it('serializes and deserializes', () => {
    const p = new Page(42);
    p.insertTuple(new Uint8Array([10, 20, 30]));
    const bytes = p.toBytes();
    const p2 = Page.fromBytes(bytes);
    assert.equal(p2.id, 42);
    assert.equal(p2.getNumSlots(), 1);
    assert.deepStrictEqual([...p2.getTuple(0)], [10, 20, 30]);
  });

  it('reports free space correctly', () => {
    const p = new Page(0);
    const initial = p.freeSpace();
    p.insertTuple(new Uint8Array(100));
    assert.ok(p.freeSpace() < initial);
    assert.ok(p.freeSpace() > 0);
  });
});

// ===== Tuple Encoding Tests =====
describe('Tuple Encoding', () => {
  it('roundtrips integers', () => {
    const values = [42, -100, 0, 2147483647];
    assert.deepStrictEqual(decodeTuple(encodeTuple(values)), values);
  });

  it('roundtrips strings', () => {
    const values = ['hello', 'world', ''];
    assert.deepStrictEqual(decodeTuple(encodeTuple(values)), values);
  });

  it('roundtrips mixed types', () => {
    const values = [1, 'alice', true, null, 3.14];
    const decoded = decodeTuple(encodeTuple(values));
    assert.equal(decoded[0], 1);
    assert.equal(decoded[1], 'alice');
    assert.equal(decoded[2], true);
    assert.equal(decoded[3], null);
    assert.ok(Math.abs(decoded[4] - 3.14) < 0.001);
  });

  it('handles null values', () => {
    const values = [null, null, null];
    assert.deepStrictEqual(decodeTuple(encodeTuple(values)), values);
  });

  it('handles booleans', () => {
    assert.deepStrictEqual(decodeTuple(encodeTuple([true, false])), [true, false]);
  });

  it('handles long strings', () => {
    const long = 'x'.repeat(1000);
    assert.equal(decodeTuple(encodeTuple([long]))[0], long);
  });
});

// ===== Buffer Pool Tests =====
describe('BufferPool', () => {
  it('stores and retrieves pages', () => {
    const pool = new BufferPool(4);
    const p = new Page(1);
    pool.putPage(p);
    assert.equal(pool.getPage(1), p);
  });

  it('returns null for missing pages', () => {
    const pool = new BufferPool(4);
    assert.equal(pool.getPage(99), null);
  });

  it('evicts LRU page when full', () => {
    const pool = new BufferPool(2);
    pool.putPage(new Page(1));
    pool.putPage(new Page(2));
    pool.putPage(new Page(3)); // should evict page 1
    assert.equal(pool.getPage(1), null);
    assert.ok(pool.getPage(2));
    assert.ok(pool.getPage(3));
  });

  it('updates LRU on access', () => {
    const pool = new BufferPool(2);
    pool.putPage(new Page(1));
    pool.putPage(new Page(2));
    pool.getPage(1); // access page 1, making page 2 LRU
    pool.putPage(new Page(3)); // should evict page 2
    assert.ok(pool.getPage(1));
    assert.equal(pool.getPage(2), null);
    assert.ok(pool.getPage(3));
  });

  it('tracks dirty pages', () => {
    const pool = new BufferPool(4);
    pool.putPage(new Page(1), true);
    pool.putPage(new Page(2), false);
    assert.equal(pool.getDirtyPages().length, 1);
  });

  it('flushes dirty pages', () => {
    const pool = new BufferPool(4);
    pool.putPage(new Page(1), true);
    pool.putPage(new Page(2), true);
    const flushed = pool.flushAll();
    assert.equal(flushed.length, 2);
    assert.equal(pool.getDirtyPages().length, 0);
  });

  it('respects pinned pages during eviction', () => {
    const pool = new BufferPool(2);
    pool.putPage(new Page(1));
    pool.pin(1);
    pool.putPage(new Page(2));
    pool.putPage(new Page(3)); // should evict page 2 (page 1 is pinned)
    assert.ok(pool.getPage(1));
    assert.equal(pool.getPage(2), null);
  });

  it('reports size', () => {
    const pool = new BufferPool(4);
    pool.putPage(new Page(1));
    pool.putPage(new Page(2));
    assert.equal(pool.size, 2);
  });
});

// ===== Heap File Tests =====
describe('HeapFile', () => {
  it('inserts and retrieves rows', () => {
    const heap = new HeapFile('test');
    const rid = heap.insert([1, 'alice', true]);
    const row = heap.get(rid.pageId, rid.slotIdx);
    assert.deepStrictEqual(row, [1, 'alice', true]);
  });

  it('inserts many rows across pages', () => {
    const heap = new HeapFile('test');
    const rids = [];
    for (let i = 0; i < 100; i++) {
      rids.push(heap.insert([i, `user_${i}`, 'x'.repeat(20)]));
    }
    assert.equal(heap.tupleCount, 100);
    assert.ok(heap.pageCount > 1); // should span multiple pages
    // Verify all rows readable
    for (let i = 0; i < 100; i++) {
      const row = heap.get(rids[i].pageId, rids[i].slotIdx);
      assert.equal(row[0], i);
    }
  });

  it('deletes rows', () => {
    const heap = new HeapFile('test');
    const rid = heap.insert([1, 'delete-me']);
    assert.ok(heap.delete(rid.pageId, rid.slotIdx));
    assert.equal(heap.get(rid.pageId, rid.slotIdx), null);
  });

  it('scans all live rows', () => {
    const heap = new HeapFile('test');
    heap.insert([1, 'a']);
    heap.insert([2, 'b']);
    const rid3 = heap.insert([3, 'c']);
    heap.delete(rid3.pageId, rid3.slotIdx);
    const rows = [...heap.scan()];
    assert.equal(rows.length, 2);
    assert.equal(rows[0].values[0], 1);
    assert.equal(rows[1].values[0], 2);
  });

  it('handles empty scan', () => {
    const heap = new HeapFile('empty');
    assert.deepStrictEqual([...heap.scan()], []);
  });

  it('page count starts at 0', () => {
    const heap = new HeapFile('test');
    assert.equal(heap.pageCount, 0);
    heap.insert([1]);
    assert.equal(heap.pageCount, 1);
  });
});
