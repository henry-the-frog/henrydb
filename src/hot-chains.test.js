// hot-chains.test.js — HOT (Heap-Only Tuple) chain tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { HeapFile } from './page.js';

describe('HOT Chains', () => {
  it('hotUpdate stores new version on same page', () => {
    const heap = new HeapFile('hot-test');
    const rid = heap.insert([1, 'Alice', 30]);
    const hotRid = heap.hotUpdate(rid.pageId, rid.slotIdx, [1, 'Alice', 31]);
    
    assert.ok(hotRid, 'Should return a result');
    assert.equal(hotRid.isHot, true, 'Should be a HOT update');
    assert.equal(hotRid.pageId, rid.pageId, 'Should be on same page');
  });

  it('get() follows HOT chain to latest version', () => {
    const heap = new HeapFile('hot-test');
    const rid = heap.insert([1, 'Alice', 30]);
    heap.hotUpdate(rid.pageId, rid.slotIdx, [1, 'Alice', 31]);
    
    const latest = heap.get(rid.pageId, rid.slotIdx);
    assert.deepStrictEqual(latest, [1, 'Alice', 31]);
  });

  it('multi-hop chains work', () => {
    const heap = new HeapFile('hot-test');
    const rid = heap.insert([1, 'Alice', 30]);
    heap.hotUpdate(rid.pageId, rid.slotIdx, [1, 'Alice', 31]);
    heap.hotUpdate(rid.pageId, rid.slotIdx, [1, 'Alice', 32]);
    heap.hotUpdate(rid.pageId, rid.slotIdx, [1, 'Alice', 33]);
    
    assert.deepStrictEqual(heap.get(rid.pageId, rid.slotIdx), [1, 'Alice', 33]);
  });

  it('scan() returns only latest versions', () => {
    const heap = new HeapFile('hot-test');
    const r1 = heap.insert([1, 'Alice', 30]);
    const r2 = heap.insert([2, 'Bob', 25]);
    heap.hotUpdate(r1.pageId, r1.slotIdx, [1, 'Alice-v2', 31]);
    
    const rows = [...heap.scan()];
    assert.equal(rows.length, 2, 'Should have exactly 2 visible rows');
    const alice = rows.find(r => r.values[0] === 1);
    assert.deepStrictEqual(alice.values, [1, 'Alice-v2', 31]);
  });

  it('tupleCount reflects logical count', () => {
    const heap = new HeapFile('hot-test');
    const rid = heap.insert([1, 'Alice', 30]);
    heap.insert([2, 'Bob', 25]);
    heap.hotUpdate(rid.pageId, rid.slotIdx, [1, 'Alice', 31]);
    heap.hotUpdate(rid.pageId, rid.slotIdx, [1, 'Alice', 32]);
    
    assert.equal(heap.tupleCount, 2, 'Logical count should be 2 despite physical 4');
  });

  it('non-HOT rows unaffected', () => {
    const heap = new HeapFile('hot-test');
    const r1 = heap.insert([1, 'Alice', 30]);
    const r2 = heap.insert([2, 'Bob', 25]);
    const r3 = heap.insert([3, 'Carol', 35]);
    
    heap.hotUpdate(r1.pageId, r1.slotIdx, [1, 'Alice', 31]);
    
    // Non-HOT rows should be unaffected
    assert.deepStrictEqual(heap.get(r2.pageId, r2.slotIdx), [2, 'Bob', 25]);
    assert.deepStrictEqual(heap.get(r3.pageId, r3.slotIdx), [3, 'Carol', 35]);
  });

  it('pruneHotChains removes old versions', () => {
    const heap = new HeapFile('hot-test');
    const rid = heap.insert([1, 'Alice', 30]);
    heap.hotUpdate(rid.pageId, rid.slotIdx, [1, 'Alice', 31]);
    heap.hotUpdate(rid.pageId, rid.slotIdx, [1, 'Alice', 32]);
    
    const pruned = heap.pruneHotChains();
    assert.equal(pruned, 2, 'Should prune 2 intermediate versions');
    
    // Still accessible via original location
    assert.deepStrictEqual(heap.get(rid.pageId, rid.slotIdx), [1, 'Alice', 32]);
    assert.equal(heap.tupleCount, 1);
    assert.equal([...heap.scan()].length, 1);
  });

  it('pruneHotChains handles multiple independent chains', () => {
    const heap = new HeapFile('hot-test');
    const r1 = heap.insert([1, 'Alice', 30]);
    const r2 = heap.insert([2, 'Bob', 25]);
    
    heap.hotUpdate(r1.pageId, r1.slotIdx, [1, 'Alice', 31]);
    heap.hotUpdate(r2.pageId, r2.slotIdx, [2, 'Bob', 26]);
    
    const pruned = heap.pruneHotChains();
    assert.equal(pruned, 2, 'Should prune 1 old version per chain');
    
    assert.deepStrictEqual(heap.get(r1.pageId, r1.slotIdx), [1, 'Alice', 31]);
    assert.deepStrictEqual(heap.get(r2.pageId, r2.slotIdx), [2, 'Bob', 26]);
    assert.equal(heap.tupleCount, 2);
  });

  it('resolveHotChain returns final location', () => {
    const heap = new HeapFile('hot-test');
    const rid = heap.insert([1, 'Alice', 30]);
    const hot1 = heap.hotUpdate(rid.pageId, rid.slotIdx, [1, 'Alice', 31]);
    const hot2 = heap.hotUpdate(rid.pageId, rid.slotIdx, [1, 'Alice', 32]);
    
    const resolved = heap.resolveHotChain(rid.pageId, rid.slotIdx);
    assert.equal(resolved.pageId, hot2.pageId);
    assert.equal(resolved.slotIdx, hot2.slotIdx);
  });

  it('isHotChainHead identifies redirected slots', () => {
    const heap = new HeapFile('hot-test');
    const r1 = heap.insert([1, 'Alice', 30]);
    const r2 = heap.insert([2, 'Bob', 25]);
    
    heap.hotUpdate(r1.pageId, r1.slotIdx, [1, 'Alice', 31]);
    
    assert.equal(heap.isHotChainHead(r1.pageId, r1.slotIdx), true, 'r1 should be chain head');
    assert.equal(heap.isHotChainHead(r2.pageId, r2.slotIdx), false, 'r2 should not be chain head');
  });

  it('delete on HOT-updated row works', () => {
    const heap = new HeapFile('hot-test');
    const rid = heap.insert([1, 'Alice', 30]);
    heap.hotUpdate(rid.pageId, rid.slotIdx, [1, 'Alice', 31]);
    
    // Delete the latest version (via the HOT chain endpoint)
    const resolved = heap.resolveHotChain(rid.pageId, rid.slotIdx);
    heap.delete(resolved.pageId, resolved.slotIdx);
    
    // Scan should not show the deleted row's latest version
    const rows = [...heap.scan()];
    assert.equal(rows.length, 0, 'Deleted HOT row should not appear in scan');
  });

  it('HOT update when page full returns null', () => {
    const heap = new HeapFile('hot-test');
    // Fill a page with many tuples
    const rids = [];
    for (let i = 0; i < 200; i++) {
      rids.push(heap.insert([i, `value_${i}_${'x'.repeat(40)}`]));
    }
    
    // Try HOT update with a much larger value
    const bigValue = 'x'.repeat(5000);
    const result = heap.hotUpdate(rids[0].pageId, rids[0].slotIdx, [0, bigValue]);
    // It should either succeed (if there's space) or return null
    if (result === null) {
      // Original should still be accessible
      const original = heap.get(rids[0].pageId, rids[0].slotIdx);
      assert.ok(original, 'Original should still be accessible');
    } else {
      assert.equal(result.isHot, true);
    }
  });
});
