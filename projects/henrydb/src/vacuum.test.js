// vacuum.test.js — VACUUM garbage collection tests
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { MVCCManager, MVCCHeap } from './mvcc.js';
import { HeapFile } from './page.js';

describe('VACUUM Garbage Collection', () => {
  let mgr, heap;

  beforeEach(() => {
    mgr = new MVCCManager();
    heap = new MVCCHeap(new HeapFile('test'));
  });

  describe('Dead Tuple Identification', () => {
    it('removes dead tuples when no active transactions', () => {
      // Insert and delete some rows
      const tx1 = mgr.begin();
      const r1 = heap.insert([1, 'Alice'], tx1);
      const r2 = heap.insert([2, 'Bob'], tx1);
      const r3 = heap.insert([3, 'Carol'], tx1);
      tx1.commit();

      const tx2 = mgr.begin();
      heap.delete(r2.pageId, r2.slotIdx, tx2);
      tx2.commit();

      // No active transactions — VACUUM should remove Bob
      const result = heap.vacuum(mgr);
      assert.equal(result.deadTuplesRemoved, 1);
      assert.ok(result.bytesFreed > 0);

      // Verify Bob is gone, Alice and Carol remain
      const tx3 = mgr.begin();
      const rows = [...heap.scan(tx3)];
      assert.equal(rows.length, 2);
      const names = rows.map(r => r.values[1]).sort();
      assert.deepEqual(names, ['Alice', 'Carol']);
    });

    it('does NOT remove tuples visible to active transactions', () => {
      const tx1 = mgr.begin();
      const r1 = heap.insert([1, 'Alice'], tx1);
      tx1.commit();

      // tx2 starts and sees Alice
      const tx2 = mgr.begin();
      assert.equal([...heap.scan(tx2)].length, 1);

      // tx3 deletes Alice and commits
      const tx3 = mgr.begin();
      heap.delete(r1.pageId, r1.slotIdx, tx3);
      tx3.commit();

      // VACUUM with tx2 still active should NOT remove Alice
      const result = heap.vacuum(mgr);
      assert.equal(result.deadTuplesRemoved, 0);

      // tx2 should still see Alice
      assert.equal([...heap.scan(tx2)].length, 1);
    });

    it('removes dead tuples after all readers finish', () => {
      const tx1 = mgr.begin();
      const r1 = heap.insert([1, 'Alice'], tx1);
      tx1.commit();

      const tx2 = mgr.begin();
      // tx2 sees Alice
      assert.equal([...heap.scan(tx2)].length, 1);

      const tx3 = mgr.begin();
      heap.delete(r1.pageId, r1.slotIdx, tx3);
      tx3.commit();

      // tx2 finishes
      tx2.commit();

      // Now VACUUM should remove Alice
      const result = heap.vacuum(mgr);
      assert.equal(result.deadTuplesRemoved, 1);
    });
  });

  describe('Page Compaction', () => {
    it('compacts page after removing dead tuples', () => {
      const tx1 = mgr.begin();
      // Insert enough rows to create gaps
      const rids = [];
      for (let i = 0; i < 10; i++) {
        rids.push(heap.insert([i, `row${i}`, 'padding'.repeat(10)], tx1));
      }
      tx1.commit();

      // Delete every other row
      const tx2 = mgr.begin();
      for (let i = 0; i < 10; i += 2) {
        heap.delete(rids[i].pageId, rids[i].slotIdx, tx2);
      }
      tx2.commit();

      // Vacuum should compact
      const result = heap.vacuum(mgr);
      assert.equal(result.deadTuplesRemoved, 5);
      assert.ok(result.pagesCompacted > 0);

      // Remaining rows should still be accessible
      const tx3 = mgr.begin();
      const rows = [...heap.scan(tx3)];
      assert.equal(rows.length, 5);
      const ids = rows.map(r => r.values[0]).sort((a, b) => a - b);
      assert.deepEqual(ids, [1, 3, 5, 7, 9]);
    });
  });

  describe('Space Reclamation', () => {
    it('new inserts reuse freed space after VACUUM', () => {
      const tx1 = mgr.begin();
      for (let i = 0; i < 50; i++) {
        heap.insert([i, `row${i}`], tx1);
      }
      tx1.commit();
      const pagesBeforeDelete = heap.pageCount;

      // Delete all rows
      const tx2 = mgr.begin();
      for (const { pageId, slotIdx } of heap.heap.scan()) {
        // Need to check if we can delete (may have version issues for already-deleted)
        try { heap.delete(pageId, slotIdx, tx2); } catch(e) {}
      }
      tx2.commit();

      // VACUUM
      heap.vacuum(mgr);

      // Re-insert — should reuse existing pages
      const tx3 = mgr.begin();
      for (let i = 0; i < 50; i++) {
        heap.insert([100 + i, `new${i}`], tx3);
      }
      tx3.commit();

      // Page count should be <= original (reusing freed space)
      assert.ok(heap.pageCount <= pagesBeforeDelete + 1,
        `Expected <= ${pagesBeforeDelete + 1} pages, got ${heap.pageCount}`);
    });
  });

  describe('xmin Horizon', () => {
    it('horizon is nextTxId when no active transactions', () => {
      const tx1 = mgr.begin();
      tx1.commit();
      assert.equal(mgr.computeXminHorizon(), mgr.nextTxId);
    });

    it('horizon is min active txId', () => {
      const tx1 = mgr.begin(); // txId 1
      const tx2 = mgr.begin(); // txId 2
      const tx3 = mgr.begin(); // txId 3
      assert.equal(mgr.computeXminHorizon(), 1);

      tx1.commit();
      assert.equal(mgr.computeXminHorizon(), 2);

      tx2.commit();
      assert.equal(mgr.computeXminHorizon(), 3);
    });
  });

  describe('VACUUM with Concurrent Transactions', () => {
    it('handles VACUUM running alongside active readers', () => {
      // Setup: many rows
      const setup = mgr.begin();
      for (let i = 0; i < 20; i++) {
        heap.insert([i, `data${i}`], setup);
      }
      setup.commit();

      // Reader starts
      const reader = mgr.begin();
      const snapshot = [...heap.scan(reader)];
      assert.equal(snapshot.length, 20);

      // Writer deletes half
      const writer = mgr.begin();
      const allRows = [...heap.scan(writer)];
      for (let i = 0; i < 10; i++) {
        heap.delete(allRows[i].pageId, allRows[i].slotIdx, writer);
      }
      writer.commit();

      // VACUUM while reader is active — should not remove anything
      const result1 = heap.vacuum(mgr);
      assert.equal(result1.deadTuplesRemoved, 0);

      // Reader still sees all 20
      assert.equal([...heap.scan(reader)].length, 20);

      // Reader commits
      reader.commit();

      // Now VACUUM can clean up
      const result2 = heap.vacuum(mgr);
      assert.equal(result2.deadTuplesRemoved, 10);

      // New reader sees 10
      const newReader = mgr.begin();
      assert.equal([...heap.scan(newReader)].length, 10);
    });

    it('multiple VACUUMs are idempotent', () => {
      const tx1 = mgr.begin();
      const r = heap.insert([1, 'test'], tx1);
      tx1.commit();

      const tx2 = mgr.begin();
      heap.delete(r.pageId, r.slotIdx, tx2);
      tx2.commit();

      const result1 = heap.vacuum(mgr);
      assert.equal(result1.deadTuplesRemoved, 1);

      const result2 = heap.vacuum(mgr);
      assert.equal(result2.deadTuplesRemoved, 0); // Already cleaned
    });
  });
});
