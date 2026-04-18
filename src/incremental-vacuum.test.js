// incremental-vacuum.test.js — Tests for incremental VACUUM
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { MVCCManager, MVCCHeap } from './mvcc.js';
import { HeapFile } from './page.js';

describe('Incremental VACUUM', () => {
  let mgr, heap;

  beforeEach(() => {
    mgr = new MVCCManager();
    heap = new MVCCHeap(new HeapFile('incr-vac'));
  });

  it('processes limited pages per call', () => {
    // Insert many rows (spread across pages by inserting large-ish values)
    const tx1 = mgr.begin();
    const rids = [];
    for (let i = 0; i < 20; i++) {
      rids.push(heap.insert([i, `value_${i}_${'x'.repeat(100)}`], tx1));
    }
    tx1.commit();

    // Delete all rows
    const tx2 = mgr.begin();
    for (const rid of rids) {
      heap.delete(rid.pageId, rid.slotIdx, tx2);
    }
    tx2.commit();

    // Incremental vacuum — process 2 pages at a time
    const result1 = heap.vacuumIncremental(mgr, 2, 0);
    assert.ok(result1.pagesProcessed <= 2, `Should process at most 2 pages: ${result1.pagesProcessed}`);
    assert.ok(typeof result1.cursor === 'number', 'Should return cursor');
    assert.ok(typeof result1.done === 'boolean', 'Should return done flag');
  });

  it('multi-pass vacuum removes all dead tuples', () => {
    const tx1 = mgr.begin();
    const rids = [];
    for (let i = 0; i < 10; i++) {
      rids.push(heap.insert([i, `val${i}`], tx1));
    }
    tx1.commit();

    const tx2 = mgr.begin();
    for (let i = 0; i < 5; i++) {
      heap.delete(rids[i].pageId, rids[i].slotIdx, tx2);
    }
    tx2.commit();

    // Run vacuum in multiple passes
    let totalDead = 0;
    let cursor = 0;
    let passes = 0;
    while (passes < 20) { // Safety limit
      passes++;
      const result = heap.vacuumIncremental(mgr, 1, cursor);
      totalDead += result.deadTuplesRemoved;
      cursor = result.cursor;
      if (result.done) break;
    }

    assert.equal(totalDead, 5, `Should remove all 5 dead tuples across passes: ${totalDead}`);
    assert.ok(passes <= 15, `Should complete within reasonable passes: ${passes}`);
  });

  it('cursor resumes from correct position', () => {
    const tx1 = mgr.begin();
    const rids = [];
    for (let i = 0; i < 20; i++) {
      rids.push(heap.insert([i, `val${i}`], tx1));
    }
    tx1.commit();

    // Delete odd-numbered rows
    const tx2 = mgr.begin();
    for (let i = 1; i < 20; i += 2) {
      heap.delete(rids[i].pageId, rids[i].slotIdx, tx2);
    }
    tx2.commit();

    // First pass
    const r1 = heap.vacuumIncremental(mgr, 1, 0);
    
    // Second pass should start from cursor
    if (!r1.done) {
      const r2 = heap.vacuumIncremental(mgr, 1, r1.cursor);
      // Total should not re-process first pass's pages
      assert.ok(r2.cursor >= r1.cursor || r2.done, 
        `Cursor should advance: ${r1.cursor} → ${r2.cursor}`);
    }
  });

  it('done flag is true when all pages processed', () => {
    const tx1 = mgr.begin();
    const rid = heap.insert([1, 'a'], tx1);
    tx1.commit();

    const tx2 = mgr.begin();
    heap.delete(rid.pageId, rid.slotIdx, tx2);
    tx2.commit();

    // Large enough maxPages to process everything
    const result = heap.vacuumIncremental(mgr, 100, 0);
    assert.equal(result.done, true, 'Should be done after processing all pages');
    assert.equal(result.cursor, 0, 'Cursor should reset to 0 when done');
    assert.equal(result.deadTuplesRemoved, 1, 'Should remove 1 dead tuple');
  });

  it('full vacuum gives same result as multi-pass incremental', () => {
    // Create two identical heaps
    const mgr2 = new MVCCManager();
    const heap2 = new MVCCHeap(new HeapFile('full-vac'));

    const tx1a = mgr.begin();
    const tx1b = mgr2.begin();
    const rids1 = [], rids2 = [];
    for (let i = 0; i < 10; i++) {
      rids1.push(heap.insert([i, `val${i}`], tx1a));
      rids2.push(heap2.insert([i, `val${i}`], tx1b));
    }
    tx1a.commit();
    tx1b.commit();

    const tx2a = mgr.begin();
    const tx2b = mgr2.begin();
    for (let i = 0; i < 5; i++) {
      heap.delete(rids1[i].pageId, rids1[i].slotIdx, tx2a);
      heap2.delete(rids2[i].pageId, rids2[i].slotIdx, tx2b);
    }
    tx2a.commit();
    tx2b.commit();

    // Full vacuum on heap
    const fullResult = heap.vacuum(mgr);

    // Incremental vacuum on heap2
    let totalDead = 0;
    let cursor = 0;
    for (let pass = 0; pass < 50; pass++) {
      const r = heap2.vacuumIncremental(mgr2, 1, cursor);
      totalDead += r.deadTuplesRemoved;
      cursor = r.cursor;
      if (r.done) break;
    }

    assert.equal(fullResult.deadTuplesRemoved, totalDead, 
      `Full (${fullResult.deadTuplesRemoved}) should match incremental (${totalDead})`);
  });

  it('incremental vacuum with no dead tuples', () => {
    const tx1 = mgr.begin();
    for (let i = 0; i < 5; i++) {
      heap.insert([i, `val${i}`], tx1);
    }
    tx1.commit();

    // No deletions — vacuum should find nothing
    const result = heap.vacuumIncremental(mgr, 10, 0);
    assert.equal(result.deadTuplesRemoved, 0);
    assert.equal(result.done, true);
  });

  it('incremental vacuum respects transaction horizon', () => {
    const tx1 = mgr.begin();
    const rids = [];
    for (let i = 0; i < 5; i++) {
      rids.push(heap.insert([i, `val${i}`], tx1));
    }
    tx1.commit();

    // Start a long-running transaction (keeps horizon low)
    const txLong = mgr.begin();

    // Delete rows in a new transaction
    const tx2 = mgr.begin();
    for (const rid of rids) {
      heap.delete(rid.pageId, rid.slotIdx, tx2);
    }
    tx2.commit();

    // Vacuum should NOT remove tuples (txLong still active)
    const result = heap.vacuumIncremental(mgr, 100, 0);
    assert.equal(result.deadTuplesRemoved, 0, 'Should not vacuum while long tx active');

    // End long transaction
    txLong.commit();

    // Now vacuum should work
    const result2 = heap.vacuumIncremental(mgr, 100, 0);
    assert.equal(result2.deadTuplesRemoved, 5, 'Should vacuum after long tx ends');
  });
});
