// mvcc-stress.test.js — Adversarial MVCC concurrency tests
// Goal: find bugs in snapshot isolation, visibility rules, VACUUM safety

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { MVCCManager, MVCCHeap } from './mvcc.js';
import { HeapFile } from './page.js';

function makeHeap() {
  const heap = new HeapFile();
  return new MVCCHeap(heap);
}

describe('MVCC Stress Tests', () => {

  // ========== WRITE SKEW ANOMALY ==========
  
  describe('Write skew anomaly', () => {
    it('classic write skew: both readers see old values, both write', () => {
      const mgr = new MVCCManager();
      const heap = makeHeap();
      
      // Initial state: two on-call doctors
      const setupTx = mgr.begin();
      heap.insert(['Alice', true], setupTx);  // name, onCall
      heap.insert(['Bob', true], setupTx);
      setupTx.commit();
      
      // tx1: Alice reads — both on call
      const tx1 = mgr.begin();
      const rows1 = [...heap.scan(tx1)].map(r => r.values);
      const onCall1 = rows1.filter(r => r[1] === true).length;
      assert.strictEqual(onCall1, 2);
      
      // tx2: Bob reads — both on call  
      const tx2 = mgr.begin();
      const rows2 = [...heap.scan(tx2)].map(r => r.values);
      const onCall2 = rows2.filter(r => r[1] === true).length;
      assert.strictEqual(onCall2, 2);
      
      // tx1 sets Alice off-call
      const aliceRow = [...heap.scan(tx1)].find(r => r.values[0] === 'Alice');
      heap.update(aliceRow.pageId, aliceRow.slotIdx, ['Alice', false], tx1);
      
      // tx2 sets Bob off-call
      const bobRow = [...heap.scan(tx2)].find(r => r.values[0] === 'Bob');
      heap.update(bobRow.pageId, bobRow.slotIdx, ['Bob', false], tx2);
      
      // Both commit (snapshot isolation allows write skew)
      tx1.commit();
      tx2.commit();
      
      // Verify anomaly: nobody is on call!
      const finalTx = mgr.begin();
      const final = [...heap.scan(finalTx)].map(r => r.values);
      const onCallFinal = final.filter(r => r[1] === true).length;
      assert.strictEqual(onCallFinal, 0, 'Write skew: both went off call');
      finalTx.commit();
    });
  });

  // ========== VACUUM WITH LONG-RUNNING READERS ==========
  
  describe('VACUUM safety with concurrent readers', () => {
    it('VACUUM does not remove tuples needed by active readers', () => {
      const mgr = new MVCCManager();
      const heap = makeHeap();
      
      // Insert and commit
      const setupTx = mgr.begin();
      heap.insert([1, 'old'], setupTx);
      heap.insert([2, 'old'], setupTx);
      setupTx.commit();
      
      // Start long-running reader
      const reader = mgr.begin();
      const snapshot = [...heap.scan(reader)];
      assert.strictEqual(snapshot.length, 2, 'Reader sees 2 rows');
      
      // Another transaction updates and commits
      const writer = mgr.begin();
      const wRows = [...heap.scan(writer)];
      heap.update(wRows[0].pageId, wRows[0].slotIdx, [1, 'new'], writer);
      writer.commit();
      
      // VACUUM runs — should NOT remove old version
      const vacResult = heap.vacuum(mgr);
      
      // Reader should still see old data
      const readerRows = [...heap.scan(reader)];
      assert.strictEqual(readerRows.length, 2, 'Reader still sees 2 rows after VACUUM');
      
      reader.commit();
    });
    
    it('VACUUM reclaims after all readers finish', () => {
      const mgr = new MVCCManager();
      const heap = makeHeap();
      
      const t1 = mgr.begin();
      heap.insert([1], t1);
      heap.insert([2], t1);
      t1.commit();
      
      const t2 = mgr.begin();
      const rows = [...heap.scan(t2)];
      heap.delete(rows[0].pageId, rows[0].slotIdx, t2);
      t2.commit();
      
      const result = heap.vacuum(mgr);
      assert.ok(result.deadTuplesRemoved >= 1, 'VACUUM reclaimed dead tuples');
    });
  });

  // ========== PHANTOM PREVENTION ==========
  
  describe('Phantom prevention', () => {
    it('new inserts by concurrent tx are invisible to snapshot reader', () => {
      const mgr = new MVCCManager();
      const heap = makeHeap();
      
      const t1 = mgr.begin();
      heap.insert([1], t1);
      heap.insert([2], t1);
      t1.commit();
      
      // Reader takes snapshot
      const reader = mgr.begin();
      const before = [...heap.scan(reader)];
      assert.strictEqual(before.length, 2);
      
      // Writer inserts new rows
      const writer = mgr.begin();
      heap.insert([3], writer);
      heap.insert([4], writer);
      writer.commit();
      
      // Reader should NOT see new rows
      const after = [...heap.scan(reader)];
      assert.strictEqual(after.length, 2, 'No phantom reads');
      
      reader.commit();
      
      // New tx sees all 4
      const t3 = mgr.begin();
      assert.strictEqual([...heap.scan(t3)].length, 4);
      t3.commit();
    });
    
    it('deleted rows by concurrent tx still visible to snapshot reader', () => {
      const mgr = new MVCCManager();
      const heap = makeHeap();
      
      const t1 = mgr.begin();
      heap.insert([1], t1);
      heap.insert([2], t1);
      t1.commit();
      
      const reader = mgr.begin();
      assert.strictEqual([...heap.scan(reader)].length, 2);
      
      // Writer deletes a row
      const writer = mgr.begin();
      const rows = [...heap.scan(writer)];
      heap.delete(rows[0].pageId, rows[0].slotIdx, writer);
      writer.commit();
      
      // Reader should still see both rows
      assert.strictEqual([...heap.scan(reader)].length, 2, 'Deleted row still visible');
      
      reader.commit();
    });
  });

  // ========== TRANSACTION INTERLEAVING ==========
  
  describe('Complex transaction interleaving', () => {
    it('many concurrent writers on separate rows', () => {
      const mgr = new MVCCManager();
      const heap = makeHeap();
      
      // Insert 10 rows
      const setupTx = mgr.begin();
      for (let i = 0; i < 10; i++) {
        heap.insert([i, 0], setupTx);  // id, counter
      }
      setupTx.commit();
      
      // 10 concurrent txns, each updating different row
      const txns = [];
      for (let i = 0; i < 10; i++) {
        const tx = mgr.begin();
        const rows = [...heap.scan(tx)];
        const target = rows[i];
        heap.update(target.pageId, target.slotIdx, [i, 1], tx);
        txns.push(tx);
      }
      
      // All should commit without conflict
      for (const tx of txns) {
        tx.commit();
      }
      
      // Verify all counters = 1
      const verifyTx = mgr.begin();
      const final = [...heap.scan(verifyTx)].map(r => r.values);
      assert.strictEqual(final.length, 10);
      for (const row of final) {
        assert.strictEqual(row[1], 1, `Row ${row[0]} counter should be 1`);
      }
      verifyTx.commit();
    });
    
    it('write-write conflict on same row', () => {
      const mgr = new MVCCManager();
      const heap = makeHeap();
      
      const setupTx = mgr.begin();
      heap.insert([1, 'original'], setupTx);
      setupTx.commit();
      
      const tx1 = mgr.begin();
      const tx2 = mgr.begin();
      
      const rows1 = [...heap.scan(tx1)];
      heap.update(rows1[0].pageId, rows1[0].slotIdx, [1, 'tx1'], tx1);
      
      // tx2 should fail on same row
      const rows2 = [...heap.scan(tx2)];
      assert.throws(() => {
        heap.update(rows2[0].pageId, rows2[0].slotIdx, [1, 'tx2'], tx2);
      }, /conflict|already deleted/i);
      
      tx1.commit();
    });
    
    it('rollback makes changes invisible', () => {
      const mgr = new MVCCManager();
      const heap = makeHeap();
      
      const t1 = mgr.begin();
      heap.insert([1], t1);
      t1.commit();
      
      const t2 = mgr.begin();
      heap.insert([2], t2);
      assert.strictEqual([...heap.scan(t2)].length, 2, 't2 sees own insert');
      t2.rollback();
      
      const t3 = mgr.begin();
      assert.strictEqual([...heap.scan(t3)].length, 1, 'Rolled-back insert invisible');
      t3.commit();
    });
    
    it('read-only transactions never conflict', () => {
      const mgr = new MVCCManager();
      const heap = makeHeap();
      
      const t1 = mgr.begin();
      heap.insert([1], t1);
      heap.insert([2], t1);
      t1.commit();
      
      const readers = [];
      for (let i = 0; i < 5; i++) {
        const r = mgr.begin();
        assert.strictEqual([...heap.scan(r)].length, 2, `Reader ${i} sees 2 rows`);
        readers.push(r);
      }
      
      for (const r of readers) r.commit();
    });
    
    it('sequential transaction chain', () => {
      const mgr = new MVCCManager();
      const heap = makeHeap();
      
      for (let i = 0; i < 20; i++) {
        const tx = mgr.begin();
        heap.insert([i], tx);
        tx.commit();
      }
      
      const final = mgr.begin();
      assert.strictEqual([...heap.scan(final)].length, 20);
      final.commit();
    });
  });

  // ========== VISIBILITY EDGE CASES ==========
  
  describe('Visibility edge cases', () => {
    it('aborted transaction changes are invisible', () => {
      const mgr = new MVCCManager();
      const heap = makeHeap();
      
      const t1 = mgr.begin();
      heap.insert([1], t1);
      t1.commit();
      
      const t2 = mgr.begin();
      heap.insert([2], t2);
      t2.rollback();
      
      const t3 = mgr.begin();
      heap.insert([3], t3);
      t3.commit();
      
      const reader = mgr.begin();
      const rows = [...heap.scan(reader)].map(r => r.values);
      assert.strictEqual(rows.length, 2);
      const ids = rows.map(r => r[0]).sort();
      assert.deepStrictEqual(ids, [1, 3]);
      reader.commit();
    });
    
    it('own uncommitted writes are visible', () => {
      const mgr = new MVCCManager();
      const heap = makeHeap();
      
      const tx = mgr.begin();
      heap.insert([1], tx);
      assert.strictEqual([...heap.scan(tx)].length, 1);
      tx.commit();
    });
    
    it('mixed committed and uncommitted data', () => {
      const mgr = new MVCCManager();
      const heap = makeHeap();
      
      const t1 = mgr.begin();
      heap.insert(['committed'], t1);
      t1.commit();
      
      // Active writer (uncommitted)
      const writer = mgr.begin();
      heap.insert(['uncommitted'], writer);
      
      // Reader sees only committed
      const reader = mgr.begin();
      const rows = [...heap.scan(reader)].map(r => r.values);
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0][0], 'committed');
      
      reader.commit();
      writer.commit();
    });
    
    it('VACUUM is idempotent', () => {
      const mgr = new MVCCManager();
      const heap = makeHeap();
      
      const t1 = mgr.begin();
      heap.insert([1], t1);
      heap.insert([2], t1);
      t1.commit();
      
      const t2 = mgr.begin();
      const rows = [...heap.scan(t2)];
      heap.delete(rows[0].pageId, rows[0].slotIdx, t2);
      t2.commit();
      
      const r1 = heap.vacuum(mgr);
      const r2 = heap.vacuum(mgr);
      assert.strictEqual(r2.deadTuplesRemoved, 0, 'Second VACUUM finds nothing');
      
      const t3 = mgr.begin();
      assert.strictEqual([...heap.scan(t3)].length, 1);
      t3.commit();
    });
  });
});

describe('PostgreSQL-style Snapshot', () => {
  it('handles out-of-order commits correctly', () => {
    const mgr = new MVCCManager();
    const heap = new MVCCHeap(new HeapFile('ooo'));
    
    // tx1 starts, inserts row
    const tx1 = mgr.begin(); // txId=1
    heap.insert([1, 'from_tx1'], tx1);
    
    // tx2 starts, inserts row
    const tx2 = mgr.begin(); // txId=2
    heap.insert([2, 'from_tx2'], tx2);
    
    // tx3 starts — snapshot captures tx1 and tx2 as active
    const tx3 = mgr.begin(); // txId=3
    
    // tx2 commits BEFORE tx1 (out of order!)
    tx2.commit();
    
    // tx3 should NOT see tx2's row (tx2 was active in tx3's snapshot)
    const rows = [...heap.scan(tx3)].map(r => r.values);
    assert.equal(rows.length, 0, 'tx3 should see no rows (both tx1 and tx2 were active at snapshot time)');
    
    // Now tx1 commits
    tx1.commit();
    
    // tx3 still shouldn't see either row (snapshot was taken before both committed)
    const rows2 = [...heap.scan(tx3)].map(r => r.values);
    assert.equal(rows2.length, 0, 'tx3 still sees no rows after tx1 commits');
    
    tx3.commit();
    
    // tx4 starts fresh — should see both committed rows
    const tx4 = mgr.begin();
    const rows3 = [...heap.scan(tx4)].map(r => r.values);
    assert.equal(rows3.length, 2, 'tx4 sees both committed rows');
    tx4.commit();
  });

  it('snapshot xip_list only affects txids in [xmin, xmax) range', () => {
    const mgr = new MVCCManager();
    const heap = new MVCCHeap(new HeapFile('xip'));
    
    // Setup: committed data
    const setup = mgr.begin(); // txId=1
    heap.insert([1, 'committed'], setup);
    setup.commit();
    
    // tx2 starts but doesn't commit yet
    const tx2 = mgr.begin(); // txId=2
    heap.insert([2, 'uncommitted'], tx2);
    
    // tx3 starts — snapshot: xmin=2, xmax=3, activeSet={2}
    const tx3 = mgr.begin(); // txId=3
    
    // tx3 should see committed row (txId=1 < xmin=2, so always visible)
    const rows = [...heap.scan(tx3)].map(r => r.values);
    assert.equal(rows.length, 1);
    assert.deepEqual(rows[0], [1, 'committed']);
    
    tx2.commit();
    tx3.commit();
  });

  it('snapshot representation matches PostgreSQL format', () => {
    const mgr = new MVCCManager();
    
    const tx1 = mgr.begin(); // txId=1
    const tx2 = mgr.begin(); // txId=2
    tx1.commit();
    const tx3 = mgr.begin(); // txId=3, snapshot should have xmin=2, xmax=3, activeSet={2}
    
    assert.equal(tx3.snapshot.xmin, 2);
    assert.equal(tx3.snapshot.xmax, 3);
    assert.ok(tx3.snapshot.activeSet.has(2));
    assert.equal(tx3.snapshot.activeSet.size, 1);
    
    tx2.commit();
    tx3.commit();
  });
});

describe('Hint Bits Performance', () => {
  it('benchmark: repeated scans benefit from hint bits', () => {
    const mgr = new MVCCManager();
    const heap = new MVCCHeap(new HeapFile('hintbench'));
    
    // Insert 10,000 rows across 100 transactions
    for (let t = 0; t < 100; t++) {
      const tx = mgr.begin();
      for (let i = 0; i < 100; i++) {
        heap.insert([t * 100 + i, `row_${t * 100 + i}`], tx);
      }
      tx.commit();
    }
    
    // Create a reader transaction
    const reader = mgr.begin();
    
    // First scan (cold — no hint bits set)
    const t0 = performance.now();
    let count1 = 0;
    for (const _row of heap.scan(reader)) count1++;
    const firstScanMs = performance.now() - t0;
    
    // Second scan (warm — hint bits should be set from first scan)
    const t1 = performance.now();
    let count2 = 0;
    for (const _row of heap.scan(reader)) count2++;
    const secondScanMs = performance.now() - t1;
    
    // Third scan
    const t2 = performance.now();
    let count3 = 0;
    for (const _row of heap.scan(reader)) count3++;
    const thirdScanMs = performance.now() - t2;
    
    console.log(`    10K rows, 100 txns:`);
    console.log(`    First scan (cold):  ${firstScanMs.toFixed(1)}ms (${count1} rows)`);
    console.log(`    Second scan (warm): ${secondScanMs.toFixed(1)}ms (${count2} rows)`);
    console.log(`    Third scan (warm):  ${thirdScanMs.toFixed(1)}ms (${count3} rows)`);
    console.log(`    Speedup: ${(firstScanMs / thirdScanMs).toFixed(1)}x`);
    
    assert.equal(count1, 10000);
    assert.equal(count2, 10000);
    assert.equal(count3, 10000);
    
    reader.commit();
  });
});
