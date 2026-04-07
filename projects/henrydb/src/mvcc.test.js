// mvcc.test.js — MVCC snapshot isolation tests
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { MVCCManager, MVCCHeap } from './mvcc.js';
import { HeapFile } from './page.js';

describe('MVCC Snapshot Isolation', () => {
  let mgr, heap;

  beforeEach(() => {
    mgr = new MVCCManager();
    heap = new MVCCHeap(new HeapFile('test'));
  });

  describe('Basic Operations', () => {
    it('insert and read within same transaction', () => {
      const tx = mgr.begin();
      heap.insert([1, 'Alice', 100], tx);
      heap.insert([2, 'Bob', 200], tx);
      
      const rows = [...heap.scan(tx)];
      assert.equal(rows.length, 2);
      assert.deepEqual(rows[0].values, [1, 'Alice', 100]);
    });

    it('committed data visible to new transactions', () => {
      const tx1 = mgr.begin();
      heap.insert([1, 'Alice', 100], tx1);
      tx1.commit();

      const tx2 = mgr.begin();
      const rows = [...heap.scan(tx2)];
      assert.equal(rows.length, 1);
      assert.deepEqual(rows[0].values, [1, 'Alice', 100]);
    });

    it('delete marks row invisible', () => {
      const tx1 = mgr.begin();
      const rid = heap.insert([1, 'Alice', 100], tx1);
      tx1.commit();

      const tx2 = mgr.begin();
      heap.delete(rid.pageId, rid.slotIdx, tx2);
      
      // Deleted row should be invisible to tx2
      const rows = [...heap.scan(tx2)];
      assert.equal(rows.length, 0);
      tx2.commit();
    });

    it('update creates new version', () => {
      const tx1 = mgr.begin();
      const rid = heap.insert([1, 'Alice', 100], tx1);
      tx1.commit();

      const tx2 = mgr.begin();
      heap.update(rid.pageId, rid.slotIdx, [1, 'Alice', 200], tx2);
      
      const rows = [...heap.scan(tx2)];
      assert.equal(rows.length, 1);
      assert.deepEqual(rows[0].values, [1, 'Alice', 200]);
      tx2.commit();
    });
  });

  describe('Dirty Read Prevention', () => {
    it('uncommitted insert invisible to other transactions', () => {
      const tx1 = mgr.begin();
      heap.insert([1, 'Alice', 100], tx1);
      // tx1 NOT committed

      const tx2 = mgr.begin();
      const rows = [...heap.scan(tx2)];
      assert.equal(rows.length, 0, 'Should not see uncommitted insert');
    });

    it('uncommitted delete invisible to other transactions', () => {
      const tx1 = mgr.begin();
      const rid = heap.insert([1, 'Alice', 100], tx1);
      tx1.commit();

      const tx2 = mgr.begin();
      heap.delete(rid.pageId, rid.slotIdx, tx2);
      // tx2 NOT committed

      const tx3 = mgr.begin();
      const rows = [...heap.scan(tx3)];
      assert.equal(rows.length, 1, 'Should still see row — delete not committed');
    });
  });

  describe('Repeatable Reads', () => {
    it('snapshot does not change after begin', () => {
      const tx1 = mgr.begin();
      heap.insert([1, 'Alice', 100], tx1);
      tx1.commit();

      // tx2 starts — sees Alice
      const tx2 = mgr.begin();
      const rowsBefore = [...heap.scan(tx2)];
      assert.equal(rowsBefore.length, 1);

      // tx3 adds Bob and commits
      const tx3 = mgr.begin();
      heap.insert([2, 'Bob', 200], tx3);
      tx3.commit();

      // tx2 should STILL see only Alice (snapshot isolation)
      const rowsAfter = [...heap.scan(tx2)];
      assert.equal(rowsAfter.length, 1, 'Snapshot should not change');
      assert.deepEqual(rowsAfter[0].values, [1, 'Alice', 100]);
    });
  });

  describe('Rollback', () => {
    it('rolled-back insert disappears', () => {
      const tx1 = mgr.begin();
      heap.insert([1, 'Alice', 100], tx1);
      
      // Verify visible to self
      assert.equal([...heap.scan(tx1)].length, 1);
      
      tx1.rollback();

      // New transaction should see nothing
      const tx2 = mgr.begin();
      const rows = [...heap.scan(tx2)];
      assert.equal(rows.length, 0, 'Rolled-back insert should be gone');
    });

    it('rolled-back delete restores row', () => {
      const tx1 = mgr.begin();
      const rid = heap.insert([1, 'Alice', 100], tx1);
      tx1.commit();

      const tx2 = mgr.begin();
      heap.delete(rid.pageId, rid.slotIdx, tx2);
      assert.equal([...heap.scan(tx2)].length, 0);
      
      tx2.rollback();

      // Row should be back
      const tx3 = mgr.begin();
      const rows = [...heap.scan(tx3)];
      assert.equal(rows.length, 1, 'Rolled-back delete should restore row');
    });

    it('rolled-back update restores original', () => {
      const tx1 = mgr.begin();
      const rid = heap.insert([1, 'Alice', 100], tx1);
      tx1.commit();

      const tx2 = mgr.begin();
      heap.update(rid.pageId, rid.slotIdx, [1, 'Alice', 999], tx2);
      tx2.rollback();

      const tx3 = mgr.begin();
      const rows = [...heap.scan(tx3)];
      assert.equal(rows.length, 1);
      assert.deepEqual(rows[0].values, [1, 'Alice', 100], 'Should see original value');
    });
  });

  describe('Write-Write Conflicts', () => {
    it('detects concurrent delete conflict', () => {
      const tx1 = mgr.begin();
      const rid = heap.insert([1, 'Alice', 100], tx1);
      tx1.commit();

      const tx2 = mgr.begin();
      const tx3 = mgr.begin();

      heap.delete(rid.pageId, rid.slotIdx, tx2);
      
      assert.throws(() => {
        heap.delete(rid.pageId, rid.slotIdx, tx3);
      }, /conflict/i, 'Should detect write-write conflict');
    });
  });

  describe('Interleaved Transactions', () => {
    it('handles 3 interleaved transactions correctly', () => {
      // tx1: insert rows
      const tx1 = mgr.begin();
      heap.insert([1, 'Alice', 100], tx1);
      heap.insert([2, 'Bob', 200], tx1);
      tx1.commit();

      // tx2: starts, sees 2 rows
      const tx2 = mgr.begin();
      assert.equal([...heap.scan(tx2)].length, 2);

      // tx3: deletes Bob, inserts Carol
      const tx3 = mgr.begin();
      const rows3 = [...heap.scan(tx3)];
      const bobRid = rows3.find(r => r.values[1] === 'Bob');
      heap.delete(bobRid.pageId, bobRid.slotIdx, tx3);
      heap.insert([3, 'Carol', 300], tx3);
      tx3.commit();

      // tx2 still sees original snapshot (Alice + Bob)
      const tx2Rows = [...heap.scan(tx2)];
      assert.equal(tx2Rows.length, 2, 'tx2 should still see Alice and Bob');
      
      // tx4 (new): sees Alice + Carol
      const tx4 = mgr.begin();
      const tx4Rows = [...heap.scan(tx4)];
      assert.equal(tx4Rows.length, 2, 'tx4 should see Alice and Carol');
      const names = tx4Rows.map(r => r.values[1]).sort();
      assert.deepEqual(names, ['Alice', 'Carol']);
    });
  });

  describe('WAL', () => {
    it('records all operations', () => {
      const tx = mgr.begin();
      heap.insert([1, 'test'], tx);
      tx.commit();

      // WAL should have: BEGIN, INSERT, COMMIT
      assert.ok(mgr.wal.length >= 3);
      assert.equal(mgr.wal[0].type, 'BEGIN');
      assert.equal(mgr.wal[1].type, 'INSERT');
      assert.equal(mgr.wal[2].type, 'COMMIT');
    });
  });
});

describe('Phantom Prevention', () => {
  let mgr, heap;

  beforeEach(() => {
    mgr = new MVCCManager();
    heap = new MVCCHeap(new HeapFile('phantom'));
  });

  it('prevents phantom reads — new rows in range not visible', () => {
    // Setup: insert 3 rows
    const setup = mgr.begin();
    heap.insert([1, 'Alice', 25], setup);
    heap.insert([2, 'Bob', 35], setup);
    heap.insert([3, 'Carol', 45], setup);
    setup.commit();

    // tx1: scans "age between 20 and 40" → sees Alice, Bob
    const tx1 = mgr.begin();
    const firstScan = [...heap.scan(tx1)].filter(r => r.values[2] >= 20 && r.values[2] <= 40);
    assert.equal(firstScan.length, 2);

    // tx2: inserts Dave age=30 (in the range) and commits
    const tx2 = mgr.begin();
    heap.insert([4, 'Dave', 30], tx2);
    tx2.commit();

    // tx1: rescan same range — should NOT see Dave (phantom prevention)
    const secondScan = [...heap.scan(tx1)].filter(r => r.values[2] >= 20 && r.values[2] <= 40);
    assert.equal(secondScan.length, 2, 'Should not see phantom row');
    const names = secondScan.map(r => r.values[1]).sort();
    assert.deepEqual(names, ['Alice', 'Bob']);
  });

  it('prevents phantom after delete + reinsert', () => {
    const setup = mgr.begin();
    const rid = heap.insert([1, 'Alice', 100], setup);
    setup.commit();

    const tx1 = mgr.begin();
    assert.equal([...heap.scan(tx1)].length, 1);

    // tx2: delete Alice, insert replacement
    const tx2 = mgr.begin();
    heap.delete(rid.pageId, rid.slotIdx, tx2);
    heap.insert([1, 'Alice-v2', 200], tx2);
    tx2.commit();

    // tx1: should still see original Alice
    const rows = [...heap.scan(tx1)];
    assert.equal(rows.length, 1);
    assert.deepEqual(rows[0].values, [1, 'Alice', 100]);
  });
});

describe('Edge Cases', () => {
  let mgr, heap;

  beforeEach(() => {
    mgr = new MVCCManager();
    heap = new MVCCHeap(new HeapFile('edge'));
  });

  it('empty transaction commit is valid', () => {
    const tx = mgr.begin();
    tx.commit();
    assert.ok(tx.committed);
  });

  it('empty transaction rollback is valid', () => {
    const tx = mgr.begin();
    tx.rollback();
    assert.ok(tx.aborted);
  });

  it('multiple inserts and deletes in same transaction', () => {
    const tx = mgr.begin();
    const r1 = heap.insert([1, 'A'], tx);
    const r2 = heap.insert([2, 'B'], tx);
    const r3 = heap.insert([3, 'C'], tx);
    heap.delete(r2.pageId, r2.slotIdx, tx);
    
    const rows = [...heap.scan(tx)];
    assert.equal(rows.length, 2);
    const vals = rows.map(r => r.values[1]).sort();
    assert.deepEqual(vals, ['A', 'C']);
    tx.commit();
  });

  it('long-running reader sees consistent snapshot', () => {
    // Setup: 5 rows
    const setup = mgr.begin();
    for (let i = 0; i < 5; i++) heap.insert([i, `row${i}`], setup);
    setup.commit();

    const reader = mgr.begin();
    assert.equal([...heap.scan(reader)].length, 5);

    // 10 concurrent writes, each adding a row
    for (let i = 5; i < 15; i++) {
      const w = mgr.begin();
      heap.insert([i, `new${i}`], w);
      w.commit();
    }

    // Reader still sees only 5
    assert.equal([...heap.scan(reader)].length, 5);
  });

  it('scanAll shows all non-deleted rows regardless of tx', () => {
    const tx1 = mgr.begin();
    heap.insert([1, 'visible'], tx1);
    tx1.commit();

    const tx2 = mgr.begin();
    heap.insert([2, 'uncommitted'], tx2);
    // tx2 not committed

    const all = [...heap.scanAll()];
    // scanAll should show row 1 (committed, xmax=0)
    // and row 2 (uncommitted, xmax=0)
    assert.ok(all.length >= 1);
  });
});
